use crate::{StorageError, index_utils::{make_index_cursor_key, make_sort_key}, query_op::{DecodeCtx, PrefixKey, QueryOp, TransationContext, process_query::{ParentData, decode_row, get_id_from_index_key, get_prefix, maybe_rev, range_keys_iter}, process_where::process_where}, utils::get_data};

pub fn process_query_many<'a, U, F>
  (query: &'a QueryOp, ctx: &mut TransationContext<'a, U, F>, parent: Option<ParentData>) -> Result<Vec<U>, StorageError>
  where U: Clone, F: Fn(DecodeCtx<U>) -> U {
  process_query_many_limited(query, ctx, parent, None)
}

/// hard_limit — дополнительный потолок количества строк (используется findFirst)
pub fn process_query_many_limited<'a, U, F>
  (query: &'a QueryOp, ctx: &mut TransationContext<'a, U, F>, parent: Option<ParentData>, hard_limit: Option<usize>) -> Result<Vec<U>, StorageError>
  where U: Clone, F: Fn(DecodeCtx<U>) -> U {

  // Граница курсора для путей без позиционирования диапазоном:
  // post_sort сравнивает ключи сортировки, ленивые id-сканы — сами id
  let mut cursor_gate: Option<Vec<u8>> = None;
  if let Some(cursor_id) = &query.cursor {
    if query.post_sort {
      let Some(sort) = &query.sort else { return Ok(vec![]) };
      let tree = ctx.get_tree(&query.entity.name)?;
      // Строка курсора удалена — позиция в порядке сортировки неизвестна
      let Some(row) = tree.get(cursor_id)? else { return Ok(vec![]) };
      cursor_gate = Some(make_sort_key(query.entity, sort.field(), cursor_id, &row, ctx.schema));
    } else {
      cursor_gate = Some(cursor_id.clone());
    }
  }

  match &query.prefix_key {
    Some(PrefixKey::ParentIndexTree(index_tree_name)) => {
      let index_tree = ctx.get_tree(index_tree_name)?;
      let tree = ctx.get_tree(&query.entity.name)?;
      let parent_id = parent.unwrap().1;
      let parent_id_len = parent_id.len();

      let iter = maybe_rev(index_tree.prefix_keys(&parent_id)?, query.reverse)
        .map(|item| {
          let key = item?;
          let id = key[parent_id_len..].to_vec();
          let value = tree.get(&id)?.unwrap();
          Ok((id, value))
        });
      collect_rows(iter, ctx, query, hard_limit, cursor_gate)
    },
    Some(PrefixKey::IndexRange { start, end, tree_name, fixed_size }) => {
      let index_tree = ctx.get_tree(tree_name)?;
      let tree = ctx.get_tree(&query.entity.name)?;

      // Курсор при скане индекса: продолжаем диапазон от ключа строки курсора
      let cursor_bound: Option<Vec<u8>>;
      let (start, end) = if let Some(cursor_id) = &query.cursor && !query.post_sort {
        // Строка курсора удалена — её позиция в индексе неизвестна
        let Some(row) = tree.get(cursor_id)? else { return Ok(vec![]) };
        let sort_field = query.sort.as_ref().unwrap().field();
        let Some(value) = get_data(query.entity, sort_field, cursor_id, &row, ctx.schema) else { return Ok(vec![]) };
        let mut key = make_index_cursor_key(sort_field, value, cursor_id);

        if query.reverse {
          // desc: всё строго до ключа курсора (правая граница диапазона эксклюзивна)
          cursor_bound = Some(key);
          (start, &cursor_bound)
        } else {
          // asc: минимальный ключ строго после курсора
          key.push(0);
          cursor_bound = Some(key);
          (&cursor_bound, end)
        }
      } else {
        (start, end)
      };

      let iter = range_keys_iter(&index_tree, start, end, query.reverse)?
        .map(|key| {
          let id = get_id_from_index_key(&key?, *fixed_size);
          let value = tree.get(&id)?.unwrap();
          Ok((id, value))
        });
      // Без post_sort курсор уже учтён границами диапазона; id-граница здесь неприменима
      let gate = if query.post_sort { cursor_gate } else { None };
      collect_rows(iter, ctx, query, hard_limit, gate)
    },
    Some(prefix_key) => {
      let Some(prefix) = get_prefix(prefix_key, parent, ctx.schema) else {
        return Ok(vec![]);
      };
      let tree = ctx.get_tree(&query.entity.name)?;
      let iter = maybe_rev(tree.prefix(&prefix)?, query.reverse);
      collect_rows(iter, ctx, query, hard_limit, cursor_gate)
    },
    None => {
      let tree = ctx.get_tree(&query.entity.name)?;

      // Курсор в порядке id: id и есть ключ первичного дерева — позиционируемся диапазоном
      if let Some(cursor_id) = &query.cursor && !query.post_sort {
        let iter = if query.reverse {
          maybe_rev(tree.range(..cursor_id.as_slice())?, true)
        } else {
          let mut start = cursor_id.clone();
          start.push(0); // минимальный ключ строго после курсора
          maybe_rev(tree.range(start.as_slice()..)?, false)
        };
        return collect_rows(iter, ctx, query, hard_limit, None);
      }

      let iter = maybe_rev(tree.iter()?, query.reverse);
      collect_rows(iter, ctx, query, hard_limit, cursor_gate)
    }
  }
}

fn effective_limit(query: &QueryOp, hard_limit: Option<usize>) -> usize {
  match (query.limit, hard_limit) {
    (Some(limit), Some(hard)) => limit.min(hard),
    (Some(limit), None) => limit,
    (None, Some(hard)) => hard,
    (None, None) => usize::MAX
  }
}

/// Прогоняет строки скана через фильтр, курсор, skip/limit и декод.
/// Ленивый путь даёт ранний выход по limit; post_sort сортирует строки в памяти до декода.
/// cursor_gate: в post_sort-пути — ключ сортировки строки курсора, в ленивом — её id
fn collect_rows<'a, U, F, I, A, B>(iter: I, ctx: &mut TransationContext<'a, U, F>, query: &'a QueryOp, hard_limit: Option<usize>, cursor_gate: Option<Vec<u8>>) -> Result<Vec<U>, StorageError>
  where U: Clone, F: Fn(DecodeCtx<U>) -> U, I: Iterator<Item = Result<(A, B), canopydb::Error>>, A: AsRef<[u8]>, B: AsRef<[u8]> {

  let skip = query.skip.unwrap_or(0);
  let limit = effective_limit(query, hard_limit);
  if limit == 0 {
    return Ok(vec![]);
  }

  if query.post_sort && let Some(sort) = &query.sort {
    let sort_field = sort.field();

    // Собираем прошедшие фильтр строки вместе с ключом сортировки
    let mut rows: Vec<(Vec<u8>, Vec<u8>, Vec<u8>)> = vec![];
    for item in iter {
      let (id, data) = item?;
      let (id, data) = (id.as_ref(), data.as_ref());
      if let Some(where_op) = &query.filter && !process_where(id, data, ctx, query.entity, where_op)? {
        continue;
      }
      let key = make_sort_key(query.entity, sort_field, id, data, ctx.schema);
      // Оставляем только строки строго после позиции курсора в порядке сортировки
      if let Some(gate) = &cursor_gate {
        let after_cursor = if sort.is_desc() { &key < gate } else { &key > gate };
        if !after_cursor { continue; }
      }
      rows.push((key, id.to_vec(), data.to_vec()));
    }
    rows.sort_by(|a, b| a.0.cmp(&b.0));

    let mut results = Vec::new();
    for (_, id, data) in maybe_rev(rows.iter(), sort.is_desc()).skip(skip).take(limit) {
      results.push(decode_row(id, data, ctx, query)?);
    }
    return Ok(results);
  }

  // Ленивый путь: курсор → фильтр → skip → декод, ранний выход по limit
  let mut results = Vec::new();
  let mut skipped = 0;
  for item in iter {
    let (id, data) = item?;
    let (id, data) = (id.as_ref(), data.as_ref());
    // Скан идёт в порядке id — отбрасываем строки до позиции курсора
    if let Some(gate) = &cursor_gate {
      let after_cursor = if query.reverse { id < gate.as_slice() } else { id > gate.as_slice() };
      if !after_cursor { continue; }
    }
    if let Some(where_op) = &query.filter && !process_where(id, data, ctx, query.entity, where_op)? {
      continue;
    }
    if skipped < skip {
      skipped += 1;
      continue;
    }
    results.push(decode_row(id, data, ctx, query)?);
    if results.len() >= limit {
      break;
    }
  }
  Ok(results)
}
