use crate::{index_utils::make_sort_key, query_op::{DecodeCtx, PrefixKey, QueryOp, TransationContext, process_query::{ParentData, decode_row, get_id_from_index_key, get_prefix, maybe_rev, range_keys_iter}, process_where::process_where}};

pub fn process_query_many<'a, U, F>
  (query: &'a QueryOp, ctx: &mut TransationContext<'a, F>, parent: Option<ParentData>) -> Vec<U>
  where F: Fn(DecodeCtx<U>) -> U {
  process_query_many_limited(query, ctx, parent, None)
}

/// hard_limit — дополнительный потолок количества строк (используется findFirst)
pub fn process_query_many_limited<'a, U, F>
  (query: &'a QueryOp, ctx: &mut TransationContext<'a, F>, parent: Option<ParentData>, hard_limit: Option<usize>) -> Vec<U>
  where F: Fn(DecodeCtx<U>) -> U {

  match &query.prefix_key {
    Some(PrefixKey::ParentIndexTree(index_tree_name)) => {
      let index_tree = ctx.get_tree(index_tree_name);
      let tree = ctx.get_tree(&query.entity.name);
      let parent_id = parent.unwrap().1;
      let parent_id_len = parent_id.len();

      let iter = maybe_rev(index_tree.prefix_keys(&parent_id).unwrap(), query.reverse)
        .map(|item| {
          let key = item.unwrap();
          let id = key[parent_id_len..].to_vec();
          let value = tree.get(&id).unwrap().unwrap();
          (id, value)
        });
      collect_rows(iter, ctx, query, hard_limit)
    },
    Some(PrefixKey::IndexRange { start, end, tree_name, fixed_size }) => {
      let index_tree = ctx.get_tree(tree_name);
      let tree = ctx.get_tree(&query.entity.name);

      let iter = range_keys_iter(&index_tree, start, end, query.reverse)
        .map(|key| {
          let id = get_id_from_index_key(&key, *fixed_size);
          let value = tree.get(&id).unwrap().unwrap();
          (id, value)
        });
      collect_rows(iter, ctx, query, hard_limit)
    },
    Some(prefix_key) => {
      let Some(prefix) = get_prefix(prefix_key, parent, ctx.schema) else {
        return vec![];
      };
      let tree = ctx.get_tree(&query.entity.name);
      let iter = maybe_rev(tree.prefix(&prefix).unwrap(), query.reverse).map(|item| item.unwrap());
      collect_rows(iter, ctx, query, hard_limit)
    },
    None => {
      let tree = ctx.get_tree(&query.entity.name);
      let iter = maybe_rev(tree.iter().unwrap(), query.reverse).map(|item| item.unwrap());
      collect_rows(iter, ctx, query, hard_limit)
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

/// Прогоняет строки скана через фильтр, skip/limit и декод.
/// Ленивый путь даёт ранний выход по limit; post_sort сортирует строки в памяти до декода
fn collect_rows<'a, U, F, I, A, B>(iter: I, ctx: &mut TransationContext<'a, F>, query: &'a QueryOp, hard_limit: Option<usize>) -> Vec<U>
  where F: Fn(DecodeCtx<U>) -> U, I: Iterator<Item = (A, B)>, A: AsRef<[u8]>, B: AsRef<[u8]> {

  let skip = query.skip.unwrap_or(0);
  let limit = effective_limit(query, hard_limit);
  if limit == 0 {
    return vec![];
  }

  if query.post_sort && let Some(sort) = &query.sort {
    let sort_field = sort.field();

    // Собираем прошедшие фильтр строки вместе с ключом сортировки
    let mut rows: Vec<(Vec<u8>, Vec<u8>, Vec<u8>)> = vec![];
    for (id, data) in iter {
      let (id, data) = (id.as_ref(), data.as_ref());
      if let Some(where_op) = &query.filter && !process_where(id, data, ctx, query.entity, where_op) {
        continue;
      }
      rows.push((make_sort_key(query.entity, sort_field, id, data, ctx.schema), id.to_vec(), data.to_vec()));
    }
    rows.sort_by(|a, b| a.0.cmp(&b.0));

    let mut results = Vec::new();
    for (_, id, data) in maybe_rev(rows.iter(), sort.is_desc()).skip(skip).take(limit) {
      results.push(decode_row(id, data, ctx, query));
    }
    return results;
  }

  // Ленивый путь: фильтр → skip → декод, ранний выход по limit
  let mut results = Vec::new();
  let mut skipped = 0;
  for (id, data) in iter {
    let (id, data) = (id.as_ref(), data.as_ref());
    if let Some(where_op) = &query.filter && !process_where(id, data, ctx, query.entity, where_op) {
      continue;
    }
    if skipped < skip {
      skipped += 1;
      continue;
    }
    results.push(decode_row(id, data, ctx, query));
    if results.len() >= limit {
      break;
    }
  }
  results
}
