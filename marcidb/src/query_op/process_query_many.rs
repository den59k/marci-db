use crate::{StorageError, index_utils::{make_index_cursor_key, make_sort_key}, query_op::{DecodeCtx, PrefixKey, QueryOp, TransationContext, process_query::{ParentData, decode_row, get_id_from_index_key, get_prefix, maybe_rev, range_keys_iter}, process_where::process_where}, utils::get_data};

pub fn process_query_many<'a, U, F>
  (query: &'a QueryOp, ctx: &mut TransationContext<'a, U, F>, parent: Option<ParentData>) -> Result<Vec<U>, StorageError>
  where U: Clone, F: Fn(DecodeCtx<U>) -> U {
  process_query_many_limited(query, ctx, parent, None)
}

/// hard_limit — additional cap on the number of rows (used by findFirst)
pub fn process_query_many_limited<'a, U, F>
  (query: &'a QueryOp, ctx: &mut TransationContext<'a, U, F>, parent: Option<ParentData>, hard_limit: Option<usize>) -> Result<Vec<U>, StorageError>
  where U: Clone, F: Fn(DecodeCtx<U>) -> U {

  // Cursor boundary for paths without range positioning:
  // post_sort compares sort keys, lazy id scans compare the ids themselves
  let mut cursor_gate: Option<Vec<u8>> = None;
  if let Some(cursor_id) = &query.cursor {
    if query.post_sort {
      let Some(sort) = &query.sort else { return Ok(vec![]) };
      let tree = ctx.get_tree(&query.entity.name)?;
      // The cursor row was deleted — its position in sort order is unknown
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

      // Cursor during an index scan: continue the range from the cursor row's key
      let cursor_bound: Option<Vec<u8>>;
      let (start, end) = if let Some(cursor_id) = &query.cursor && !query.post_sort {
        // The cursor row was deleted — its position in the index is unknown
        let Some(row) = tree.get(cursor_id)? else { return Ok(vec![]) };
        let sort_field = query.sort.as_ref().unwrap().field();
        let Some(value) = get_data(query.entity, sort_field, cursor_id, &row, ctx.schema) else { return Ok(vec![]) };
        let mut key = make_index_cursor_key(sort_field, value, cursor_id);

        if query.reverse {
          // desc: everything strictly before the cursor key (the range's right bound is exclusive)
          cursor_bound = Some(key);
          (start, &cursor_bound)
        } else {
          // asc: smallest key strictly after the cursor
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
      // Without post_sort the cursor is already accounted for by the range bounds; an id boundary is not applicable here
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

      // Cursor in id order: the id is the primary tree's key — position via a range
      if let Some(cursor_id) = &query.cursor && !query.post_sort {
        let iter = if query.reverse {
          maybe_rev(tree.range(..cursor_id.as_slice())?, true)
        } else {
          let mut start = cursor_id.clone();
          start.push(0); // smallest key strictly after the cursor
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

/// Runs scanned rows through the filter, cursor, skip/limit and decode.
/// The lazy path exits early on limit; post_sort sorts rows in memory before decoding.
/// cursor_gate: in the post_sort path — the cursor row's sort key, in the lazy path — its id
fn collect_rows<'a, U, F, I, A, B>(iter: I, ctx: &mut TransationContext<'a, U, F>, query: &'a QueryOp, hard_limit: Option<usize>, cursor_gate: Option<Vec<u8>>) -> Result<Vec<U>, StorageError>
  where U: Clone, F: Fn(DecodeCtx<U>) -> U, I: Iterator<Item = Result<(A, B), canopydb::Error>>, A: AsRef<[u8]>, B: AsRef<[u8]> {

  let skip = query.skip.unwrap_or(0);
  let limit = effective_limit(query, hard_limit);
  if limit == 0 {
    return Ok(vec![]);
  }

  if query.post_sort && let Some(sort) = &query.sort {
    let sort_field = sort.field();

    // Collect rows that passed the filter together with their sort key
    let mut rows: Vec<(Vec<u8>, Vec<u8>, Vec<u8>)> = vec![];
    for item in iter {
      let (id, data) = item?;
      let (id, data) = (id.as_ref(), data.as_ref());
      if let Some(where_op) = &query.filter && !process_where(id, data, ctx, query.entity, where_op)? {
        continue;
      }
      let key = make_sort_key(query.entity, sort_field, id, data, ctx.schema);
      // Keep only rows strictly after the cursor position in sort order
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

  // Lazy path: cursor → filter → skip → decode, with an early exit on limit
  let mut results = Vec::new();
  let mut skipped = 0;
  for item in iter {
    let (id, data) = item?;
    let (id, data) = (id.as_ref(), data.as_ref());
    // The scan runs in id order — drop rows before the cursor position
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
