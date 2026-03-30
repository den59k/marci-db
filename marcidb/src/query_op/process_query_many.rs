use crate::query_op::{DecodeCtx, PrefixKey, QueryOp, TransationContext, process_query::{ParentData, get_ids_by_prefix, get_ids_by_range, get_prefix, process_data}};

pub fn process_query_many<'a, U, F>
  (query: &'a QueryOp, ctx: &mut TransationContext<'a, F>, parent: Option<ParentData>) -> Vec<U>
  where F: Fn(DecodeCtx<U>) -> U {

  match &query.prefix_key {
    Some(PrefixKey::ParentIndexTree(index_tree_name)) => {
      let index_tree = ctx.get_tree(index_tree_name);
      let ids = get_ids_by_prefix(&index_tree, parent.unwrap().1);
      if ids.is_empty() {
        return vec![];
      }
      let tree = ctx.get_tree(&query.entity.name);

      return ids.into_iter().filter_map(|id| {
        let value = tree.get(&id).unwrap().unwrap();
        process_data(&id, &value, ctx, query)
      }).collect()
    },
    Some(PrefixKey::IndexRange { start, end, tree_name, fixed_size }) => {
      let index_tree = ctx.get_tree(tree_name);
      let ids = get_ids_by_range(&index_tree, start, end, *fixed_size);

      let tree = ctx.get_tree(&query.entity.name);

      return ids.into_iter().filter_map(|id| {
        let value = tree.get(&id).unwrap().unwrap();
        process_data(&id, &value, ctx, query)
      }).collect()
    },
    Some(prefix_key) => {
      let Some(prefix) = get_prefix(prefix_key, parent, ctx.schema) else {
        return vec![];
      };
      let tree = ctx.get_tree(&query.entity.name);
      return tree.prefix(&prefix).unwrap().filter_map(|item| {
        let (id, value) = item.unwrap();
        process_data(&id, &value, ctx, query)
      }).collect();
    },
    _ => {}
  }
  
  let tree = ctx.get_tree(&query.entity.name);
  return tree.iter().unwrap().filter_map(|item| {
    let (id, value) = item.unwrap();
    process_data(&id, &value, ctx, query)
  }).collect()
}
