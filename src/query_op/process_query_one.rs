use crate::query_op::{DecodeCtx, PrefixKey, QueryOp, TransationContext, process_query::{ParentData, get_first_id, get_prefix, process_data}};

pub fn process_query_one<'a, U,F>
  (query: &'a QueryOp, ctx: &mut TransationContext<'a, F>, parent: Option<ParentData>) -> Option<U>
  where F: Fn(DecodeCtx<U>) -> U {

  
  let prefix = match &query.prefix_key {
    Some(PrefixKey::ParentIndexTree(index_tree_name)) => {
      let index_tree = ctx.get_tree(index_tree_name);
      let item_id = get_first_id(&index_tree, parent.unwrap().1)?;
      
      let tree = ctx.get_tree(&query.entity.name);
      let value = tree.get(&item_id).unwrap().unwrap();
      return process_data(&item_id, &value, ctx, query);
    },
    Some(prefix_key) => get_prefix(prefix_key, parent, ctx.schema)?,
    _ => panic!("QueryOne without prefix is not supported")
  };

  let tree = ctx.get_tree(&query.entity.name);
  return tree.get(prefix).unwrap().and_then(|value| {
    process_data(prefix, &value, ctx, query)
  });
}
