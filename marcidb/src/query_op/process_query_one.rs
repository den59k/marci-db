use crate::query_op::{DecodeCtx, PrefixKey, QueryOp, TransationContext, process_query::{ParentData, get_first_id_by_prefix, get_ids_by_range, get_prefix, process_data}};

pub fn process_query_one<'a, U, F>(
    query: &'a QueryOp,
    ctx: &mut TransationContext<'a, F>,
    parent: Option<ParentData>,
) -> Option<U>
where
    F: Fn(DecodeCtx<U>) -> U,
{
    match &query.prefix_key {
        Some(PrefixKey::ParentIndexTree(index_tree_name)) => {
            let index_tree = ctx.get_tree(index_tree_name);
            let id = get_first_id_by_prefix(&index_tree, parent?.1)?;

            let tree = ctx.get_tree(&query.entity.name);
            let value = tree.get(&id).ok().flatten()?;

            return process_data(&id, &value, ctx, query);
        }

        Some(PrefixKey::IndexRange { start, end, tree_name, fixed_size }) => {
            let index_tree = ctx.get_tree(tree_name);
            let ids = get_ids_by_range(&index_tree, start, end, *fixed_size, Some(1));
            if ids.is_empty() {
              return None
            }
            
            let tree = ctx.get_tree(&query.entity.name);
            ids.iter().find_map(|id| {
                let value = tree.get(id).unwrap().unwrap();
                return process_data(id, &value, ctx, query)
            })
        }

        Some(prefix_key) => {
            let prefix = get_prefix(prefix_key, parent, ctx.schema)?;
            let tree = ctx.get_tree(&query.entity.name); 
            return tree.get(prefix).unwrap().and_then(|value| { 
              process_data(prefix, &value, ctx, query)
            });
        }

        None => {
            let tree = ctx.get_tree(&query.entity.name);
            
            tree.iter().unwrap().find_map(| item | {
                let (id, data) = item.unwrap();
                process_data(&id, &data, ctx, query)
            })
        }
    }
}