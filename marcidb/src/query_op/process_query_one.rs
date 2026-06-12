use crate::query_op::{DecodeCtx, PrefixKey, QueryOp, TransationContext, process_query::{ParentData, get_first_id_by_prefix, get_id_from_index_key, get_prefix, process_data, range_keys_iter}, process_query_many::process_query_many_limited};

pub fn process_query_one<'a, U, F>(
    query: &'a QueryOp,
    ctx: &mut TransationContext<'a, F>,
    parent: Option<ParentData>,
) -> Option<U>
where
    F: Fn(DecodeCtx<U>) -> U,
{
    // Нестандартный порядок, смещение или курсор: переиспользуем механизм many с лимитом 1
    if query.post_sort || query.reverse || query.skip.is_some() || query.cursor.is_some() {
        return process_query_many_limited(query, ctx, parent, Some(1)).pop();
    }

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
            let tree = ctx.get_tree(&query.entity.name);

            // Лениво идём по диапазону до первой строки, прошедшей полный фильтр
            for key in range_keys_iter(&index_tree, start, end, false) {
                let id = get_id_from_index_key(&key, *fixed_size);
                let value = tree.get(&id).unwrap().unwrap();
                if let Some(result) = process_data(&id, &value, ctx, query) {
                    return Some(result);
                }
            }
            None
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
