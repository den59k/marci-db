use crate::{StorageError, query_op::{DecodeCtx, PrefixKey, QueryOp, TransationContext, process_query::{ParentData, get_first_id_by_prefix, get_id_from_index_key, get_prefix, process_data, range_keys_iter}, process_query_many::process_query_many_limited}};

pub fn process_query_one<'a, U, F>(
    query: &'a QueryOp,
    ctx: &mut TransationContext<'a, U, F>,
    parent: Option<ParentData>,
) -> Result<Option<U>, StorageError>
where
    U: Clone,
    F: Fn(DecodeCtx<U>) -> U,
{
    // Non-standard order, offset or cursor: reuse the many mechanism with a limit of 1
    if query.post_sort || query.reverse || query.skip.is_some() || query.cursor.is_some() {
        return Ok(process_query_many_limited(query, ctx, parent, Some(1))?.pop());
    }

    match &query.prefix_key {
        Some(PrefixKey::ParentIndexTree(index_tree_name)) => {
            let index_tree = ctx.get_tree(index_tree_name)?;
            let Some((_, parent_id, _)) = parent else { return Ok(None) };
            let Some(id) = get_first_id_by_prefix(&index_tree, parent_id)? else { return Ok(None) };

            let tree = ctx.get_tree(&query.entity.name)?;
            let Some(value) = tree.get(&id)? else { return Ok(None) };

            return process_data(&id, &value, ctx, query);
        }

        Some(PrefixKey::IndexRange { start, end, tree_name, fixed_size }) => {
            let index_tree = ctx.get_tree(tree_name)?;
            let tree = ctx.get_tree(&query.entity.name)?;

            // Lazily walk the range until the first row that passes the full filter
            for key in range_keys_iter(&index_tree, start, end, false)? {
                let id = get_id_from_index_key(&key?, *fixed_size);
                let value = tree.get(&id)?.unwrap();
                if let Some(result) = process_data(&id, &value, ctx, query)? {
                    return Ok(Some(result));
                }
            }
            Ok(None)
        }

        Some(prefix_key) => {
            let schema = ctx.schema;
            let Some(prefix) = get_prefix(prefix_key, parent, schema) else { return Ok(None) };

            // many-to-one relations (FieldValue) are shared across rows of the result set —
            // cache the decoded result by id to avoid re-reading and re-decoding.
            // If no repeats are encountered in the first rows, the relations are unique —
            // the cache for this include is disabled so we don't pay for it in vain
            const CACHE_DISABLE_AFTER_MISSES: u32 = 256;

            let mut use_cache = matches!(prefix_key, PrefixKey::ParentField(_));
            if use_cache {
                let cache_ptr = query as *const QueryOp as usize;
                if let Some(cache) = ctx.include_cache.get_mut(&cache_ptr) {
                    if cache.hits == 0 && cache.misses >= CACHE_DISABLE_AFTER_MISSES {
                        if !cache.items.is_empty() {
                            cache.items = std::collections::HashMap::new();
                        }
                        use_cache = false;
                    } else if let Some(cached) = cache.items.get(prefix) {
                        cache.hits += 1;
                        return Ok(cached.clone());
                    }
                }
            }

            let tree = ctx.get_tree(&query.entity.name)?;
            let result = match tree.get(prefix)? {
              Some(value) => process_data(prefix, &value, ctx, query)?,
              None => None,
            };

            if use_cache {
                let cache = ctx.include_cache.entry(query as *const QueryOp as usize).or_default();
                cache.misses += 1;
                cache.items.insert(prefix.to_vec(), result.clone());
            }
            return Ok(result);
        }

        None => {
            let tree = ctx.get_tree(&query.entity.name)?;

            for item in tree.iter()? {
                let (id, data) = item?;
                if let Some(result) = process_data(&id, &data, ctx, query)? {
                    return Ok(Some(result));
                }
            }
            Ok(None)
        }
    }
}
