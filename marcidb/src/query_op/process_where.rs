use memchr::memmem;

use crate::{query_op::{FieldCompare, FieldCompareRef, PrefixKey, TransationContext, Where, process_query::{get_ids_by_prefix, get_prefix}}, schema::{Entity}, utils::get_data};

pub fn process_where<'a, 'b, F>(id: &'b [u8], body: &'b [u8], ctx: &mut TransationContext<'a, F>, entity: &Entity, where_op: &Where<'a>) -> bool {

  match where_op {
    Where::True => true,
    Where::And(items) => items.iter().all(|f| process_where(id, body, ctx, entity, f)),
    Where::Or(items) => items.iter().any(|f| process_where(id, body, ctx, entity, f)),
    Where::Not(where_op) => !process_where(id, body, ctx, entity, where_op),
    Where::Field(field, field_compare) => {
      if let FieldCompare::Ref(entity, prefix_key, field_compare_ref) = field_compare {
        match field_compare_ref {
          FieldCompareRef::Every(where_op) => {
            return has_items(ctx,  *entity, where_op, prefix_key, entity, id, body, true)
          },
          FieldCompareRef::Some(where_op) => {
            return has_items(ctx,  *entity, where_op, prefix_key, entity, id, body, false)
          },
          FieldCompareRef::None(where_op) => {
            return !has_items(ctx,  *entity, where_op, prefix_key, entity, id, body, false)
          },
          FieldCompareRef::Eq(where_op) => {
            return has_one_item(ctx, *entity, where_op, prefix_key, entity, id, body)
          },
          FieldCompareRef::Ne(where_op) => {
            return !has_one_item(ctx, *entity, where_op, prefix_key, entity, id, body)
          }
        }
      }

      let Some(data) = get_data(entity, field, id, body, ctx.schema) else {
        return match field_compare {
          FieldCompare::EqNull => true,
          FieldCompare::NeNull => false,
          FieldCompare::Ne(_) => true,
          FieldCompare::In(_, has_null) => *has_null,
          FieldCompare::NotIn(_, has_null) => !(*has_null),
          _ => false
        }
      };

      match field_compare {
        FieldCompare::EqNull => false,
        FieldCompare::NeNull => true,

        FieldCompare::In(items, _) => items.iter().any(|f| f == data),
        FieldCompare::NotIn(items, _) => items.iter().all(|f| f != data),
        FieldCompare::Eq(f) => f == data,
        FieldCompare::Ne(f) => f != data,

        FieldCompare::Gt(num_value) => num_value.compare_with_bytes(data).map(|f| f.is_gt()).unwrap_or(false),
        FieldCompare::Gte(num_value) => num_value.compare_with_bytes(data).map(|f| f.is_ge()).unwrap_or(false),
        FieldCompare::Lt(num_value) => num_value.compare_with_bytes(data).map(|f| f.is_lt()).unwrap_or(false),
        FieldCompare::Lte(num_value) => num_value.compare_with_bytes(data).map(|f| f.is_le()).unwrap_or(false),

        FieldCompare::StringStartsWith(value) => data.starts_with(value),
        FieldCompare::StringIncludes(value) => memmem::find(data, value).is_some(),

        _ => false
      }
    },
  }
}

pub fn has_items<'a, F>(
  ctx: &mut TransationContext<'a, F>, 
  entity: &Entity, 
  where_op: &Where<'a>, 
  prefix_key: &PrefixKey, 
  parent_entity: &Entity,
  parent_id: &[u8], 
  parent_body: &[u8],
  check_all: bool
) -> bool {
  if let PrefixKey::ParentIndexTree(index_tree_name) = prefix_key {
    let index_tree = ctx.get_tree(index_tree_name);
    let ids = get_ids_by_prefix(&index_tree, parent_id);

    if ids.is_empty() {
      return check_all;
    }
    let tree = ctx.get_tree(&entity.name);
    if check_all {
      return ids.into_iter().all(|id| {
        let value = tree.get(&id).unwrap().unwrap();
        process_where(&id, &value, ctx, entity, where_op)
      });
    } else {
      return ids.into_iter().any(|id| {
        let value = tree.get(&id).unwrap().unwrap();
        process_where(&id, &value, ctx, entity, where_op)
      });
    }
  }

  let Some(prefix) = get_prefix(prefix_key, Some((parent_entity, parent_id, parent_body)), ctx.schema) else {
    return false;
  };
  
  let tree = ctx.get_tree(&entity.name);

  if check_all {
    tree.prefix(&prefix).unwrap().all(| item | {
      let (id, value) = item.unwrap();
      process_where(&id, &value, ctx, entity, where_op)
    })
  } else {
     tree.prefix(&prefix).unwrap().any(| item | {
      let (id, value) = item.unwrap();
      process_where(&id, &value, ctx, entity, where_op)
    })
  }
}

pub fn has_one_item<'a, F>(
  ctx: &mut TransationContext<'a, F>, 
  entity: &Entity, 
  where_op: &Where<'a>, 
  prefix_key: &PrefixKey, 
  parent_entity: &Entity,
  parent_id: &[u8], 
  parent_body: &[u8],
) -> bool {

  let Some(prefix) = get_prefix(prefix_key, Some((parent_entity, parent_id, parent_body)), ctx.schema) else {
    return false;
  };

  let tree = ctx.get_tree(&entity.name);
  let Some(value) = tree.get(prefix).unwrap() else {
    return false;
  };
  return process_where(&prefix, &value, ctx, entity, where_op)
}