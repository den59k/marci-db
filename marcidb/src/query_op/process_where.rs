use memchr::memmem;

use crate::{StorageError, query_op::{FieldCompare, FieldCompareRef, PrefixKey, TransationContext, Where, process_query::{get_ids_by_prefix, get_prefix}}, schema::{Entity}, utils::get_data};

pub fn process_where<'a, 'b, U, F>(id: &'b [u8], body: &'b [u8], ctx: &mut TransationContext<'a, U, F>, entity: &Entity, where_op: &Where<'a>) -> Result<bool, StorageError> {

  match where_op {
    Where::True => Ok(true),
    Where::And(items) => {
      for f in items.iter() {
        if !process_where(id, body, ctx, entity, f)? { return Ok(false); }
      }
      Ok(true)
    },
    Where::Or(items) => {
      for f in items.iter() {
        if process_where(id, body, ctx, entity, f)? { return Ok(true); }
      }
      Ok(false)
    },
    Where::Not(where_op) => Ok(!process_where(id, body, ctx, entity, where_op)?),
    Where::Field(field, field_compare) => {
      if let FieldCompare::Ref(ref_entity, prefix_key, field_compare_ref) = field_compare {
        return match field_compare_ref {
          FieldCompareRef::Every(where_op) => {
            has_items(ctx,  *ref_entity, where_op, prefix_key, entity, id, body, true)
          },
          FieldCompareRef::Some(where_op) => {
            has_items(ctx,  *ref_entity, where_op, prefix_key, entity, id, body, false)
          },
          FieldCompareRef::None(where_op) => {
            Ok(!has_items(ctx,  *ref_entity, where_op, prefix_key, entity, id, body, false)?)
          },
          FieldCompareRef::Eq(where_op) => {
            has_one_item(ctx, *ref_entity, where_op, prefix_key, entity, id, body)
          },
          FieldCompareRef::Ne(where_op) => {
            Ok(!has_one_item(ctx, *ref_entity, where_op, prefix_key, entity, id, body)?)
          },
          FieldCompareRef::Exists => {
            has_one_item_exists(ctx, *ref_entity, prefix_key, entity, id, body)
          },
          FieldCompareRef::NotExists => {
            Ok(!has_one_item_exists(ctx, *ref_entity, prefix_key, entity, id, body)?)
          }
        };
      }

      let Some(data) = get_data(entity, field, id, body, ctx.schema) else {
        return Ok(match field_compare {
          FieldCompare::EqNull => true,
          FieldCompare::NeNull => false,
          FieldCompare::Ne(_) => true,
          FieldCompare::In(_, has_null) => *has_null,
          FieldCompare::NotIn(_, has_null) => !(*has_null),
          _ => false
        })
      };

      Ok(match field_compare {
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
      })
    },
  }
}

pub fn has_items<'a, U, F>(
  ctx: &mut TransationContext<'a, U, F>,
  entity: &Entity,
  where_op: &Where<'a>,
  prefix_key: &PrefixKey,
  parent_entity: &Entity,
  parent_id: &[u8],
  parent_body: &[u8],
  check_all: bool
) -> Result<bool, StorageError> {
  if let PrefixKey::ParentIndexTree(index_tree_name) = prefix_key {
    let index_tree = ctx.get_tree(index_tree_name)?;
    let ids = get_ids_by_prefix(&index_tree, parent_id)?;

    if ids.is_empty() {
      return Ok(check_all);
    }
    let tree = ctx.get_tree(&entity.name)?;
    for id in ids {
      let value = tree.get(&id)?.unwrap();
      let matched = process_where(&id, &value, ctx, entity, where_op)?;
      // Every: первый не прошедший → false; Some: первый прошедший → true
      if matched != check_all {
        return Ok(matched);
      }
    }
    return Ok(check_all);
  }

  let Some(prefix) = get_prefix(prefix_key, Some((parent_entity, parent_id, parent_body)), ctx.schema) else {
    return Ok(false);
  };

  let tree = ctx.get_tree(&entity.name)?;
  for item in tree.prefix(&prefix)? {
    let (id, value) = item?;
    let matched = process_where(&id, &value, ctx, entity, where_op)?;
    if matched != check_all {
      return Ok(matched);
    }
  }
  Ok(check_all)
}

pub fn has_one_item<'a, U, F>(
  ctx: &mut TransationContext<'a, U, F>,
  entity: &Entity,
  where_op: &Where<'a>,
  prefix_key: &PrefixKey,
  parent_entity: &Entity,
  parent_id: &[u8],
  parent_body: &[u8],
) -> Result<bool, StorageError> {

  let Some(item_id) = get_prefix(prefix_key, Some((parent_entity, parent_id, parent_body)), ctx.schema) else {
    return Ok(false);
  };

  if where_op.only_id_required(entity) {
    return process_where(&item_id, &[], ctx, entity, where_op)
  }

  let tree = ctx.get_tree(&entity.name)?;
  let Some(value) = tree.get(item_id)? else {
    return Ok(false);
  };
  return process_where(&item_id, &value, ctx, entity, where_op)
}

pub fn has_one_item_exists<'a, U, F>(
  ctx: &mut TransationContext<'a, U, F>,
  entity: &Entity,
  prefix_key: &PrefixKey,
  parent_entity: &Entity,
  parent_id: &[u8],
  parent_body: &[u8],
) -> Result<bool, StorageError> {
  let Some(item_id) = get_prefix(prefix_key, Some((parent_entity, parent_id, parent_body)), ctx.schema) else {
    return Ok(false);
  };

  let tree = ctx.get_tree(&entity.name)?;
  return Ok(tree.prefix_keys(&item_id)?.next().transpose()?.is_some())
}
