use std::cmp::Ordering;

use memchr::memmem;
use serde_json::Value;

use crate::{StorageError, json_parsers::jsonb, query_op::{FieldCompare, FieldCompareRef, JsonFilter, JsonOp, PrefixKey, TransationContext, Where, process_query::{get_ids_by_prefix, get_prefix}}, schema::{Entity}, utils::{decode_id_list, get_data}};

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

      // JSON path filter: navigate the blob (which may be absent) and apply every path condition (ANDed).
      if let FieldCompare::Json(filters) = field_compare {
        let blob = get_data(entity, field, id, body, ctx.schema);
        return Ok(filters.iter().all(|jf| eval_json_filter(blob, jf)));
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

        // Module search is resolved up front via the provider (QueryOp.search); a residual occurrence
        // here (e.g. inside an include) is a match-everything no-op.
        FieldCompare::Search(_) => true,

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
      // Every: first one that fails → false; Some: first one that passes → true
      if matched != check_all {
        return Ok(matched);
      }
    }
    return Ok(check_all);
  }

  // `@list` relation: the related ids come from the parent's inline array
  if let PrefixKey::ParentIdList { field, id_size } = prefix_key {
    let ids = decode_id_list(get_data(parent_entity, field, parent_id, parent_body, ctx.schema), *id_size);
    if ids.is_empty() {
      return Ok(check_all);
    }
    let tree = ctx.get_tree(&entity.name)?;
    for id in ids {
      // A dangling id is treated as not-a-row: it can't satisfy $some and doesn't fail $every
      let Some(value) = tree.get(&id)? else { continue };
      let matched = process_where(&id, &value, ctx, entity, where_op)?;
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

/// Evaluates one JSON path condition against the field's blob (`None` if the field itself is null/absent).
/// Navigates to the leaf, then applies the operator. A missing leaf or a type mismatch is "no match",
/// except `$ne`/`$notIn`, which also match a missing leaf (mirroring scalar `$ne` on a null field).
fn eval_json_filter(blob: Option<&[u8]>, jf: &JsonFilter) -> bool {
  let leaf = blob.and_then(|b| jsonb::navigate(b, &jf.path));
  match &jf.op {
    JsonOp::Exists(want) => leaf.is_some() == *want,
    JsonOp::Type(name) => leaf.and_then(jsonb::type_name).is_some_and(|n| n == name),
    op => {
      let Some(leaf) = leaf else {
        return matches!(op, JsonOp::Ne(_) | JsonOp::NotIn(_));
      };
      let Ok(val) = jsonb::decode_to_value(leaf) else { return false };
      match op {
        JsonOp::Eq(x) => json_eq(&val, x),
        JsonOp::Ne(x) => !json_eq(&val, x),
        JsonOp::In(xs) => xs.iter().any(|x| json_eq(&val, x)),
        JsonOp::NotIn(xs) => !xs.iter().any(|x| json_eq(&val, x)),
        JsonOp::Gt(x) => json_cmp(&val, x).is_some_and(Ordering::is_gt),
        JsonOp::Gte(x) => json_cmp(&val, x).is_some_and(Ordering::is_ge),
        JsonOp::Lt(x) => json_cmp(&val, x).is_some_and(Ordering::is_lt),
        JsonOp::Lte(x) => json_cmp(&val, x).is_some_and(Ordering::is_le),
        JsonOp::StringStartsWith(s) => val.as_str().is_some_and(|v| v.starts_with(s.as_str())),
        JsonOp::StringIncludes(s) => val.as_str().is_some_and(|v| v.contains(s.as_str())),
        JsonOp::Contains(x) => val.as_array().is_some_and(|a| a.iter().any(|e| json_eq(e, x))),
        JsonOp::Exists(_) | JsonOp::Type(_) => unreachable!("handled above"),
      }
    }
  }
}

/// JSON equality with numeric leniency: `5` equals `5.0`. Other types use structural `Value` equality.
fn json_eq(a: &Value, b: &Value) -> bool {
  match (a, b) {
    (Value::Number(x), Value::Number(y)) => match (x.as_f64(), y.as_f64()) {
      (Some(x), Some(y)) => x == y,
      _ => false,
    },
    _ => a == b,
  }
}

/// Ordering for `$gt`/`$lt`… — defined only between two numbers (by value) or two strings (lexicographic);
/// any other pairing (including a type mismatch) is `None`, i.e. no match.
fn json_cmp(a: &Value, b: &Value) -> Option<Ordering> {
  match (a, b) {
    (Value::Number(x), Value::Number(y)) => x.as_f64()?.partial_cmp(&y.as_f64()?),
    (Value::String(x), Value::String(y)) => Some(x.as_str().cmp(y.as_str())),
    _ => None,
  }
}
