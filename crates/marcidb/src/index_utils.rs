use smallvec::SmallVec;

use crate::{Entity, Field, FieldLocation, FieldType, num_utils::NumberValue, query_op::{FieldCompare, PrefixKey, Where}, schema::{FieldIndex, FieldIndexNum, Schema}, utils::get_data};

/// Increments the buffer by one bit
pub fn increase_bit(start: &[u8]) -> Option<Vec<u8>> {
  let mut end = SmallVec::<u8, 256>::from_slice_copy(start);
  for (i, b) in end.iter_mut().enumerate().rev() {
      if *b < u8::MAX {
          *b += 1;
          end.truncate(i + 1);
          return Some(end.to_vec());
      }
  }
  return None;
}

#[inline]
/// For exact index comparison (Eq) we compare by prefix
fn generate_prefix<'a>(val: Vec<u8>, tree_name: &String) -> Option<PrefixKey<'a>> {
  let end = increase_bit(&val);
  let fixed_size = Some(val.len());

  Some(PrefixKey::IndexRange { start: Some(val), end, tree_name: tree_name.clone(), fixed_size })
}

#[inline]
fn generate_prefix_starts_with<'a>(val: Vec<u8>, tree_name: &String) -> Option<PrefixKey<'a>> {
  let end = increase_bit(&val);
  Some(PrefixKey::IndexRange { start: Some(val), end, tree_name: tree_name.clone(), fixed_size: None })
}

pub fn encode_full_index(field: &Field, index: &FieldIndex, id: &[u8], value: &[u8]) -> Vec<u8> {
  let value = encode_index(field, index, value);
  [ value.as_slice(), &id ].concat()
}

pub fn encode_index(field: &Field, index: &FieldIndex, value: &[u8]) -> Vec<u8> {
  match index {
    FieldIndex::Value { .. } => {
      encode_index_data(field, value)
    }
    FieldIndex::Number { ty, .. } => {
      encode_index_number(ty, value)
    }
    _ => panic!("Cannot use custom index in ID")
  }
}

/// Encodes a number for lexicographic comparison
pub fn encode_index_number(ty: &FieldIndexNum, val: &[u8]) -> Vec<u8> {
  match ty {
    FieldIndexNum::Int64 => encode_i64(val),
    FieldIndexNum::Float => encode_f32(val),
    FieldIndexNum::Double => encode_f64(val),
    _ => val.to_vec()
  }
}

/// Appends a null terminator at the end if the size is unknown
pub fn encode_index_data(field: &Field, val: &[u8]) -> Vec<u8> {
    // if field.get_size().is_none() && val[val.len() - 1] != b'\0' {
    if field.get_size().is_none() {
        let mut result = val.to_vec();
        result.push(b'\0');
        return result;
    }
    val.to_vec()
}

/// Encodes a number for lexicographic comparison
fn encode_num_wh(value: &NumberValue) -> Vec<u8> {
  match value {
    NumberValue::DateTime(val) | NumberValue::Int64(val) => encode_i64(&val.to_be_bytes()),
    NumberValue::UInt64(val) => val.to_be_bytes().to_vec(),
    NumberValue::Float(val) => encode_f32(&val.to_be_bytes()),
    NumberValue::Double(val) => encode_f64(&val.to_be_bytes())
  }
}

/// Inverse of encode_index_number: index key bytes → value be-bytes
pub fn decode_index_number(ty: &FieldIndexNum, data: &[u8]) -> Vec<u8> {
  match ty {
    FieldIndexNum::Int64 => {
      let mut out = data.to_vec();
      out[0] ^= 0x80;
      out
    },
    FieldIndexNum::UInt64 => data.to_vec(),
    FieldIndexNum::Float | FieldIndexNum::Double => {
      let mut out = data.to_vec();
      if out[0] & 0x80 != 0 {
        // Was positive — restore the sign bit
        out[0] ^= 0x80;
      } else {
        // Was negative — invert all bits back
        for b in &mut out {
          *b = !*b;
        }
      }
      out
    }
  }
}

/// Extracts the field value from the index key (encoded_value ++ id)
/// and returns it in the get_data format (value be-bytes)
pub fn decode_index_key_value(field: &Field, index: &FieldIndex, key: &[u8]) -> Vec<u8> {
  match index {
    FieldIndex::Number { ty, .. } => {
      let size = field.get_size().expect("Number index field must have fixed size");
      decode_index_number(ty, &key[..size])
    },
    FieldIndex::Value { .. } => {
      match field.get_size() {
        Some(size) => key[..size].to_vec(),
        // Dynamic size: value up to the null terminator
        None => {
          let pos = key.iter().position(|&b| b == b'\0').unwrap_or(key.len());
          key[..pos].to_vec()
        }
      }
    },
    FieldIndex::Custom { .. } => panic!("Cannot decode value from custom index")
  }
}

/// i64 be_bytes → lexicographically sortable bytes
/// Flip only the most significant (sign) bit
pub fn encode_i64(bytes: &[u8]) -> Vec<u8> {
    debug_assert_eq!(bytes.len(), 8, "i64 must be 8 bytes");
    let mut out = bytes.to_vec();
    out[0] ^= 0x80;
    out
}

/// f32 be_bytes → lexicographically sortable bytes
pub fn encode_f32(bytes: &[u8]) -> Vec<u8> {
    debug_assert_eq!(bytes.len(), 4, "f32 must be 4 bytes");
    let mut out = bytes.to_vec();
    if out[0] & 0x80 != 0 {
        // Negative number — flip all bits
        for b in &mut out {
            *b = !*b;
        }
    } else {
        // Positive — flip only the sign bit
        out[0] ^= 0x80;
    }
    out
}

/// f64 be_bytes → lexicographically sortable bytes
pub fn encode_f64(bytes: &[u8]) -> Vec<u8> {
    debug_assert_eq!(bytes.len(), 8, "f64 must be 8 bytes");
    let mut out = bytes.to_vec();
    if out[0] & 0x80 != 0 {
        for b in &mut out {
            *b = !*b;
        }
    } else {
        out[0] ^= 0x80;
    }
    out
}

/// Position of a row in the field's index tree: the same encoding used for keys stored in the index
/// (encoded_value ++ id). Used for the keyset cursor when scanning the index
pub fn make_index_cursor_key(field: &Field, value: &[u8], id: &[u8]) -> Vec<u8> {
  let mut key = match &field.ty {
    FieldType::Primitive(ty) if ty.get_num_type().is_some() => {
      encode_index_number(&ty.get_num_type().unwrap(), value)
    },
    // Strings get a null terminator (as in indexes), everything else is compared as is
    _ => encode_index_data(field, value)
  };
  key.extend_from_slice(id);
  key
}

/// In-memory sort key. Byte comparison of keys yields the same order
/// as scanning the corresponding index tree (the same value encoding + id as tie-break).
/// null values go to the end with asc and to the beginning with desc
pub fn make_sort_key(entity: &Entity, field: &Field, id: &[u8], data: &[u8], schema: &Schema) -> Vec<u8> {
  let Some(value) = get_data(entity, field, id, data, schema) else {
    let mut key = Vec::with_capacity(id.len() + 1);
    key.push(0xFF);
    key.extend_from_slice(id);
    return key;
  };

  let mut key = Vec::with_capacity(value.len() + id.len() + 2);
  key.push(0x00);
  key.extend(make_index_cursor_key(field, value, id));
  key
}

// Condition priorities for choosing the index access path (lower — more selective)
const SCORE_ID: u8 = 0;          // exact primary key
const SCORE_UNIQUE_EQ: u8 = 1;   // eq on a unique index
const SCORE_ID_PREFIX: u8 = 2;   // prefix of a composite primary key
const SCORE_EQ: u8 = 3;          // eq on a regular index
const SCORE_STARTS_WITH: u8 = 4;
const SCORE_RANGE_BOUNDED: u8 = 5;  // range bounded on both sides
const SCORE_RANGE: u8 = 6;          // half-open range

// Generates an index for Where
pub fn generate_prefix_from_where<'a>(entity: &'a Entity, where_op: &Where) -> Option<PrefixKey<'a>> {
  generate_prefix_scored(entity, where_op).map(|(prefix, _)| prefix)
}

fn id_prefix_score(prefix: &PrefixKey) -> u8 {
  match prefix {
    PrefixKey::Id(_) => SCORE_ID,
    _ => SCORE_ID_PREFIX
  }
}

/// The range bound a comparison contributes to a numeric index scan, if any. `start` bounds are
/// inclusive and `end` bounds exclusive, matching the half-open range the scan is given.
fn range_bound(compare: &FieldCompare) -> Option<(bool, Option<Vec<u8>>)> {
  match compare {
    // Index keys are `encoded_value ++ id`, so stepping past a value means stepping past every key
    // that shares it — hence `increase_bit` for an exclusive lower / inclusive upper bound.
    FieldCompare::Gte(v) => Some((true, Some(encode_num_wh(v)))),
    FieldCompare::Gt(v) => Some((true, increase_bit(&encode_num_wh(v)))),
    FieldCompare::Lte(v) => Some((false, increase_bit(&encode_num_wh(v)))),
    FieldCompare::Lt(v) => Some((false, Some(encode_num_wh(v)))),
    _ => None,
  }
}

/// Fuses a lower and an upper bound on the same numerically-indexed field into one bounded range.
/// Which pair is picked when a field carries several bounds doesn't affect correctness — every
/// condition is still re-checked per row — so the first complete pair wins.
fn fuse_range_bounds<'a>(items: &[Where<'_>]) -> Option<(PrefixKey<'a>, u8)> {
  for (i, item) in items.iter().enumerate() {
    let Where::Field(field, compare) = item else { continue };
    let Some((true, start)) = range_bound(compare) else { continue };

    // Only the field's first non-custom index is considered, as in the single-condition path above
    let Some(FieldIndex::Number { tree_name, .. }) =
      field.indexes.iter().find(|i| !matches!(i, FieldIndex::Custom { .. })) else { continue };

    for other in items.iter().skip(i + 1) {
      let Where::Field(other_field, other_compare) = other else { continue };
      if !std::ptr::eq(*field, *other_field) { continue }
      let Some((false, end)) = range_bound(other_compare) else { continue };

      return Some((
        PrefixKey::IndexRange { start, end, tree_name: tree_name.clone(), fixed_size: field.get_size() },
        SCORE_RANGE_BOUNDED,
      ));
    }
  }
  None
}

/// Returns the access path along with its priority. Among several indexed
/// conditions the most selective one is chosen by static priority
fn generate_prefix_scored<'a>(entity: &'a Entity, where_op: &Where) -> Option<(PrefixKey<'a>, u8)> {
  match where_op {
    Where::True => None,
    Where::And(items) => {

      // Try to assemble a prefix for the ID
      let key_fields: Vec<(&Field,&Vec<u8>)> = items.iter().filter_map(|f| {
        match f {
          Where::Field(field, FieldCompare::Eq(value)) => Some((*field, value)),
          _ => None
        }
      }).collect();
      let id_prefix = try_to_generate_id_prefix(entity, key_fields);
      if let Some(prefix) = &id_prefix && id_prefix_score(prefix) == SCORE_ID {
        return id_prefix.map(|p| (p, SCORE_ID));
      }

      // A lower and an upper bound on one field (`{ $gte: a, $lt: b }`) arrive here as two separate
      // conditions. Considered individually each is only a half-open range, so fuse them first.
      let best = items.iter()
        .filter_map(|f| generate_prefix_scored(entity, f))
        .chain(fuse_range_bounds(items))
        .min_by_key(|(_, score)| *score);

      match (id_prefix, best) {
        (Some(prefix), Some((best_prefix, best_score))) => {
          let prefix_score = id_prefix_score(&prefix);
          if best_score < prefix_score { Some((best_prefix, best_score)) } else { Some((prefix, prefix_score)) }
        },
        (Some(prefix), None) => { let score = id_prefix_score(&prefix); Some((prefix, score)) },
        (None, best) => best
      }
    },
    Where::Or(_) => None,
    Where::Not(_) => None,
    Where::Field(field, field_compare) => {
      // Check whether this could be the sole field for the ID
      if matches!(field.location, FieldLocation::Key { index } if index == 0) && let FieldCompare::Eq(value) = field_compare {
        if let Some(prefix) = try_to_generate_id_prefix(entity, vec![( field, value )]) {
          let score = id_prefix_score(&prefix);
          return Some((prefix, score))
        }
      }

      for index in field.indexes.iter() {
        match index {
            FieldIndex::Value { tree_name, unique } => {
              return match field_compare {
                  FieldCompare::Eq(val) => generate_prefix(encode_index_data(field, val), tree_name)
                    .map(|p| (p, if *unique { SCORE_UNIQUE_EQ } else { SCORE_EQ })),
                  // Here we don't append a null terminator, since the string lengths don't have to match
                  FieldCompare::StringStartsWith(val) => generate_prefix_starts_with(val.clone(), tree_name)
                    .map(|p| (p, SCORE_STARTS_WITH)),
                  _ => None
              }
            }
            FieldIndex::Number { tree_name, ty, unique } => {
              return match field_compare {
                FieldCompare::Eq(val) => generate_prefix( encode_index_number(ty, val), tree_name)
                  .map(|p| (p, if *unique { SCORE_UNIQUE_EQ } else { SCORE_EQ })),
                // A lone bound leaves the other side of the range open; a matching pair is fused into a
                // bounded range earlier, by `fuse_range_bounds`.
                compare => range_bound(compare).map(|(is_start, bound)| {
                  let (start, end) = if is_start { (bound, None) } else { (None, bound) };
                  (PrefixKey::IndexRange { start, end, tree_name: tree_name.clone(), fixed_size: field.get_size() }, SCORE_RANGE)
                })
              }
            },
            FieldIndex::Custom { .. } => {
              // Module (`@custom`) indexes don't provide a value-prefix access path. The dedicated search
              // operator (`$near`/`$match`) dispatches to the provider separately; here we just skip them.
              continue;
            },
        }
      }

      return None
    },
  }
}

pub fn try_to_generate_id_prefix<'a>(entity: &Entity, items: Vec<(&Field,&Vec<u8>)>) -> Option<PrefixKey<'a>> {
  let mut prefix_value = vec![];

  for field in entity.fields.iter() {
    if !matches!(field.location, FieldLocation::Key { .. }) {
      return Some(PrefixKey::Id(prefix_value))
    }
    let Some(value) = items
      .iter()
      .find_map(|i| std::ptr::eq(field, i.0).then_some(i.1))
      else { break; };
    prefix_value.extend_from_slice(value); 
    if field.get_size().is_none() {
      prefix_value.push(b'\0');
    }
  }

  if prefix_value.is_empty() {
    return None
  }
  return Some(PrefixKey::IdPrefix(prefix_value))
}