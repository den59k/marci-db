use crate::{Field};
use crate::schema::{Entity, FieldExistsCondition, FieldLocation, FieldType, Schema};

#[inline(always)]
pub fn get_offset(data: &[u8], offset_pos: usize) -> usize {
    debug_assert!(offset_pos + 4 <= data.len());

    unsafe {
        let ptr = data.as_ptr().add(offset_pos) as *const u32;
        u32::from_be(ptr.read_unaligned()) as usize
    }
}

#[inline(always)]
pub fn get_end(data: &[u8], offset_pos: usize, payload_offset: usize) -> usize {
    debug_assert!(offset_pos + 4 <= payload_offset);
    debug_assert!(payload_offset <= data.len());

    let mut i = offset_pos + 4;

    unsafe {
        let ptr = data.as_ptr();

        while i + 4 <= payload_offset {
            let val = (ptr.add(i) as *const u32).read_unaligned();
            let off = u32::from_be(val);

            if off != 0 {
                return off as usize;
            }

            i += 4;
        }
    }

    data.len()
}

#[inline(always)]
pub fn get_end_optimized(data: &[u8], field: &Field, offset_start: usize, offset_pos: usize, payload_offset: usize) -> usize {
  field.get_size()
    .map(|f| offset_start + f)
    .unwrap_or_else(|| get_end(&data, offset_pos, payload_offset))
}

// Computes the value size from the ID
pub fn get_id_field_size<'a>(id: &'a [u8], field: &Field, offset: usize, schema: &Schema) -> usize {
  if let Some(size) = field.get_size() {
    return size
  }
  match &field.ty {
    FieldType::Primitive(_) => {
      id[offset..].iter().position(|&b| b == b'\0').unwrap_or(id.len()-offset)
    },
    FieldType::Ref (ref_info) => {
        let ref_entity = &schema.models[ref_info.model_index];
        let mut id_size = 0;
        let ref_id = &id[offset..];
        for ref_field in ref_entity.fields.iter() {
            if matches!(ref_field.location, FieldLocation::Key { .. }) {
                id_size += get_id_field_size(ref_id, ref_field, id_size, schema);
            }
        }
        id_size
    },  
    _ => { panic!("Non primitive types in key is not supported") }
  }
}

/// Returns the slice for the next element in the ID
pub fn get_next_id_value<'a>(field: &Field, id: &'a[u8], schema: &Schema, offset: usize) -> &'a[u8] {
  let size = get_id_field_size(id, field, offset, &schema);
  return &id[offset..offset+size];
}

/// The row header length (the offset table) — stored in the row itself (bytes 2..4) on write.
/// Rows written under a schema with fewer fields have a shorter header;
/// a field whose slot is beyond it is considered missing (added by a later migration).
/// This way the reader stays forward-compatible without rewriting rows.
#[inline(always)]
pub fn row_header_len(body: &[u8]) -> usize {
  u16::from_be_bytes([body[2], body[3]]) as usize
}

pub fn get_body_data<'a>(field: &Field, body: &'a[u8], offset_pos: usize) -> Option<&'a[u8]> {
  let header_len = row_header_len(body);
  // The slot is beyond this row's header — the field was added after the row was written
  if offset_pos + 4 > header_len {
    return None;
  }
  let offset_start = get_offset(body, offset_pos);
  if offset_start == 0 {
    return None;
  }
  let offset_end = get_end_optimized(body, field, offset_start, offset_pos, header_len);
  return Some(&body[offset_start..offset_end]);
}

pub fn get_data<'a>(entity: &Entity, field: &Field, id: &'a[u8], body: &'a[u8], schema: &Schema) -> Option<&'a[u8]> {
  match field.location {
    FieldLocation::Key { index: field_location_index } => {
      let mut offset = 0;
      for another_field in entity.fields.iter() {
        match another_field.location {
            FieldLocation::Key { index: another_field_location_index } => {
              let data = get_next_id_value(field, id, schema, offset);
              if another_field_location_index == field_location_index {
                return Some(data);
              }
              if another_field_location_index > field_location_index {
                break;
              }
              offset += data.len();
            }
            _ => { return None }
        }
      }
      return None;
    },
    FieldLocation::Body { offset_pos } => {
      return get_body_data(field, body, offset_pos);
    },
    FieldLocation::Virtual => { panic!("Trying to get value from virtual field") }
  }
}

pub fn check_exists_condition(entity: &Entity, condition: &FieldExistsCondition, id: &[u8], data: &[u8], schema: &Schema) -> bool {
  match condition {
      FieldExistsCondition::EnumValue { field_index, variants } => {
        let Some(val) = get_data(entity, &entity.fields[*field_index], &id, &data, schema) else {
            return false;
        };
        let val = u16::from_be_bytes(val.try_into().unwrap());
        return variants.contains(&val);
      },
      FieldExistsCondition::None => {
        return true
      }
  }
}

pub fn move_offsets(dst: &mut Vec<u8>, start: usize, payload_offset: usize, inc: u32) {
  let mut i = start;
  while i + 4 <= payload_offset {
    let ptr = dst.as_mut_ptr().wrapping_add(i) as *mut u32;
    unsafe {
        let val = u32::from_be(*ptr);
        if val != 0 {
          ptr.write_unaligned((val + inc).to_be());
        }
    }
    i += 4;
  }
}

pub fn move_offsets_left(dst: &mut Vec<u8>, start: usize, payload_offset: usize, inc: u32) {
  let mut i = start;
  while i + 4 <= payload_offset {
    let ptr = dst.as_mut_ptr().wrapping_add(i) as *mut u32;
    unsafe {
        let val = u32::from_be(*ptr);
        if val == 0 { i += 4; continue; }
        if val != 0 {
          ptr.write_unaligned((val - inc).to_be());
        }
    }
    i += 4;
  }
}

#[cfg(test)]
pub fn get_offsets(data: &[u8], model: &Entity) -> Vec<usize> {
  let mut arr = vec![];
  for field in model.fields.iter() {
    let FieldLocation::Body { offset_pos } = field.location else {
      continue;
    };
    let offset = get_offset(data, offset_pos);
    arr.push(offset);
  }
  return arr;
}

#[cfg(test)]
mod tests {
  use serde_json::json;
  use crate::{parse_insert, parse_schema, utils::{get_data, row_header_len}};

  /// A row written under a schema with fewer fields is read correctly by a newer schema:
  /// the existing fields are in place, the one added later — is missing. This is the basis of the v2 format:
  /// the reader uses the header length from the row itself, so the add-field migration doesn't rewrite rows.
  #[test]
  fn forward_compatible_short_row() {
    let old = parse_schema("
      model User {
        name String
        age  UInt
      }
    ");
    let new = parse_schema("
      model User {
        name String
        age  UInt
        bio  String
      }
    ");

    // The row is written under the old schema (2 body fields → short header)
    let user_old = &old.models[0];
    let row = parse_insert(&old, user_old, &json!({ "name": "Alice", "age": 30 })).unwrap();

    // The new schema: one more body field → its payload_offset is longer than the old row's header
    let user_new = &new.models[0];
    assert!(user_new.payload_offset > row_header_len(&row.data));

    let field = |name: &str| user_new.fields.iter().find(|f| f.name == name).unwrap();

    // Existing fields are read correctly...
    assert_eq!(get_data(user_new, field("name"), &row.id, &row.data, &new).unwrap(), b"Alice");
    assert!(get_data(user_new, field("age"), &row.id, &row.data, &new).is_some());
    // ...and a field added later (slot beyond the old row's header) — is missing
    assert_eq!(get_data(user_new, field("bio"), &row.id, &row.data, &new), None);
  }
}