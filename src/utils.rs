use crate::{Field};
use crate::schema::{Entity, FieldExistsCondition, FieldLocation, FieldType, Schema};

pub fn get_offset<'a>(data: &'a [u8], offset_pos: usize) -> usize {
  return u32::from_be_bytes(data[offset_pos..offset_pos + 4].try_into().unwrap()) as usize;
}

pub fn get_end(data: &[u8], offset_pos: usize, payload_offset: usize) -> usize {
  for j in ((offset_pos+4)..payload_offset).step_by(4) {
    let off_j = get_offset(data, j);
    if off_j != 0 {
      return off_j;
    }
  }

  return data.len();
}

// Вычисляет размер значения из ID
pub fn get_id_field_size<'a>(id: &'a [u8], field: &Field, offset: usize, schema: &Schema) -> usize {
  match &field.ty {
    FieldType::Primitive(primitive) => {
        primitive.get_size().unwrap_or_else(|| {
            id[offset..].iter().position(|&b| b == b'\0').unwrap_or(id.len()-offset)
        })
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

/// Возвращает slice для следующего элемента в ID
pub fn get_next_id_value<'a>(field: &Field, id: &'a[u8], schema: &Schema, offset: usize) -> &'a[u8] {
  let size = get_id_field_size(id, field, offset, &schema);
  return &id[offset..offset+size];
}

pub fn get_body_data<'a>(entity: &Entity, field: &Field, body: &'a[u8], offset_pos: usize) -> Option<&'a[u8]> {
  let offset = get_offset(body, offset_pos);
  if offset == 0 {
    return None;
  }
  let offset_end = field.get_size()
    .map(|f| offset + f)
    .unwrap_or_else(|| get_end(body, offset_pos, entity.payload_offset));

  return Some(&body[offset..offset_end]);
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
    FieldLocation::Body { offset: offset_pos } => {
      return get_body_data(entity, field, body, offset_pos);
    },
    FieldLocation::Virtual => { panic!("Trying to get value from virtual field") }
  }
}

pub fn check_exists_condition(entity: &Entity, condition: &FieldExistsCondition, id: &[u8], data: &[u8], schema: &Schema) -> bool {
  match condition {
      FieldExistsCondition::EnumValue { field_index, variant } => {
        let Some(val) = get_data(entity, &entity.fields[*field_index], &id, &data, schema) else {
            return false;
        };
        let val = u16::from_be_bytes(val.try_into().unwrap());
        if val != *variant {
            return false;
        }
        return true;
      },
      FieldExistsCondition::None => {
        return true
      }
  }
}

#[cfg(test)]
pub fn get_offsets(data: &[u8], model: &Entity) -> Vec<usize> {
  let mut arr = vec![];
  for field in model.fields.iter() {
    let FieldLocation::Body { offset } = field.location else {
      continue;
    };
    let offset = get_offset(data, offset);
    arr.push(offset);
  }
  return arr;
}