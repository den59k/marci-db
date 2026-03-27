use canopydb::WriteTransaction;

use crate::{Field, index_utils::encode_full_index, schema::{Entity, Schema}, update_op::{UpdateError, UpdateField, UpdateOp, UpdateValue}, utils::{get_end, get_end_optimized, get_offset, move_offsets, move_offsets_left}};

pub fn process_update(tx: &WriteTransaction, entity: &Entity, id: &[u8], update: &UpdateOp, schema: &Schema) -> Result<(), UpdateError> { 

  if !update.fields.is_empty() {
    let mut tree = tx.get_tree(entity.name.as_bytes()).unwrap().unwrap();
    
    let Some(data) = tree.get(id).unwrap() else {
      return Err(UpdateError::ItemNotFound)
    };

    let resp = update_fields(&update.fields, &data, entity, | field, old_value, new_value | {
      for index in field.field.indexes.iter() {
        let mut tree = tx.get_tree(index.tree_name()).unwrap().unwrap();
        if let Some(old_value) = old_value {
          tree.delete(&encode_full_index(field.field, index, id, old_value)).unwrap();
        }
        if let Some(new_value) = new_value {
          tree.insert(&encode_full_index(field.field, index, id, new_value), &[]).unwrap();
        }
      }
    });
    
    if let Some(new_data) = resp {
      tree.insert(id, &new_data).unwrap();
    }
  }

  Ok(())
}

fn update_fields<F>(fields: &[UpdateField], source_data: &[u8], entity: &Entity, on_change: F) -> Option<Vec<u8>>
  where F: Fn(&UpdateField, Option<&[u8]>, Option<&[u8]>) {

  let mut cloned_data: Option<Vec<u8>> = None;

  for update_field in fields.iter() {
    let data = cloned_data.as_deref().unwrap_or(source_data);
    let offset_start = get_offset(data, update_field.offset_pos);

    match &update_field.value {
      UpdateValue::Null => {
        if offset_start == 0 { continue; }
        let offset_end = get_end_optimized(data, update_field.field, offset_start, update_field.offset_pos, entity.payload_offset);
        let old_value = &data[offset_start..offset_end];
        on_change(update_field, Some(old_value), None); // <-- перед изменением

        let buf = cloned_data.get_or_insert_with(|| source_data.to_vec());
        set_null(buf, entity, update_field.field, update_field.offset_pos, offset_start);
      },
      UpdateValue::Value(item_data) => {
        if offset_start == 0 {
          on_change(update_field, None, Some(item_data.as_slice())); // <-- insert

          let buf = cloned_data.get_or_insert_with(|| source_data.to_vec());
          insert_data(buf, item_data, entity, update_field.offset_pos);
          continue;
        }
        let offset_end = get_end_optimized(data, update_field.field, offset_start, update_field.offset_pos, entity.payload_offset);
        let old_slice = &data[offset_start..offset_end];
        if old_slice == item_data.as_slice() { continue; }
        on_change(update_field, Some(old_slice), Some(item_data.as_slice())); // <-- update

        let buf = cloned_data.get_or_insert_with(|| source_data.to_vec());
        update_data(buf, item_data, entity, update_field.offset_pos, offset_start, offset_end);
      },
      UpdateValue::Increment(number_value) => {
        if offset_start == 0 { continue; }
        let offset_end = get_end_optimized(data, update_field.field, offset_start, update_field.offset_pos, entity.payload_offset);

        let old_value = &data[offset_start..offset_end];
        let new_value = number_value.increment_bytes(old_value);

        on_change(update_field, Some(old_value), Some(new_value.as_slice())); // <-- update

        let buf = cloned_data.get_or_insert_with(|| source_data.to_vec());
        update_data(buf, &new_value, entity, update_field.offset_pos, offset_start, offset_end);
      },
    }
  }

  cloned_data
}

// Удаление данных
fn set_null(dst: &mut Vec<u8>, entity: &Entity, field: &Field, offset_pos: usize, offset_start: usize) {
  let offset_end = get_end_optimized(dst, field, offset_start, offset_pos, entity.payload_offset);
  dst[offset_pos..offset_pos + 4].copy_from_slice(&[ 0, 0, 0, 0 ]);

  if offset_start != offset_end {
    dst.drain(offset_start..offset_end);
    move_offsets_left(dst, offset_pos+4, entity.payload_offset, (offset_end - offset_start) as u32);
  }
}

// Вставка данных
fn insert_data(dst: &mut Vec<u8>, item_data: &[u8], entity: &Entity, offset_pos: usize) {
  let insert_place = get_end(dst, offset_pos, entity.payload_offset);
  dst[offset_pos..offset_pos + 4].copy_from_slice(&insert_place.to_be_bytes());

  if !item_data.is_empty() {
    dst.splice(insert_place..insert_place, item_data.iter().cloned());
    move_offsets(dst, offset_pos+4, entity.payload_offset, item_data.len() as u32);
  }
}

// Обновление данных
fn update_data(dst: &mut Vec<u8>, item_data: &[u8], entity: &Entity, offset_pos: usize, offset_start: usize, offset_end: usize) {
  let new_offset_end = offset_start + item_data.len();
  if new_offset_end == offset_end {
    dst[offset_start..offset_end].copy_from_slice(item_data);
  } else {
    dst.splice(offset_start..offset_end, item_data.iter().cloned());
    if new_offset_end > offset_end {
      move_offsets(dst, offset_pos+4, entity.payload_offset, (new_offset_end - offset_end) as u32);
    } else {
      move_offsets_left(dst, offset_pos+4, entity.payload_offset, (offset_end - new_offset_end) as u32);
    }
  }
}

#[cfg(test)]
mod tests {
use serde_json::json;
use crate::{parse_insert, parse_schema, parse_update, update_op::process_update::update_fields};

#[test]
fn test_update_op() {
  let schema = parse_schema("
    model User {
        name        String
        age         Int
        info        UserInfo?
    }
    struct UserInfo {
        bio         String
    }"
  );

  let user_model = &schema.models[0];

  let encoded = parse_insert(&schema, user_model, &json!({
    "name": "Alice",
    "age": 18
  })).unwrap();

  {
    let update_op = parse_update(&schema, user_model, &json!({
      "name": null
    })).unwrap();
  
    let updated = update_fields(&update_op.fields, &encoded.data, user_model, |_, _, _| {});
       
    let encoded_resp = parse_insert(&schema, user_model, &json!({
      "name": null,
      "age": 18
    })).unwrap();

    assert_eq!(&updated.unwrap(), &encoded_resp.data);
  }

  {
    let update_op = parse_update(&schema, user_model, &json!({
      "name": "Alice New",
      "age": { "$increment": 10 }
    })).unwrap();
  
    let updated = update_fields(&update_op.fields, &encoded.data, user_model, |_, _, _| {});
       
    let encoded_resp = parse_insert(&schema, user_model, &json!({
      "name": "Alice New",
      "age": 28
    })).unwrap();

    assert_eq!(&updated.unwrap(), &encoded_resp.data);
  }
}
}