use canopydb::{Transaction, Tree, WriteTransaction};

use crate::{Field, MarciDB, StorageError, delete_op::process_delete, error::RequireTree, index_provider::{RowRef, on_field_update}, index_utils::{encode_full_index, encode_index}, schema::{Entity, FieldIndex, RefBinding, RefInfo, Schema}, update_op::{UpdateError, UpdateField, UpdateOp, UpdateRelationOp, UpdateValue}, utils::{check_exists_condition, get_data, get_end, get_end_optimized, get_offset, move_offsets, move_offsets_left}, write_op::{process_write, write_ref_indexes}};

pub fn process_update(tx: &WriteTransaction, entity: &Entity, id: &[u8], update: &UpdateOp, db: &MarciDB) -> Result<bool, UpdateError> { 

  let mut tree = tx.require_tree(entity.name.as_bytes())?;

  let Some(data) = tree.get(id)? else {
    return Ok(false)
  };

  let resp = update_fields(&update.update_fields, id, &data, entity, &db.schema, | field, old_value, new_value | {
    for field_index in field.indexes.iter() {
      // Module (`@custom`) indexes are maintained below via the provider hooks, not by the inline
      // value/number index path.
      if matches!(field_index, FieldIndex::Custom { .. }) { continue; }
      let mut tree = tx.require_tree(field_index.tree_name())?;

      if let Some(old_value) = old_value {
        tree.delete(&encode_full_index(field, field_index, id, old_value))?;
      }
      if let Some(new_value) = new_value {
        let index_data = encode_index(field, field_index, new_value);
        if field_index.is_unique() {
          if let Some(exists) = tree.prefix_keys(&index_data)?.next() {
            let exists_id = exists?[index_data.len()..].to_vec();
            return Err(UpdateError::UniqueViolation(field.full_name.clone(), exists_id))
          }
        }
        tree.insert(&[ index_data.as_slice(), &id ].concat(), &[])?;
      }
    }

    // Live `@custom` index maintenance (full-text, …): re-index the changed value. The hook fires only for
    // fields that actually changed; a null↔value transition arrives as old/new = None and is resolved by the
    // provider. The pre-update body is passed for sibling-field access (unused by single-field providers).
    on_field_update(&db.providers, tx, field, RowRef { id, body: &data, entity, schema: &db.schema }, old_value, new_value)?;
    Ok(())
  })?;

  if let Some(new_data) = resp {
    tree.insert(id, &new_data)?;
  }

  for update_ref in update.update_refs.iter() {
    match &update_ref.op {
        UpdateRelationOp::Remove(delete_op) => {
          if let Some(item_id) = get_id_from_ref_info(tx, entity, update_ref.field, update_ref.ref_info, id, &data, &db.schema)? {
            process_delete(tx, &item_id, update_ref.st, delete_op, &db.schema, &db.providers, None).map_err(|e| UpdateError::DeleteError(e))?;
          }
        },
        UpdateRelationOp::DisconnectAll => {
          return Err(UpdateError::Unsupported("disconnecting a relation (set null / $connect on a single ref) is not supported yet"));
        },
        UpdateRelationOp::Create(write_op) => {
          process_write(tx, update_ref.st, write_op, db, Some(id)).map_err(|e| UpdateError::InsertError(e))?;
        },
        UpdateRelationOp::Update(update_op) => {
          if let Some(item_id) = get_id_from_ref_info(tx, entity, update_ref.field, update_ref.ref_info, id, &data, &db.schema)? {
            process_update(tx, update_ref.st, &item_id, update_op, db)?;
          }
        },
        UpdateRelationOp::RemoveAll(delete_op) => {
          let mut tree = tx.require_tree(db.schema.models[update_ref.ref_info.model_index].name.as_bytes())?;
          for item_id in get_ids_from_ref_info(tx, &tree, update_ref.ref_info, id)? {
            // println!("ready to delete {} {:#?}", update_ref.field.name, delete_op);
            process_delete(tx, &item_id, update_ref.st, delete_op, &db.schema, &db.providers, Some(&mut tree)).map_err(|e| UpdateError::DeleteError(e))?;
          }
        },
        UpdateRelationOp::Push(write_ops) => {
          for write_op in write_ops {
            process_write(tx, update_ref.st, write_op, db, Some(id)).map_err(|e| UpdateError::InsertError(e))?;
          }
        },
        UpdateRelationOp::RemoveItems(_items, _delete_op) =>
          return Err(UpdateError::Unsupported("$remove of specific items from an owned relation list is not supported yet")),
        UpdateRelationOp::Connect(item_ids) => {
          write_ref_indexes(tx, update_ref.ref_info, &db.schema, id, item_ids).map_err(|e| UpdateError::WriteIndexesError(e))?
        },
        UpdateRelationOp::Disconnect(_items) =>
          return Err(UpdateError::Unsupported("$remove (disconnect specific items) from a relation list is not supported yet")),
    }
  }


  Ok(true)
}

fn update_fields<F>(fields: &[UpdateField], id: &[u8], source_data: &[u8], entity: &Entity, schema: &Schema, on_change: F) -> Result<Option<Vec<u8>>,UpdateError>
  where F: Fn(&Field, Option<&[u8]>, Option<&[u8]>) -> Result<(),UpdateError> {

  let mut cloned_data: Option<Vec<u8>> = None;

  for update_field in fields.iter() {
    let data = cloned_data.as_deref().unwrap_or(source_data);

    // Skip fields whose enum variant does not match the current enum value.
    // The check uses the already-modified data: if this same update changes the enum itself,
    // it comes before its fields in the list, so by this point the value is already the new one
    if !check_exists_condition(entity, &update_field.field.condition, id, data, schema) {
      continue;
    }

    let offset_start = get_offset(data, update_field.offset_pos);

    match &update_field.value {
      UpdateValue::Null => {
        if offset_start == 0 { continue; }
        let offset_end = get_end_optimized(data, update_field.field, offset_start, update_field.offset_pos, entity.payload_offset);
        let old_value = &data[offset_start..offset_end];
        on_change(update_field.field, Some(old_value), None)?; // <-- before the change

        let buf = cloned_data.get_or_insert_with(|| source_data.to_vec());
        set_null(buf, entity, update_field.field, update_field.offset_pos, offset_start);
      },
      UpdateValue::Value(item_data) => {
        if offset_start == 0 {
          on_change(update_field.field, None, Some(item_data.as_slice()))?; // <-- insert

          let buf = cloned_data.get_or_insert_with(|| source_data.to_vec());
          insert_data(buf, item_data, entity, update_field.offset_pos);
          continue;
        }
        let offset_end = get_end_optimized(data, update_field.field, offset_start, update_field.offset_pos, entity.payload_offset);
        let old_slice = &data[offset_start..offset_end];
        if old_slice == item_data.as_slice() { continue; }
        on_change(update_field.field, Some(old_slice), Some(item_data.as_slice()))?; // <-- update

        let buf = cloned_data.get_or_insert_with(|| source_data.to_vec());
        update_data(buf, item_data, entity, update_field.offset_pos, offset_start, offset_end);
      },
      UpdateValue::Increment(number_value) => {
        if offset_start == 0 { continue; }
        let offset_end = get_end_optimized(data, update_field.field, offset_start, update_field.offset_pos, entity.payload_offset);

        let old_value = &data[offset_start..offset_end];
        let new_value = number_value.increment_bytes(old_value);

        on_change(update_field.field, Some(old_value), Some(new_value.as_slice()))?; // <-- update

        let buf = cloned_data.get_or_insert_with(|| source_data.to_vec());
        update_data(buf, &new_value, entity, update_field.offset_pos, offset_start, offset_end);
      },
    }
  }

  Ok(cloned_data)
}

// Deleting data
fn set_null(dst: &mut Vec<u8>, entity: &Entity, field: &Field, offset_pos: usize, offset_start: usize) {
  let offset_end = get_end_optimized(dst, field, offset_start, offset_pos, entity.payload_offset);
  dst[offset_pos..offset_pos + 4].copy_from_slice(&[ 0, 0, 0, 0 ]);

  if offset_start != offset_end {
    dst.drain(offset_start..offset_end);
    move_offsets_left(dst, offset_pos+4, entity.payload_offset, (offset_end - offset_start) as u32);
  }
}

// Inserting data
fn insert_data(dst: &mut Vec<u8>, item_data: &[u8], entity: &Entity, offset_pos: usize) {
  let insert_place = get_end(dst, offset_pos, entity.payload_offset);
  dst[offset_pos..offset_pos + 4].copy_from_slice(&(insert_place as u32).to_be_bytes());

  if !item_data.is_empty() {
    dst.splice(insert_place..insert_place, item_data.iter().cloned());
    move_offsets(dst, offset_pos+4, entity.payload_offset, item_data.len() as u32);
  }
}

// Updating data
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

fn get_id_from_ref_info<'a>(tx: &Transaction, entity: &Entity, field: &Field, ref_info: &RefInfo, id: &'a [u8], body: &'a [u8], schema: &Schema) -> Result<Option<Vec<u8>>, StorageError> {
  match &ref_info.binding {
    RefBinding::CurrentId => {
      Ok(Some(id.to_vec()))
    },
    RefBinding::FieldValue => {
      Ok(get_data(entity, field, id, body, &schema).map(|i| i.to_vec()))
    },
    RefBinding::IndexTree(tree_name) => {
      let item_id_len = id.len();
      let index_tree = tx.require_tree(tree_name.as_bytes())?;
      match index_tree.prefix_keys(&id)?.next() {
        Some(e) => Ok(Some(e?[item_id_len..].to_vec())),
        None => Ok(None),
      }
    },
  }
}

/// Fetches the ids of related objects
fn get_ids_from_ref_info(tx: &Transaction, obj_tree: &Tree, ref_info: &RefInfo, id: &[u8]) -> Result<Vec<Vec<u8>>, StorageError> {
  match &ref_info.binding {
    RefBinding::CurrentId => {
      obj_tree.prefix_keys(&id)?.map(|key| Ok(key?.to_vec())).collect()
    },
    RefBinding::FieldValue => panic!("RefList cannot be in FieldValue"),
    RefBinding::IndexTree(tree_name) => {
      let item_id_len = id.len();
      let index_tree = tx.require_tree(tree_name.as_bytes())?;
      index_tree.prefix_keys(&id)?.map(|e| Ok(e?[item_id_len..].to_vec())).collect()
    },
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
        name        String?
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

    let updated = update_fields(&update_op.update_fields, &encoded.id, &encoded.data, user_model, &schema, |_, _, _| { Ok(()) }).unwrap();
       
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

    let updated = update_fields(&update_op.update_fields, &encoded.id, &encoded.data, user_model, &schema, |_, _, _| { Ok(()) }).unwrap();
       
    let encoded_resp = parse_insert(&schema, user_model, &json!({
      "name": "Alice New",
      "age": 28
    })).unwrap();

    assert_eq!(&updated.unwrap(), &encoded_resp.data);
  }
}
}