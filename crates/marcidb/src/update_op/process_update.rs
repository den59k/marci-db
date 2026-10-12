use canopydb::{Transaction, Tree, WriteTransaction};

use crate::{Field, MarciDB, StorageError, delete_op::process_delete, id_text, json_parsers::{EncodeError, parse_update, decode_id}, error::RequireTree, index_provider::{RowRef, on_field_update}, index_utils::{encode_full_index, encode_index, encode_unique_group}, schema::{Entity, FieldIndex, FieldLocation, RefBinding, RefInfo, Schema}, update_op::{ListOp, UpdateError, UpdateField, UpdateOp, UpdateRelationOp, UpdateValue}, utils::{check_exists_condition, decode_id_list, decode_primitive_list, encode_id_list, encode_primitive_list, get_body_data, get_data, get_end, get_end_optimized, get_offset, move_offsets, move_offsets_left, row_header_len}, write_op::{delete_ref_indexes, process_write, write_ref_indexes}};

pub fn process_update(tx: &WriteTransaction, entity: &Entity, id: &[u8], update: &UpdateOp, db: &MarciDB) -> Result<bool, UpdateError> { 

  // The read is scoped so the tree handle is released before anything below mutates. A handle that has
  // served a `get` holds the looked-up node cached; writing back through that same handle asks canopydb
  // to dirty a page it is still referencing, which trips an internal uniqueness assertion as soon as the
  // page is already dirty — i.e. from the second update onwards within one transaction. The body is
  // copied out for the same reason: it is a refcounted view over the tree's page and outlives the read.
  let data = {
    let tree = tx.require_tree(entity.name.as_bytes())?;
    let Some(data) = tree.get(id)? else {
      return Ok(false)
    };
    data.to_vec()
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
            return Err(UpdateError::UniqueViolation(field.full_name.clone(), crate::id_text(&exists_id, entity, &db.schema)))
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

  if let Some(new_data) = &resp {
    let mut tree = tx.require_tree(entity.name.as_bytes())?;
    tree.insert(id, new_data)?;
  }

  // The composite unique indexes: a group whose key changed moves its entry; a taken key is a violation
  // naming the group's fields and the row holding it (a part gone null drops the entry, as on insert).
  if let Some(new_data) = &resp {
    for group in entity.unique_groups.iter() {
      let old_key = encode_unique_group(entity, group, id, &data, &db.schema);
      let new_key = encode_unique_group(entity, group, id, new_data, &db.schema);
      if old_key == new_key { continue }
      let mut tree = tx.require_tree(group.tree_name.as_bytes())?;
      if let Some(old_key) = old_key {
        tree.delete(&[ old_key.as_slice(), id ].concat())?;
      }
      if let Some(new_key) = new_key {
        if let Some(exists) = tree.prefix_keys(&new_key.as_slice())?.next() {
          let exists_id = exists?[new_key.len()..].to_vec();
          return Err(UpdateError::UniqueViolation(group.fields_text(entity), id_text(&exists_id, entity, &db.schema)));
        }
        tree.insert(&[ new_key.as_slice(), id ].concat(), &[])?;
      }
    }
  }

  for update_ref in update.update_refs.iter() {
    match &update_ref.op {
        UpdateRelationOp::Remove(delete_op) => {
          if let Some(item_id) = get_id_from_ref_info(tx, entity, update_ref.field, update_ref.ref_info, id, &data, &db.schema)? {
            process_delete(tx, &item_id, update_ref.st, delete_op, db, None).map_err(|e| UpdateError::DeleteError(e))?;
          }
        },
        UpdateRelationOp::DisconnectAll => {
          // Break the single-ref relation without deleting the related object. The forward FK (body
          // field) is already nulled/overwritten by the `update_fields` pass above; here we only tear
          // down the index entries for the previously-connected object. `data` still holds the
          // pre-update body, so the old target id is recoverable. On the `@bind` side of a one-to-one
          // the link is the other row's field: that row is unlinked (its own update clears the tree).
          let ids: Vec<Vec<u8>> = get_id_from_ref_info(tx, entity, update_ref.field, update_ref.ref_info, id, &data, &db.schema)?
            .into_iter().collect();
          if let Some(child_field) = bound_child_field(&db.schema, update_ref.field, update_ref.ref_info) {
            relink_children(tx, db, entity, id, update_ref.field, update_ref.ref_info, child_field, &ids, false)?;
          } else {
            delete_ref_indexes(tx, update_ref.ref_info, &db.schema, id, &ids).map_err(|e| UpdateError::WriteIndexesError(e))?;
          }
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
            process_delete(tx, &item_id, update_ref.st, delete_op, db, Some(&mut tree)).map_err(|e| UpdateError::DeleteError(e))?;
          }
        },
        UpdateRelationOp::Push(write_ops) => {
          for write_op in write_ops {
            process_write(tx, update_ref.st, write_op, db, Some(id)).map_err(|e| UpdateError::InsertError(e))?;
          }
        },
        UpdateRelationOp::RemoveItems(item_ids, delete_op) => {
          // $remove on an owned (autoinsert) list: delete the named children. The client addresses
          // a child by its own key fields; the storage key adds the parent prefix, so another
          // parent's children are unreachable by construction. A missing id makes `process_delete`
          // a no-op (removing an absent child already is the requested end state).
          let mut tree = tx.require_tree(db.schema.models[update_ref.ref_info.model_index].name.as_bytes())?;
          for item_id in item_ids {
            let full_id = [id, item_id.as_slice()].concat();
            process_delete(tx, &full_id, update_ref.st, delete_op, db, Some(&mut tree)).map_err(|e| UpdateError::DeleteError(e))?;
          }
        },
        UpdateRelationOp::Connect(item_ids) => {
          if let Some(child_field) = bound_child_field(&db.schema, update_ref.field, update_ref.ref_info) {
            relink_children(tx, db, entity, id, update_ref.field, update_ref.ref_info, child_field, item_ids, true)?;
          } else {
            write_ref_indexes(tx, entity, update_ref.ref_info, &db.schema, id, item_ids).map_err(|e| UpdateError::link(e, update_ref.field))?
          }
        },
        UpdateRelationOp::Disconnect(item_ids) => {
          if let Some(child_field) = bound_child_field(&db.schema, update_ref.field, update_ref.ref_info) {
            relink_children(tx, db, entity, id, update_ref.field, update_ref.ref_info, child_field, item_ids, false)?;
          } else {
            delete_ref_indexes(tx, update_ref.ref_info, &db.schema, id, item_ids).map_err(|e| UpdateError::WriteIndexesError(e))?;
          }
        },
        UpdateRelationOp::SetLinks(item_ids) => {
          // Replace link membership: diff against the currently-linked ids, then disconnect/connect
          // the difference. The rows themselves are never created or deleted. The read is scoped so
          // the tree handle is released before the writes (see the note at the top of this function)
          let current = {
            let tree = tx.require_tree(db.schema.models[update_ref.ref_info.model_index].name.as_bytes())?;
            get_ids_from_ref_info(tx, &tree, update_ref.ref_info, id)?
          };
          let removed: Vec<Vec<u8>> = current.iter().filter(|i| !item_ids.contains(i)).cloned().collect();
          let added: Vec<Vec<u8>> = item_ids.iter().filter(|i| !current.contains(i)).cloned().collect();
          if let Some(child_field) = bound_child_field(&db.schema, update_ref.field, update_ref.ref_info) {
            relink_children(tx, db, entity, id, update_ref.field, update_ref.ref_info, child_field, &removed, false)?;
            relink_children(tx, db, entity, id, update_ref.field, update_ref.ref_info, child_field, &added, true)?;
          } else {
            if !removed.is_empty() {
              delete_ref_indexes(tx, update_ref.ref_info, &db.schema, id, &removed).map_err(|e| UpdateError::WriteIndexesError(e))?;
            }
            if !added.is_empty() {
              write_ref_indexes(tx, entity, update_ref.ref_info, &db.schema, id, &added).map_err(|e| UpdateError::link(e, update_ref.field))?;
            }
          }
        },
        UpdateRelationOp::UpdateItems(items) => {
          // $update on an owned (autoinsert) list: partial update of single children. The client
          // addresses a child by its own key fields; the storage key adds the parent prefix, so
          // another parent's children are unreachable by construction. Unlike $remove, a missing
          // child is an error — editing something that isn't there is a mistake, not an end state
          for (item_id, update_op) in items {
            let full_id = [id, item_id.as_slice()].concat();
            if !process_update(tx, update_ref.st, &full_id, update_op, db)? {
              return Err(UpdateError::ItemNotFound);
            }
          }
        },
        UpdateRelationOp::SetList(item_ids) => {
          apply_id_list_update(tx, entity, update_ref.field, update_ref.ref_info, id, db, IdListChange::Set(item_ids))?;
        },
        UpdateRelationOp::ConnectList(item_ids) => {
          apply_id_list_update(tx, entity, update_ref.field, update_ref.ref_info, id, db, IdListChange::Connect(item_ids))?;
        },
        UpdateRelationOp::ConnectUniqueList(item_ids) => {
          apply_id_list_update(tx, entity, update_ref.field, update_ref.ref_info, id, db, IdListChange::ConnectUnique(item_ids))?;
        },
        UpdateRelationOp::DisconnectList(item_ids) => {
          apply_id_list_update(tx, entity, update_ref.field, update_ref.ref_info, id, db, IdListChange::Disconnect(item_ids))?;
        },
    }
  }


  Ok(true)
}

/// Rows written before an add-field migration carry a shorter offset table than the current schema
/// (see [`row_header_len`]). Reads tolerate that — a slot past the row's header is simply "missing" —
/// but the write path below indexes the offset table directly against `entity.payload_offset`, so a
/// short row must first be grown to the current layout: append the missing slots (`0` = absent) and
/// shift the payload right. Returns `None` when the row is already current.
fn widen_row(source_data: &[u8], entity: &Entity) -> Option<Vec<u8>> {
  let header_len = row_header_len(source_data);
  if header_len >= entity.payload_offset {
    return None;
  }

  let delta = entity.payload_offset - header_len;
  let mut buf = Vec::with_capacity(source_data.len() + delta);
  buf.extend_from_slice(&source_data[..header_len]);
  buf.resize(entity.payload_offset, 0);
  buf.extend_from_slice(&source_data[header_len..]);

  // The payload moved `delta` bytes further in, so every stored offset shifts with it
  move_offsets(&mut buf, 4, header_len, delta as u32);
  buf[2..4].copy_from_slice(&(entity.payload_offset as u16).to_be_bytes());
  Some(buf)
}

fn update_fields<F>(fields: &[UpdateField], id: &[u8], source_data: &[u8], entity: &Entity, schema: &Schema, on_change: F) -> Result<Option<Vec<u8>>,UpdateError>
  where F: Fn(&Field, Option<&[u8]>, Option<&[u8]>) -> Result<(),UpdateError> {

  // A widened row is already a modified buffer, so it doubles as the copy-on-write seed: the row gets
  // rewritten in the current format even if no field ends up changing.
  let mut cloned_data: Option<Vec<u8>> = widen_row(source_data, entity);

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
      UpdateValue::Increment(delta) => {
        if offset_start == 0 { continue; }
        let offset_end = get_end_optimized(data, update_field.field, offset_start, update_field.offset_pos, entity.payload_offset);

        let old_value = &data[offset_start..offset_end];
        // Out of range is a rejection, not a wrap: returning here aborts the whole transaction.
        let Some(new_value) = delta.checked_apply(old_value) else {
          return Err(UpdateError::IncrementOutOfRange(update_field.field.full_name.clone()))
        };

        on_change(update_field.field, Some(old_value), Some(new_value.as_slice()))?; // <-- update

        let buf = cloned_data.get_or_insert_with(|| source_data.to_vec());
        update_data(buf, &new_value, entity, update_field.offset_pos, offset_start, offset_end);
      },
      UpdateValue::ListOp { op, items, elem_size } => {
        // In-place primitive-list edit: decode the current elements, apply the sequence op,
        // re-encode. An absent field (offset 0) reads as an empty list
        let (old_items, offset_end) = if offset_start == 0 {
          (vec![], 0)
        } else {
          let offset_end = get_end_optimized(data, update_field.field, offset_start, update_field.offset_pos, entity.payload_offset);
          (decode_primitive_list(Some(&data[offset_start..offset_end]), *elem_size), offset_end)
        };

        let mut new_items = old_items.clone();
        match op {
          ListOp::Push => new_items.extend(items.iter().cloned()),
          ListOp::PushUnique => {
            for item in items {
              if !new_items.contains(item) { new_items.push(item.clone()); }
            }
          }
          ListOp::Remove => new_items.retain(|i| !items.contains(i)),
        }
        if new_items == old_items { continue; }

        let encoded = encode_primitive_list(&new_items, *elem_size);
        if offset_start == 0 {
          on_change(update_field.field, None, Some(&encoded))?; // <-- insert

          let buf = cloned_data.get_or_insert_with(|| source_data.to_vec());
          insert_data(buf, &encoded, entity, update_field.offset_pos);
        } else {
          on_change(update_field.field, Some(&data[offset_start..offset_end]), Some(&encoded))?; // <-- update

          let buf = cloned_data.get_or_insert_with(|| source_data.to_vec());
          update_data(buf, &encoded, entity, update_field.offset_pos, offset_start, offset_end);
        }
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

/// The membership change a `@list` update applies to the inline id array.
/// The array is a sequence — the same id may appear several times.
enum IdListChange<'a> {
  /// Replace the whole array (also the reorder op)
  Set(&'a [Vec<u8>]),
  /// Append to the end — an already-present id gains another occurrence
  Connect(&'a [Vec<u8>]),
  /// Append only the ids not already present (also dedupes within the batch)
  ConnectUnique(&'a [Vec<u8>]),
  /// Remove every occurrence of the given ids
  Disconnect(&'a [Vec<u8>]),
}

/// Applies a `@list` membership change: rewrites the inline id array in the row body and keeps the
/// reverse index tree in sync (only actual membership changes touch the tree — a pure reorder is
/// just a body rewrite)
fn apply_id_list_update(tx: &WriteTransaction, entity: &Entity, field: &Field, ref_info: &RefInfo, id: &[u8], db: &MarciDB, change: IdListChange) -> Result<(), UpdateError> {
  let FieldLocation::Body { offset_pos } = field.location else {
    panic!("@list field must be body-located: {}", field.full_name);
  };
  let id_size = crate::schema::fixed_id_size(&db.schema.models, &db.schema.models[ref_info.model_index])
    .expect("@list target id must be fixed-size");

  // Re-read the current row: the scalar-field pass above may have already rewritten it.
  // Scoped so the read handle is released before the write below (see the note at the top of process_update)
  let data = {
    let tree = tx.require_tree(entity.name.as_bytes())?;
    let Some(data) = tree.get(id)? else { return Ok(()) };
    data.to_vec()
  };
  // Rows written before an add-field migration must first grow to the current layout
  let mut data = widen_row(&data, entity).unwrap_or(data);

  let old_ids = decode_id_list(get_body_data(field, &data, offset_pos), id_size);

  let new_ids: Vec<Vec<u8>> = match change {
    IdListChange::Set(ids) => ids.to_vec(),
    IdListChange::Connect(ids) => {
      let mut out = old_ids.clone();
      out.extend(ids.iter().cloned());
      out
    },
    IdListChange::ConnectUnique(ids) => {
      let mut out = old_ids.clone();
      for item in ids {
        if !out.contains(item) { out.push(item.clone()); }
      }
      out
    },
    IdListChange::Disconnect(ids) => old_ids.iter().filter(|i| !ids.contains(i)).cloned().collect(),
  };

  // The reverse tree is a membership index (one entry per distinct pair), so the diff is on
  // membership, not occurrences: an id still present after the change keeps its entry, and
  // duplicate inserts/deletes of the same key are idempotent in the tree
  let removed: Vec<Vec<u8>> = old_ids.iter().filter(|i| !new_ids.contains(i)).cloned().collect();
  let added: Vec<Vec<u8>> = new_ids.iter().filter(|i| !old_ids.contains(i)).cloned().collect();
  if !removed.is_empty() {
    delete_ref_indexes(tx, ref_info, &db.schema, id, &removed).map_err(|e| UpdateError::WriteIndexesError(e))?;
  }
  if !added.is_empty() {
    write_ref_indexes(tx, entity, ref_info, &db.schema, id, &added).map_err(|e| UpdateError::link(e, field))?;
  }

  let offset_start = get_offset(&data, offset_pos);
  if new_ids.is_empty() {
    if offset_start != 0 {
      set_null(&mut data, entity, field, offset_pos, offset_start);
    }
  } else {
    let encoded = encode_id_list(&new_ids);
    if offset_start == 0 {
      insert_data(&mut data, &encoded, entity, offset_pos);
    } else {
      let offset_end = get_end_optimized(&data, field, offset_start, offset_pos, entity.payload_offset);
      update_data(&mut data, &encoded, entity, offset_pos, offset_start, offset_end);
    }
  }

  let mut tree = tx.require_tree(entity.name.as_bytes())?;
  tree.insert(id, &data)?;
  Ok(())
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
    RefBinding::IdList { .. } => panic!("A to-one relation cannot use an IdList binding"),
  }
}

/// The holder's own relation field when `field` is the `@bind` side of a relation the other row stores:
/// a one-to-many's list (`User.posts Post[] @bind(Post.author)`) or a one-to-one's virtual ref
/// (`User.passport Passport? @bind(Passport.user)`). That side stores nothing itself — the link IS the
/// other row's `author` / `user`. A many-to-many (both sides lists) and a `@list` have no such field and
/// keep their index trees.
pub fn bound_child_field<'a>(schema: &'a Schema, field: &Field, ref_info: &RefInfo) -> Option<&'a Field> {
  if !matches!(field.location, FieldLocation::Virtual) { return None }
  let rev_idx = ref_info.rev_field_idx?;
  let child_field = &schema.models[ref_info.model_index].fields[rev_idx];
  match (&child_field.ty, &child_field.location) {
    (crate::schema::FieldType::Ref(_), FieldLocation::Body { .. } | FieldLocation::Key { .. }) => Some(child_field),
    _ => None,
  }
}

/// Links (`attach`) or unlinks the other rows through the `@bind` side (a one-to-many's list, a one-to-one's
/// virtual ref): each row's own relation field is written, through its update, so its body, its indexes
/// and the reverse tree agree — before this, a `$connect` on `User.posts` wrote the tree alone and
/// `Post.author` stayed null.
/// A child that does not exist is a foreign-key violation of the list field; a required child field
/// cannot be unlinked (the child would be left without its parent); a field in the child's key cannot
/// change at all.
pub fn relink_children(tx: &WriteTransaction, db: &MarciDB, parent: &Entity, parent_id: &[u8], list_field: &Field, ref_info: &RefInfo, child_field: &Field, child_ids: &[Vec<u8>], attach: bool) -> Result<(), UpdateError> {
  if child_ids.is_empty() { return Ok(()) }
  let child_entity = &db.schema.models[ref_info.model_index];
  if matches!(child_field.location, FieldLocation::Key { .. }) {
    return Err(UpdateError::Encode(EncodeError::OnlyBodyKeyAvailableToEdit(child_field.full_name.clone())));
  }
  if !attach && !child_field.nullable {
    return Err(UpdateError::RequiredRelation(child_field.full_name.clone(), list_field.full_name.clone()));
  }
  let target: serde_json::Value = if attach {
    serde_json::json!({ "$connect": serde_json::from_str::<serde_json::Value>(&decode_id(parent_id, parent, &db.schema)).expect("decode_id is JSON") })
  } else {
    serde_json::Value::Null
  };
  let op = parse_update(&db.schema, child_entity, &serde_json::json!({ child_field.name.clone(): target })).map_err(UpdateError::Encode)?;
  for child_id in child_ids {
    if !process_update(tx, child_entity, child_id, &op, db)? {
      return Err(UpdateError::ForeignKeyViolation(list_field.full_name.clone(), id_text(child_id, child_entity, &db.schema)));
    }
  }
  Ok(())
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
    // Only autoinsert (owned) lists reach here, and those are never @list
    RefBinding::IdList { .. } => panic!("An owned-list operation cannot target an IdList binding"),
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