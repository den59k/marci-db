use canopydb::{Transaction, Tree, WriteTransaction};

use crate::{Field, ProviderRegistry, delete_op::{DeleteError, DeleteIndex, DeleteOp, DependencyAction, DependencyActionType, RefToDelete}, error::RequireTree, index_provider::{RowRef, on_field_delete, on_field_update}, index_utils::{encode_full_index, increase_bit}, schema::{Entity, FieldIndex, RefBinding, Schema}, utils::{get_body_data, get_data, get_end_optimized, get_offset, move_offsets_left, row_header_len}};

pub fn process_delete<'a>(
  tx: &'a WriteTransaction,
  id: &[u8],
  entity: &Entity,
  action: &DeleteOp,
  schema: &Schema,
  providers: &ProviderRegistry,
  tree: Option<&mut Tree<'a>>
) -> Result<bool, DeleteError> {
  let mut body_value: Option<Vec<u8>> = None;
  {
    let mut owned;
    let tree: &mut Tree<'a> = match tree {
        Some(t) => t,
        None => {
          owned = tx.require_tree(entity.name.as_bytes())?;
          &mut owned
        }
    };

    if action.is_body_need() {
      let Some(body) = tree.get(id)? else {
        return Ok(false)
      };
      body_value = Some(body.to_vec());
    }

    if !tree.delete(id)? {
      return Ok(false)
    }
  }

  for delete_index in action.indexes_to_delete.iter() {
    match delete_index {
      DeleteIndex::Value { index, key } => {
        let mut tree = tx.require_tree(index.tree_name())?;
        tree.delete(key)?;
      },
      DeleteIndex::BodyValue { index, offset_pos, field } => {
        let Some(value) = get_body_data(field, body_value.as_ref().unwrap(), *offset_pos) else {
          continue;
        };
        let mut tree = tx.require_tree(index.tree_name())?;
        tree.delete(&encode_full_index(field, index, id, value))?;
      },
      DeleteIndex::KeyValue { index, field, .. } => {
        let Some(value) = get_data(entity, field, &id, &[], schema) else {
          continue;
        };
        let mut tree = tx.require_tree(index.tree_name())?;
        tree.delete(&encode_full_index(field, index, id, value))?;
      },
      DeleteIndex::Custom { field } => {
        // The old field value lives in the body; `is_body_need()` guaranteed it was read above.
        let body = body_value.as_deref().expect("a Custom delete-index forces is_body_need");
        on_field_delete(providers, tx, field, RowRef { id, body, entity, schema })?;
      }
    }
  }

  for dep in action.dependencies.iter() {
    let item_ids = get_dep_ids(tx, dep, entity, schema, id, &body_value)?;
    if item_ids.is_empty() { continue; }

    match &dep.action_type {
      DependencyActionType::Delete (action) => {
        for item_id in item_ids.iter() {
          process_delete(tx, &item_id, dep.rev_entity, action, schema, providers, None)?;
        }
      },
      DependencyActionType::SetNull { offset_pos } => {
        for item_id in item_ids.iter() {
          // The read is scoped so the tree handle is released before the write below: a handle that has
          // served a `get` still references the looked-up page, and writing through it trips canopydb's
          // dirty-page uniqueness assertion. The body is copied out for the same reason.
          let Some(old_body) = ({
            let tree = tx.require_tree(dep.rev_entity.name.as_bytes())?;
            tree.get(item_id)?.map(|body| body.to_vec())
          }) else { continue };

          let mut body = old_body.clone();
          let Some(old_value) = set_null(dep.rev_field, &mut body, *offset_pos) else { continue };

          {
            let mut tree = tx.require_tree(dep.rev_entity.name.as_bytes())?;
            tree.insert(item_id, &body)?;
          }

          // The foreign key is gone, so its index entries must go with it — otherwise a lookup by the
          // now-dangling old value still finds this row.
          for index in dep.rev_field.indexes.iter() {
            if matches!(index, FieldIndex::Custom { .. }) { continue; }
            let mut index_tree = tx.require_tree(index.tree_name())?;
            index_tree.delete(&encode_full_index(dep.rev_field, index, item_id, &old_value))?;
          }
          // Live `@custom` (module) indexes see the same transition as a `$update` to null.
          on_field_update(providers, tx, dep.rev_field,
            RowRef { id: item_id, body: &old_body, entity: dep.rev_entity, schema }, Some(&old_value), None)?;
        }
      },
      DependencyActionType::RemoveIndex { tree_name } => {
        let mut tree = tx.require_tree(tree_name.as_bytes())?;
        for item_id in item_ids.iter() {
          tree.delete(&[ item_id, id ].concat())?;
        }
      },
      DependencyActionType::RemoveFromIdList { tree_name } => {
        // item_ids are the owners whose `@list` array contains the deleted id (from the reverse tree):
        // splice the id out of each owner's body and drop the reverse-tree entry (`deleted ++ owner`)
        let mut owner_tree = tx.require_tree(dep.rev_entity.name.as_bytes())?;
        let mut index_tree = tx.require_tree(tree_name.as_bytes())?;
        for item_id in item_ids.iter() {
          // The Bytes view must be dropped (copied out) before writing through the same handle —
          // a live refcounted view of the page trips canopydb's dirty-page uniqueness assertion
          let row = owner_tree.get(item_id)?.map(|row| row.to_vec());
          if let Some(mut body) = row {
            if remove_from_id_list_row(dep.rev_field, &mut body, id) {
              owner_tree.insert(item_id, &body)?;
            }
          }
          index_tree.delete(&[ id, item_id.as_slice() ].concat())?;
        }
      },
      DependencyActionType::Restrict => {
        return Err(DeleteError::RestrictConstraints(dep.rev_field.full_name.clone(), item_ids))
      },
    }
  }

  for ref_to_delete in &action.refs_to_delete {
    // A `@list` owner with a hidden reverse tree: its members are in the (already-read) body —
    // drop the `member ++ owner` reverse entries for each
    if let RefToDelete::IdListRev { tree_name, field } = ref_to_delete {
      let body = body_value.as_deref().expect("an IdListRev ref forces is_body_need");
      let crate::schema::FieldType::RefList(ref_info) = &field.ty else { panic!("IdListRev on a non-list field") };
      let crate::schema::FieldLocation::Body { offset_pos } = field.location else { panic!("IdListRev on a non-body field") };
      let id_size = crate::schema::fixed_id_size(&schema.models, &schema.models[ref_info.model_index])
        .expect("@list target id must be fixed-size");
      let members = crate::utils::decode_id_list(crate::utils::get_body_data(field, body, offset_pos), id_size);
      let mut tree = tx.require_tree(tree_name.as_bytes())?;
      for member in members {
        tree.delete(&[ member.as_slice(), id ].concat())?;
      }
      continue;
    }
    // An owned (CurrentId) collection whose children carry their OWN dependencies/indexes can't be
    // bulk-removed by prefix — each child must be deleted individually so its cleanup runs. Children
    // are stored under this row's key prefix. (When a Cascade dependency already deleted them above,
    // this prefix scan simply finds nothing.)
    if let RefToDelete::ChildEntity { entity: child, delete_op } = ref_to_delete && !delete_op.is_empty() {
      let child_ids: Vec<Vec<u8>> = {
        let tree = tx.require_tree(child.name.as_bytes())?;
        tree.prefix_keys(&id)?.map(|e| Ok(e?.to_vec())).collect::<Result<Vec<_>, canopydb::Error>>()?
      };
      for child_id in child_ids.iter() {
        process_delete(tx, child_id, child, delete_op, schema, providers, None)?;
      }
      continue;
    }
    delete_ref_data(tx, ref_to_delete, id)?;
  }

  Ok(true)
}

pub fn delete_ref_data(tx: &Transaction, ref_to_delete: &RefToDelete, parent_id: &[u8]) -> Result<(), DeleteError> {
  match ref_to_delete {
    RefToDelete::Index { tree_name } => {
      let mut tree = tx.require_tree(tree_name.as_bytes())?;
      delete_by_prefix(&mut tree, parent_id)?;
    }
    RefToDelete::ChildEntity { entity, delete_op: action } => {
      let mut tree = tx.require_tree(entity.name.as_bytes())?;
      if action.is_empty() {
        delete_by_prefix(&mut tree, parent_id)?;
      } else {
        // A cascade into a nested owned collection that has its own dependencies is not implemented yet
        return Err(DeleteError::Unsupported("cascade delete of a nested owned collection with its own dependencies is not supported yet"));
      }
    }
    RefToDelete::IdListRev { .. } => unreachable!("IdListRev is handled inline in process_delete (needs the body)"),
  }
  Ok(())
}

fn get_dep_ids(tx: &Transaction, dep: &DependencyAction, entity: &Entity, schema: &Schema, id: &[u8], body: &Option<Vec<u8>>) -> Result<Vec<Vec<u8>>, DeleteError> {
  let item_ids = match dep.binding {
    Some((_, RefBinding::CurrentId)) => {
      let tree = tx.require_tree(dep.rev_entity.name.as_bytes())?;
      tree.prefix_keys(&id)?
        .map(|e| Ok(e?.to_vec()))
        .collect::<Result<Vec<_>, canopydb::Error>>()?
    },
    Some((field, RefBinding::FieldValue)) => {
      let Some(value) = get_data(entity, field, &id, body.as_ref().unwrap(), &schema) else {
        return Ok(vec![]);
      };
      vec![value.to_vec()]
    },
    Some((field, RefBinding::IdList { .. })) => {
      // The deleted row's own `@list` array holds the related ids directly
      let crate::schema::FieldType::RefList(ref_info) = &field.ty else { panic!("IdList binding on a non-list field") };
      let id_size = crate::schema::fixed_id_size(&schema.models, &schema.models[ref_info.model_index])
        .expect("@list target id must be fixed-size");
      crate::utils::decode_id_list(get_data(entity, field, &id, body.as_ref().unwrap(), &schema), id_size)
    },
    Some((_, RefBinding::IndexTree(tree_name))) => {
      let item_id_len = id.len();
      let index_tree = tx.require_tree(tree_name.as_bytes())?;
      index_tree.prefix_keys(&id)?
        .map(|e| Ok(e?[item_id_len..].to_vec()))
        .collect::<Result<Vec<_>, canopydb::Error>>()?
    },
    None => {
      match &dep.rev_binding {
        RefBinding::CurrentId => {
          return Err(DeleteError::Unsupported("delete dependency with CurrentId reverse-binding and no forward binding is not supported"));
        },
        RefBinding::FieldValue => {
          let tree = tx.require_tree(dep.rev_entity.name.as_bytes())?;
          let mut item_ids = vec![];
          for items in tree.iter()? {
            let (item_id, item_body) = items?;
            if let Some(item) = get_data(dep.rev_entity, dep.rev_field, &item_id, &item_body, &schema) && item == id {
              item_ids.push(item_id.to_vec());
            }
          }
          item_ids
        },
        RefBinding::IndexTree(tree_name) => {
          // No forward field to locate index entries directly. The index maps `parent_id ++ item_id`,
          // so scan it and match the deleted item's id as the suffix; RemoveIndex then deletes
          // `parent_id ++ id`. Yields nothing for an index that was never maintained (no forward binding),
          // which is exactly the composite-key case where children are reached by key prefix instead.
          let id_len = id.len();
          let index_tree = tx.require_tree(tree_name.as_bytes())?;
          let mut item_ids = vec![];
          for entry in index_tree.iter()? {
            let (key, _) = entry?;
            if key.len() >= id_len && &key[key.len() - id_len..] == id {
              item_ids.push(key[..key.len() - id_len].to_vec());
            }
          }
          item_ids
        },
        RefBinding::IdList { rev_tree } => {
          // `@list` with no declared back-reference: the hidden reverse tree maps
          // `member_id ++ owner_id`, so the owners are a prefix scan by the deleted id
          let id_len = id.len();
          let index_tree = tx.require_tree(rev_tree.as_bytes())?;
          index_tree.prefix_keys(&id)?
            .map(|e| Ok(e?[id_len..].to_vec()))
            .collect::<Result<Vec<_>, canopydb::Error>>()?
        },
      }
    },
  };
  Ok(item_ids)
}

/// Splices `target_id` out of the row's inline `@list` array, shifting later offsets left.
/// Returns whether the row changed. Tolerates short (pre-migration) rows — nothing stored, nothing removed.
fn remove_from_id_list_row(field: &Field, body: &mut Vec<u8>, target_id: &[u8]) -> bool {
  let crate::schema::FieldLocation::Body { offset_pos } = field.location else {
    panic!("@list field must be body-located: {}", field.full_name);
  };
  let Some(old_value) = crate::utils::get_body_data(field, body, offset_pos) else { return false };
  let ids = crate::utils::decode_id_list(Some(old_value), target_id.len());
  let new_ids: Vec<Vec<u8>> = ids.into_iter().filter(|i| i != target_id).collect();

  // Offsets are bounded by the row's own header — the row may be shorter than the current schema
  let header_len = crate::utils::row_header_len(body);
  let offset_start = get_offset(body, offset_pos);
  let offset_end = crate::utils::get_end(body, offset_pos, header_len);
  let old_len = offset_end - offset_start;

  if new_ids.is_empty() {
    body[offset_pos..offset_pos + 4].copy_from_slice(&0u32.to_be_bytes());
    body.drain(offset_start..offset_end);
    crate::utils::move_offsets_left(body, offset_pos + 4, header_len, old_len as u32);
  } else {
    let encoded = crate::utils::encode_id_list(&new_ids);
    if encoded.len() == old_len {
      return false; // the id was not in the array (already removed) — nothing changed
    }
    let shrink = (old_len - encoded.len()) as u32;
    body.splice(offset_start..offset_end, encoded.into_iter());
    crate::utils::move_offsets_left(body, offset_pos + 4, header_len, shrink);
  }
  true
}

/// Clears a body field in place: zeroes its offset slot, removes its bytes, and shifts every LATER
/// offset left by the removed length. Returns the removed value (the caller needs it to tear down the
/// field's index entries), or `None` when the field is already absent.
///
/// Two invariants this must not break, both of which produced silently corrupt rows before:
///  * every offset after the hole has to move — dropping bytes without `move_offsets_left` leaves each
///    following field pointing `len` bytes too far right, so neighbouring scalars decode as garbage;
///  * bounds come from the row's OWN header (`row_header_len`), not the current schema's
///    `payload_offset` — a row written before an add-field migration has a shorter offset table, and
///    walking past it would rewrite payload bytes as if they were offsets.
fn set_null(field: &Field, body: &mut Vec<u8>, offset_pos: usize) -> Option<Vec<u8>> {
  let header_len = row_header_len(body);
  // The slot is beyond this row's header — the field was added after the row was written
  if offset_pos + 4 > header_len {
    return None
  }

  let offset_start = get_offset(body, offset_pos);
  if offset_start == 0 {
    return None
  }
  let offset_end = get_end_optimized(body, field, offset_start, offset_pos, header_len);

  let old_value = body[offset_start..offset_end].to_vec();
  body[offset_pos..offset_pos + 4].copy_from_slice(&0u32.to_be_bytes());
  if offset_end > offset_start {
    body.drain(offset_start..offset_end);
    move_offsets_left(body, offset_pos + 4, header_len, (offset_end - offset_start) as u32);
  }
  Some(old_value)
}

#[inline(always)]
fn delete_by_prefix(tree: &mut Tree, prefix: &[u8]) -> Result<(), canopydb::Error> {
  if let Some(end) = increase_bit(&prefix) {
    tree.delete_range(prefix..end.as_slice())?;
  } else {
    tree.delete_range(prefix..)?;
  }
  Ok(())
}