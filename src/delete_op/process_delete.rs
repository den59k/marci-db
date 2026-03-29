use canopydb::{Transaction, Tree, WriteTransaction};

use crate::{Field, delete_op::{DeleteError, DeleteIndex, DeleteOp, DependencyAction, DependencyActionType, RefToDelete}, index_utils::{encode_full_index, increase_bit}, schema::{Entity, RefBinding, Schema}, utils::{get_body_data, get_data, get_end_optimized, get_offset}};

pub fn process_delete<'a>(
  tx: &'a WriteTransaction, 
  id: &[u8],
  entity: &Entity, 
  action: &DeleteOp,
  schema: &Schema,
  tree: Option<&mut Tree<'a>>
) -> Result<bool, DeleteError> {
  let mut body_value: Option<Vec<u8>> = None;
  {
    let mut owned;
    let tree: &mut Tree<'a> = match tree {
        Some(t) => t,
        None => {
          owned = tx.get_tree(entity.name.as_bytes()).unwrap().unwrap();
          &mut owned
        }
    };

    if action.is_body_need() {
      let Some(body) = tree.get(id).unwrap() else {
        return Ok(false)
      };
      body_value = Some(body.to_vec());
    }

    if !tree.delete(id).unwrap() {
      return Ok(false)
    }
  }

  for delete_index in action.indexes_to_delete.iter() {
    match delete_index {
      DeleteIndex::Value { index, key } => {
        let mut tree = tx.get_tree(index.tree_name()).unwrap().unwrap();
        tree.delete(key).unwrap();
      },
      DeleteIndex::BodyValue { index, offset_pos, field } => {
        let Some(value) = get_body_data(entity, field, body_value.as_ref().unwrap(), *offset_pos) else { 
          continue; 
        };
        let mut tree = tx.get_tree(index.tree_name()).unwrap().unwrap();
        tree.delete(&encode_full_index(field, index, id, value)).unwrap();
      },
      DeleteIndex::KeyValue { index, field, .. } => {
        let Some(value) = get_data(entity, field, &id, &[], schema) else { 
          continue;
        };
        let mut tree = tx.get_tree(index.tree_name()).unwrap().unwrap();
        tree.delete(&encode_full_index(field, index, id, value)).unwrap();
      }
    }
  }
  
  for dep in action.dependencies.iter() {
    let item_ids = get_dep_ids(tx, dep, entity, schema, id, &body_value);
    if item_ids.is_empty() { continue; }

    match &dep.action_type {
      DependencyActionType::Delete (action) => {
        for item_id in item_ids.iter() {
          process_delete(tx, &item_id, dep.rev_entity, action, schema, None)?;
        }
      },
      DependencyActionType::SetNull { offset_pos } => {
        let mut tree = tx.get_tree(dep.rev_entity.name.as_bytes()).unwrap().unwrap();
        for item_id in item_ids.iter() {
          let mut body = tree.get(item_id).unwrap().unwrap().to_vec();
          set_null(dep.rev_entity, dep.rev_field, &mut body, *offset_pos);
          tree.insert(item_id, &body).unwrap();
        }
      },
      DependencyActionType::RemoveIndex { tree_name } => {
        let mut tree = tx.get_tree(tree_name.as_bytes()).unwrap().unwrap();
        for item_id in item_ids.iter() {
          tree.delete(&[ item_id, id ].concat()).unwrap();
        }
      },
      DependencyActionType::Restrict => {
        return Err(DeleteError::RestrictConstraints(dep.rev_field.full_name.clone(), item_ids))
      },
    }
  }

  for ref_to_delete in &action.refs_to_delete {
    delete_ref_data(tx, ref_to_delete, id);
  }
  
  Ok(true)
}

pub fn delete_ref_data(tx: &Transaction, ref_to_delete: &RefToDelete, parent_id: &[u8]) {
  match ref_to_delete {
    RefToDelete::Index { tree_name } => {
      let mut tree = tx.get_tree(tree_name.as_bytes()).unwrap().unwrap();
      delete_by_prefix(&mut tree, parent_id);
    }
    RefToDelete::ChildEntity { entity, delete_op: action } => {
      let mut tree = tx.get_tree(entity.name.as_bytes()).unwrap().unwrap();
      if action.is_empty() {
        delete_by_prefix(&mut tree, parent_id);
      } else {
        todo!()
      }
    }
  }
}

fn get_dep_ids(tx: &Transaction, dep: &DependencyAction, entity: &Entity, schema: &Schema, id: &[u8], body: &Option<Vec<u8>>) -> Vec<Vec<u8>> { 
  return match dep.binding {
    Some((_, RefBinding::CurrentId)) => {
      let tree = tx.get_tree(dep.rev_entity.name.as_bytes()).unwrap().unwrap();
      let item_ids: Vec<Vec<u8>> = tree
        .prefix_keys(&id)
        .unwrap()
        .map(|e| e.unwrap().to_vec())
        .collect();
      item_ids
    },
    Some((field, RefBinding::FieldValue)) => {
      let Some(value) = get_data(entity, field, &id, body.as_ref().unwrap(), &schema) else {
        return vec![];
      };
      vec![value.to_vec()]
    },
    Some((_, RefBinding::IndexTree(tree_name))) => {
      let item_id_len = id.len();
      let index_tree = tx.get_tree(tree_name.as_bytes()).unwrap().unwrap();
      let item_ids: Vec<Vec<u8>> = index_tree
        .prefix_keys(&id)
        .unwrap()
        .map(|e| e.unwrap()[item_id_len..].to_vec())
        .collect();

      item_ids
    },
    None => {
      match &dep.rev_binding {
        RefBinding::CurrentId => {
          panic!("Cannot use binding as current ID here")
        },
        RefBinding::FieldValue => {
          let tree = tx.get_tree(dep.rev_entity.name.as_bytes()).unwrap().unwrap();
          let mut item_ids = vec![];
          for items in tree.iter().unwrap() {
            let (item_id, item_body) = items.unwrap();
            if let Some(item) = get_data(dep.rev_entity, dep.rev_field, &item_id, &item_body, &schema) && item == id {
              item_ids.push(item_id.to_vec());
            }
          }
          item_ids
        },
        RefBinding::IndexTree(_) => todo!(),
      }
    },
  };
}

fn set_null(entity: &Entity, field: &Field, body: &mut Vec<u8>, offset_pos: usize) {
  let offset_start = get_offset(body, offset_pos);
  if offset_start == 0 {
    return
  }

  let offset_end = get_end_optimized(body, field, offset_start, offset_pos, entity.payload_offset);

  body[offset_pos..offset_pos + 4].copy_from_slice(&0u32.to_be_bytes());
  body.drain(offset_start..offset_end);
}

#[inline(always)]
fn delete_by_prefix(tree: &mut Tree, prefix: &[u8]) {
  if let Some(end) = increase_bit(&prefix) {
    tree.delete_range(prefix..end.as_slice()).unwrap();
  } else {
    tree.delete_range(prefix..).unwrap();
  }
}