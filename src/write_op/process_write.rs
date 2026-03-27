use std::sync::atomic::Ordering;

use canopydb::{Tree, WriteTransaction};

use crate::{MarciDB, schema::{Entity, FieldDefault, FieldType, RefBinding, RefInfo, Schema}, write_op::{WriteDefault, WriteIndex, WriteOp, WriteRelation}};

#[derive(Debug)]
pub enum InsertError {
  ForeignKeyViolation(String, u64),
  ItemNotFound,
  UniqueViolation(String, Vec<u8>),
  DuplicateKey(Vec<u8>),
  CannotChangePrimaryKey(String),
  ParentIdRequired
}

pub fn write_data(tx: &WriteTransaction, entity: &Entity, insert: &WriteOp, db: &MarciDB, parent_id: Option<&[u8]>) -> Result<Vec<u8>, InsertError> {

  let mut write_id = insert.id.clone();
  let mut temp_data: Option<Vec<u8>> = None;
  
  for field in insert.defaults.iter() {
    match field {
      WriteDefault::Key(offset, FieldDefault::Counter(counter_idx)) => {
        let next_value = db.counters[*counter_idx].fetch_add(1, Ordering::Relaxed);
        write_id[*offset..*offset+8].copy_from_slice(&next_value.to_be_bytes());
      }
      WriteDefault::Body(offset, FieldDefault::Counter(counter_idx)) => {
        let next_value = db.counters[*counter_idx].fetch_add(1, Ordering::Relaxed);
        let write_body = temp_data.get_or_insert_with(|| { insert.data.clone() });
        write_body[*offset..*offset+8].copy_from_slice(&next_value.to_be_bytes());
      }
      WriteDefault::ParentId(offset) => {
        let Some(parent_id) = parent_id else {
          return Err(InsertError::ParentIdRequired)
        };
        write_id.splice(offset..offset, parent_id.iter().copied());
      },
      _ => {}
    }
  }

  let data = temp_data.as_deref().unwrap_or(&insert.data);

  { 
    let mut tree = tx.get_tree(entity.name.as_bytes()).unwrap().unwrap();
    tree.insert(&write_id, data).unwrap();
  }
  
  for write_index in insert.write_indexes.iter() {
    match write_index {
      WriteIndex::Value(field_index, data) => {
        let mut tree = tx.get_tree(field_index.tree_name()).unwrap().unwrap();
        let val = [ data.as_slice(), write_id.as_slice() ].concat();
        tree.insert(&val, &[]).unwrap();
      }
    }
  }
  
  for st in insert.refs.iter() {
    match st {
      WriteRelation::Create { op, st, .. } => {
        write_data(tx, st, op, db, Some(&write_id))?;
      }
      WriteRelation::CreateMany { ops, st, .. } => {
        for op in ops {
          write_data(tx, st, op, db, Some(&write_id))?;
        }
      }
      WriteRelation::Connect { field, ids, .. } => {
        match &field.ty {
          FieldType::Ref(ref_info) => {
            write_index(tx, ref_info, &db.schema, &write_id, &ids);
          },
          FieldType::RefList(ref_info) => {
            write_index(tx, ref_info, &db.schema, &write_id, &ids);
          },
          _ => panic!("Trying to connect to non-Ref field")
        }
      }
      _ => {}
    }
  }

  Ok(write_id)
}

#[inline(always)]
fn insert_index(tree: &mut Tree, left: &[u8], right: &[u8]) {
  let mut key = Vec::with_capacity(left.len() + right.len());
  key.extend_from_slice(left);
  key.extend_from_slice(right);
  tree.insert(&key, &[1]).unwrap();
}

fn write_index(tx: &WriteTransaction, ref_info: &RefInfo, schema: &Schema, id: &[u8], ids: &Vec<Vec<u8>>) {
  
  if let RefBinding::IndexTree(tree_name) = &ref_info.binding {
    let mut tree = tx.get_tree(tree_name.as_bytes()).unwrap().unwrap();
    for item_id in ids {
      insert_index(&mut tree, id, item_id);
    }
  }

  if let Some(ref_field_idx) = ref_info.rev_field_idx {
    match &schema.models[ref_info.model_index].fields[ref_field_idx].ty {
      FieldType::Ref(ref_info) => {
        write_index_opposite(tx, ref_info, id, ids);
      },
      FieldType::RefList(ref_info) => {
        write_index_opposite(tx, ref_info, id, ids);
      },
      _ => panic!("Trying to connect to non-Ref field")
    }
  }
}

fn write_index_opposite(tx: &WriteTransaction, ref_info: &RefInfo, id: &[u8], ids: &Vec<Vec<u8>>) {
  if let RefBinding::IndexTree(tree_name) = &ref_info.binding {
    let mut tree = tx.get_tree(tree_name.as_bytes()).unwrap().unwrap();
    for item_id in ids {
      insert_index(&mut tree, item_id, id);
    }
  }
}