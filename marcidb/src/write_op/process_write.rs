use std::{sync::atomic::Ordering};

use canopydb::{Tree, WriteTransaction};

use crate::{Field, MarciDB, StorageError, error::RequireTree, schema::{Entity, FieldDefault, FieldType, RefBinding, RefInfo, Schema}, utils::move_offsets, write_op::{WriteDefault, WriteDefaultInsert, WriteIndex, WriteOp, WriteRelation}};

#[derive(Debug,PartialEq)]
pub enum InsertError {
  ForeignKeyViolation(String, u64),
  ItemNotFound,
  IdViolation(String, Vec<u8>),
  UniqueViolation(String, Vec<u8>),
  DuplicateKey(Vec<u8>),
  CannotChangePrimaryKey(String),
  ParentIdRequired,
  Storage(StorageError)
}

impl From<canopydb::Error> for InsertError {
  fn from(e: canopydb::Error) -> Self {
    InsertError::Storage(StorageError::Backend(e))
  }
}

impl From<StorageError> for InsertError {
  fn from(e: StorageError) -> Self {
    InsertError::Storage(e)
  }
}

pub fn process_write(tx: &WriteTransaction, entity: &Entity, insert: &WriteOp, db: &MarciDB, parent_id: Option<&[u8]>) -> Result<Vec<u8>, InsertError> {

  let mut write_id = insert.id.clone();
  let mut temp_data: Option<Vec<u8>> = None;
  
  // First fill in the counter values: their offsets are recorded relative to the original buffer.
  // The parent_id insertion (splice) is the second phase and shifts the already-filled bytes
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
      _ => {}
    }
  }

  for field in insert.defaults.iter() {
    match field {
      WriteDefault::KeyInsert(offset, WriteDefaultInsert::ParentId) => {
        let Some(parent_id) = parent_id else {
          return Err(InsertError::ParentIdRequired)
        };
        write_id.splice(offset..offset, parent_id.iter().copied());
      },
      WriteDefault::BodyInsert(offset_pos, offset, WriteDefaultInsert::ParentId) => {
        let Some(parent_id) = parent_id else {
          return Err(InsertError::ParentIdRequired)
        };
        let write_body = temp_data.get_or_insert_with(|| { insert.data.clone() });
        write_body.splice(offset..offset, parent_id.iter().copied());
        move_offsets(write_body, offset_pos+4, entity.payload_offset, parent_id.len() as u32);
      },
      _ => {}
    }
  }

  let data = temp_data.as_deref().unwrap_or(&insert.data);

  {
    let mut tree = tx.require_tree(entity.name.as_bytes())?;
    if tree.get(&write_id)?.is_some() {
      return Err(InsertError::IdViolation(entity.name.clone(), write_id.clone()));
    }
    tree.insert(&write_id, data)?;
  }

  for write_index in insert.write_indexes.iter() {
    match write_index {
      WriteIndex::Value(field, field_index, data) => {
        let mut tree = tx.require_tree(field_index.tree_name())?;
        if field_index.is_unique() {
          if let Some(exists) = tree.prefix_keys(&data.as_slice())?.next() {
            let exists_id = exists?[data.len()..].to_vec();
            return Err(InsertError::UniqueViolation(field.full_name.clone(), exists_id))
          }
        }
        let val = [ data.as_slice(), write_id.as_slice() ].concat();
        tree.insert(&val, &[])?;
      }
    }
  }
  
  for st in insert.refs.iter() {
    match st {
      WriteRelation::Create { op, st, .. } => {
        process_write(tx, st, op, db, Some(&write_id))?;
      }
      WriteRelation::CreateMany { ops, st, .. } => {
        for op in ops {
          process_write(tx, st, op, db, Some(&write_id))?;
        }
      }
      WriteRelation::Connect { ref_info, ids, field, .. } => {
        write_ref_indexes(tx, ref_info, &db.schema, &write_id, &ids).map_err(|e| e.to_insert_error(field))?;
      }
    }
  }

  Ok(write_id)
}



#[inline(always)]
fn insert_index(tree: &mut Tree, left: &[u8], right: &[u8]) -> Result<(), canopydb::Error> {
  tree.insert(&[ left, right ].concat(), &[])?;
  Ok(())
}

pub fn write_ref_indexes(tx: &WriteTransaction, ref_info: &RefInfo, schema: &Schema, id: &[u8], ids: &Vec<Vec<u8>>) -> Result<(), WriteIndexesError> {
  if let RefBinding::IndexTree(tree_name) = &ref_info.binding {
    let mut tree = tx.require_tree(tree_name.as_bytes())?;
    for item_id in ids {
      insert_index(&mut tree, id, item_id)?;
    }
  }
  
  let is_unique = ref_info.is_unique;
  if let Some(ref_field_idx) = ref_info.rev_field_idx {
    match &schema.models[ref_info.model_index].fields[ref_field_idx].ty {
      FieldType::Ref(ref_info) => {
        write_ref_index_opposite(tx, ref_info, id, ids, is_unique)?;
      },
      FieldType::RefList(ref_info) => {
        write_ref_index_opposite(tx, ref_info, id, ids, is_unique)?;
      },
      _ => panic!("Trying to connect to non-Ref field")
    }
  }

  Ok(())
}

fn write_ref_index_opposite(tx: &WriteTransaction, ref_info: &RefInfo, id: &[u8], ids: &Vec<Vec<u8>>, check_unique: bool) -> Result<(), WriteIndexesError> {
  if let RefBinding::IndexTree(tree_name) = &ref_info.binding {
    let mut tree = tx.require_tree(tree_name.as_bytes())?;
    for item_id in ids {
      if check_unique {
        if let Some(exists) = tree.prefix_keys(&item_id)?.next() {
          let exists_id = exists?[item_id.len()..].to_vec();
          return Err(WriteIndexesError::UniqueViolation(exists_id))
        }
      }
      insert_index(&mut tree, item_id, id)?;
    }
  }

  Ok(())
}

#[derive(Debug,PartialEq)]
pub enum WriteIndexesError {
  UniqueViolation(Vec<u8>),
  Storage(StorageError)
}

impl From<canopydb::Error> for WriteIndexesError {
  fn from(e: canopydb::Error) -> Self {
    WriteIndexesError::Storage(StorageError::Backend(e))
  }
}

impl From<StorageError> for WriteIndexesError {
  fn from(e: StorageError) -> Self {
    WriteIndexesError::Storage(e)
  }
}

impl WriteIndexesError {
  fn to_insert_error(self, field: &Field) -> InsertError {
    match self {
        WriteIndexesError::UniqueViolation(exists_id) => InsertError::UniqueViolation(field.full_name.clone(), exists_id),
        WriteIndexesError::Storage(e) => InsertError::Storage(e),
    }
  }
}