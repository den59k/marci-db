use std::sync::atomic::Ordering;

use canopydb::WriteTransaction;

use crate::{MarciDB, schema::FieldDefault, write_op::{WriteDefault, WriteOp, WriteRelation}};

#[derive(Debug)]
pub enum InsertError {
  ForeignKeyViolation(String, u64),
  ItemNotFound,
  UniqueViolation(String, Vec<u8>),
  DuplicateKey(Vec<u8>),
  CannotChangePrimaryKey(String),
  ParentIdRequired
}

pub fn write_data(insert: &WriteOp, tx: &WriteTransaction, db: &MarciDB, parent_id: Option<&[u8]>) -> Result<Vec<u8>, InsertError> {

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
      }
    }
  }

  let data = temp_data.as_deref().unwrap_or(&insert.data);

  { 
    let mut tree = tx.get_tree(insert.entity.name.as_bytes()).unwrap().unwrap();
    tree.insert(&write_id, data).unwrap();
  }
  
  for st in insert.refs.iter() {
    match st {
      WriteRelation::Create { op, .. } => {
        write_data(op, tx, db, Some(&write_id))?;
      }
      WriteRelation::CreateMany { ops, .. } => {
        for op in ops {
          write_data(op, tx, db, Some(&write_id))?;
        }
      }
      _ => {}
    }
  }

  Ok(write_id)
}