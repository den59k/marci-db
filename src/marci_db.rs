use std::{collections::HashMap, sync::{Arc, atomic::AtomicU64}};

use canopydb::{Database, Transaction, Tree};

use crate::{delete_op::{DeleteError, process_delete, prepare_delete}, query_op::{DecodeCtx, QueryOp, TransationContext, process_query_many, process_query_one}, schema::{Entity, FieldDefault, FieldType, RefBinding, Schema, parse_schema}, update_op::{UpdateError, UpdateOp, process_update}, write_op::{InsertError, WriteOp, process_write}};

pub struct MarciDB {
  pub schema: Schema,
  db: Database,
  pub(crate) counters: Vec<Arc<AtomicU64>>,
  model_by_name: HashMap<String, usize>
}

impl MarciDB {

  pub fn new(schema_str: &str, path: &str) -> MarciDB {
    let schema = parse_schema(schema_str);

    let db = canopydb::Database::new(path).unwrap();
    let model_by_name = schema.build_model_name_map();

    let tx = db.begin_write().unwrap();

    for model in schema.models.iter() {
      tx.get_or_create_tree(model.name.as_bytes()).unwrap();

      for field in model.fields.iter() {
        if let FieldType::Ref(ref_info) | FieldType::RefList(ref_info) = &field.ty {
          if let RefBinding::IndexTree(tree_name) = &ref_info.binding {
            tx.get_or_create_tree(tree_name.as_bytes()).unwrap();
          }
        }

        for index in field.indexes.iter() {
          tx.get_or_create_tree(index.tree_name()).unwrap();
        }
      }
    }

    let counters = build_counters(&schema, &tx);
    tx.commit().unwrap();

    MarciDB {
      db,
      schema,
      counters,
      model_by_name
    }
  }

  pub fn get_model(&self, name: &str) -> Option<&Entity> {
    self.model_by_name.get(name).and_then(|idx| { Some(&self.schema.models[*idx]) })
  }
  
  pub fn find_many<U, F>(&self, query: &QueryOp, f: F) -> Vec<U> where F: Fn(DecodeCtx<U>) -> U { 
    let rx = self.db.begin_read().unwrap();
    let mut ctx = TransationContext::new(&rx, &self.schema, f);
    return process_query_many(query, &mut ctx,None);
  }

  pub fn find_unique<U, F>(&self, query: &QueryOp, f: F) -> Option<U> where F: Fn(DecodeCtx<U>) -> U { 
    let rx = self.db.begin_read().unwrap();
    let mut ctx = TransationContext::new(&rx, &self.schema, f);
    return process_query_one(query, &mut ctx,None);
  }

  pub fn count(&self, entity: &Entity) -> u64 {
    let rx = self.db.begin_read().unwrap();
    let tree = rx.get_tree(entity.name.as_bytes()).unwrap().unwrap();
    return tree.len();
  }

  pub fn count_dev(&self, tree_name: &str) -> u64 { 
    let rx = self.db.begin_read().unwrap();
    let tree = rx.get_tree(tree_name.as_bytes()).unwrap().unwrap();
    return tree.len();
  }

  pub fn insert_item(&self, entity: &Entity, insert: &WriteOp) -> Result<Vec<u8>, InsertError> {
    let tx = self.db.begin_write().unwrap();
    let item_id = process_write(&tx, entity, insert,  &self, None)?;
    tx.commit().unwrap();
    Ok(item_id)
  }

  pub fn update_item(&self, entity: &Entity, id: &[u8], update_op: &UpdateOp) -> Result<(), UpdateError> {
    let tx = self.db.begin_write().unwrap();
    process_update(&tx, entity, id, update_op, &self)?;
    tx.commit().unwrap();
    Ok(())
  }

  pub fn delete_item(&self, entity: &Entity, id: &[u8]) -> Result<bool, DeleteError> {
    let action = prepare_delete(&self.schema, entity, Some(id), None);
    let tx = self.db.begin_write().unwrap();
    let is_delete = process_delete(&tx, &id, entity, &action, &self.schema, None)?;
    tx.commit().unwrap();
    Ok(is_delete)
  }
}

fn build_counters(schema: &Schema, rx: &Transaction) -> Vec<Arc<AtomicU64>> {
  let mut counters = Vec::with_capacity(schema.models.len());
  for model in schema.models.iter() {
    let model_tree = rx.get_tree(model.name.as_bytes()).unwrap().unwrap();

    for field in model.fields.iter() {
      if let Some(FieldDefault::Counter(counter_idx)) = field.default_value {
        counters.resize(counter_idx+1, Arc::new(AtomicU64::new(0)));
        
        let max_id = get_max_id(&model_tree);
        counters[counter_idx] = Arc::new(AtomicU64::new(max_id));
      }
    }
  }
  return counters;
}

pub fn get_max_id(tree: &Tree) -> u64 {
  return tree.last().unwrap()
    .map(|(key, _)| u64::from_be_bytes(key.as_ref().try_into().unwrap()) + 1)
    .unwrap_or(1);
}
