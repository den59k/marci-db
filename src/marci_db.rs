use std::{collections::HashMap, sync::{Arc, atomic::{AtomicU64, Ordering}}};

use bitvec::vec::BitVec;
use canopydb::{Bytes, Database, Environment};

use crate::schema::{Attribute, FieldType, Model, Schema};

pub struct MarciDB {
  pub db: Database,
  pub schema: Schema,
  counters_map: HashMap<String, Arc<AtomicU64>>
}

const HEADER_OFFSET: usize = 3;

impl MarciDB {

  pub fn new(schema: Schema) -> MarciDB {
    let env = Environment::new("./data").unwrap(); 
    let db = env.get_or_create_database("mydb.db").unwrap();

    let mut counters_map = HashMap::new();

    let tx = db.begin_write().unwrap();
    for model in schema.models.iter() {
      let tree = tx.get_or_create_tree(model.name.as_bytes()).unwrap();

      let index = tree.last().unwrap()
          .map(|(key, _)| u64::from_be_bytes(key.as_ref().try_into().unwrap()) + 1)
          .unwrap_or(1);

      counters_map.insert(model.name.clone(), Arc::new(AtomicU64::new(index)));

      for field in model.fields.iter() {

        let is_index = field.attributes.iter().any(|i| matches!(i, Attribute::Index));
        let is_ref = matches!(field.ty, FieldType::ModelRef(_));

        if is_index || is_ref {
          let index_name = format!("{}.{}", model.name, field.name);
          tx.get_or_create_tree(index_name.as_bytes()).unwrap();
        }
      }
    }
    tx.commit().unwrap();

    MarciDB {
      db,
      schema,
      counters_map
    }
  }

  pub fn next_id(&self, collection: &str) -> u64 {
    self.counters_map[collection].fetch_add(1, Ordering::Relaxed)
  }
  
  pub fn get_model(&self, name: &str) -> Option<&Model> {
    return self.schema.models.iter().find(|i| i.name == name);
  }

  pub fn insert_data(&self, collection: &str, data: &[u8]) -> u64 {
    let id = self.next_id(collection);

    let tx = self.db.begin_write().unwrap();
    
    {
      let mut tree = tx.get_tree(collection.as_bytes()).unwrap().unwrap();
      tree.insert(&id.to_be_bytes(), data).unwrap();
    }
    
    tx.commit().unwrap();

    return id
  }

  pub fn get_all<U, F: Fn(u64, &[u8]) -> U>(&self, collection: &str, f: F) -> Vec<U> {
    
    let rx = self.db.begin_read().unwrap();

    let tree = rx.get_tree(collection.as_bytes()).unwrap().unwrap();

    tree.iter().unwrap().map(|item| {
      let (key, value) = item.unwrap();
      let id = u64::from_be_bytes(key.as_ref().try_into().unwrap());

      f(id, value.as_ref())
    }).collect()
  }

  pub fn get_item<U, F: FnOnce(&[u8]) -> U>(&self, collection: &str, key: &str, f: F) -> Option<U> {

    let rx = self.db.begin_read().unwrap();
    let tree = rx.get_tree(collection.as_bytes()).unwrap().unwrap();

    return tree.get(key.as_bytes()).unwrap().map(|item| f(item.as_ref()))
  }

  pub fn update(&self, collection: &str, id: u64, new_data: &[u8], changed_mask: BitVec) -> Option<u64> {
    let tx = self.db.begin_write().unwrap();

    {
      let mut tree = tx.get_tree(collection.as_bytes()).unwrap().unwrap();

      let Some(data) = tree.get(&id.to_be_bytes()).unwrap() else {
        return None
      };

      let mut data = data.to_vec();

      let model = self.get_model(collection).unwrap();

      for j in (HEADER_OFFSET..model.payload_offset).step_by(4) {
        let update_offset = u32::from_be_bytes(new_data[j..j+4].try_into().unwrap()) as usize;
        // Skip if hasn't new data
        if !*changed_mask.get((j-HEADER_OFFSET) / 4).unwrap() {
          continue;
        }

        let offset = u32::from_be_bytes(data[j..j+4].try_into().unwrap()) as usize;

        if offset == 0 && update_offset == 0 {
          continue;
        }

        let end = if offset == 0 { 0 } else { get_end(&data, j, model.payload_offset) };
        let update_end = if update_offset == 0 { 0 } else { get_end(new_data, j, model.payload_offset) };

        // println!("update_offset: {} update_end: {} offset: {} end: {}", update_offset, update_end, offset, end);

        let diff = ((update_end-update_offset) as isize) - ((end-offset) as isize);

        if diff == 0 {
          if update_offset == 0 {
            // Set null value (just update offset value)
            data[j..j+4].fill(0u8);
          } else {
            // Copy value
            data[offset..end].copy_from_slice(&new_data[update_offset..update_end]);
          }
          continue;
        }

        let new_offset = if offset == 0 { end } else { offset };
        let new_end = (end as isize + diff) as usize;

        if diff > 0 {
          let len = data.len();
          data.resize(((data.len() as isize) + diff) as usize, 0u8);
          data.copy_within(end..len, new_end);
        } else {
          data.copy_within(end.., new_end);
          data.resize(((data.len() as isize) + diff) as usize, 0u8);
        }

        if update_end > update_offset {
          data[new_offset..new_end].copy_from_slice(&new_data[update_offset..update_end]);
        }
        if update_offset == 0 {
          data[j..j+4].fill(0u8);
        } else if offset == 0 { 
          data[j..j+4].copy_from_slice(&(new_offset as u32).to_be_bytes());
        }

        // update offsets
        for j2 in (j+4..model.payload_offset as usize).step_by(4) {
          let offset = u32::from_be_bytes(data[j2..j2+4].try_into().unwrap());
          if offset != 0 {
            let new_offset = (offset as isize + diff) as u32;
            data[j2..j2+4].copy_from_slice(&new_offset.to_be_bytes());
          }
        }
      }
      
      tree.insert(&id.to_be_bytes(), &data).unwrap();
    }

    tx.commit().unwrap();

    return Some(id);
  }

  pub fn delete(&self, collection: &str, id: u64) -> bool {
    let tx = self.db.begin_write().unwrap();
    {
      let mut tree = tx.get_tree(collection.as_bytes()).unwrap().unwrap();
      if !tree.delete(&id.to_be_bytes()).unwrap() {
        return false;
      }
    }
    tx.commit().unwrap();
    return true;
  }

}

pub fn get_end(data: &[u8], j: usize, payload_offset: usize) -> usize {
  for j in ((j+4)..payload_offset).step_by(4) {
    let off_j = u32::from_be_bytes(data[j..j+4].try_into().unwrap()) as usize;
    if off_j != 0 {
      return off_j;
    }
  }

  return data.len();
}
