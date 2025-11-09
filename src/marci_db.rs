use std::{collections::HashMap, sync::{Arc, atomic::{AtomicU64, Ordering}}};

use bitvec::vec::BitVec;
use canopydb::{Bytes, Database, Environment, ReadTransaction, Tree};

use crate::{schema::{Attribute, Field, FieldType, Model, Schema}};

pub struct MarciDB {
  pub db: Database,
  pub schema: Schema,
  counters_map: HashMap<String, Arc<AtomicU64>>
}

pub struct MarciSelectInclude {
  pub offset: usize,
  pub field_index: usize,
  pub model_index: usize,
  pub select: Box<MarciSelect>
}

pub struct MarciSelect {
  pub select: BitVec,
  pub includes: Vec<MarciSelectInclude>
}

const HEADER_OFFSET: usize = 3;

#[derive(Debug)]
pub enum InsertError {
  ForeignKeyViolation(String),
  ItemNotFound(u64)
}

pub enum IncludeResult<U> {
  One(usize,U),
  Many(usize,Vec<U>)
}

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

  pub fn next_id(&self, model: &Model) -> u64 {
    self.counters_map[&model.name].fetch_add(1, Ordering::Relaxed)
  }
  
  pub fn get_model(&self, name: &str) -> Option<&Model> {
    return self.schema.models.iter().find(|i| i.name == name);
  }

  pub fn insert_data(&self, model: &Model, data: &[u8]) -> Result<u64, InsertError> {

    let foreign_keys = get_foreign_keys(data, model);

    let id = self.next_id(model);
    let tx = self.db.begin_write().unwrap();
    
    {
      for (model_index, field_index, item_id) in foreign_keys {
        let tree = tx.get_tree(self.schema.models[model_index].name.as_bytes()).unwrap().unwrap();
        if tree.get(&item_id.to_be_bytes()).unwrap().is_none() {
          return Err(InsertError::ForeignKeyViolation(model.fields[field_index].name.clone()))
        }
      }

      let mut tree = tx.get_tree(model.name.as_bytes()).unwrap().unwrap();
      tree.insert(&id.to_be_bytes(), data).unwrap();
    }
    
    tx.commit().unwrap();

    return Ok(id)
  }

  fn process_data<U, F>(
      &self,
      id: u64,
      data: &[u8],
      rx: &ReadTransaction,
      select: &MarciSelect,
      model: &Model,
      f: &F,
  ) -> U
  where
      F: Fn(u64, &[u8], &Model, &BitVec, Option<Vec<IncludeResult<U>>>) -> U,
  {
      if select.includes.is_empty() {
          f(id, data, model, &select.select, None)
      } else {
          let mut includes_arr = Vec::with_capacity(select.includes.len());
          for include in select.includes.iter() {
              let Some(item_id) = get_value(data, include.offset, 8) else {
                  continue;
              };
              let model = &self.schema.models[include.model_index];
              let tree = rx
                  .get_tree(model.name.as_bytes())
                  .unwrap()
                  .unwrap();
              let data = tree.get(item_id).unwrap().unwrap();

              let item_id_val = u64::from_be_bytes(item_id.try_into().unwrap());
              let item = self.process_data(item_id_val, data.as_ref(), rx, &include.select, model, f);
              includes_arr.push(IncludeResult::One(include.field_index, item));
          }

          f(id, data, model, &select.select, Some(includes_arr))
      }
  }

  pub fn get_all<U, F>(
      &self,
      model: &Model,
      select: &MarciSelect,
      f: F
  ) -> Vec<U>
  where
      F: Fn(u64, &[u8], &Model, &BitVec, Option<Vec<IncludeResult<U>>>) -> U,
  {
      let rx = self.db.begin_read().unwrap();
      let tree = rx.get_tree(model.name.as_bytes()).unwrap().unwrap();

      tree.iter().unwrap().map(|item| {
          let (key, value) = item.unwrap();
          let id = u64::from_be_bytes(key.as_ref().try_into().unwrap());
          let data = value.as_ref();
          self.process_data(id, data, &rx, select, model, &f)
      }).collect()
  }


  pub fn get_item<U, F: FnOnce(&[u8]) -> U>(&self, model: &Model, key: &str, f: F) -> Option<U> {

    let rx = self.db.begin_read().unwrap();
    let tree = rx.get_tree(model.name.as_bytes()).unwrap().unwrap();

    return tree.get(key.as_bytes()).unwrap().map(|item| f(item.as_ref()))
  }

  pub fn update(&self, model: &Model, id: u64, new_data: &[u8], changed_mask: BitVec) -> Result<u64, InsertError> {
    
    let foreign_keys = get_foreign_keys(new_data, model);

    let tx = self.db.begin_write().unwrap();

    {
      let mut tree = tx.get_tree(model.name.as_bytes()).unwrap().unwrap();

      let Some(data) = tree.get(&id.to_be_bytes()).unwrap() else {
        return Err(InsertError::ItemNotFound(id))
      };

      for (model_index, field_index, item_id) in foreign_keys {
        let tree = tx.get_tree(self.schema.models[model_index].name.as_bytes()).unwrap().unwrap();
        if tree.get(&item_id.to_be_bytes()).unwrap().is_none() {
          return Err(InsertError::ForeignKeyViolation(model.fields[field_index].name.clone()))
        }
      }

      let mut data = data.to_vec();

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

    return Ok(id);
  }

  pub fn delete(&self, model: &Model, id: u64) -> bool {
    let tx = self.db.begin_write().unwrap();
    {
      let mut tree = tx.get_tree(model.name.as_bytes()).unwrap().unwrap();
      if !tree.delete(&id.to_be_bytes()).unwrap() {
        return false;
      }
    }
    tx.commit().unwrap();
    return true;
  }

}

#[inline(always)]
pub fn get_end(data: &[u8], j: usize, payload_offset: usize) -> usize {
  for j in ((j+4)..payload_offset).step_by(4) {
    let off_j = u32::from_be_bytes(data[j..j+4].try_into().unwrap()) as usize;
    if off_j != 0 {
      return off_j;
    }
  }

  return data.len();
}

#[inline(always)]
fn get_value<'a>(data: &'a[u8], offset_pos: usize, size: usize) -> Option<&'a[u8]> {
  let offset: usize = u32::from_be_bytes(data[offset_pos..offset_pos+4].try_into().unwrap()) as usize;
  if offset == 0 {
    return None;
  }
  return Some(&data[offset..offset+size]);
}


#[inline(always)]
fn get_foreign_keys<'a>(data: &'a[u8], model: &Model) -> Vec<(usize, usize, u64)> {
  let mut foreign_keys = vec![];
  for (index, field) in model.fields.iter().enumerate() {
    if let FieldType::ModelRef(model_index) = field.ty {
      if let Some(bytes) = get_value(data, field.offset_pos, 8) {
        let item_id = u64::from_be_bytes(bytes.try_into().unwrap());
        foreign_keys.push((model_index, index, item_id));
      }
    }
  }
  return foreign_keys;
}