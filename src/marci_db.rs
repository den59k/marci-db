use std::{collections::HashMap, sync::{Arc, atomic::{AtomicU64, Ordering}}};

use bitvec::vec::BitVec;
use canopydb::{Database, Environment, ReadTransaction};

use crate::schema::{Field, FieldType, Model, Schema};

pub struct MarciDB {
  pub db: Database,
  pub schema: Schema,
  counters_map: HashMap<String, Arc<AtomicU64>>
}

pub struct MarciSelectInclude<'a> {
  pub offset: usize,
  pub field_index: usize,
  pub is_array: bool,
  pub model: &'a Model,
  pub select: Box<MarciSelect<'a>>
}

pub struct MarciSelectVirtual<'a> {
  pub field_index: usize,
  pub index_name: &'a[u8],
  pub model: &'a Model,
  pub select: Box<MarciSelect<'a>>
}

pub struct MarciSelect<'a> {
  pub select: BitVec,
  pub includes: Vec<MarciSelectInclude<'a>>,
  pub virtual_fields: Vec<MarciSelectVirtual<'a>>
}

pub struct DecodeCtx<'a, U> {
  pub id: u64,
  pub data: &'a [u8],
  pub model: &'a Model,
  pub select: &'a BitVec,
  pub includes: Option<Vec<IncludeResult<U>>>,
}

#[derive(Debug)]
pub enum InsertError {
  ForeignKeyViolation(String, u64),
  ItemNotFound(u64)
}

pub enum IncludeResult<U> {
  None(usize),
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
        if let Some(index_name) = &field.index_name {
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
    
    let indexes = get_indexes(data, id, model);

    let tx = self.db.begin_write().unwrap();

    {
      for (model_index, field_index, item_id) in foreign_keys {
        let tree = tx.get_tree(self.schema.models[model_index].name.as_bytes()).unwrap().unwrap();
        if tree.get(item_id).unwrap().is_none() {
          return Err(InsertError::ForeignKeyViolation(model.fields[field_index].name.clone(), u64::from_be_bytes(*item_id)))
        }
      }

      for (tree_name, index_key) in indexes {
        let mut index_tree = tx.get_tree(tree_name).unwrap().unwrap();
        index_tree.insert(&index_key, &[1]).unwrap();
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
      F: Fn(DecodeCtx<'_, U>) -> U,
  {
      if select.includes.is_empty() && select.virtual_fields.is_empty() {
        f(DecodeCtx { id, data, model, select: &select.select, includes: None })
      } else {
          let mut includes_arr = Vec::with_capacity(select.includes.len() + select.virtual_fields.len());
          for include in select.includes.iter() {
              if include.is_array {
                let Some(arr) = get_array::<8>(data, include.offset) else {
                  includes_arr.push(IncludeResult::Many(include.field_index, vec![]));
                  continue;
                };
                if arr.is_empty() {
                  includes_arr.push(IncludeResult::Many(include.field_index, vec![]));
                  continue;
                }
                let len = arr.len();
                
                let nested_tree = rx
                    .get_tree(include.model.name.as_bytes())
                    .unwrap()
                    .unwrap();
                
                let mut values = Vec::with_capacity(len);
                for item_id in arr {
                  let data = nested_tree.get(item_id).unwrap().unwrap();

                  let item_id_val = u64::from_be_bytes(*item_id);
                  let item = self.process_data(item_id_val, data.as_ref(), rx, &include.select, include.model, f);
                  values.push(item);
                }
                includes_arr.push(IncludeResult::Many(include.field_index, values));
              } else {
                let Some(item_id) = get_value::<8>(data, include.offset) else {
                  includes_arr.push(IncludeResult::None(include.field_index));
                  continue;
                };
                let nested_tree = rx
                    .get_tree(include.model.name.as_bytes())
                    .unwrap()
                    .unwrap();
                let data = nested_tree.get(item_id).unwrap().unwrap();
  
                let item_id_val = u64::from_be_bytes(*item_id);
                let item = self.process_data(item_id_val, data.as_ref(), rx, &include.select, include.model, f);
                includes_arr.push(IncludeResult::One(include.field_index, item));
              }
          }

          for item in select.virtual_fields.iter() {
            let tree = rx.get_tree(item.index_name).unwrap().unwrap();
            let nested_tree = rx.get_tree(item.model.name.as_bytes()).unwrap().unwrap();
            let iter = tree.prefix_keys(&id.to_be_bytes()).unwrap();
            
            let mut values = Vec::with_capacity(iter.size_hint().0);

            // TODO: correct encode with lexical order
            for key in iter {
              let key = key.unwrap();
              let item_id = &key.as_ref()[8..16];

              let Some(data) = nested_tree.get(item_id).unwrap() else {
                println!("Missing value in virtual field {} {}", item.model.fields[item.field_index].name, u64::from_be_bytes(item_id.try_into().unwrap()));
                continue;
              };
              
              let item_id_val = u64::from_be_bytes(item_id.try_into().unwrap());
              let item = self.process_data(item_id_val, data.as_ref(), rx, &item.select, item.model, f);
              values.push(item);
            }

            includes_arr.push(IncludeResult::Many(item.field_index, values));
          }

          f(DecodeCtx { id, data, model, select: &select.select, includes: Some(includes_arr) })
      }
  }

  pub fn get_all<U, F>(
      &self,
      model: &Model,
      select: &MarciSelect,
      f: F
  ) -> Vec<U>
  where
      F: Fn(DecodeCtx<'_, U>) -> U,
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
        if tree.get(item_id).unwrap().is_none() {
          return Err(InsertError::ForeignKeyViolation(model.fields[field_index].name.clone(), u64::from_be_bytes(*item_id)))
        }
      }

      let mut data = data.to_vec();

      for (field_index, field) in model.fields.iter().enumerate() {
        let j = field.offset_pos;
        let update_offset = u32::from_be_bytes(new_data[j..j+4].try_into().unwrap()) as usize;
        // Skip if hasn't new data
        if !*changed_mask.get(field_index).unwrap() {
          continue;
        }

        let offset = u32::from_be_bytes(data[j..j+4].try_into().unwrap()) as usize;

        if offset == 0 && update_offset == 0 {
          continue;
        }

        let end = if offset == 0 { 0 } else { get_end(&data, j, model.payload_offset) };
        let update_end = if update_offset == 0 { 0 } else { get_end(new_data, j, model.payload_offset) };
        let update_len = update_end - update_offset;
        
        if let Some((index_name, index_value)) = get_index(&data, field, model, id) {
          let mut tree = tx.get_tree(index_name).unwrap().unwrap();
          tree.delete(&index_value).unwrap();
        }
        if let Some((index_name, index_value)) = get_index(&new_data, field, model, id) {
          let mut index_tree: canopydb::Tree<'_> = tx.get_tree(index_name).unwrap().unwrap();
          index_tree.insert(&index_value, &[1]).unwrap();
        }

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
        
        let end = get_end(&data, j, model.payload_offset);
        let new_offset = if offset == 0 { end } else { offset };
        let new_end = (new_offset + update_len) as usize;

        if diff > 0 {
          let len = data.len();
          data.resize(data.len() + diff as usize, 0u8);
          data.copy_within(end..len, new_end);
        } else {
          data.copy_within(end.., new_end);
          data.resize(((data.len() as isize) + diff) as usize, 0u8);
        }

        // Write if has data (maybe null, maybe empty value)
        if update_offset != update_end {
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
    let off_j = get_offset(data, j);
    if off_j != 0 {
      return off_j;
    }
  }

  return data.len();
}

#[inline(always)]
fn get_value<'a, const SIZE: usize>(
    data: &'a [u8],
    offset_pos: usize,
) -> Option<&'a [u8; SIZE]> {
    let offset = get_offset(data, offset_pos);
    if offset == 0 {
        return None;
    }
    Some(data[offset..offset + SIZE].try_into().ok()?)
}

#[inline(always)]
fn get_offset<'a>(data: &'a [u8], offset_pos: usize) -> usize {
  return u32::from_be_bytes(data[offset_pos..offset_pos + 4].try_into().unwrap()) as usize;
}

struct ManyIter<'a, const SIZE: usize> {
    data: &'a [u8],
    pos: usize,
    end: usize,
}

impl<'a, const SIZE: usize> ManyIter<'a, SIZE> {
  pub fn is_empty(&self) -> bool { return self.pos == self.end }
}

impl<'a, const SIZE: usize> ExactSizeIterator for ManyIter<'a, SIZE> {
    fn len(&self) -> usize {
        (self.end - self.pos) / SIZE
    }
}

impl<'a, const SIZE: usize> Iterator for ManyIter<'a, SIZE> {
    type Item = &'a [u8; SIZE];

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.end {
            return None;
        }

        let item = self.data[self.pos..self.pos + SIZE].try_into().ok()?;
        self.pos += SIZE;
        Some(item)
    }
}

fn get_array<'a, const SIZE: usize>(data: &'a[u8], offset_pos: usize) -> Option<ManyIter<'a, SIZE>> {

  let offset = get_offset(data, offset_pos);
  if offset == 0 {
    return None;
  }
  
  // читаем длину (константно 4 байта)
  let len_bytes: &[u8; 4] = data[offset..offset+4].try_into().unwrap();
  let len = u32::from_be_bytes(*len_bytes) as usize;

  let start = offset + 4;
  let end = start + len * SIZE;

  Some(ManyIter { data, pos: start, end })
}

#[inline(always)]
fn get_value_with_len<'a>(
    data: &'a[u8],
    offset_pos: usize,
    model: &Model
) -> Option<&'a[u8]> {
  let offset = get_offset(data, offset_pos);
  if offset == 0 {
    return None;
  }

  let mut offset_end = data.len();
  for j in offset_pos+4..model.payload_offset {
    let offset = get_offset(data, j);
    if offset != 0 { 
      offset_end = offset;
      break;
    }
  }

  return Some(&data[offset..offset_end])
}

#[inline(always)]
fn get_foreign_keys<'a>(data: &'a[u8], model: &Model) -> Vec<(usize, usize, &'a[u8;8])> {
  let mut foreign_keys = Vec::with_capacity(model.fields.len());

  for (index, field) in model.fields.iter().enumerate() {
    if field.derived_from.is_some() { continue; }
    match field.ty {
        FieldType::ModelRef(model_index) => {
          if let Some(bytes) = get_value::<8>(data, field.offset_pos) {
            foreign_keys.push((model_index, index, bytes));
          }
        }
        FieldType::ModelRefList(model_index) => {
          if let Some(arr) = get_array::<8>(data, field.offset_pos) {
            for bytes in arr {
              foreign_keys.push((model_index, index, bytes));
            }
          }
        }
        _  => { }
    }
  }
  return foreign_keys;
}

#[inline(always)]
/// В этой функции собираем все индексы с данных
fn get_indexes<'a>(data: &'a[u8], item_id: u64, model: &'a Model) -> Vec<(&'a[u8], Vec<u8>)> {

  let mut indexes = vec![];
  for field in model.fields.iter() {

    let Some(index) = get_index(data, field, model, item_id) else {
      continue;
    };
    // Тут по идее должен записаться один индекс на каждое поле. Не должно быть ситуации, когда создается одинаковый индекс в разных деревьях
    indexes.push(index);
  }
  
  return indexes;
}

#[inline(always)]
fn get_index<'a>(data: &'a[u8], field: &'a Field, model: &Model, item_id: u64) -> Option<(&'a[u8], Vec<u8>)> {
  if field.derived_from.is_some() {
    return None;
  }

  let Some(value) = get_value_with_len(data, field.offset_pos, model) else {
    return None;
  };

  for item in field.ext_indexes.iter() {
    return Some((item.index_name.as_bytes(), [value, &item_id.to_be_bytes()].concat()));
  }

  if let Some(index_name) = field.index_name.as_ref() {
    // TODO: encode value for lexical sorting support (int, float)
    return Some((index_name.as_bytes(), [value, &item_id.to_be_bytes()].concat()));
  }

  return None;
}