use std::{collections::HashMap, sync::{Arc, atomic::{AtomicU64, Ordering}}};

use bitvec::{index, vec::BitVec};
use canopydb::{Database, Environment, ReadTransaction, Tree};

use crate::schema::{Field, FieldType, InsertedIndex, Model, Schema, Struct, WithFields};

pub struct MarciDB {
  pub db: Database,
  pub schema: Schema,
  counters: Vec<Arc<AtomicU64>>
}

pub struct MarciSelectInclude<'a> {
  pub field_index: usize,
  pub model: &'a dyn WithFields,
  pub select: MarciSelect<'a>,
  pub binding: MarciSelectBinding<'a>,
}

pub enum MarciSelectBinding<'a> {
  One (usize),
  Many(&'a[u8]),
  OneStruct(),
  ManyStruct(),
}

pub struct MarciSelectVirtual<'a> {
  pub field_index: usize,
  pub index_name: &'a[u8],
  pub model: &'a Model,
  pub select: Box<MarciSelect<'a>>
}

pub struct MarciSelect<'a> {
  pub select: BitVec,
  pub includes: Vec<MarciSelectInclude<'a>>
}

pub struct DecodeCtx<'a, U> {
  pub id: u64,
  pub data: &'a [u8],
  pub fields: &'a [Field],
  pub payload_offset: usize,
  pub select: &'a BitVec,
  pub includes: Vec<IncludeResult<U>>,
}

#[derive(Debug)]
pub enum InsertStruct<'a> {
    None {
        st: &'a Struct,
    },
    Empty {
        st: &'a Struct,
    },
    One {
        st: &'a Struct,
        changed_values: BitVec,
        data: Vec<u8>,
    },
    Many {
        st: &'a Struct,
        counter_idx: usize,
        data: Vec<Vec<u8>>,
    },
    Connect {
        field: &'a Field,
        ref_model: usize,
        ids: Vec<u64>
    },
    Update {
        st: &'a Struct,
        changed_values: BitVec,
        counter_idx: usize,
        data: Vec<u8>,
        id: u64
    },
    Push {
        st: &'a Struct,
        changed_values: BitVec,
        counter_idx: usize,
        data: Vec<u8>,
    },
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

  pub fn new(mut schema: Schema) -> MarciDB {
    let env = Environment::new("./data").unwrap(); 
    let db = env.get_or_create_database("mydb.db").unwrap();

    let mut counters = Vec::with_capacity(schema.models.len());

    let mut model_names = HashMap::new();
    for (idx, model) in schema.models.iter().enumerate() {
      model_names.insert(idx, model.name.clone());
    }

    let tx = db.begin_write().unwrap();
    for model in schema.models.iter_mut() {
      let tree = tx.get_or_create_tree(model.name.as_bytes()).unwrap();

      let max_id = get_max_id(&tree);
      model.counter_idx = counters.len();
      counters.push(Arc::new(AtomicU64::new(max_id)));

      for field in model.fields.iter_mut() {
        for index in &field.inserted_indexes {
          match index {
            InsertedIndex::Direct { tree_name } => {
              tx.get_or_create_tree(tree_name.as_bytes()).unwrap();
            },
            InsertedIndex::Rev { tree_name: _ } => {},
          };
        }

        if let FieldType::Struct(st) = &field.ty {
          tx.get_or_create_tree(st.name.as_bytes()).unwrap();
        }
        if let FieldType::StructList(ref st, ref mut counter_idx) = field.ty {
          let tree = tx.get_or_create_tree(st.name.as_bytes()).unwrap();
          let max_id = get_max_id(&tree);
          *counter_idx = counters.len();
          counters.push(Arc::new(AtomicU64::new(max_id)));
        }
      }
    }
    tx.commit().unwrap();

    MarciDB {
      db,
      schema,
      counters
    }
  }
  
  pub fn next_id(&self, model: &Model) -> u64 {
    self.counters[model.counter_idx].fetch_add(1, Ordering::Relaxed)
  }
  pub fn next_idc(&self, counter_idx: usize) -> u64 {
    self.counters[counter_idx].fetch_add(1, Ordering::Relaxed)
  }
  
  pub fn get_model(&self, name: &str) -> Option<&Model> {
    return self.schema.models.iter().find(|i| i.name == name);
  }

  pub fn insert_data(&self, model: &Model, data: &[u8], structs: &Vec<InsertStruct>) -> Result<u64, InsertError> {

    let mut foreign_keys = get_foreign_keys(data, model);
    // Проверяем foreign_keys в дочерних структурах
    for st in structs {
      match st {
        InsertStruct::Connect { field, ref_model, ids } => {
          for item_id in ids.iter() {
            foreign_keys.push((*ref_model, field, item_id.to_be_bytes()));
          }
        }
        InsertStruct::Many { st, data, .. } => {
          for item_data in data {
            foreign_keys.extend(get_foreign_keys(item_data, *st));
          }
        },
        InsertStruct::One { st, data, .. } => {
          foreign_keys.extend(get_foreign_keys(data, *st));
        }
        _ => {}
      }
    }
    
    let id = self.next_id(model);
    let mut indexes = get_indexes(data, id, model);
    for st in structs {
      match st {
        InsertStruct::One { st, data, .. } => {
          indexes.extend(get_indexes(data, id, *st));
        }
        _ => {}
      }
    }

    let tx = self.db.begin_write().unwrap();

    // Сначала проверяем foreign keys
    for (model_index, field, item_id) in foreign_keys {
      let tree = tx.get_tree(self.schema.models[model_index].name.as_bytes()).unwrap().unwrap();
      if tree.get(&item_id).unwrap().is_none() {
        return Err(InsertError::ForeignKeyViolation(field.name.clone(), u64::from_be_bytes(item_id)))
      }
    }

    // Добавляем само значение
    {
      let mut tree = tx.get_tree(model.name.as_bytes()).unwrap().unwrap();
      tree.insert(&id.to_be_bytes(), data).unwrap();
    }

    // Добавляем зависимые структуры
    for st in structs {
      match st {
        InsertStruct::Many { st, data, counter_idx, .. } => {
          let mut tree = tx.get_tree(st.name.as_bytes()).unwrap().unwrap();
          for item_data in data {
            let item_id: u64 = self.next_idc(*counter_idx);
            tree.insert(&make_key(id, item_id), item_data).unwrap();

            indexes.extend(get_indexes(item_data, item_id, *st));
          }
        },
        InsertStruct::One { st, data, .. } => {
          let mut tree = tx.get_tree(st.name.as_bytes()).unwrap().unwrap();
          tree.insert(&id.to_be_bytes(), data).unwrap()
        }
        InsertStruct::Connect { field, ids, .. } => {
          for index in field.inserted_indexes.iter() {
            // println!("Insert {}", str::from_utf8(index.tree_name()).unwrap());
            let mut tree = tx.get_tree(index.tree_name()).unwrap().unwrap();
            match index {
              InsertedIndex::Direct { .. } => for &cid in ids { insert_index(&mut tree, id, cid); },
              InsertedIndex::Rev { .. } => for &cid in ids { insert_index(&mut tree, cid, id); },
            }
          }
        }
        _ => {}
      }
    }

    // Обновляем индексы
    for (tree_name, index_key) in indexes {
      let mut index_tree = tx.get_tree(tree_name).unwrap().unwrap();
      // println!("Insert {}", str::from_utf8(tree_name).unwrap());

      index_tree.insert(&index_key, &[1]).unwrap();
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
      model: &dyn WithFields,
      f: &F,
  ) -> U
  where
      F: Fn(DecodeCtx<U>) -> U,
  {

    let includes: Vec<IncludeResult<U>> = select.includes.iter().map(|include| {
      match include.binding {
        MarciSelectBinding::One(offset_pos) => {
          let Some(item_id) = get_value::<8>(data, offset_pos) else {
            return IncludeResult::None(include.field_index);
          };
          let nested_tree = rx.get_tree(include.model.tree_name()).unwrap().unwrap();
          let data = nested_tree.get(item_id).unwrap().unwrap();
          let item_id_val = u64::from_be_bytes(*item_id);
          let item = self.process_data(item_id_val, data.as_ref(), rx, &include.select, include.model, f);
          return IncludeResult::One(include.field_index, item);
        },
        MarciSelectBinding::Many(tree_name) => {
          let keys: Vec<Vec<u8>> = {
            let item_id = &id.to_be_bytes();
            let index_tree = rx.get_tree(tree_name).unwrap()
              .unwrap_or_else(|| panic!("Index {} not found", str::from_utf8(tree_name).unwrap()));

            let iter = index_tree.prefix_keys(item_id).unwrap();
            iter.map(|k| k.unwrap()[8..].to_vec()).collect()
          };
          
          if keys.is_empty() {
            return IncludeResult::Many(include.field_index, vec![]);
          }

          let nested_tree = rx.get_tree(include.model.tree_name()).unwrap().unwrap();
          let items = keys.iter().map(|key| {
            let data = nested_tree.get(&key).unwrap().unwrap();
            let item_id = u64::from_be_bytes(key.as_slice().try_into().unwrap());
            return self.process_data(item_id, data.as_ref(), rx, &include.select, include.model, f);
          }).collect();

          return IncludeResult::Many(include.field_index, items);
        },
        MarciSelectBinding::OneStruct() => {
          let item_id = &id.to_be_bytes();
          let st_tree = rx.get_tree(include.model.tree_name()).unwrap().unwrap();
          let Some(data) = st_tree.get(item_id).unwrap() else {
            return IncludeResult::None(include.field_index);
          };
          let item = self.process_data(id, data.as_ref(), rx, &include.select, include.model, f);
          return IncludeResult::One(include.field_index, item);
        },
        MarciSelectBinding::ManyStruct() => {

          let item_id = &id.to_be_bytes();
          let st_tree = rx.get_tree(include.model.tree_name()).unwrap().unwrap();

          let items = st_tree.prefix(item_id).unwrap().map(|item| {
            let (key, data) = item.unwrap();
            let st_item_id = u64::from_be_bytes(key[8..].try_into().unwrap());
            return self.process_data(st_item_id, data.as_ref(), rx, &include.select, include.model, f);
          }).collect();

          return IncludeResult::Many(include.field_index, items);
        },
      }
    }).collect();

    return f(DecodeCtx { id, data, fields: model.fields(), payload_offset: model.payload_offset(), select: &select.select, includes });
  }

  pub fn get_all<U, F, T>(
      &self,
      model: &T,
      select: &MarciSelect,
      f: F
  ) -> Vec<U>
  where
    T: WithFields,
    F: Fn(DecodeCtx<'_, U>) -> U,
  {
      let rx = self.db.begin_read().unwrap();
      let tree = rx.get_tree(model.tree_name()).unwrap().unwrap();

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

      for (model_index, field, item_id) in foreign_keys {
        let tree = tx.get_tree(self.schema.models[model_index].name.as_bytes()).unwrap().unwrap();
        if tree.get(&item_id).unwrap().is_none() {
          return Err(InsertError::ForeignKeyViolation(field.name.clone(), u64::from_be_bytes(item_id)))
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
    payload_offset: usize
) -> Option<&'a[u8]> {
  let offset = get_offset(data, offset_pos);
  if offset == 0 {
    return None;
  }

  let mut offset_end = data.len();
  for j in offset_pos+4..payload_offset {
    let offset = get_offset(data, j);
    if offset != 0 { 
      offset_end = offset;
      break;
    }
  }

  return Some(&data[offset..offset_end])
}

#[inline(always)]
fn get_foreign_keys<'a, T>(data: &'a[u8], model: &'a T) -> Vec<(usize, &'a Field, [u8;8])> where T: WithFields {
  let mut foreign_keys = Vec::with_capacity(model.fields().len());

  for field in model.fields().iter() {
    if field.derived_from.is_some() { continue; }
    match field.ty {
        FieldType::ModelRef(model_index) => {
          if let Some(bytes) = get_value::<8>(data, field.offset_pos) {
            foreign_keys.push((model_index, field, *bytes));
          }
        }
        _  => { }
    }
  }
  return foreign_keys;
}

#[inline(always)]
fn make_key(a: u64, b: u64) -> [u8; 16] {
  let mut key = [0u8; 16];
  key[..8].copy_from_slice(&a.to_be_bytes());
  key[8..].copy_from_slice(&b.to_be_bytes());
  key
}

#[inline(always)]
fn insert_index(tree: &mut Tree, left: u64, right: u64) {
    let key = make_key(left, right);
    tree.insert(&key, &[1]).unwrap();
}

#[inline(always)]
/// В этой функции собираем все индексы с данных
fn get_indexes<'a, T>(data: &'a[u8], item_id: u64, model: &'a T) -> Vec<(&'a[u8], Vec<u8>)> where T: WithFields {

  let mut indexes = vec![];
  for field in model.fields() {
    if field.offset_pos == 0 { continue; }
    let Some(value) = get_value_with_len(data, field.offset_pos, model.payload_offset()) else {
      continue;
    };
    for index in &field.inserted_indexes {
      if let InsertedIndex::Rev { tree_name } = index {
        let value = [value, &item_id.to_be_bytes()].concat();
        indexes.push((tree_name.as_bytes(), value));
      }
      if let InsertedIndex::Direct { tree_name } = index {
        let value = [&item_id.to_be_bytes(), value].concat();
        indexes.push((tree_name.as_bytes(), value));
      }
    }
  }
  
  return indexes;
}


#[inline(always)]
pub fn get_max_id(tree: &Tree) -> u64 {
  return tree.last().unwrap()
    .map(|(key, _)| u64::from_be_bytes(key.as_ref().try_into().unwrap()) + 1)
    .unwrap_or(1);
}