use std::{collections::HashMap, sync::{Arc, atomic::{AtomicU64, Ordering}}, u64};

use bitvec::{index, vec::BitVec};
use canopydb::{Database, Environment, ReadTransaction, Transaction, Tree, WriteTransaction};

use crate::{schema::{Field, FieldType, InsertedIndex, Model, Schema, Struct, WithFields}, update_data::update_data};

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
        changed_mask: BitVec,
        data: Vec<u8>,
    },
    Many {
        st: &'a Struct,
        counter_idx: usize,
        data: Vec<(Option<u64>,Vec<u8>)>,
    },
    Connect {
        field: &'a Field,
        ref_model: usize,
        ids: Vec<u64>
    },
    Update {
        st: &'a Struct,
        changed_mask: BitVec,
        counter_idx: usize,
        data: Vec<u8>,
        id: u64
    },
    Push {
        st: &'a Struct,
        changed_mask: BitVec,
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

  pub fn insert_data(&self, model: &Model, data: &[u8], structs: &[InsertStruct]) -> Result<u64, InsertError> {

    let foreign_keys = collect_foreign_keys(data, &model.fields, structs, &self.schema);
    
    let id = self.next_id(model);
    let mut indexes = get_indexes(data, id, model, None);
    for st in structs {
      match st {
        InsertStruct::One { st, data, .. } => {
          indexes.extend(get_indexes(data, id, *st, None));
        }
        _ => {}
      }
    }

    let tx = self.db.begin_write().unwrap();
    check_foreign_keys(&tx, &foreign_keys)?;

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
          for (item_id, item_data) in data {
            let item_id: u64 = item_id.unwrap_or_else(|| self.next_idc(*counter_idx));
            tree.insert(&make_key(id, item_id), item_data).unwrap();
            indexes.extend(get_indexes(item_data, item_id, *st, None));
          }
        },
        InsertStruct::One { st, data, .. } => {
          let mut tree = tx.get_tree(st.name.as_bytes()).unwrap().unwrap();
          tree.insert(&id.to_be_bytes(), data).unwrap()
        }
        InsertStruct::Connect { field, ids, .. } => {
          insert_indexes(&tx, field, id, ids);
        }
        _ => {}
      }
    }

    // Обновляем индексы
    for index in indexes {
      let mut index_tree = tx.get_tree(index.tree_name).unwrap().unwrap();
      index_tree.insert(&index.key, &[1]).unwrap();
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
          let keys = find_by_direct(rx, tree_name, id);
          
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

  pub fn update(&self, model: &Model, id: u64, new_data: &[u8], changed_mask: BitVec, structs: &[InsertStruct]) -> Result<u64, InsertError> {
    
    let foreign_keys = collect_foreign_keys(new_data, &model.fields, structs, &self.schema);

    let mut indexes = get_indexes(new_data, id, model, None);
    for st in structs {
      match st {
        InsertStruct::One { st, data, .. } => {
          indexes.extend(get_indexes(data, id, *st, None));
        }
        _ => {}
      }
    }

    let mut indexes_to_remove = vec![];

    let tx = self.db.begin_write().unwrap();

    check_foreign_keys(&tx, &foreign_keys)?;

    // Обновляем значение. Выдаем ошибку, если значения не существует
    {
      let mut tree = tx.get_tree(model.name.as_bytes()).unwrap().unwrap();

      let Some(data) = tree.get(&id.to_be_bytes()).unwrap() else {
        return Err(InsertError::ItemNotFound(id))
      };

      let updated_data = update_data(&model.fields, model.payload_offset, &data, new_data, &changed_mask);
      tree.insert(&id.to_be_bytes(), &updated_data).unwrap();

      indexes_to_remove.extend(get_indexes(&data, id, model, Some(&changed_mask)));
    };

    
    // Добавляем зависимые структуры
    for st in structs {
      match st {
        InsertStruct::Empty { st } => {
          let mut tree = tx.get_tree(st.name.as_bytes()).unwrap().unwrap();
          tree.delete_range(id.to_be_bytes()..(id+1).to_be_bytes()).unwrap();

          // TODO: Delete old indexes here (from model_ref -> struct values)
        }
        InsertStruct::Many { st, data: new_data, counter_idx, .. } => {
          let mut tree = tx.get_tree(st.name.as_bytes()).unwrap().unwrap();
          for (item_id, item_data) in new_data {
            let item_id: u64 = item_id.unwrap_or_else(|| self.next_idc(*counter_idx));
            tree.insert(&make_key(id, item_id), item_data).unwrap();
            indexes.extend(get_indexes(item_data, item_id, *st, None));

            // TODO: Delete old indexes here (from model_ref -> struct values)
          }
        },
        InsertStruct::One { st, data: new_data, changed_mask } => {
          let mut tree = tx.get_tree(st.name.as_bytes()).unwrap().unwrap();
          if let Some(data) = tree.get(&id.to_be_bytes()).unwrap() {

            let updated_data = update_data(&st.fields, st.payload_offset, &data.as_ref(), new_data, &changed_mask);
            tree.insert(&id.to_be_bytes(), &updated_data).unwrap();

            indexes_to_remove.extend(get_indexes(&data, id, *st, Some(&changed_mask)));
          } else {
            tree.insert(&id.to_be_bytes(), new_data).unwrap()
          }
        }
        InsertStruct::Connect { field, ids, .. } => {
          remove_indexes(&tx, &field, id);
          insert_indexes(&tx, field, id, ids);
        },
        InsertStruct::None { st } => {
          let mut tree = tx.get_tree(st.name.as_bytes()).unwrap().unwrap();
          tree.delete(&id.to_be_bytes()).unwrap();
        },
        _ => {}
      }
    }
    
    for index in indexes_to_remove {
      let mut index_tree = tx.get_tree(index.tree_name).unwrap().unwrap();
      index_tree.delete(&index.key).unwrap();
    }

    // Обновляем индексы (сносим старые, ставим новые)
    for index in indexes {
      let mut index_tree = tx.get_tree(index.tree_name).unwrap().unwrap();

      // Здесь удаление по префиксу по сути не нужно
      // if let Some(prefix) = index.prefix {
      //   let end = increment_bytes_be(prefix);
      //   index_tree.delete_range(prefix..&end).unwrap();
      // }

      index_tree.insert(&index.key, &[1]).unwrap();
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
pub fn get_offset<'a>(data: &'a [u8], offset_pos: usize) -> usize {
  return u32::from_be_bytes(data[offset_pos..offset_pos + 4].try_into().unwrap()) as usize;
}

#[inline(always)]
pub fn set_offset<'a>(data: &'a mut [u8], offset_pos: usize, offset: usize) {
  data[offset_pos..offset_pos+4].copy_from_slice(&(offset as u32).to_be_bytes());
}

#[inline(always)]
pub fn get_end(data: &[u8], offset_pos: usize, payload_offset: usize) -> usize {
  for j in ((offset_pos+4)..payload_offset).step_by(4) {
    let off_j = get_offset(data, j);
    if off_j != 0 {
      return off_j;
    }
  }

  return data.len();
}

pub fn move_offsets<'a>(data: &'a mut [u8], offset_start: usize, offset_end: usize, diff: isize) {
  for j2 in (offset_start..offset_end).step_by(4) {
    let offset = u32::from_be_bytes(data[j2..j2+4].try_into().unwrap());
    if offset != 0 {
      let new_offset = (offset as isize + diff) as u32;
      data[j2..j2+4].copy_from_slice(&new_offset.to_be_bytes());
    }
  }
}

#[inline(always)]
pub fn set_offset_null<'a>(data: &'a mut [u8], offset_pos: usize) {
  data[offset_pos..offset_pos+4].fill(0u8);
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

#[inline(always)]
fn increment_bytes_be(bytes: &[u8]) -> Vec<u8> {
    let mut result = bytes.to_vec();
    for b in result.iter_mut().rev() {
        if *b == 0xFF {
            *b = 0;
        } else {
            *b += 1;
            return result;
        }
    }
    // если было переполнение (все байты = 0xFF)
    result.insert(0, 1);
    result
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
  for j in ((offset_pos+4)..payload_offset).step_by(4) {
    let offset = get_offset(data, j);
    if offset != 0 { 
      offset_end = offset;
      break;
    }
  }

  return Some(&data[offset..offset_end])
}

struct ForeignKey<'a> {
  model: &'a Model,
  field: &'a Field,
  id: [u8;8]
}

#[inline(always)]
fn get_foreign_keys<'a>(data: &'a[u8], fields: &'a [Field], schema: &'a Schema) -> Vec<ForeignKey<'a>> {
  let mut foreign_keys = Vec::with_capacity(fields.len());

  for field in fields.iter() {
    if field.derived_from.is_some() { continue; }
    match field.ty {
        FieldType::ModelRef(model_index) => {
          if let Some(bytes) = get_value::<8>(data, field.offset_pos) {
            foreign_keys.push(ForeignKey { model: &schema.models[model_index], field, id: bytes.clone() });
          }
        }
        _  => { }
    }
  }
  return foreign_keys;
}

#[inline(always)]
fn collect_foreign_keys<'a>(data: &'a[u8], fields: &'a [Field], structs: &'a [InsertStruct], schema: &'a Schema) -> Vec<ForeignKey<'a>> {
  let mut foreign_keys = get_foreign_keys(data, fields, schema);
  // Проверяем foreign_keys в дочерних структурах
  for st in structs {
    match st {
      InsertStruct::Connect { field, ref_model, ids } => {
        for item_id in ids.iter() {
          let model = &schema.models[*ref_model];
          foreign_keys.push(ForeignKey { model, field, id: item_id.to_be_bytes() });
        }
      }
      InsertStruct::Many { st, data, .. } => {
        for item_data in data {
          foreign_keys.extend(get_foreign_keys(&item_data.1, &st.fields, schema));
        }
      },
      InsertStruct::One { st, data, .. } => {
        foreign_keys.extend(get_foreign_keys(data, &st.fields, schema));
      }
      _ => {}
    }
  }
  return foreign_keys;
}

#[inline(always)]
fn check_foreign_keys(tx: &Transaction, foreign_keys: &[ForeignKey]) -> Result<(), InsertError> {
  for item in foreign_keys {
    let tree = tx.get_tree(item.model.name.as_bytes()).unwrap().unwrap();
    if tree.get(&item.id).unwrap().is_none() {
      return Err(InsertError::ForeignKeyViolation(item.field.name.clone(), u64::from_be_bytes(item.id)))
    }
  }
  return Ok(());
}

#[inline(always)]
/// Находит все ключи в индексе через ключ A, возвращает массив ключей B
fn find_by_direct(rx: &Transaction, tree_name: &[u8], item_id: u64) -> Vec<Vec<u8>> {
  let index_tree = rx.get_tree(tree_name).unwrap()
    .unwrap_or_else(|| panic!("Index {} not found", str::from_utf8(tree_name).unwrap()));

  let iter = index_tree.prefix_keys(&item_id.to_be_bytes()).unwrap();
  iter.map(|k| k.unwrap()[8..].to_vec()).collect()
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

struct IndexData<'a> {
  tree_name: &'a[u8],
  key: Vec<u8>
}

#[inline(always)]
/// В этой функции собираем все индексы с данных. Обычно это собирается только с OneToMany
fn get_indexes<'a, T>(data: &[u8], item_id: u64, model: &'a T, mask: Option<&BitVec>) -> Vec<IndexData<'a>> where T: WithFields {

  let mut indexes = vec![];
  for field in model.fields() {
    if field.offset_pos == 0 || field.inserted_indexes.is_empty() { continue; }
    if mask.is_some_and(|f| !f[field.offset_index]) { continue; }
    let Some(value) = get_value_with_len(data, field.offset_pos, model.payload_offset()) else {
      continue;
    };
    for index in &field.inserted_indexes {
      match index {
        InsertedIndex::Rev { tree_name } => {
          let key = [value, &item_id.to_be_bytes()].concat();
          indexes.push(IndexData { tree_name: tree_name.as_bytes(), key });
        },
        InsertedIndex::Direct { tree_name } => {
          let key = [&item_id.to_be_bytes(), value].concat();
          indexes.push(IndexData { tree_name: tree_name.as_bytes(), key });
        }
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

pub fn get_offsets(data: &[u8], model: &Model) -> Vec<usize> {
  let mut arr = vec![];
  for field in model.fields.iter() {
    let offset = get_offset(data, field.offset_pos);
    arr.push(offset);
  }
  return arr;
}

#[inline(always)]
fn insert_indexes(tx: &WriteTransaction, field: &Field, id: u64, ids: &[u64]) {
  if ids.is_empty() {
    return;
  }
  for index in field.inserted_indexes.iter() {
    // println!("Insert {}", str::from_utf8(index.tree_name()).unwrap());
    let mut tree = tx.get_tree(index.tree_name()).unwrap().unwrap();

    match index {
      InsertedIndex::Direct { .. } => for &cid in ids { insert_index(&mut tree, id, cid); },
      InsertedIndex::Rev { .. } => for &cid in ids { insert_index(&mut tree, cid, id); },
    }
  }
}


#[inline(always)]
pub fn remove_indexes(tx: &WriteTransaction, field: &Field, id: u64) {
  if field.inserted_indexes.is_empty() {
    return;
  }

  let direct_index = field.inserted_indexes.iter()
    .find(|i| matches!(i, InsertedIndex::Direct { tree_name: _ })).expect("Direct index must be defined for batch update");
  
  let rev_indexes: Vec<&InsertedIndex> = field.inserted_indexes.iter()
    .filter(|i| matches!(i, InsertedIndex::Rev { tree_name: _ })).collect();
  
  if !rev_indexes.is_empty() {
    let keys = find_by_direct(tx, direct_index.tree_name(), id);
    if keys.is_empty() {
      return;
    }
    for index in rev_indexes {
      let InsertedIndex::Rev { tree_name } = index else { continue };
      let mut tree = tx.get_tree(tree_name.as_bytes()).unwrap().unwrap();
      for key in keys.iter() {
        tree.delete(&key).unwrap();
      }
    }
  }

  for index in field.inserted_indexes.iter() {
    let InsertedIndex::Direct { tree_name } = index else { continue };
    let mut tree = tx.get_tree(tree_name.as_bytes()).unwrap().unwrap();
    tree.delete_range(id.to_be_bytes()..(id+1).to_be_bytes()).unwrap();
  }
}
