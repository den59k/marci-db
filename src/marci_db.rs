use std::{collections::HashMap, sync::{Arc, atomic::{AtomicU64, Ordering}}, u64};

use bitvec::{index, vec::BitVec};
use canopydb::{Database, Environment, ReadTransaction, Transaction, Tree, WriteTransaction};

use crate::{schema::{Field, FieldType, InsertedIndex, Entity, Schema}, update_data::update_data};

pub struct MarciDB {
  pub db: Database,
  pub schema: Schema,
  counters: Vec<Arc<AtomicU64>>
}

#[derive(Debug)]
pub struct MarciSelectInclude<'a> {
  pub field: &'a Field,
  pub model: &'a Entity,
  pub select: MarciSelect<'a>,
  pub select_only_id: bool,
  pub binding: MarciSelectBinding<'a>,
  pub injected: Option<Injected<'a>>
}

#[derive(Debug)]
pub struct Injected<'a> {
  pub st: &'a Entity,
  pub mask: BitVec,
  pub aliases: Option<HashMap<usize,&'a str>>
}

#[derive(Debug)]
pub enum MarciSelectBinding<'a> {
  One (),
  Many(&'a[u8]),
  OneStruct(),
  ManyStruct(),
}

pub type EnumSelect<'a> = HashMap<usize, HashMap<u16, MarciSelect<'a>>>;

#[derive(Debug)]
pub struct MarciSelect<'a> {
  pub mask: BitVec,
  pub includes: Vec<MarciSelectInclude<'a>>,
  pub enum_selects: EnumSelect<'a>
}

pub struct DecodeCtx<'a, U> {
  pub id: &'a [u8],
  pub data: &'a [u8],
  pub entity: &'a Entity,
  pub select: &'a BitVec,
  pub includes: Vec<IncludeResult<'a, U>>,
  pub inject: Option<U>,
  pub aliases: Option<&'a HashMap<usize,&'a str>>
}

#[derive(Debug)]
pub enum InsertStruct<'a> {
    None {
        st: &'a Entity,
    },
    Empty {
      st: &'a Entity,
    },
    One {
        st: &'a Entity,
        changed_mask: BitVec,
        data: Vec<u8>,
    },
    Many {
        st: &'a Entity,
        data: Vec<(Vec<u8>,Vec<u8>)>,
    },
    Connect {
        field: &'a Field,
        ref_model: usize,
        ids: Vec<Vec<u8>>
    },
    Update {
        st: &'a Entity,
        changed_mask: BitVec,
        counter_idx: usize,
        data: Vec<u8>,
        id: u64
    },
    Push {
        st: &'a Entity,
        changed_mask: BitVec,
        counter_idx: usize,
        data: Vec<u8>,
    },
}


#[derive(Debug)]
pub enum InsertError {
  ForeignKeyViolation(String, u64),
  ItemNotFound
}

pub enum IncludeResult<'a, U> {
  None(&'a Field),
  One(&'a Field,U),
  Many(&'a Field,Vec<U>)
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

      for field in model.fields.iter_mut() {
        if field.counter_idx.is_some() {
          let max_id = get_max_id(&tree);
          field.counter_idx = Some(counters.len());
          counters.push(Arc::new(AtomicU64::new(max_id)));
        }
      }

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
        if let FieldType::StructList(st) = &field.ty {
          let tree = tx.get_or_create_tree(st.name.as_bytes()).unwrap();
          if field.counter_idx.is_some() {
            let max_id = get_max_id_struct(&tree);
            field.counter_idx = Some(counters.len());
            counters.push(Arc::new(AtomicU64::new(max_id)));
          }
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
  
  pub fn next_idc(&self, counter_idx: usize) -> u64 {
    self.counters[counter_idx].fetch_add(1, Ordering::Relaxed)
  }
  
  pub fn get_model(&self, name: &str) -> Option<&Entity> {
    return self.schema.models.iter().find(|i| i.name == name);
  }

  pub fn insert_counter_value(&self, model: &Entity, id: &mut [u8]) {
    for field in model.fields.iter() {
      let Some(idx) = field.id_idx else { continue; };
      if let Some(counter_idx) = field.counter_idx {
        let field_id = self.next_idc(counter_idx);
        id[idx..idx+8].copy_from_slice(&field_id.to_be_bytes());
      }
    }
  }

  pub fn insert_data(&self, model: &Entity, id: &mut [u8], data: &[u8], structs: &[InsertStruct]) -> Result<(), InsertError> {

    let foreign_keys = collect_foreign_keys(id, data, model, structs, &self.schema);

    let tx = self.db.begin_write().unwrap();
    check_foreign_keys(&tx, &foreign_keys)?;

    self.insert_counter_value(model, id);

    // После получения ID - получаем индексы
    let mut indexes = get_indexes(data, id, model, None);

    // Добавляем само значение
    {
      let mut tree = tx.get_tree(model.name.as_bytes()).unwrap().unwrap();
      tree.insert(id, data).unwrap();
    }

    // Добавляем зависимые структуры
    for st in structs {
      match st {
        InsertStruct::Many { st, data, .. } => {
          let mut tree = tx.get_tree(st.name.as_bytes()).unwrap().unwrap();
          for (item_id, item_data) in data {

            let mut new_item_id = item_id.clone();
            new_item_id[0..8].copy_from_slice(id);
            
            self.insert_counter_value(*st, &mut new_item_id);

            tree.insert(&new_item_id, item_data).unwrap();

            // NOTE: здесь бы не запутаться. Мы расширяем ID для структуры, но данные в ID хранятся без этого префикса
            indexes.extend(get_indexes(item_data, &new_item_id, *st, None));
          }
        },
        InsertStruct::One { st, data, .. } => {
          let mut tree = tx.get_tree(st.name.as_bytes()).unwrap().unwrap();
          tree.insert(id, data).unwrap();
          indexes.extend(get_indexes(data, id, *st, None));
        }
        InsertStruct::Connect { field, ids, .. } => {
          insert_indexes(&tx, field, id, ids);
        }
        _ => {}
      }
    }

    // Обновляем индексы
    for index in indexes {
      println!("Insert index to {:#?} {:?}", str::from_utf8(index.tree_name).unwrap(), index.key);
      
      let mut index_tree = tx.get_tree(index.tree_name).unwrap().unwrap();
      index_tree.insert(&index.key, &[1]).unwrap();
    }
    
    tx.commit().unwrap();

    return Ok(())
  }

  fn process_data<U, F>(
      &self,
      id: &[u8],
      data: &[u8],
      rx: &ReadTransaction,
      select: &MarciSelect,
      entity: &Entity,
      f: &F,
      mut inject: Option<U>
  ) -> U
  where
      F: Fn(DecodeCtx<U>) -> U,
  {

    let includes: Vec<IncludeResult<U>> = select.includes.iter().map(|include| {
      match include.binding {
        MarciSelectBinding::One() => {
          let Some(item_id) = get_value_from_data(include.field, id, data, 8) else {
            return IncludeResult::None(include.field);
          };

          let injected_tree = include.injected.as_ref()
            .and_then(|i| Some((i, rx.get_tree(i.st.name.as_bytes()).unwrap().unwrap())));

          if include.select_only_id {
            let injected_data = get_injected_data(item_id, &injected_tree, f);
            // We send empty data because only ID bytes is using
            let item = self.process_data(item_id, &[], rx, &include.select, include.model, f, injected_data); 
            return IncludeResult::One(include.field, item);
          }
          let nested_tree = rx.get_tree(include.model.name.as_bytes()).unwrap().unwrap();
          let Some(data) = nested_tree.get(item_id).unwrap() else {
            println!("Warning: not found entry for key {:?}", item_id);
            return IncludeResult::None(include.field);
          };
          let injected_data: Option<U> = get_injected_data(item_id, &injected_tree, f);
          let item = self.process_data(item_id, data.as_ref(), rx, &include.select, include.model, f, injected_data);
          return IncludeResult::One(include.field, item);
        },
        MarciSelectBinding::Many(tree_name) => {
          let keys = find_by_direct(rx, tree_name, id);
          
          if keys.is_empty() {
            return IncludeResult::Many(include.field, vec![]);
          }
          
          let injected_tree = include.injected.as_ref()
            .and_then(|i| Some((i, rx.get_tree(i.st.name.as_bytes()).unwrap().unwrap())));

          if include.select_only_id {
            let items = keys.iter().map(|key| {
              let injected_data = get_injected_data(key, &injected_tree, f);
              return self.process_data(key, &[], rx, &include.select, include.model, f, injected_data);
            }).collect();

            return IncludeResult::Many(include.field, items);
          }

          let nested_tree = rx.get_tree(include.model.name.as_bytes()).unwrap().unwrap();
          let items = keys.iter().map(|key| {
            let Some(data) = nested_tree.get(&key[..8]).unwrap() else {
              panic!("Not found value in tree {}. Key: {:?}", str::from_utf8(include.model.name.as_bytes()).unwrap(), key);
            };
            let injected_data = get_injected_data(key, &injected_tree, f);
            return self.process_data(key, data.as_ref(), rx, &include.select, include.model, f, injected_data);
          }).collect();

          return IncludeResult::Many(include.field, items);
        },
        MarciSelectBinding::OneStruct() => {
          let st_tree = rx.get_tree(include.model.name.as_bytes()).unwrap().unwrap();
          let Some(data) = st_tree.get(id).unwrap() else {
            return IncludeResult::None(include.field);
          };
          let item = self.process_data(id, data.as_ref(), rx, &include.select, include.model, f, None);
          return IncludeResult::One(include.field, item);
        },
        MarciSelectBinding::ManyStruct() => {

          let st_tree = rx.get_tree(include.model.name.as_bytes()).unwrap().unwrap();

          if include.select_only_id {
            let items = st_tree.prefix_keys(&id).unwrap().map(|item| {
              let key = item.unwrap();
              return self.process_data(&key, &[], rx, &include.select, include.model, f, None);
            }).collect();
            return IncludeResult::Many(include.field, items);
          }

          let items = st_tree.prefix(&id).unwrap().map(|item| {
            let (key, data) = item.unwrap();
            return self.process_data(&key, data.as_ref(), rx, &include.select, include.model, f, None);
          }).collect();

          return IncludeResult::Many(include.field, items);
        },
      }
    }).collect();

    for (field_index, variants_map) in &select.enum_selects {
      let field = &entity.fields[*field_index];
      let FieldType::Enum(en) = &field.ty else {
        panic!("Field type is not enum");
      };
      let offset = get_offset(data, field.offset_pos);
      if offset != 0 {
        let variant = &u16::from_be_bytes(data[offset..offset+2].try_into().unwrap());
        if let Some(variant_select) = variants_map.get(variant) {
          let variant_resp = self.process_data(&[], &data[offset..], rx, variant_select, &en.variants[*variant as usize], f, inject.take());
          inject = Some(variant_resp);
        }
      }
    }
    return f(DecodeCtx { id, data, entity, select: &select.mask, includes, inject, aliases: None });
  }

  pub fn get_all<U, F>(
      &self,
      model: &Entity,
      select: &MarciSelect,
      f: F
  ) -> Vec<U>
  where
    F: Fn(DecodeCtx<'_, U>) -> U,
  {
      let rx = self.db.begin_read().unwrap();
      let tree = rx.get_tree(model.name.as_bytes()).unwrap().unwrap();

      tree.iter().unwrap().map(|item| {
          let (id, value) = item.unwrap();
          
          self.process_data(&id, &value, &rx, select, model, &f, None)
      }).collect()
  }

  pub fn get_item<U, F: FnOnce(&[u8]) -> U>(&self, model: &Entity, key: &str, f: F) -> Option<U> {

    let rx = self.db.begin_read().unwrap();
    let tree = rx.get_tree(model.name.as_bytes()).unwrap().unwrap();

    return tree.get(key.as_bytes()).unwrap().map(|item| f(item.as_ref()))
  }

  pub fn update(&self, model: &Entity, id: &[u8], new_data: &[u8], changed_mask: BitVec, structs: &[InsertStruct]) -> Result<(), InsertError> {
    
    let foreign_keys = collect_foreign_keys(id, new_data, model, structs, &self.schema);

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

      let Some(data) = tree.get(id).unwrap() else {
        return Err(InsertError::ItemNotFound)
      };

      let updated_data = update_data(&model.fields, model.payload_offset, &data, new_data, &changed_mask);
      tree.insert(id, &updated_data).unwrap();

      indexes_to_remove.extend(get_indexes(&data, id, model, Some(&changed_mask)));
    };

    
    // Добавляем зависимые структуры
    for st in structs {
      match st {
        InsertStruct::Empty { st } => {
          let mut tree = tx.get_tree(st.name.as_bytes()).unwrap().unwrap();
          let end = increment_bytes_be(id);
          tree.delete_range(id..&end).unwrap();

          // TODO: Delete old indexes here (from model_ref -> struct values)
        }
        InsertStruct::Many { st, data: new_data, .. } => {
          let mut tree = tx.get_tree(st.name.as_bytes()).unwrap().unwrap();
          for (item_id, item_data) in new_data {
            let mut new_item_id = item_id.clone();
            new_item_id[0..8].copy_from_slice(id);
            
            // TODO: Do not insert counter value in UPDATE struct request
            self.insert_counter_value(*st, &mut new_item_id);

            tree.insert(&new_item_id, item_data).unwrap();
            indexes.extend(get_indexes(item_data, &new_item_id, *st, None));
            
            // TODO: Delete old indexes here (from model_ref -> struct values)
          }
        },
        InsertStruct::One { st, data: new_data, changed_mask } => {
          let mut tree = tx.get_tree(st.name.as_bytes()).unwrap().unwrap();
          if let Some(data) = tree.get(id).unwrap() {

            let updated_data = update_data(&st.fields, st.payload_offset, &data.as_ref(), new_data, &changed_mask);
            tree.insert(id, &updated_data).unwrap();

            indexes_to_remove.extend(get_indexes(&data, id, *st, Some(&changed_mask)));
          } else {
            tree.insert(id, new_data).unwrap()
          }
        }
        InsertStruct::Connect { field, ids, .. } => {
          remove_indexes(&tx, &field, id);
          insert_indexes(&tx, field, id, ids);
        },
        InsertStruct::None { st } => {
          let mut tree = tx.get_tree(st.name.as_bytes()).unwrap().unwrap();
          tree.delete(id).unwrap();
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

    return Ok(());
  }

  pub fn delete(&self, model: &Entity, id: u64) -> bool {
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

// TODO: Make this method without size
#[inline(always)]
fn get_value_from_data<'a>(field: &'a Field, id: &'a[u8], data: &'a[u8], size: usize) -> Option<&'a[u8]> {
  if let Some(id_idx) = field.id_idx {
    if id.len() < id_idx*8+8 {
      panic!("ID too small. Field: {}, ID: {:?}, idx: {}", field.name, id, id_idx);
    }
    let value = &id[id_idx*8..id_idx*8+8];
    return Some(value)
  } else {
    if field.offset_pos == 0 {
      panic!("ERROR: Try to get zero offset value {}", field.name)
    }
    let offset = get_offset(data, field.offset_pos);
    if offset == 0 {
      return None;
    }
    Some(&data[offset..offset + size])
  }
}

#[inline(always)]
fn get_value<'a, const SIZE: usize>(
    data: &'a [u8],
    offset_pos: usize,
) -> Option<&'a [u8; SIZE]> {
    if offset_pos == 0 {
      panic!("ERROR: Try to get zero offset value")
    }
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

#[derive(Debug)]
struct ForeignKey<'a> {
  model: &'a Entity,
  field: &'a Field,
  id: &'a[u8]
}

#[inline(always)]
fn get_foreign_keys<'a>(id: &'a[u8], data: &'a[u8], model: &'a Entity, schema: &'a Schema) -> Vec<ForeignKey<'a>> {
  // Maybe create vec with capacity of foreign keys?
  let mut foreign_keys = Vec::new();

  // for idx in model.key_fields() {
  //   let field = model.get_field(*idx);
  //   match field.ty {
  //       FieldType::ModelRef(model_index) => {
  //         // TODO: write foreign_key to correct place
  //         let bytes = &id[idx*8..idx*8+8];
  //         foreign_keys.push(ForeignKey { model: &schema.models[model_index], field, id: bytes });
  //       }
  //       _  => { }
  //   }
  // }

  for field in model.fields.iter() {
    match field.ty {
        FieldType::ModelRef(model_index) => {
          if field.name.starts_with("@") {
            continue;
          }
          if let Some(bytes) = get_value_from_data(field, id, data, 8) {
            foreign_keys.push(ForeignKey { model: &schema.models[model_index], field, id: bytes });
          }
        }
        _  => { }
    }
  }
  return foreign_keys;
}

#[inline(always)]
fn collect_foreign_keys<'a>(id: &'a [u8], data: &'a[u8], model: &'a Entity, structs: &'a [InsertStruct], schema: &'a Schema) -> Vec<ForeignKey<'a>> {
  let mut foreign_keys = get_foreign_keys(id, data, model, schema);
  // Проверяем foreign_keys в дочерних структурах
  for st in structs {
    match st {
      InsertStruct::Connect { field, ref_model, ids } => {
        for item_id in ids.iter() {
          let model = &schema.models[*ref_model];
          foreign_keys.push(ForeignKey { model, field, id: item_id });
        }
      }
      InsertStruct::Many { st, data, .. } => {
        for item_data in data {
          foreign_keys.extend(get_foreign_keys(&item_data.0, &item_data.1, *st, schema));
        }
      },
      InsertStruct::One { st, data, .. } => {
        foreign_keys.extend(get_foreign_keys(id, data, *st, schema));
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
      // TODO: Incorrect u64::from_be_bytes. Write full key instead
      return Err(InsertError::ForeignKeyViolation(item.field.name.clone(), u64::from_be_bytes(item.id.try_into().unwrap())))
    }
  }
  return Ok(());
}

#[inline(always)]
/// Находит все ключи в индексе через ключ A, возвращает массив ключей B
fn find_by_direct(rx: &Transaction, tree_name: &[u8], item_id: &[u8]) -> Vec<Vec<u8>> {
  let index_tree = rx.get_tree(tree_name).unwrap()
    .unwrap_or_else(|| panic!("Index {} not found", str::from_utf8(tree_name).unwrap()));

  let iter = index_tree.prefix_keys(&item_id).unwrap();
  iter.map(|k| k.unwrap()[8..].to_vec()).collect()
}

#[inline(always)]
fn get_injected_data<U,F>(id: &[u8], injected_tree: &Option<(&Injected<'_>, Tree<'_>)>, f: F) -> Option<U> where F: Fn(DecodeCtx<U>) -> U, {
  let Some((injected, tree)) = injected_tree else { return None };

  let Some(data) = tree.get(id).unwrap() else {
    panic!("Not found key {:?} in tree {}", id, injected.st.name)
  };
  return Some(f(DecodeCtx { 
    id, 
    data: &data, 
    entity: injected.st, 
    select: &injected.mask,
    includes: vec![], 
    inject: None, 
    aliases: injected.aliases.as_ref() 
  }))
}

// #[inline(always)]
// fn make_key(a: u64, b: u64) -> [u8; 16] {
//   let mut key = [0u8; 16];
//   key[..8].copy_from_slice(&a.to_be_bytes());
//   key[8..].copy_from_slice(&b.to_be_bytes());
//   key
// }

#[inline(always)]
fn insert_index(tree: &mut Tree, left: &[u8], right: &[u8]) {
  let mut key = Vec::with_capacity(left.len() + right.len());
  key.extend_from_slice(left);
  key.extend_from_slice(right);
  tree.insert(&key, &[1]).unwrap();
}

#[derive(Debug)]
struct IndexData<'a> {
  tree_name: &'a[u8],
  key: Vec<u8>
}

#[inline(always)]
/// В этой функции собираем все индексы с данных. Обычно это собирается только с OneToMany
fn get_indexes<'a>(data: &[u8], id: &[u8], model: &'a Entity, mask: Option<&BitVec>) -> Vec<IndexData<'a>> {

  let mut indexes = vec![];
  for (field_index, field) in model.fields.iter().enumerate(){
    // Skip values without indexes
    if field.inserted_indexes.is_empty() { continue; }
    // Skip derived values
    if field.id_idx.is_none() && field.offset_pos == 0 { continue; } 
    // Skip not changed values
    if mask.is_some_and(|f| !f[field_index]) { continue; }

    let Some(value) = get_value_from_data(field, id, data, 8) else {
      continue;
    };
    for index in &field.inserted_indexes {
      match index {
        InsertedIndex::Rev { tree_name } => {
          let mut key = Vec::with_capacity(value.len() + id.len());
          key.extend_from_slice(value);
          key.extend_from_slice(id);
          indexes.push(IndexData { tree_name: tree_name.as_bytes(), key });
        },
        InsertedIndex::Direct { tree_name } => {
          let mut key = Vec::with_capacity(value.len() + id.len());
          key.extend_from_slice(id);
          key.extend_from_slice(value);
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

#[inline(always)]
pub fn get_max_id_struct(tree: &Tree) -> u64 {
  return tree.last().unwrap()
    .map(|(key, _)| u64::from_be_bytes(key.as_ref()[8..16].try_into().unwrap()) + 1)
    .unwrap_or(1);
}

pub fn get_offsets(data: &[u8], model: &Entity) -> Vec<usize> {
  let mut arr = vec![];
  for field in model.fields.iter() {
    if field.offset_pos == 0 {
      continue;
    }
    let offset = get_offset(data, field.offset_pos);
    arr.push(offset);
  }
  return arr;
}

#[inline(always)]
fn insert_indexes(tx: &WriteTransaction, field: &Field, id: &[u8], ids: &[Vec<u8>]) {
  if ids.is_empty() {
    return;
  }
  for index in field.inserted_indexes.iter() {
    // println!("Insert {}", str::from_utf8(index.tree_name()).unwrap());
    let mut tree = tx.get_tree(index.tree_name()).unwrap().unwrap();

    match index {
      InsertedIndex::Direct { .. } => for cid in ids { insert_index(&mut tree, id, cid); },
      InsertedIndex::Rev { .. } => for cid in ids { insert_index(&mut tree, cid, id); },
    }
  }
}


#[inline(always)]
pub fn remove_indexes(tx: &WriteTransaction, field: &Field, id: &[u8]) {
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
        tree.delete(&key[..8]).unwrap();
      }
    }
  }

  for index in field.inserted_indexes.iter() {
    let InsertedIndex::Direct { tree_name } = index else { continue };
    let mut tree = tx.get_tree(tree_name.as_bytes()).unwrap().unwrap();
    let end = increment_bytes_be(id);
    tree.delete_range(id..&end).unwrap();
  }
}
