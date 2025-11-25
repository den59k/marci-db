use std::{collections::HashMap, sync::{Arc, atomic::{AtomicU64, Ordering}}, u64};

use bitvec::vec::BitVec;
use canopydb::{Database, Environment, Transaction, Tree, WriteTransaction};

use crate::{schema::{DeleteConstraint, Entity, Field, FieldType, InsertedIndex, Schema}, select::{DecodeCtx, MarciSelect, ProcessDataContext, TransationContext, get_value_from_data, get_value_from_id, process_data}, update_data::{set_field_null, update_data}};

pub struct MarciDB {
  pub db: Database,
  pub schema: Schema,
  counters: Vec<Arc<AtomicU64>>,
  model_by_name: HashMap<String, usize>
}


#[derive(Debug)]
pub enum InsertError {
  ForeignKeyViolation(String, u64),
  ItemNotFound
}

#[derive(Debug)]
pub enum DeleteError {
  ItemNotFound,
  RestrictConstraints(String,Vec<u64>)
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

    let model_by_name: HashMap<String, usize> = schema
      .models
      .iter()
      .enumerate()
      .map(|(i, model)| (model.name.clone(), i)).collect();

    MarciDB {
      db,
      schema,
      counters,
      model_by_name
    }
  }
  
  pub fn next_idc(&self, counter_idx: usize) -> u64 {
    self.counters[counter_idx].fetch_add(1, Ordering::Relaxed)
  }
  
  pub fn get_model(&self, name: &str) -> Option<(usize, &Entity)> {
    self.model_by_name.get(name).and_then(|model_index| {
      Some((*model_index, &self.schema.models[*model_index]))
    })
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

      let mut tctx = TransationContext::new(&rx, f);
      let mut ctx = ProcessDataContext::new(select);

      tree.iter().unwrap().map(|item| {
          let (id, value) = item.unwrap();
          
          process_data(&id, &value, model, &mut tctx, &mut ctx, None)
      }).collect()
  }

  pub fn get_by_ids<U, F>(
      &self,
      ids: &[Vec<u8>],
      model: &Entity,
      select: &MarciSelect,
      f: F
  ) -> Vec<U>
  where
    F: Fn(DecodeCtx<'_, U>) -> U,
  {
      let rx = self.db.begin_read().unwrap();
      let tree = rx.get_tree(model.name.as_bytes()).unwrap().unwrap();

      let mut tctx = TransationContext::new(&rx, f);
      let mut ctx = ProcessDataContext::new(select);

      ids.iter().map(|id| {
        let value = tree.get(id).unwrap().unwrap();
        process_data(&id, &value, model, &mut tctx, &mut ctx, None)
      }).collect()
  }

  pub fn get_all_filter<U, F>(
      &self,
      model: &Entity,
      select: &MarciSelect,
      f: F
  ) -> Vec<U>
  where
    F: Fn(DecodeCtx<'_, Option<U>>) -> Option<U>,
  {
      let rx = self.db.begin_read().unwrap();
      let tree = rx.get_tree(model.name.as_bytes()).unwrap().unwrap();

      let mut tctx = TransationContext::new(&rx, f);
      let mut ctx = ProcessDataContext::new(select);

      tree.iter().unwrap().filter_map(|item| {
          let (id, value) = item.unwrap();
          
          process_data(&id, &value, model, &mut tctx, &mut ctx, None)
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

      let updated_data = update_data(&model, &data, new_data, &changed_mask);
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

            let updated_data = update_data(&st, &data.as_ref(), new_data, &changed_mask);
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

  pub fn delete(&self, model_index: usize, model: &Entity, id: &[u8]) -> Result<(), DeleteError> {
    let tx = self.db.begin_write().unwrap();
    {
      let mut tree = tx.get_tree(model.name.as_bytes()).unwrap().unwrap();
      if !tree.delete(id).unwrap() {
        return Err(DeleteError::ItemNotFound);
      }
    }

    for field in model.fields.iter() {
      match &field.ty {
          FieldType::Struct(st) => {
            let mut tree = tx.get_tree(st.name.as_bytes()).unwrap().unwrap();
            tree.delete(id).unwrap();
          },
          FieldType::StructList(st) => {
            let mut tree = tx.get_tree(st.name.as_bytes()).unwrap().unwrap();
            let end = increment_bytes_be(id);
            tree.delete_range(id..&end).unwrap();
          }
          _ => {}
      }
    }

    for (field_ref, constraint) in self.schema.foreign_bindings[model_index].iter() {
      let field = self.schema.get_field(field_ref);
      let entity = self.schema.get_field_entity(field_ref);

      match &field.ty {
          FieldType::ModelRef(idx) => {
            if *idx != model_index { println!("Wrong foreign_key. Skip it"); continue; }

            // В обратном поле rev_index - это direct к нашему. И также direct_index - это reverse к нам
            let keys = match field.get_rev_index()  {
              Some(rev_index) => {
                find_by_direct(&tx, rev_index.tree_name(), id)
              }
              None => {
                find_keys_by_field(&tx, entity, field, id)
              }
            };
            if keys.is_empty() {
              continue;
            }

            match constraint {
                &DeleteConstraint::Cascade => {
                  let mut tree = tx.get_tree(entity.name.as_bytes()).unwrap().unwrap();
                  for key in keys.iter() {
                    tree.delete(key).unwrap();
                  }
                  println!("Cascade delete {} documents from {}", keys.len(), entity.name);
                }
                &DeleteConstraint::SetNull => {
                  let mut tree = tx.get_tree(entity.name.as_bytes()).unwrap().unwrap();
                  for key in keys.iter() {
                    let data = tree.get(key).unwrap().unwrap();
                    let Some(new_data) = set_field_null(entity, &data, field_ref.field_index, field_ref.enum_variant_index) else {
                      continue;
                    };
                    tree.insert(key, &new_data).unwrap();
                  }
                }
                &DeleteConstraint::Restrict => {
                  return Err(DeleteError::RestrictConstraints(
                    field.full_name.clone(), 
                    keys.iter().map(|i| u64::from_be_bytes(i.as_slice().try_into().unwrap())).collect()
                  ));
                }
                _ => {}
            }
          },
          FieldType::ModelRefList(idx) => {
            if *idx != model_index { println!("Wrong foreign_key. Skip it"); continue; }
            
            // В обратном поле rev_index - это direct к нашему. И также direct_index - это reverse к нам
            let keys = match field.get_rev_index()  {
              Some(rev_index) => {
                find_by_direct(&tx, rev_index.tree_name(), id)
              }
              None => match field.get_direct_index() {
                Some(direct) => find_by_rev(&tx, direct.tree_name(), id),
                None => vec![]
              }
            };
            if keys.is_empty() {
              continue;
            }
            
            match constraint {
              &DeleteConstraint::Cascade => {
                panic!("You cannot using cascade delete in list field {}", field.name)
              }
              &DeleteConstraint::RemoveItem => {
                let rev_indexes = field.get_direct_indexes();
                remove_indexes_by_keys(&tx, id, &rev_indexes, &keys);
              }
              &DeleteConstraint::Restrict => {
                return Err(DeleteError::RestrictConstraints(
                  field.full_name.clone(), 
                  keys.iter().map(|i| u64::from_be_bytes(i.as_slice().try_into().unwrap())).collect()
                ));
              }
              _ => {}
            }
          },
          _ => {}
      }
    }

    // Delete another keys
    for field in model.fields.iter() {
      remove_indexes(&tx, field, id);
    }

    tx.commit().unwrap();
    return Ok(());
  }

}




// #[inline(always)]
// fn get_value<'a, const SIZE: usize>(
//     data: &'a [u8],
//     offset_pos: usize,
// ) -> Option<&'a [u8; SIZE]> {
//     if offset_pos == 0 {
//       panic!("ERROR: Try to get zero offset value")
//     }
//     let offset = get_offset(data, offset_pos);
//     if offset == 0 {
//         return None;
//     }
//     Some(data[offset..offset + SIZE].try_into().ok()?)
// }

#[inline(always)]
pub fn get_offset<'a>(data: &'a [u8], offset_pos: usize) -> usize {
  return u32::from_be_bytes(data[offset_pos..offset_pos + 4].try_into().unwrap()) as usize;
}

pub fn get_offset_from_field<'a>(data: &'a [u8], field: &Field) -> usize {
  if field.offset_pos == 0 {
    if field.id_idx.is_some() {
      panic!("ERROR: Try to get offet from ID field {}", field.full_name);
    } else {
      panic!("ERROR: Try to get offet from virtual field {}", field.full_name);
    }
  }
  return u32::from_be_bytes(data[field.offset_pos..field.offset_pos + 4].try_into().unwrap()) as usize;
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
pub fn find_by_direct(rx: &Transaction, tree_name: &[u8], item_id: &[u8]) -> Vec<Vec<u8>> {
  let index_tree = rx.get_tree(tree_name).unwrap()
    .unwrap_or_else(|| panic!("Index {} not found", str::from_utf8(tree_name).unwrap()));

  let iter = index_tree.prefix_keys(&item_id).unwrap();
  iter.map(|k| k.unwrap()[8..].to_vec()).collect()
}

#[inline(always)]
/// **Перебирает** все ключи, и находит нужные
pub fn find_by_rev(rx: &Transaction, tree_name: &[u8], item_id: &[u8]) -> Vec<Vec<u8>> {
  let index_tree = rx.get_tree(tree_name).unwrap()
    .unwrap_or_else(|| panic!("Index {} not found", str::from_utf8(tree_name).unwrap()));

  let mut arr = vec![];
  for key in index_tree.keys().unwrap() {
    let key = key.unwrap();
    if &key[8..] == item_id {
      arr.push(key[..8].to_vec());
    }
  }

  return arr;
}

fn find_keys_by_field(rx: &Transaction, entity: &Entity, field: &Field, value: &[u8]) -> Vec<Vec<u8>> {
  let tree = rx.get_tree(entity.name.as_bytes()).unwrap().unwrap();

  let mut arr = vec![];
  if let Some(id_idx) = field.id_idx {
    if id_idx == 0 {
      return tree
        .prefix_keys(&value)
        .unwrap()
        .map(|i| i.unwrap().to_vec()).collect()
    };

    for entry in tree.keys().unwrap() {
      let id = entry.unwrap();
      if get_value_from_id(&id, id_idx, field) == value {
        arr.push(id.to_vec());
      }
    }
    return arr;
  }

  if field.offset_pos == 0 {
    println!("Trying to find by virtual field {}", field.name);
    return vec![];
  }

  for entry in tree.iter().unwrap() {
    let (id, data) = &entry.unwrap();

    let offset = get_offset(data, field.offset_pos);
    if offset == 0 { continue; }
    
    if &data[offset..offset+value.len()] == value {
      arr.push(id.to_vec());
    }
  }

  return arr;
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
pub fn remove_indexes_by_keys(tx: &WriteTransaction, _id: &[u8], rev_indexes: &[&InsertedIndex], keys: &[Vec<u8>]) {
  for index in rev_indexes {
    let mut tree = tx.get_tree(index.tree_name()).unwrap().unwrap();
    for key in keys.iter() {
      // Simple variant for fixed keys:
      tree.delete(&[key,_id].concat()).unwrap();
    }
  }
}

#[inline(always)]
pub fn remove_indexes(tx: &WriteTransaction, field: &Field, id: &[u8]) {
  if field.inserted_indexes.is_empty() {
    return;
  }

  let Some(direct_index) = field.get_direct_index() else {
    panic!("Direct index must be defined for batch update. Field: {}", field.full_name);
  };
  
  let rev_indexes = field.get_rev_indexes();
  
  if !rev_indexes.is_empty() {
    let keys = find_by_direct(tx, direct_index.tree_name(), id);
    if keys.is_empty() {
      return;
    }
    remove_indexes_by_keys(tx, id, &rev_indexes, &keys);
  }

  for index in field.inserted_indexes.iter() {
    let InsertedIndex::Direct { tree_name } = index else { continue };
    let mut tree = tx.get_tree(tree_name.as_bytes()).unwrap().unwrap();
    let end = increment_bytes_be(id);
    tree.delete_range(id..&end).unwrap();
  }
}

#[cfg(test)]
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
