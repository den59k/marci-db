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
        model: &'a Entity,
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

      // Создаем индексы для полей
      for field in model.fields.iter_mut() {
        if let Some(index) = field.get_direct_index() {
          tx.get_or_create_tree(index.tree_name.as_bytes()).unwrap();
        }
        if let Some(index) = field.get_field_index(){
          tx.get_or_create_tree(index.tree_name.as_bytes()).unwrap();
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
        id[idx*8..idx*8+8].copy_from_slice(&field_id.to_be_bytes());
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
    println!("=== GET_ALL: {} ===", model.name);

    let rx = self.db.begin_read().unwrap();
    let tree = rx.get_tree(model.name.as_bytes()).unwrap().unwrap();

    // Вывод информации о созданных индексах модели
    println!("\nИндексы модели {}:", model.name);
    for (field_index, field) in model.fields.iter().enumerate() {
        if field.inserted_indexes.is_empty() {
            continue;
        }

        println!("  Поле '{}' (index: {}):", field.name, field_index);

        if let Some(direct) = field.get_direct_index() {
            let index_tree = rx.get_tree(direct.tree_name()).unwrap();
            if let Some(index_tree) = index_tree {
                let keys: Vec<_> = index_tree.keys().unwrap()
                    .map(|k| k.unwrap().to_vec())
                    .collect();
                println!("    - Direct индекс '{}': {} записей", direct.tree_name, keys.len());
                for (i, key) in keys.iter().enumerate() {
                    println!("      [{}] {:?} (len: {})", i, key, key.len());
                    if key.len() >= 8 {
                        let id = u64::from_be_bytes(key[..8].try_into().unwrap());
                        println!("          ID prefix: {}", id);
                        if key.len() >= 16 {
                            let value = u64::from_be_bytes(key[8..16].try_into().unwrap());
                            println!("          Value: {}", value);
                        }
                    }
                }
            } else {
                println!("    - Direct индекс '{}': дерево не найдено", direct.tree_name);
            }
        }

        if let Some(rev) = field.get_rev_index() {
            let index_tree = rx.get_tree(rev.tree_name()).unwrap();
            if let Some(index_tree) = index_tree {
                let keys: Vec<_> = index_tree.keys().unwrap()
                    .map(|k| k.unwrap().to_vec())
                    .collect();
                println!("    - Reverse индекс '{}': {} записей", rev.tree_name, keys.len());
                for (i, key) in keys.iter().enumerate() {
                    println!("      [{}] {:?} (len: {})", i, key, key.len());
                    if key.len() >= 8 {
                        let value = u64::from_be_bytes(key[..8].try_into().unwrap());
                        println!("          Value prefix: {}", value);
                        if key.len() >= 16 {
                            let id = u64::from_be_bytes(key[8..16].try_into().unwrap());
                            println!("          ID: {}", id);
                        }
                    }
                }
            } else {
                println!("    - Reverse индекс '{}': дерево не найдено", rev.tree_name);
            }
        }

        if let Some(field_idx) = field.get_field_index() {
            let index_tree = rx.get_tree(field_idx.tree_name()).unwrap();
            if let Some(index_tree) = index_tree {
                let keys: Vec<_> = index_tree.keys().unwrap()
                    .map(|k| k.unwrap().to_vec())
                    .collect();
                println!("    - Field индекс '{}': {} записей", field_idx.tree_name, keys.len());
                for (i, key) in keys.iter().enumerate() {
                    println!("      [{}] {:?} (len: {})", i, key, key.len());

                    // Для field индекса структура: [value][null-terminator?][id]
                    // ID всегда последние 8 байт
                    if key.len() >= 8 {
                        let id_start = key.len() - 8;
                        let id = u64::from_be_bytes(key[id_start..].try_into().unwrap());
                        println!("          ID (last 8 bytes): {}", id);

                        // Выводим значение (всё до ID)
                        let value_bytes = &key[..id_start];
                        println!("          Value bytes: {:?}", value_bytes);

                        // Пытаемся интерпретировать как строку (если это строковое поле)
                        if let Ok(s) = std::str::from_utf8(value_bytes) {
                            println!("          Value as string: '{}'", s.trim_end_matches('\0'));
                        }
                    }
                }
            } else {
                println!("    - Field индекс '{}': дерево не найдено", field_idx.tree_name);
            }
        }
    }

    let record_count = tree.iter().unwrap().count();
    println!("\nОсновное дерево '{}': {} записей", model.name, record_count);

    // Выводим ключи основного дерева
    println!("Ключи основного дерева:");
    for (i, key) in tree.keys().unwrap().enumerate() {
        let key = key.unwrap();
        println!("  [{}] {:?} (len: {})", i, key, key.len());
        if key.len() >= 8 {
            let id = u64::from_be_bytes(key[..8].try_into().unwrap());
            println!("      ID: {}", id);
        }
    }

    println!("=================================\n");

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
    if ids.is_empty() {
      return vec![];
    }
  
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

  println!("=== UPDATE START ===");
  println!("Model: {}", model.name);
  println!("ID: {:?}", id);
  println!("New data length: {}", new_data.len());
  println!("Changed mask: {:?}", changed_mask);
  println!("Structs count: {}", structs.len());

  let foreign_keys = collect_foreign_keys(id, new_data, model, structs, &self.schema);
  println!("Foreign keys collected: {}", foreign_keys.len());

  let mut indexes = get_indexes(new_data, id, model, None);
  println!("Initial indexes count: {}", indexes.len());

  for st in structs {
    match st {
      InsertStruct::One { st, data, .. } => {
        println!("Processing InsertStruct::One for struct: {}", st.name);
        let new_indexes = get_indexes(data, id, *st, None);
        println!("  Added {} indexes", new_indexes.len());
        indexes.extend(new_indexes);
      }
      _ => {}
    }
  }
  println!("Total indexes after collection: {}", indexes.len());

  let mut indexes_to_remove = vec![];

  println!("Beginning transaction...");
  let tx = self.db.begin_write().unwrap();

  println!("Checking foreign keys...");
  check_foreign_keys(&tx, &foreign_keys)?;
  println!("Foreign keys OK");

  // Обновляем значение. Выдаем ошибку, если значения не существует
  {
    println!("Opening main tree: {}", model.name);
    let mut tree = tx.get_tree(model.name.as_bytes()).unwrap().unwrap();

    println!("Fetching existing data...");
    let Some(data) = tree.get(id).unwrap() else {
      println!("ERROR: Item not found!");
      return Err(InsertError::ItemNotFound)
    };
    println!("Existing data length: {}", data.len());

    println!("Updating data...");
    let updated_data = update_data(&model, &data, new_data, &changed_mask);
    println!("Updated data length: {}", updated_data.len());

    tree.insert(id, &updated_data).unwrap();
    println!("Data inserted successfully");

    let old_indexes = get_indexes(&data, id, model, Some(&changed_mask));
    println!("Old indexes to remove: {}", old_indexes.len());
    indexes_to_remove.extend(old_indexes);
  };

  println!("\n--- Processing dependent structs ---");
  // Добавляем зависимые структуры
  for (i, st) in structs.iter().enumerate() {
    println!("Struct #{}: {:?}", i, std::mem::discriminant(st));
    match st {
      InsertStruct::Empty { st } => {
          println!("  Empty struct: {}", st.name);

          // Удаляем все индексы для полей структуры
          for field in st.fields.iter() {
              if field.inserted_indexes.is_empty() {
                  continue;
              }
              println!("    Removing indexes for field: {}", field.name);
              remove_indexes(&tx, &self.schema, st, field, id);
          }

          // Удаляем сами записи структуры
          let mut struct_tree = tx.get_tree(st.name.as_bytes()).unwrap().unwrap();
          let end = increment_bytes_be(id);
          struct_tree.delete_range(id..&end).unwrap();
          println!("  Struct records deleted");
      }
      InsertStruct::Many { st, data: new_data, .. } => {
          println!("  Many struct: {}, items: {}", st.name, new_data.len());

          // Удаляем все индексы для полей структуры
          for field in st.fields.iter() {
              if field.inserted_indexes.is_empty() {
                  continue;
              }
              println!("    Removing indexes for field: {}", field.name);
              remove_indexes(&tx, &self.schema, st, field, id);
          }

          // Удаляем старые записи структуры
          let mut struct_tree = tx.get_tree(st.name.as_bytes()).unwrap().unwrap();
          let end = increment_bytes_be(id);
          struct_tree.delete_range(id..&end).unwrap();
          println!("    Old struct records deleted");

          // Добавляем новые записи
          for (idx, (item_id, item_data)) in new_data.iter().enumerate() {
              let mut new_item_id = item_id.clone();
              new_item_id[0..8].copy_from_slice(id);

              println!("    Item #{}: ID before counter: {:?}", idx, &new_item_id[..16.min(new_item_id.len())]);
              self.insert_counter_value(*st, &mut new_item_id);
              println!("    Item #{}: ID after counter: {:?}", idx, &new_item_id[..16.min(new_item_id.len())]);

              struct_tree.insert(&new_item_id, item_data).unwrap();

              let item_indexes = get_indexes(item_data, &new_item_id, *st, None);
              println!("    Item #{}: Added {} indexes", idx, item_indexes.len());
              indexes.extend(item_indexes);
          }

          println!("    Many struct processing complete");
      }
      InsertStruct::One { st, data: new_data, changed_mask } => {
        println!("  One struct: {}", st.name);
        let mut tree = tx.get_tree(st.name.as_bytes()).unwrap().unwrap();
        if let Some(data) = tree.get(id).unwrap() {
          println!("    Existing data found, updating...");
          let updated_data = update_data(&st, &data.as_ref(), new_data, &changed_mask);
          tree.insert(id, &updated_data).unwrap();

          let old_indexes = get_indexes(&data, id, *st, Some(&changed_mask));
          println!("    Old indexes to remove: {}", old_indexes.len());
          indexes_to_remove.extend(old_indexes);
        } else {
          println!("    No existing data, inserting new...");
          tree.insert(id, new_data).unwrap()
        }
      }
      InsertStruct::Connect { field, ids, model, .. } => {
        println!("  Connect: field={}, model={}, ids count={}", field.name, model.name, ids.len());
        remove_indexes(&tx, &self.schema, model, &field, id);
        insert_indexes(&tx, field, id, ids);
        println!("    Indexes updated");
      },
      InsertStruct::None { st } => {
        println!("  None struct: {}", st.name);
        let mut tree = tx.get_tree(st.name.as_bytes()).unwrap().unwrap();
        tree.delete(id).unwrap();
        println!("    Deleted");
      },
      _ => {
        println!("  Other struct variant");
      }
    }
  }

  println!("\n--- Removing old indexes ---");
  println!("Total indexes to remove: {}", indexes_to_remove.len());
  for (i, index) in indexes_to_remove.iter().enumerate() {
    println!("  Removing index #{}: tree={}, key_len={}", i,
             String::from_utf8_lossy(index.tree_name), index.key.len());
    let mut index_tree = tx.get_tree(index.tree_name).unwrap().unwrap();
    index_tree.delete(&index.key).unwrap();
  }

  println!("\n--- Inserting new indexes ---");
  println!("Total indexes to insert: {}", indexes.len());
  for (i, index) in indexes.iter().enumerate() {
    println!("  Inserting index #{}: tree={}, key_len={}", i,
             String::from_utf8_lossy(index.tree_name), index.key.len());
    let mut index_tree = tx.get_tree(index.tree_name).unwrap().unwrap();
    index_tree.insert(&index.key, &[1]).unwrap();
  }

  println!("\nCommitting transaction...");
  tx.commit().unwrap();
  println!("=== UPDATE COMPLETE ===\n");

  return Ok(());
}

pub fn delete(&self, model_index: usize, model: &Entity, id: &[u8]) -> Result<(), DeleteError> {
    println!("=== НАЧАЛО УДАЛЕНИЯ ===");
    println!("Модель: {}, ID: {:?}", model.name, id);

    let tx = self.db.begin_write().unwrap();

    println!("\n2. Обработка полей модели:");
    for field in model.fields.iter() {
        println!("   - Поле '{}':", field.name);
        if let Some(field_index) = field.get_field_index() {
            let data_tree = tx.get_tree(model.name.as_bytes()).unwrap().unwrap();

            if let Some(data) = data_tree.get(id).unwrap() {
                let known_size = field.get_size();
                if let Some(value) = get_value_from_data(field, id, &data, known_size) {
                    // Строим ключ индекса и удаляем его
                    let index_key = build_index_value(id, value, known_size.is_some());
                    let mut index_tree = tx.get_tree(field_index.tree_name()).unwrap().unwrap();
                    index_tree.delete(&index_key).unwrap();
                    println!("     ✓ Field индекс удален");
                } else {
                    println!("     ✗ Не удалось получить значение поля для удаления индекса");
                }
            } else {
                // Это не должно происходить, так как запись должна существовать
                println!("     ⚠️  Данные записи не найдены (уже удалены?)");
            }
        } else {
            println!("     ✗ Field индекс отсутствует");
        }

        match &field.ty {
            FieldType::Struct(st) => {
                let mut tree = tx.get_tree(st.name.as_bytes()).unwrap().unwrap();
                println!("   - Структура '{}': удаление по ключу {:?}", st.name, id);
                tree.delete(id).unwrap();
                println!("     ✓ Удалено");
            },
            FieldType::StructList(st) => {
                let mut tree = tx.get_tree(st.name.as_bytes()).unwrap().unwrap();
                let end = increment_bytes_be(id);
                println!("   - Список структур '{}': удаление диапазона {:?}..{:?}",
                         st.name, id, end);
                tree.delete_range(id..&end).unwrap();
                println!("     ✓ Диапазон удален");
            }
            _ => {
                println!("   - Поле '{}': тип {:?} - пропускаем", field.name, field.ty);
            }
        }
    }

    {
        let mut tree = tx.get_tree(model.name.as_bytes()).unwrap().unwrap();
        println!("1. Удаление основной записи из дерева '{}'", model.name);
        if !tree.delete(id).unwrap() {
            println!("   ОШИБКА: Запись не найдена");
            return Err(DeleteError::ItemNotFound);
        }
        println!("   ✓ Запись удалена");
    }

    println!("\n3. Обработка внешних ключей:");
    for (field_ref, constraint) in self.schema.foreign_bindings[model_index].iter() {
        let field = self.schema.get_field(field_ref);
        let entity = self.schema.get_field_entity(field_ref);

        println!("   - Поле: {}, Сущность: {}, Ограничение: {:?}",
                 field.name, entity.name, constraint);
        match &field.ty {
            FieldType::ModelRef(idx) => {
                println!("     Тип: ModelRef, целевая модель: {}", *idx);
                if *idx != model_index {
                    println!("     ⚠️  Несоответствие индексов ({} != {}). Пропускаем", *idx, model_index);
                    continue;
                }

                let keys = match field.get_rev_index()  {
                    Some(rev_index) => {
                        println!("     Поиск по reverse индексу: {}", rev_index.tree_name);
                        find_by_direct(&tx, rev_index.tree_name.as_bytes(), id)
                    }
                    None => {
                        println!("     Поиск по значению поля");
                        find_keys_by_field(&tx, entity, field, id)
                    }
                };

                println!("     Найдено связанных записей: {}", keys.len());
                if !keys.is_empty() {
                    println!("     Ключи: {:?}", keys);
                }

                if keys.is_empty() {
                    println!("     ✓ Нет связанных записей");
                    continue;
                }

                match constraint {
                    &DeleteConstraint::Cascade => {
                        println!("     Действие: CASCADE - удаление всех связанных записей");
                        let mut tree = tx.get_tree(entity.name.as_bytes()).unwrap().unwrap();
                        for key in keys.iter() {
                            println!("       Удаление ключа {:?} из {}", key, entity.name);
                            tree.delete(key).unwrap();
                        }
                        println!("     ✓ Каскадное удаление {} документов из {}",
                                keys.len(), entity.name);
                    }
                    &DeleteConstraint::SetNull => {
                        println!("     Действие: SET NULL - установка поля в NULL");
                        let mut updates = Vec::new();
                        let mut tree = tx.get_tree(entity.name.as_bytes()).unwrap().unwrap();
                        for key in keys.iter() {
                            println!("       Обработка ключа {:?}", key);
                            let data = tree.get(key).unwrap().unwrap();
                            let Some(new_data) = set_field_null(entity, &data,
                                                                field_ref.field_index,
                                                                field_ref.enum_variant_index) else {
                                println!("       ⚠️  Не удалось установить NULL. Пропускаем.");
                                continue;
                            };
                            updates.push((key.clone(), new_data));
                            println!("       ✓ Данные подготовлены для обновления");
                        }
                        if !updates.is_empty() {
                            for (key, new_data) in updates {
                                tree.insert(&key, &new_data).unwrap();
                            }
                            println!("       ✓ Все поля установлены в NULL");
                        }
                    }
                    &DeleteConstraint::Restrict => {
                        println!("     Действие: RESTRICT - запрет удаления");
                        let ids: Vec<u64> = keys.iter()
                            .filter_map(|key| {
                                match key.len() {
                                    8 => {
                                        let bytes: [u8; 8] = key.as_slice().try_into().ok()?;
                                        Some(u64::from_be_bytes(bytes))
                                    },
                                    16 => {
                                        let bytes: [u8; 8] = key[..8].try_into().ok()?;
                                        Some(u64::from_be_bytes(bytes))
                                    },
                                    _ => None
                                }
                            })
                            .collect();
                        println!("     ⚠️  Нарушение ограничения. Связанные ID: {:?}", ids);
                        return Err(DeleteError::RestrictConstraints(
                            field.full_name.clone(),
                            ids
                        ));
                    }
                    _ => {
                        println!("     Действие: {:?} - не обрабатывается", constraint);
                    }
                }
            },
            FieldType::ModelRefList(idx) => {
                println!("     Тип: ModelRefList, целевая модель: {}", *idx);
                if *idx != model_index {
                    println!("     ⚠️  Несоответствие индексов ({} != {}). Пропускаем", *idx, model_index);
                    continue;
                }

                let keys = match field.get_rev_index()  {
                    Some(rev_index) => {
                        println!("     Поиск по reverse индексу: {}", rev_index.tree_name);
                        find_by_direct(&tx, rev_index.tree_name.as_bytes(), id)
                    }
                    None => match field.get_direct_index() {
                        Some(direct) => {
                            println!("     Поиск по direct индексу: {}", direct.tree_name);
                            find_by_rev(&tx, direct.tree_name.as_bytes(), id, &self.schema)
                        },
                        None => {
                            println!("     ⚠️  Индексы не найдены");
                            vec![]
                        }
                    }
                };

                println!("     Найдено ключей: {}", keys.len());
                if !keys.is_empty() {
                    println!("     Ключи: {:?}", keys);
                }

                if keys.is_empty() {
                    println!("     ✓ Нет связанных записей");
                    continue;
                }

                match constraint {
                    &DeleteConstraint::Cascade => {
                        println!("     ⚠️  ОШИБКА: Каскадное удаление не поддерживается для списков");
                        panic!("You cannot using cascade delete in list field {}", field.name)
                    }
                    &DeleteConstraint::RemoveItem => {
                        println!("     Действие: REMOVE_ITEM - удаление из списка");
                        if let Some(direct_index) = field.get_direct_index() {
                            println!("     Удаление индексов из дерева: {}", direct_index.tree_name);
                            remove_indexes_by_keys(&tx, id, &direct_index, &keys);
                            println!("     ✓ Индексы удалены");
                        } else {
                            println!("     ⚠️  Reverse индекс не найден");
                        }
                    }
                    &DeleteConstraint::Restrict => {
                        println!("     Действие: RESTRICT - запрет удаления");
                        let ids: Vec<u64> = keys.iter()
                            .filter_map(|key| {
                                match key.len() {
                                    8 => {
                                        let bytes: [u8; 8] = key.as_slice().try_into().ok()?;
                                        Some(u64::from_be_bytes(bytes))
                                    },
                                    16 => {
                                        let bytes: [u8; 8] = key[..8].try_into().ok()?;
                                        Some(u64::from_be_bytes(bytes))
                                    },
                                    _ => None
                                }
                            })
                            .collect();
                        println!("     ⚠️  Нарушение ограничения. Связанные ID: {:?}", ids);
                        return Err(DeleteError::RestrictConstraints(
                            field.full_name.clone(),
                            ids
                        ));
                    }
                    _ => {
                        println!("     Действие: {:?} - не обрабатывается", constraint);
                    }
                }
            },
            _ => {
                println!("     Тип {:?} - не обрабатывается", field.ty);
            }
        }
    }

    println!("\n4. Удаление direct индексов для полей без ограничений:");
    for field in model.fields.iter() {
        let Some(direct_index) = field.get_direct_index() else {
            continue;
        };

        println!("   - Поле '{}': удаление direct индекса '{}'", field.name, direct_index.tree_name);
        remove_indexes(&tx, &self.schema, model, field, id);
        println!("     ✓ Индекс очищен");
    }

    println!("\n5. Фиксация транзакции");
    tx.commit().unwrap();

    println!("=== УДАЛЕНИЕ УСПЕШНО ЗАВЕРШЕНО ===");
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
          if let Some(bytes) = get_value_from_data(field, id, data, Some(8)) {
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
      InsertStruct::Connect { field, ids, model } => {
        for item_id in ids.iter() {
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
pub fn find_by_rev(rx: &Transaction, tree_name: &[u8], item_id: &[u8], schema: &Schema) -> Vec<Vec<u8>> {
  println!("[DEBUG] find_by_rev called");
  println!("[DEBUG] tree_name: {:?} ({})", tree_name, str::from_utf8(tree_name).unwrap_or("<invalid UTF-8>"));
  println!("[DEBUG] item_id: {:?} (len: {})", item_id, item_id.len());

  let index_tree = rx.get_tree(tree_name).unwrap()
    .unwrap_or_else(|| panic!("Index {} not found", str::from_utf8(tree_name).unwrap()));

  // Парсим имя дерева чтобы получить имя модели
  // Формат: "Model.field" где Model - модель с direct индексом
  let tree_name_str = str::from_utf8(tree_name).unwrap();
  let model_name = tree_name_str.split('.').next().unwrap();

  println!("[DEBUG] Parsed model name from tree: {}", model_name);

  // Находим модель по имени
  let target_model = schema.models.iter()
    .find(|m| m.name == model_name)
    .unwrap_or_else(|| panic!("Model {} not found in schema", model_name));

  let key_min_size = target_model.key_min_size();
  println!("[DEBUG] Target model key_min_size: {}", key_min_size);

  println!("[DEBUG] Tree opened successfully");

  let mut arr = vec![];
  let mut total_keys = 0;
  let mut matched_keys = 0;

  for key in index_tree.keys().unwrap() {
    let key = key.unwrap();
    total_keys += 1;

    println!("[DEBUG] Key #{}: {:?} (len: {})", total_keys, key, key.len());

    // Проверяем, что ключ достаточно длинный
    if key.len() < key_min_size + item_id.len() {
      println!("[DEBUG]   - WARNING: Key too short, skipping");
      continue;
    }

    let prefix = &key[..key_min_size];
    let suffix = &key[key_min_size..];

    println!("[DEBUG]   - Prefix ({} bytes): {:?}", key_min_size, prefix);
    println!("[DEBUG]   - Suffix (from byte {} onwards): {:?}", key_min_size, suffix);

    // Проверяем, что суффикс НАЧИНАЕТСЯ с item_id
    if suffix.starts_with(item_id) {
      matched_keys += 1;
      println!("[DEBUG]   - MATCH FOUND! Adding prefix to result");
      arr.push(prefix.to_vec());
    } else {
      println!("[DEBUG]   - No match");
    }
  }

  println!("[DEBUG] Scan complete: {} total keys, {} matches", total_keys, matched_keys);
  println!("[DEBUG] Returning {} results", arr.len());

  return arr;
}

fn find_keys_by_field(rx: &Transaction, entity: &Entity, field: &Field, value: &[u8]) -> Vec<Vec<u8>> {
  println!("=== find_keys_by_field DEBUG ===");
  println!("Entity: {}", entity.name);
  println!("Field: {}", field.name);
  println!("Search value: {:?} (len: {})", value, value.len());
  println!("Field id_idx: {:?}", field.id_idx);
  println!("Field offset_pos: {}", field.offset_pos);

  let tree = rx.get_tree(entity.name.as_bytes()).unwrap().unwrap();
  println!("Tree opened successfully");

  let mut arr = vec![];

  // Поиск по id_idx
  if let Some(id_idx) = field.id_idx {
    println!("Searching by id_idx: {}", id_idx);

    if id_idx == 0 {
      println!("Using prefix_keys search");
      let results: Vec<Vec<u8>> = tree
        .prefix_keys(&value)
        .unwrap()
        .map(|i| i.unwrap().to_vec())
        .collect();
      println!("Found {} results via prefix_keys", results.len());
      return results;
    }

    println!("Iterating through all keys");
    let mut checked = 0;
    for entry in tree.keys().unwrap() {
      let id = entry.unwrap();
      let extracted_value = get_value_from_id(&id, id_idx, field);

      if checked < 5 {  // Выводим первые 5 для примера
        println!("  Key #{}: {:?}, extracted value: {:?}", checked, id, extracted_value);
      }

      if extracted_value == value {
        println!("  ✓ MATCH found for key: {:?}", id);
        arr.push(id.to_vec());
      }
      checked += 1;
    }
    println!("Checked {} keys, found {} matches", checked, arr.len());
    return arr;
  }

  // Проверка виртуального поля
  if field.offset_pos == 0 {
    println!("⚠ Trying to find by virtual field {}", field.name);
    return vec![];
  }

  // Поиск по offset_pos
  println!("Searching by offset_pos: {}", field.offset_pos);
  let mut checked = 0;

  for entry in tree.iter().unwrap() {
    let (id, data) = &entry.unwrap();

    let offset = get_offset(data, field.offset_pos);

    if checked < 5 {  // Выводим первые 5 для примера
      println!("  Entry #{}: id={:?}, data_len={}, offset={}",
               checked, id, data.len(), offset);
    }

    if offset == 0 {
      if checked < 5 {
        println!("    Skipping (offset=0)");
      }
      checked += 1;
      continue;
    }

    if offset + value.len() > data.len() {
      println!("    ⚠ WARNING: offset({}) + value.len({}) > data.len({})",
               offset, value.len(), data.len());
      checked += 1;
      continue;
    }

    let data_slice = &data[offset..offset+value.len()];

    if checked < 5 {
      println!("    Comparing data_slice={:?} with value={:?}", data_slice, value);
    }

    if data_slice == value {
      println!("  ✓ MATCH found for id: {:?}", id);
      arr.push(id.to_vec());
    }

    checked += 1;
  }

  println!("Checked {} entries, found {} matches", checked, arr.len());
  println!("=== END DEBUG ===\n");

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

    println!("  >> get_indexes called");
    println!("     Model: {}", model.name);
    println!("     ID: {:?}", id);
    println!("     Data length: {}", data.len());
    println!("     Mask: {:?}", mask);
    println!("     Total fields: {}", model.fields.len());

    let mut indexes = vec![];
    let mut skipped_no_indexes = 0;
    let mut skipped_derived = 0;
    let mut skipped_not_changed = 0;
    let mut skipped_no_value = 0;

    for (field_index, field) in model.fields.iter().enumerate(){

      println!("     Field #{} ({}): checking...", field_index, field.name);

      // Skip values without indexes
      if field.inserted_indexes.is_empty() {
        println!("       -> Skipped: no indexes");
        skipped_no_indexes += 1;
        continue;
      }

      // Skip derived values
      if field.id_idx.is_none() && field.offset_pos == 0 {
        println!("       -> Skipped: derived value (id_idx={:?}, offset_pos={})",
                 field.id_idx, field.offset_pos);
        skipped_derived += 1;
        continue;
      }

      // Skip not changed values
      if mask.is_some_and(|f| !f[field_index]) {
        println!("       -> Skipped: not changed in mask");
        skipped_not_changed += 1;
        continue;
      }

      let size = field.get_size();
      println!("       Field size: {:?}", size);

      let Some(value) = get_value_from_data(field, id, data, size) else {
        println!("       -> Skipped: no value extracted");
        skipped_no_value += 1;
        continue;
      };

      println!("       Value extracted: {} bytes", value.len());

      if let Some(index) = field.get_rev_index() {
        let mut key = Vec::with_capacity(value.len() + id.len());
        key.extend_from_slice(value);
        key.extend_from_slice(id);
        println!("       + Rev index: tree={}, key_len={}",
                 String::from_utf8_lossy(index.tree_name()), key.len());
        indexes.push(IndexData { tree_name: index.tree_name(), key });
      }

      if let Some(index) = field.get_direct_index() {
        let mut key = Vec::with_capacity(value.len() + id.len());
        key.extend_from_slice(id);
        key.extend_from_slice(value);
        println!("       + Direct index: tree={}, key_len={}",
                 String::from_utf8_lossy(index.tree_name()), key.len());
        indexes.push(IndexData { tree_name: index.tree_name(), key });
      }

      if let Some(index) = field.get_field_index() {
        let key = build_index_value(id, value, size.is_some());
        println!("       + Field index: tree={}, key_len={}",
                 String::from_utf8_lossy(index.tree_name()), key.len());
        indexes.push(IndexData { tree_name: index.tree_name(), key });
      }
    }

    println!("     Summary:");
    println!("       Skipped (no indexes): {}", skipped_no_indexes);
    println!("       Skipped (derived): {}", skipped_derived);
    println!("       Skipped (not changed): {}", skipped_not_changed);
    println!("       Skipped (no value): {}", skipped_no_value);
    println!("       Total indexes created: {}", indexes.len());
    println!("  << get_indexes returns {} indexes", indexes.len());

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
// Метод, который используется в Connect запросах. field поле здесь игнорируется
fn insert_indexes(tx: &WriteTransaction, field: &Field, id: &[u8], ids: &[Vec<u8>]) {
  if ids.is_empty() {
    return;
  }

  if let Some(index) = field.get_direct_index() {
    let mut tree = tx.get_tree(index.tree_name()).unwrap().unwrap();
    for cid in ids { insert_index(&mut tree, id, cid); }
  }
  if let Some(index) = field.get_rev_index() {
    let mut tree = tx.get_tree(index.tree_name()).unwrap().unwrap();
    for cid in ids { insert_index(&mut tree, cid, id); }
  }
  // if let Some(index) = field.get_field_index() {
    // let mut tree = tx.get_tree(index.tree_name()).unwrap().unwrap();
    // for cid in ids { insert_index(&mut tree, id, cid); },
  // }
}

#[inline(always)]
pub fn remove_indexes_by_keys(tx: &WriteTransaction, id: &[u8], rev_index: &InsertedIndex, keys: &[Vec<u8>]) {
  let mut tree = tx.get_tree(rev_index.tree_name()).unwrap().unwrap();

  for key in keys.iter() {
    // Строим префикс: key + id
    let mut prefix = Vec::with_capacity(key.len() + id.len());
    prefix.extend_from_slice(key);
    prefix.extend_from_slice(id);

    // Удаляем все записи, начинающиеся с этого префикса
    let end = increment_bytes_be(&prefix);
    tree.delete_range(&prefix[..]..&end).unwrap();
  }
}

#[inline(always)]
pub fn remove_indexes(tx: &WriteTransaction, schema: &Schema, model: &Entity, field: &Field, id: &[u8]) {
  if field.inserted_indexes.is_empty() {
    return;
  }

  // Случай 1: Есть direct индекс - используем его для поиска связанных ключей
  if let Some(direct_index) = field.get_direct_index() {
    // Находим все связанные ключи через direct индекс
    let keys = find_by_direct(tx, direct_index.tree_name(), id);

    // Удаляем reverse индексы для найденных ключей
    if let Some(rev_index) = field.get_rev_index() {
      if !keys.is_empty() {
        remove_indexes_by_keys(tx, id, &rev_index, &keys);
      }
    }

    // Удаляем field индексы для найденных ключей
    if let Some(field_index) = field.get_field_index() {
      if !keys.is_empty() {
        let data_tree = tx.get_tree(model.name.as_bytes()).unwrap().unwrap();
        let mut index_tree = tx.get_tree(field_index.tree_name()).unwrap().unwrap();

        for key in keys {
          let Some(data) = data_tree.get(&key).unwrap() else {
            continue;
          };
          let known_size = field.get_size();
          let Some(value) = get_value_from_data(field, id, &data, known_size) else {
            continue;
          };
          index_tree.delete(&build_index_value(&key, value, known_size.is_some())).unwrap();
        }
      }
    }

    // Удаляем сам direct индекс (все записи с префиксом id)
    let mut tree = tx.get_tree(direct_index.tree_name()).unwrap().unwrap();
    let end = increment_bytes_be(id);
    tree.delete_range(id..&end).unwrap();

    return;
  }

  if let Some(rev_index) = field.get_rev_index() {
    // Находим все ключи, которые ссылаются на наш id
    // find_by_rev возвращает префиксы (referenced_id части)
    let keys = find_by_rev(tx, rev_index.tree_name(), id, schema);

    if !keys.is_empty() {
      // Удаляем найденные записи из rev индекса
      remove_indexes_by_keys(tx, id, &rev_index, &keys);

      // Если есть field индекс, удаляем его записи для найденных ключей
      if let Some(field_index) = field.get_field_index() {
        let data_tree = tx.get_tree(model.name.as_bytes()).unwrap().unwrap();
        let mut index_tree = tx.get_tree(field_index.tree_name()).unwrap().unwrap();

        for key in keys {
          let Some(data) = data_tree.get(&key).unwrap() else {
            continue;
          };
          let known_size = field.get_size();
          let Some(value) = get_value_from_data(field, id, &data, known_size) else {
            continue;
          };
          index_tree.delete(&build_index_value(&key, value, known_size.is_some())).unwrap();
        }
      }
    }

    return;
  }

  // Случай 3: Только field индекс (без direct/rev)
  if let Some(field_index) = field.get_field_index() {
    let data_tree = tx.get_tree(model.name.as_bytes()).unwrap().unwrap();

    // Получаем данные записи
    let Some(data) = data_tree.get(id).unwrap() else {
      return;
    };

    let known_size = field.get_size();
    let Some(value) = get_value_from_data(field, id, &data, known_size) else {
      return;
    };

    // Удаляем field индекс
    let mut index_tree = tx.get_tree(field_index.tree_name()).unwrap().unwrap();
    let index_key = build_index_value(id, value, known_size.is_some());
    index_tree.delete(&index_key).unwrap();
  }
}

fn build_index_value(id: &[u8], value: &[u8], has_known_size: bool) -> Vec<u8> {
  let mut out_value = Vec::with_capacity(value.len() + id.len() + (if has_known_size { 0 } else { 1 }));
  out_value.extend_from_slice(value);
  if !has_known_size {
    out_value.push(0);    // null-terminate
  }
  out_value.extend_from_slice(id);
  return out_value
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