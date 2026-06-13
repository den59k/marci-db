use std::{collections::HashMap, sync::{Arc, atomic::AtomicU64}};

use canopydb::{Database, Transaction, Tree};

use crate::{Field, MarciTransaction, StorageError, aggregate_op::{AggregateOp, AggregateResult, process_aggregate}, delete_op::DeleteError, migration::{META_TREE, MigrationApplyError, MigrationOp, apply_ops, create_entity_trees, diff}, query_op::{DecodeCtx, QueryOp, TransationContext, process_query_many, process_query_one}, schema::{Entity, FieldDefault, Schema, parse_schema}, update_op::{UpdateError, UpdateOp}, utils::get_data, write_op::{InsertError, WriteOp}};

pub struct MarciDB {
  pub schema: Schema,
  db: Database,
  pub(crate) counters: Vec<Arc<AtomicU64>>,
  model_by_name: HashMap<String, usize>,
  /// Текущая схема как текст `.marci` — источник для диффа в `migrate_to`; дублируется в `__marci_meta__`
  schema_text: String,
}

impl MarciDB {

  /// Создаёт/открывает БД со схемой из текста (schema-first путь, используется встраиванием и тестами).
  /// Схема пишется в `__marci_meta__`, чтобы её можно было реконструировать через [`MarciDB::open`]
  pub fn new(schema_str: &str, path: &str) -> MarciDB {
    let schema = parse_schema(schema_str);

    let db = canopydb::Database::new(path).unwrap();
    let model_by_name = schema.build_model_name_map();

    let tx = db.begin_write().unwrap();

    for model in schema.models.iter() {
      create_entity_trees(&tx, model).unwrap();
    }

    {
      let mut meta = tx.get_or_create_tree(META_TREE).unwrap();
      meta.insert(b"schema", schema_str.as_bytes()).unwrap();
      meta.insert(b"version", &1u64.to_be_bytes()).unwrap();
    }

    let counters = build_counters(&schema, &tx);
    tx.commit().unwrap();

    MarciDB { db, schema, counters, model_by_name, schema_text: schema_str.to_string() }
  }

  /// Открывает БД, реконструируя схему из `__marci_meta__` (состояние, оставшееся после миграций).
  /// Для новой/пустой БД схема пустая — модели появятся после первой [`MarciDB::migrate_to`].
  /// Это open-time self-migrate: миграции переживают рестарт
  pub fn open(path: &str) -> MarciDB {
    let db = canopydb::Database::new(path).unwrap();

    let schema_text = {
      let rx = db.begin_read().unwrap();
      rx.get_tree(META_TREE).unwrap()
        .and_then(|meta| meta.get(b"schema").unwrap())
        .map(|v| String::from_utf8(v.to_vec()).unwrap())
        .unwrap_or_default()
    };

    let schema = parse_schema(&schema_text);
    let model_by_name = schema.build_model_name_map();

    let rx = db.begin_read().unwrap();
    let counters = build_counters(&schema, &rx);
    drop(rx);

    MarciDB { db, schema, counters, model_by_name, schema_text }
  }

  pub fn get_model(&self, name: &str) -> Option<&Entity> {
    self.model_by_name.get(name).and_then(|idx| { Some(&self.schema.models[*idx]) })
  }
  
  pub fn get_model_index(&self, name: &str) -> Option<usize> {
    self.model_by_name.get(name).copied()
  }

  pub fn get_model_by_index<'a>(&'a self, index: usize) -> &'a Entity {
    return &self.schema.models[index]
  }

  pub fn find_many<U, F>(&self, query: &QueryOp, f: F) -> Result<Vec<U>, StorageError> where U: Clone, F: Fn(DecodeCtx<U>) -> U {
    let rx = self.db.begin_read().unwrap();
    let mut ctx = TransationContext::new(&rx, &self.schema, f);
    return process_query_many(query, &mut ctx, None);
  }

  pub fn find_first<U, F>(&self, query: &QueryOp, f: F) -> Result<Option<U>, StorageError> where U: Clone, F: Fn(DecodeCtx<U>) -> U {
    let rx = self.db.begin_read().unwrap();
    let mut ctx = TransationContext::new(&rx, &self.schema, f);
    return process_query_one(query, &mut ctx, None);
  }

  pub fn count(&self, entity: &Entity) -> Result<u64, StorageError> {
    let rx = self.db.begin_read().unwrap();
    let tree = rx.get_tree(entity.name.as_bytes())?.unwrap();
    return Ok(tree.len());
  }

  pub fn aggregate(&self, op: &AggregateOp) -> Result<AggregateResult, StorageError> {
    let rx = self.db.begin_read().unwrap();
    // Декод строк агрегациям не нужен — колбэк-заглушка нужна только для типа контекста
    let mut ctx: TransationContext<(), _> = TransationContext::new(&rx, &self.schema, |_: DecodeCtx<()>| ());
    return process_aggregate(op, &mut ctx, None);
  }

  pub fn count_dev(&self, tree_name: &str) -> u64 { 
    let rx = self.db.begin_read().unwrap();
    let tree = rx.get_tree(tree_name.as_bytes()).unwrap().unwrap();
    return tree.len();
  }

  /// Открывает write-транзакцию уровня API. Несколько операций внутри неё применяются
  /// атомарно; для фиксации нужно вызвать [`MarciTransaction::commit`], иначе при `drop`
  /// произойдёт откат. Пока транзакция открыта, она держит эксклюзивную write-блокировку
  pub fn begin_write(&self) -> MarciTransaction<'_> {
    MarciTransaction::new(self, self.db.begin_write().unwrap())
  }

  /// Выполняет блок в одной транзакции: при `Ok` — коммит, при `Err` или панике — откат.
  /// Удобная обёртка над [`MarciDB::begin_write`], когда не нужно ручное управление коммитом.
  /// Ошибка коммита оборачивается в `E` (поэтому требуется `E: From<StorageError>`)
  pub fn transaction<T, E, F>(&self, f: F) -> Result<T, E> where F: FnOnce(&MarciTransaction) -> Result<T, E>, E: From<StorageError> {
    let tx = self.begin_write();
    let result = f(&tx)?;
    tx.commit()?;
    Ok(result)
  }

  pub fn insert_item(&self, entity: &Entity, insert: &WriteOp) -> Result<Vec<u8>, InsertError> {
    let tx = self.begin_write();
    let item_id = tx.insert_item(entity, insert)?;
    tx.commit()?;
    Ok(item_id)
  }

  pub fn update_item(&self, entity: &Entity, id: &[u8], update_op: &UpdateOp) -> Result<(), UpdateError> {
    let tx = self.begin_write();
    tx.update_item(entity, id, update_op)?;
    tx.commit()?;
    Ok(())
  }

  pub fn delete_item(&self, entity: &Entity, id: &[u8]) -> Result<bool, DeleteError> {
    let tx = self.begin_write();
    let is_delete = tx.delete_item(entity, id)?;
    tx.commit()?;
    Ok(is_delete)
  }

  /// Применяет миграцию: исполняет операции против БД атомарно, сохраняет новую схему и версию
  /// в `__marci_meta__` и переключает in-memory схему. Новые поля дописываются в конец модели —
  /// слоты существующих полей не меняются, поэтому старые строки не переписываются (формат v2).
  ///
  /// v1 покрывает add/alter field и add/drop index на существующих моделях (поля только в конец);
  /// drop field, create/drop model и реордер полей пока возвращают `MigrationApplyError::Unsupported`.
  /// Декларативная миграция к новой схеме: вычисляет дифф от текущей схемы и применяет его.
  /// Это и есть «push» на сервере — клиент присылает новый текст схемы
  pub fn migrate_to(&mut self, new_schema_text: &str) -> Result<(), MigrationApplyError> {
    let ops = diff(&self.schema_text, new_schema_text)?;
    self.apply_migration(&ops, new_schema_text)
  }

  /// Применяет заранее вычисленные операции миграции к новой схеме (текст). Атомарно: при ошибке
  /// транзакция откатывается, версия и in-memory схема не меняются
  pub fn apply_migration(&mut self, ops: &[MigrationOp], new_schema_text: &str) -> Result<(), MigrationApplyError> {
    let new_schema = parse_schema(new_schema_text);

    let tx = self.db.begin_write().unwrap();
    apply_ops(&tx, ops, &self.schema, &new_schema)?;

    {
      let mut meta = tx.get_or_create_tree(META_TREE)?;
      let version = meta.get(b"version")?
        .map(|v| u64::from_be_bytes(v.as_ref().try_into().unwrap()))
        .unwrap_or(0) + 1;
      meta.insert(b"schema", new_schema_text.as_bytes())?;
      meta.insert(b"version", &version.to_be_bytes())?;
    }
    tx.commit()?;

    // Набор моделей мог измениться (create/drop model) — пересобираем counters и индекс имён
    let rx = self.db.begin_read().unwrap();
    self.counters = build_counters(&new_schema, &rx);
    drop(rx);

    self.model_by_name = new_schema.build_model_name_map();
    self.schema_text = new_schema_text.to_string();
    self.schema = new_schema;
    Ok(())
  }
}

fn build_counters(schema: &Schema, rx: &Transaction) -> Vec<Arc<AtomicU64>> {
  let mut counters = Vec::with_capacity(schema.models.len());
  for entity in schema.models.iter() {
    let model_tree = rx.get_tree(entity.name.as_bytes()).unwrap().unwrap();

    for field in entity.fields.iter() {
      if let Some(FieldDefault::Counter(counter_idx)) = field.default_value {
        counters.resize(counter_idx+1, Arc::new(AtomicU64::new(0)));
        
        let max_id = get_max_id(&model_tree, entity, field, schema);
        counters[counter_idx] = Arc::new(AtomicU64::new(max_id));
      }
    }
  }
  return counters;
}

pub fn get_max_id(tree: &Tree, entity: &Entity, field: &Field, schema: &Schema) -> u64 {
  return tree.last().unwrap()
    .map(|(id, body)| {
      let data = get_data(entity, field, &id, &body, schema).unwrap();
      u64::from_be_bytes(data.try_into().unwrap()) + 1
    })
    .unwrap_or(1);
}
