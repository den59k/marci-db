use std::{collections::HashMap, sync::{Arc, atomic::AtomicU64}};

use canopydb::{Database, Transaction, Tree};

use crate::{Field, MarciTransaction, StorageError, aggregate_op::{AggregateOp, AggregateResult, process_aggregate}, delete_op::DeleteError, migrate::{META_TREE, MigrateApplyError, apply, create_entity_trees}, query_op::{DecodeCtx, QueryOp, TransationContext, process_query_many, process_query_one}, schema::{Entity, FieldDefault, Schema, parse_schema, parse_snapshot, serialize_snapshot}, update_op::{UpdateError, UpdateOp}, utils::get_data, write_op::{InsertError, WriteOp}};

pub struct MarciDB {
  pub schema: Schema,
  db: Database,
  pub(crate) counters: Vec<Arc<AtomicU64>>,
  model_by_name: HashMap<String, usize>,
}

impl MarciDB {

  /// Creates/opens a DB with a schema from `.marci` text (the schema-first path, used by embedding and tests).
  /// The schema is written to `__marci_meta__` so it can be reconstructed via [`MarciDB::open`].
  pub fn new(schema_str: &str, path: &str) -> MarciDB {
    MarciDB::create(parse_schema(schema_str), path)
  }

  /// Creates/opens a DB from an already-materialized [`Schema`] (the engine-level constructor). Writes the
  /// snapshot into `__marci_meta__` so it can be reconstructed via [`MarciDB::open`].
  pub fn create(schema: Schema, path: &str) -> MarciDB {
    let db = canopydb::Database::new(path).unwrap();
    let model_by_name = schema.build_model_name_map();

    let tx = db.begin_write().unwrap();

    for model in schema.models.iter() {
      create_entity_trees(&tx, model).unwrap();
    }

    {
      // `__marci_meta__` stores the materialized snapshot (flat entities), not the `.marci` text:
      // open() reconstructs the schema from it without re-expanding sugar and with the same slots
      let mut meta = tx.get_or_create_tree(META_TREE).unwrap();
      meta.insert(b"schema", serialize_snapshot(&schema).as_bytes()).unwrap();
      meta.insert(b"version", &1u64.to_be_bytes()).unwrap();
    }

    let counters = build_counters(&schema, &tx);
    tx.commit().unwrap();

    MarciDB { db, schema, counters, model_by_name }
  }

  /// Opens a DB, reconstructing the schema from `__marci_meta__` (the state left after migrations).
  /// For a new/empty DB the schema is empty — models appear after the first migration ([`MarciDB::commit_schema`],
  /// driven by `$migrate` / `$sync`). The reconstructed state survives a restart
  pub fn open(path: &str) -> MarciDB {
    let db = canopydb::Database::new(path).unwrap();

    let snapshot_text = {
      let rx = db.begin_read().unwrap();
      rx.get_tree(META_TREE).unwrap()
        .and_then(|meta| meta.get(b"schema").unwrap())
        .map(|v| String::from_utf8(v.to_vec()).unwrap())
        .unwrap_or_default()
    };

    // The snapshot is already flat and validated — parse_snapshot restores the schema one-to-one
    let schema = parse_snapshot(&snapshot_text).expect("stored snapshot must be valid");
    let model_by_name = schema.build_model_name_map();

    let rx = db.begin_read().unwrap();
    let counters = build_counters(&schema, &rx);
    drop(rx);

    MarciDB { db, schema, counters, model_by_name }
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
    // Aggregations don't need row decoding — the stub callback is only needed for the context type
    let mut ctx: TransationContext<(), _> = TransationContext::new(&rx, &self.schema, |_: DecodeCtx<()>| ());
    return process_aggregate(op, &mut ctx, None);
  }

  pub fn count_dev(&self, tree_name: &str) -> u64 { 
    let rx = self.db.begin_read().unwrap();
    let tree = rx.get_tree(tree_name.as_bytes()).unwrap().unwrap();
    return tree.len();
  }

  /// Opens an API-level write transaction. Several operations within it are applied
  /// atomically; to commit you must call [`MarciTransaction::commit`], otherwise a rollback
  /// happens on `drop`. While the transaction is open, it holds an exclusive write lock
  pub fn begin_write(&self) -> MarciTransaction<'_> {
    MarciTransaction::new(self, self.db.begin_write().unwrap())
  }

  /// Runs a block in a single transaction: on `Ok` — commit, on `Err` or panic — rollback.
  /// A convenient wrapper over [`MarciDB::begin_write`] when manual commit control isn't needed.
  /// A commit error is wrapped into `E` (hence the `E: From<StorageError>` requirement)
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

  /// Atomically applies ops to the DB, writes the new snapshot+version to `__marci_meta__` and switches
  /// the in-memory schema. On error the transaction is rolled back and state is unchanged.
  ///
  /// This is the engine's single migration entry point. The caller (server `$sync`/`$migrate`, or the
  /// `marcidb-schema` authoring layer) computes `(new_schema, ops)` — by diffing a `.marci` schema, or by
  /// `evolve`-ing a `.march` action file — and hands the result here to apply + persist.
  pub fn commit_schema(&mut self, new_schema: Schema, ops: &[crate::schema::MigrateOp]) -> Result<(), MigrateApplyError> {
    let tx = self.db.begin_write().unwrap();
    apply(&tx, &self.schema, &new_schema, ops)?;

    {
      let mut meta = tx.get_or_create_tree(META_TREE)?;
      let version = meta.get(b"version")?
        .map(|v| u64::from_be_bytes(v.as_ref().try_into().unwrap()))
        .unwrap_or(0) + 1;
      meta.insert(b"schema", serialize_snapshot(&new_schema).as_bytes())?;
      meta.insert(b"version", &version.to_be_bytes())?;
    }
    tx.commit()?;

    self.swap_schema(new_schema);
    Ok(())
  }

  /// Rebuilds counters/name index for the new schema and switches the in-memory schema
  fn swap_schema(&mut self, new_schema: Schema) {
    let rx = self.db.begin_read().unwrap();
    self.counters = build_counters(&new_schema, &rx);
    drop(rx);
    self.model_by_name = new_schema.build_model_name_map();
    self.schema = new_schema;
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
