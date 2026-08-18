use std::{collections::HashMap, sync::{Arc, atomic::AtomicU64}};

use canopydb::{Database, Transaction, Tree, WriteTransaction};
use serde_json::Value;

use crate::{Field, MarciTransaction, StorageError, aggregate_op::{AggregateOp, AggregateResult, process_aggregate}, delete_op::DeleteError, error::RequireTree, index_provider::{IndexTree, ProviderError, ProviderRegistry, RowScan, SearchHit}, migrate::{META_TREE, MigrateApplyError, apply, create_entity_trees}, query_op::{DecodeCtx, QueryOp, TransationContext, decode_row, process_query_many, process_query_one, process_where}, schema::{Entity, FieldDefault, FieldIndex, Schema, parse_schema, parse_snapshot, serialize_snapshot}, update_op::{UpdateError, UpdateOp}, utils::get_data, write_op::{InsertError, WriteOp}};

/// Storage-engine options threaded into `MarciDB::try_*_with_options`. Defaults match the durable
/// `MarciDB::new`/`open` path; the embedding/test layer flips `disable_fsync` for fast, ephemeral DBs.
#[derive(Debug, Clone, Default)]
pub struct OpenOptions {
  /// Disable every `fsync`. Much faster, but durability-unsafe — a crash can lose recent writes.
  /// Intended for throwaway/integration-test databases (often paired with a temp dir / ramdisk).
  pub disable_fsync: bool,
}

/// Opens the underlying CanopyDB database for `path` with the given options. Both create and open paths
/// funnel through here so option handling lives in one spot.
fn open_database(path: &str, options: &OpenOptions) -> Result<Database, StorageError> {
  let mut env_opts = canopydb::EnvOptions::new(path);
  env_opts.disable_fsync = options.disable_fsync;
  Ok(Database::with_options(env_opts, canopydb::DbOptions::default())?)
}

pub struct MarciDB {
  pub schema: Schema,
  db: Database,
  pub(crate) counters: Vec<Arc<AtomicU64>>,
  model_by_name: HashMap<String, usize>,
  /// Registered `@custom` index providers (vector, full-text, …). Empty by default; the host installs them
  /// via [`MarciDB::with_providers`]. Shared across DBs that the same host opens. `pub(crate)` so the write
  /// paths can dispatch the live-maintenance hooks (`on_insert`/`on_update`/`on_delete`).
  pub(crate) providers: Arc<ProviderRegistry>,
}

impl MarciDB {

  /// Creates/opens a DB with a schema from `.marci` text (the schema-first path, used by embedding and tests).
  /// The schema is written to `__marci_meta__` so it can be reconstructed via [`MarciDB::open`].
  pub fn new(schema_str: &str, path: &str) -> MarciDB {
    MarciDB::create(parse_schema(schema_str), path)
  }

  /// Creates/opens a DB from an already-materialized [`Schema`] (the engine-level constructor). Writes the
  /// snapshot into `__marci_meta__` so it can be reconstructed via [`MarciDB::open`]. Panics on a storage
  /// fault — use [`MarciDB::try_create`] for the fallible variant.
  pub fn create(schema: Schema, path: &str) -> MarciDB {
    MarciDB::try_create(schema, path).unwrap_or_else(|e| panic!("failed to create database at '{}': {}", path, e))
  }

  /// Fallible [`MarciDB::create`]: returns a [`StorageError`] instead of panicking on a storage fault.
  pub fn try_create(schema: Schema, path: &str) -> Result<MarciDB, StorageError> {
    MarciDB::try_create_with_options(schema, path, &OpenOptions::default())
  }

  /// [`MarciDB::try_create`] with explicit storage options (e.g. `disable_fsync` for ephemeral DBs).
  pub fn try_create_with_options(schema: Schema, path: &str, options: &OpenOptions) -> Result<MarciDB, StorageError> {
    let db = open_database(path, options)?;
    let model_by_name = schema.build_model_name_map();

    let tx = db.begin_write()?;

    for model in schema.models.iter() {
      create_entity_trees(&tx, model)?;
    }

    {
      // `__marci_meta__` stores the materialized snapshot (flat entities), not the `.marci` text:
      // open() reconstructs the schema from it without re-expanding sugar and with the same slots
      let mut meta = tx.get_or_create_tree(META_TREE)?;
      meta.insert(b"schema", serialize_snapshot(&schema).as_bytes())?;
      meta.insert(b"version", &1u64.to_be_bytes())?;
    }

    let counters = build_counters(&schema, &tx)?;
    tx.commit()?;

    Ok(MarciDB { db, schema, counters, model_by_name, providers: Arc::new(ProviderRegistry::new()) })
  }

  /// Installs the `@custom` index providers (builder style). The same registry is typically shared across
  /// every DB a host opens: `MarciDB::open(path).with_providers(registry.clone())`.
  pub fn with_providers(mut self, providers: Arc<ProviderRegistry>) -> Self {
    self.providers = providers;
    self
  }

  /// Opens a DB, reconstructing the schema from `__marci_meta__` (the state left after migrations).
  /// For a new/empty DB the schema is empty — models appear after the first migration ([`MarciDB::commit_schema`],
  /// driven by `$migrate` / `$sync`). The reconstructed state survives a restart
  pub fn open(path: &str) -> MarciDB {
    MarciDB::try_open(path).unwrap_or_else(|e| panic!("failed to open database at '{}': {}", path, e))
  }

  /// Fallible [`MarciDB::open`]: returns a [`StorageError`] instead of panicking on a storage fault.
  /// (A structurally corrupt stored snapshot is still treated as a hard invariant and panics.)
  pub fn try_open(path: &str) -> Result<MarciDB, StorageError> {
    MarciDB::try_open_with_options(path, &OpenOptions::default())
  }

  /// [`MarciDB::try_open`] with explicit storage options (e.g. `disable_fsync` for ephemeral DBs).
  pub fn try_open_with_options(path: &str, options: &OpenOptions) -> Result<MarciDB, StorageError> {
    let db = open_database(path, options)?;

    let snapshot_text = {
      let rx = db.begin_read()?;
      match rx.get_tree(META_TREE)? {
        Some(meta) => meta.get(b"schema")?
          .map(|v| String::from_utf8(v.to_vec()).expect("stored schema must be valid UTF-8"))
          .unwrap_or_default(),
        None => String::new(),
      }
    };

    // The snapshot is already flat and validated — parse_snapshot restores the schema one-to-one
    let schema = parse_snapshot(&snapshot_text).expect("stored snapshot must be valid");
    let model_by_name = schema.build_model_name_map();

    let rx = db.begin_read()?;
    let counters = build_counters(&schema, &rx)?;
    drop(rx);

    Ok(MarciDB { db, schema, counters, model_by_name, providers: Arc::new(ProviderRegistry::new()) })
  }

  pub fn get_model(&self, name: &str) -> Option<&Entity> {
    self.model_by_name.get(name).map(|idx| &self.schema.models[*idx])
  }
  
  pub fn get_model_index(&self, name: &str) -> Option<usize> {
    self.model_by_name.get(name).copied()
  }

  pub fn get_model_by_index<'a>(&'a self, index: usize) -> &'a Entity {
    return &self.schema.models[index]
  }

  pub fn find_many<U, F>(&self, query: &QueryOp, f: F) -> Result<Vec<U>, QueryError> where U: Clone, F: Fn(DecodeCtx<U>) -> U {
    if let Some((field, payload)) = &query.search {
      return self.run_search(query, field, payload, f, None);
    }
    let rx = self.db.begin_read()?;
    let mut ctx = TransationContext::new(&rx, &self.schema, f);
    Ok(process_query_many(query, &mut ctx, None)?)
  }

  pub fn find_first<U, F>(&self, query: &QueryOp, f: F) -> Result<Option<U>, QueryError> where U: Clone, F: Fn(DecodeCtx<U>) -> U {
    if let Some((field, payload)) = &query.search {
      return Ok(self.run_search(query, field, payload, f, Some(1))?.into_iter().next());
    }
    let rx = self.db.begin_read()?;
    let mut ctx = TransationContext::new(&rx, &self.schema, f);
    Ok(process_query_one(query, &mut ctx, None)?)
  }

  /// Executes a `$near`/`$search` query: resolve ranked ids from the provider, then fetch rows in that
  /// order, applying the residual `query.filter` as a post-condition and honouring `skip`/`limit`. `max`
  /// caps the result count (used by `find_first`). Both the provider lookup and the row fetch share one
  /// read transaction. Stale index entries (row since deleted) are skipped.
  fn run_search<U, F>(&self, query: &QueryOp, field: &Field, payload: &Value, f: F, max: Option<usize>) -> Result<Vec<U>, QueryError>
    where U: Clone, F: Fn(DecodeCtx<U>) -> U {
    let rx = self.db.begin_read()?;
    let hits = self.provider_search(&rx, field, payload)?;

    let mut ctx = TransationContext::new(&rx, &self.schema, f);
    let entity = query.entity;
    let skip = query.skip.unwrap_or(0);
    let limit = max.or(query.limit);

    let mut out = Vec::new();
    let mut matched = 0usize;
    for hit in hits {
      let body = {
        let tree = ctx.get_tree(&entity.name)?;
        match tree.get(&hit.id)? {
          Some(b) => b.to_vec(),
          None => continue, // stale index entry — row was deleted since the last $reindex
        }
      };
      if let Some(filter) = &query.filter {
        if !process_where(&hit.id, &body, &mut ctx, entity, filter)? { continue; }
      }
      matched += 1;
      if matched <= skip { continue; }
      out.push(decode_row(&hit.id, &body, &mut ctx, query)?);
      if let Some(limit) = limit && out.len() >= limit { break; }
    }
    Ok(out)
  }

  pub fn count(&self, entity: &Entity) -> Result<u64, StorageError> {
    let rx = self.db.begin_read()?;
    let tree = rx.require_tree(entity.name.as_bytes())?;
    return Ok(tree.len());
  }

  pub fn aggregate(&self, op: &AggregateOp) -> Result<AggregateResult, StorageError> {
    let rx = self.db.begin_read()?;
    // Aggregations don't need row decoding — the stub callback is only needed for the context type
    let mut ctx: TransationContext<(), _> = TransationContext::new(&rx, &self.schema, |_: DecodeCtx<()>| ());
    return process_aggregate(op, &mut ctx, None);
  }

  /// Test/diagnostic helper: number of entries in a raw tree by name. Not part of the data API and not used
  /// by the server; it panics on error (a failing test should surface loudly), so it is intentionally exempt
  /// from the fallible-storage treatment the rest of the engine received.
  pub fn count_dev(&self, tree_name: &str) -> u64 {
    let rx = self.db.begin_read().expect("count_dev: begin_read failed");
    let tree = rx.get_tree(tree_name.as_bytes()).expect("count_dev: get_tree failed")
      .unwrap_or_else(|| panic!("count_dev: tree '{}' not found", tree_name));
    tree.len()
  }

  /// Test/diagnostic helper: every `(key, value)` pair of a raw tree, in key order. Companion to
  /// [`MarciDB::count_dev`], used to assert that live index maintenance matches a full `$reindex`
  /// byte-for-byte. Like `count_dev`, it panics on error so a failing test surfaces loudly.
  pub fn dump_dev(&self, tree_name: &str) -> Vec<(Vec<u8>, Vec<u8>)> {
    let rx = self.db.begin_read().expect("dump_dev: begin_read failed");
    let tree = rx.get_tree(tree_name.as_bytes()).expect("dump_dev: get_tree failed")
      .unwrap_or_else(|| panic!("dump_dev: tree '{}' not found", tree_name));
    tree.iter().expect("dump_dev: iter failed")
      .map(|e| { let (k, v) = e.expect("dump_dev: entry read failed"); (k.to_vec(), v.to_vec()) })
      .collect()
  }

  /// Opens an API-level write transaction. Several operations within it are applied
  /// atomically; to commit you must call [`MarciTransaction::commit`], otherwise a rollback
  /// happens on `drop`. While the transaction is open, it holds an exclusive write lock
  pub fn begin_write(&self) -> Result<MarciTransaction<'_>, StorageError> {
    Ok(MarciTransaction::new(self, self.db.begin_write()?))
  }

  /// Runs a block in a single transaction: on `Ok` — commit, on `Err` or panic — rollback.
  /// A convenient wrapper over [`MarciDB::begin_write`] when manual commit control isn't needed.
  /// A commit error is wrapped into `E` (hence the `E: From<StorageError>` requirement)
  pub fn transaction<T, E, F>(&self, f: F) -> Result<T, E> where F: FnOnce(&MarciTransaction) -> Result<T, E>, E: From<StorageError> {
    let tx = self.begin_write()?;
    let result = f(&tx)?;
    tx.commit()?;
    Ok(result)
  }

  pub fn insert_item(&self, entity: &Entity, insert: &WriteOp) -> Result<Vec<u8>, InsertError> {
    let tx = self.begin_write()?;
    let item_id = tx.insert_item(entity, insert)?;
    tx.commit()?;
    Ok(item_id)
  }

  pub fn update_item(&self, entity: &Entity, id: &[u8], update_op: &UpdateOp) -> Result<(), UpdateError> {
    let tx = self.begin_write()?;
    tx.update_item(entity, id, update_op)?;
    tx.commit()?;
    Ok(())
  }

  /// Applies `update_op` to every row matching `query`, atomically. Returns how many rows matched;
  /// on any error nothing is committed. See [`MarciTransaction::update_many`].
  pub fn update_many(&self, entity: &Entity, query: &QueryOp, update_op: &UpdateOp) -> Result<u64, UpdateError> {
    let tx = self.begin_write()?;
    let updated = tx.update_many(entity, query, update_op)?;
    tx.commit()?;
    Ok(updated)
  }

  pub fn delete_item(&self, entity: &Entity, id: &[u8]) -> Result<bool, DeleteError> {
    let tx = self.begin_write()?;
    let is_delete = tx.delete_item(entity, id)?;
    tx.commit()?;
    Ok(is_delete)
  }

  /// Deletes every row matching `query`, atomically. Returns how many rows were deleted; on any error
  /// nothing is committed. See [`MarciTransaction::delete_many`].
  pub fn delete_many(&self, entity: &Entity, query: &QueryOp) -> Result<u64, DeleteError> {
    let tx = self.begin_write()?;
    let deleted = tx.delete_many(entity, query)?;
    tx.commit()?;
    Ok(deleted)
  }

  /// Atomically applies ops to the DB, writes the new snapshot+version to `__marci_meta__` and switches
  /// the in-memory schema. On error the transaction is rolled back and state is unchanged.
  ///
  /// This is the engine's single migration entry point. The caller (server `$sync`/`$migrate`, or the
  /// `marcidb-schema` authoring layer) computes `(new_schema, ops)` — by diffing a `.marci` schema, or by
  /// `evolve`-ing a `.march` action file — and hands the result here to apply + persist.
  pub fn commit_schema(&mut self, new_schema: Schema, ops: &[crate::schema::MigrateOp]) -> Result<(), MigrateApplyError> {
    self.validate_added_indexes(&new_schema, ops)?;

    let tx = self.db.begin_write()?;
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

    self.swap_schema(new_schema)?;
    Ok(())
  }

  /// At migration time, validate every newly-added `@custom` (module) index: its provider must be
  /// registered, and must accept the field/args. This makes a typo (`@vektor`) or a missing module fail
  /// fast at `$sync`/`$migrate` (a clear 4xx) rather than silently creating a tree that only errors at the
  /// first reindex/query. Existing/unchanged indexes were validated when they were added, and a DB still
  /// *opens* without the provider — only changing the schema requires it.
  fn validate_added_indexes(&self, new_schema: &Schema, ops: &[crate::schema::MigrateOp]) -> Result<(), MigrateApplyError> {
    for op in ops {
      let crate::schema::MigrateOp::AddIndex { entity: entity_name, field: field_name, .. } = op else { continue };
      let Some(entity) = new_schema.models.iter().find(|m| &m.name == entity_name) else { continue };
      let Some(field) = entity.fields.iter().find(|f| &f.name == field_name) else { continue };

      for index in field.indexes.iter() {
        let FieldIndex::Custom { name, args, .. } = index else { continue };
        let provider = self.providers.get(name)
          .ok_or_else(|| MigrateApplyError::NoProvider { provider: name.clone(), field: field.full_name.clone() })?;
        provider.validate(field, args)
          .map_err(|e| MigrateApplyError::InvalidIndex { field: field.full_name.clone(), detail: e.to_string() })?;
      }
    }
    Ok(())
  }

  // ─────────────────────────────── module (`@custom`) indexes ───────────────────────────────

  /// Rebuilds every `@custom` index of `entity` from current data, in one write transaction.
  /// Returns the number of index trees rebuilt. The engine primitive behind the server's `$reindex`.
  pub fn reindex_entity(&self, entity: &Entity) -> Result<usize, ReindexError> {
    let tx = self.db.begin_write()?;
    let mut count = 0;
    for field in entity.fields.iter() {
      count += self.reindex_field_in_tx(&tx, entity, field)?;
    }
    tx.commit()?;
    Ok(count)
  }

  /// Rebuilds the `@custom` indexes of a single field, in its own transaction. Returns trees rebuilt.
  pub fn reindex_field(&self, entity: &Entity, field: &Field) -> Result<usize, ReindexError> {
    let tx = self.db.begin_write()?;
    let count = self.reindex_field_in_tx(&tx, entity, field)?;
    tx.commit()?;
    Ok(count)
  }

  fn reindex_field_in_tx(&self, tx: &WriteTransaction, entity: &Entity, field: &Field) -> Result<usize, ReindexError> {
    let mut count = 0;
    for index in field.indexes.iter() {
      let FieldIndex::Custom { tree_name, name, args } = index else { continue };
      let provider = self.providers.get(name).ok_or_else(|| ReindexError::NoProvider(name.clone()))?;
      provider.validate(field, args).map_err(ReindexError::Provider)?;

      let mut store = IndexTree::new(tx.get_or_create_tree(tree_name.as_bytes())?);
      store.clear().map_err(ReindexError::Provider)?;

      let model_tree = tx.require_tree(entity.name.as_bytes())?;
      let scan = RowScan::new(Box::new(model_tree.iter()?), entity, field, &self.schema);
      provider.rebuild(field, args, scan, &mut store).map_err(ReindexError::Provider)?;
      count += 1;
    }
    Ok(count)
  }

  /// Runs a `@custom` index search on `field` with the raw operator `payload`, returning ranked hits.
  /// Used by the query executor (`$near`/`$match`) and callable directly for tests/tools.
  pub fn search_custom(&self, field: &Field, payload: &Value) -> Result<Vec<SearchHit>, ReindexError> {
    let rx = self.db.begin_read()?;
    self.provider_search(&rx, field, payload)
  }

  /// Resolves `field`'s `@custom` provider + index tree and runs a search against it within `rx`.
  /// Shared by `search_custom` (its own read tx) and `run_search` (which reuses the row-fetch tx).
  fn provider_search(&self, rx: &Transaction, field: &Field, payload: &Value) -> Result<Vec<SearchHit>, ReindexError> {
    let (tree_name, name, args) = field.indexes.iter().find_map(|i| match i {
      FieldIndex::Custom { tree_name, name, args } => Some((tree_name, name, args)),
      _ => None,
    }).ok_or_else(|| ReindexError::NotCustom(field.full_name.clone()))?;

    let provider = self.providers.get(name).ok_or_else(|| ReindexError::NoProvider(name.clone()))?;
    let tree = rx.get_tree(tree_name.as_bytes())?
      .ok_or_else(|| ReindexError::NotCustom(field.full_name.clone()))?;
    let store = IndexTree::new(tree);
    provider.search(field, args, payload, &store).map_err(ReindexError::Provider)
  }

  /// Rebuilds counters/name index for the new schema and switches the in-memory schema. On a storage fault
  /// nothing is mutated (the `?` returns before any field is reassigned), so `self` stays internally
  /// consistent on the old schema while the new one is already committed to disk.
  fn swap_schema(&mut self, new_schema: Schema) -> Result<(), StorageError> {
    let rx = self.db.begin_read()?;
    let counters = build_counters(&new_schema, &rx)?;
    drop(rx);
    self.counters = counters;
    self.model_by_name = new_schema.build_model_name_map();
    self.schema = new_schema;
    Ok(())
  }
}

/// Failure of a `@custom` index reindex/search.
#[derive(Debug)]
pub enum ReindexError {
  /// No provider registered for the field's `@custom(<name>)` (module not compiled in / not registered).
  NoProvider(String),
  /// `search_custom` was called on a field that has no `@custom` index.
  NotCustom(String),
  Provider(ProviderError),
  Storage(StorageError),
}

impl std::fmt::Display for ReindexError {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      ReindexError::NoProvider(name) => write!(f, "no index provider registered for '@custom({})'", name),
      ReindexError::NotCustom(field) => write!(f, "field '{}' has no @custom index", field),
      ReindexError::Provider(e) => write!(f, "{}", e),
      ReindexError::Storage(e) => write!(f, "{:?}", e),
    }
  }
}
impl std::error::Error for ReindexError {}

impl From<canopydb::Error> for ReindexError { fn from(e: canopydb::Error) -> Self { ReindexError::Storage(StorageError::Backend(e)) } }
impl From<StorageError> for ReindexError { fn from(e: StorageError) -> Self { ReindexError::Storage(e) } }

/// Error from a read query. `Storage` → a 5xx; `Search` carries the provider/registry error for a
/// `$near`/`$search` (a bad payload or missing provider is a 4xx — see the server's mapping).
#[derive(Debug)]
pub enum QueryError {
  Storage(StorageError),
  Search(ReindexError),
}

impl std::fmt::Display for QueryError {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      QueryError::Storage(e) => write!(f, "{:?}", e),
      QueryError::Search(e) => write!(f, "{}", e),
    }
  }
}
impl std::error::Error for QueryError {}

impl From<StorageError> for QueryError { fn from(e: StorageError) -> Self { QueryError::Storage(e) } }
impl From<canopydb::Error> for QueryError { fn from(e: canopydb::Error) -> Self { QueryError::Storage(StorageError::Backend(e)) } }
impl From<ReindexError> for QueryError { fn from(e: ReindexError) -> Self { QueryError::Search(e) } }

fn build_counters(schema: &Schema, rx: &Transaction) -> Result<Vec<Arc<AtomicU64>>, StorageError> {
  let mut counters = Vec::with_capacity(schema.models.len());
  for entity in schema.models.iter() {
    let model_tree = rx.require_tree(entity.name.as_bytes())?;

    for field in entity.fields.iter() {
      if let Some(FieldDefault::Counter(counter_idx)) = field.default_value {
        counters.resize(counter_idx+1, Arc::new(AtomicU64::new(0)));

        let max_id = get_max_id(&model_tree, entity, field, schema)?;
        counters[counter_idx] = Arc::new(AtomicU64::new(max_id));
      }
    }
  }
  Ok(counters)
}

pub fn get_max_id(tree: &Tree, entity: &Entity, field: &Field, schema: &Schema) -> Result<u64, StorageError> {
  // The I/O part (reading the last row) propagates; the data shape (a present 8-byte counter value) is a
  // structural invariant of a counter column, so a violation there is a corruption panic, not a fault.
  Ok(tree.last()?
    .map(|(id, body)| {
      let data = get_data(entity, field, &id, &body, schema).expect("counter field must be present on its row");
      u64::from_be_bytes(data.try_into().expect("counter value must be 8 bytes")) + 1
    })
    .unwrap_or(1))
}
