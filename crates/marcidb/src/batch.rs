use std::fmt;

use serde_json::Value;

use crate::{DeleteError, InsertError, MarciDB, MarciTransaction, QueryError, ReindexError, StorageError, UpdateError, aggregate_to_json, array_to_json, decode_document, decode_id, parse_aggregate, parse_id, parse_insert, parse_query, parse_update};

/// A batch transaction error with the index of the operation on which it occurred.
/// `index == ops.len()` means an error at commit (after all operations)
#[derive(Debug)]
pub struct BatchError {
  pub index: usize,
  pub kind: BatchErrorKind,
}

#[derive(Debug)]
pub enum BatchErrorKind {
  /// The operation is not a JSON object
  NotAnObject,
  /// A required field is missing (`model` / `action` / `data` / `id` / `query`)
  MissingField(&'static str),
  UnknownModel(String),
  UnknownAction(String),
  /// Invalid `$ref` reference to the result of a previous operation
  BadRef(String),
  /// Error parsing the input JSON (encode / parse)
  Parse(String),
  Insert(InsertError),
  Update(UpdateError),
  Delete(DeleteError),
  /// Storage error (including at commit)
  Storage(StorageError),
}

impl fmt::Display for BatchError {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "batch op #{}: {:?}", self.index, self.kind)
  }
}

impl std::error::Error for BatchError {}

/// Executes a list of operations in a single atomic transaction.
///
/// On success returns the results, one per operation (in the same order), and commits.
/// On the very first error the whole transaction is rolled back, and the index
/// of the failed operation is returned in [`BatchError`]. References `{"$ref":"<i>.<path>"}` to the result of operation `i`
/// are supported (for example, a generated id), which are resolved before the operation is executed.
///
/// Result format by operation type:
/// * `insert` — id object (as for a single insert)
/// * `update` — `null`
/// * `updateMany` — number of matched rows
/// * `delete` — `true`/`false`
/// * `findFirst` — object or `null`
/// * `findMany` — array of objects
/// * `aggregate` — aggregates object; `count` — number
pub fn execute_batch(db: &MarciDB, ops: &[Value]) -> Result<Vec<Value>, BatchError> {
  let tx = db.begin_write().map_err(|e| BatchError { index: 0, kind: BatchErrorKind::Storage(e) })?;

  let mut results: Vec<Value> = Vec::with_capacity(ops.len());
  for (index, op) in ops.iter().enumerate() {
    let result = apply_op(&tx, db, op, &results).map_err(|kind| BatchError { index, kind })?;
    results.push(result);
  }

  // Any early exit above drops tx without a commit → rollback
  tx.commit().map_err(|e| BatchError { index: ops.len(), kind: BatchErrorKind::Storage(e) })?;
  Ok(results)
}

fn apply_op(tx: &MarciTransaction, db: &MarciDB, op: &Value, prior: &[Value]) -> Result<Value, BatchErrorKind> {
  let obj = op.as_object().ok_or(BatchErrorKind::NotAnObject)?;
  let model = obj.get("model").and_then(|m| m.as_str()).ok_or(BatchErrorKind::MissingField("model"))?;
  let action = obj.get("action").and_then(|a| a.as_str()).ok_or(BatchErrorKind::MissingField("action"))?;

  let entity = db.get_model(model).ok_or_else(|| BatchErrorKind::UnknownModel(model.to_string()))?;

  match action {
    "insert" => {
      let data = resolve_refs(field(obj, "data")?, prior)?;
      let write_op = parse_insert(&db.schema, entity, &data).map_err(parse_err)?;
      let id = tx.insert_item(entity, &write_op).map_err(BatchErrorKind::Insert)?;
      Ok(json_value(decode_id(&id, entity, &db.schema)))
    },
    "update" => {
      let id = parse_id(&db.schema, entity, &resolve_refs(field(obj, "id")?, prior)?).map_err(parse_err)?;
      let data = resolve_refs(field(obj, "data")?, prior)?;
      let update_op = parse_update(&db.schema, entity, &data).map_err(parse_err)?;
      tx.update_item(entity, &id, &update_op).map_err(BatchErrorKind::Update)?;
      Ok(Value::Null)
    },
    "updateMany" => {
      let query_json = update_many_query(&resolve_refs(field(obj, "query")?, prior)?);
      let query = parse_query(&db.schema, entity, &query_json).map_err(parse_err)?;
      let data = resolve_refs(field(obj, "data")?, prior)?;
      let update_op = parse_update(&db.schema, entity, &data).map_err(parse_err)?;
      let updated = tx.update_many(entity, &query, &update_op).map_err(BatchErrorKind::Update)?;
      Ok(Value::from(updated))
    },
    "delete" => {
      let id = parse_id(&db.schema, entity, &resolve_refs(field(obj, "id")?, prior)?).map_err(parse_err)?;
      let deleted = tx.delete_item(entity, &id).map_err(BatchErrorKind::Delete)?;
      Ok(Value::Bool(deleted))
    },
    "findFirst" => {
      let query = parse_query(&db.schema, entity, &resolve_refs(field(obj, "query")?, prior)?).map_err(parse_err)?;
      let item = tx.find_first(&query, |ctx| decode_document(ctx).unwrap()).map_err(BatchErrorKind::Storage)?;
      Ok(item.map(json_value).unwrap_or(Value::Null))
    },
    "findMany" => {
      let query = parse_query(&db.schema, entity, &resolve_refs(field(obj, "query")?, prior)?).map_err(parse_err)?;
      let items = tx.find_many(&query, |ctx| decode_document(ctx).unwrap()).map_err(BatchErrorKind::Storage)?;
      Ok(json_value(array_to_json(&items)))
    },
    "count" => {
      let mut agg = parse_aggregate(&db.schema, entity, &resolve_refs(field(obj, "query")?, prior)?).map_err(parse_err)?;
      agg.count = true;
      let result = tx.aggregate(&agg).map_err(BatchErrorKind::Storage)?;
      Ok(Value::from(result.count))
    },
    "aggregate" => {
      let agg = parse_aggregate(&db.schema, entity, &resolve_refs(field(obj, "query")?, prior)?).map_err(parse_err)?;
      let result = tx.aggregate(&agg).map_err(BatchErrorKind::Storage)?;
      Ok(json_value(aggregate_to_json(&agg, &result)))
    },
    other => Err(BatchErrorKind::UnknownAction(other.to_string())),
  }
}

fn field<'a>(obj: &'a serde_json::Map<String, Value>, name: &'static str) -> Result<&'a Value, BatchErrorKind> {
  obj.get(name).ok_or(BatchErrorKind::MissingField(name))
}

/// Narrows an `updateMany` query to the part that selects rows. `$`-keys are kept — `$where` drives the
/// scan, and the ones `update_many` rejects (`$limit`/`$skip`/`$cursor`) must survive so they still
/// surface as an error rather than being silently ignored. Everything else is a field selection or a
/// relation include, which would only decode rows that updateMany discards.
pub fn update_many_query(query: &Value) -> Value {
  let Some(obj) = query.as_object() else { return query.clone() };
  Value::Object(obj.iter().filter(|(k, _)| k.starts_with('$')).map(|(k, v)| (k.clone(), v.clone())).collect())
}

/// Error from a single (non-transactional) operation dispatched by [`execute_op`].
#[derive(Debug)]
pub enum OpError {
  /// The operation is not a JSON object
  NotAnObject,
  /// A required field is missing (`model` / `action` / `data` / `id` / `query`)
  MissingField(&'static str),
  UnknownModel(String),
  UnknownAction(String),
  /// Error parsing the input JSON (encode / parse)
  Parse(String),
  Insert(InsertError),
  Update(UpdateError),
  Delete(DeleteError),
  Query(QueryError),
  Reindex(ReindexError),
  Storage(StorageError),
}

impl OpError {
  /// Whether this is an internal storage fault (→ 5xx) as opposed to a client/payload error (→ 4xx).
  /// Mirrors the server's `ApiError` mapping so the embedded transport reports the same error class.
  pub fn is_storage(&self) -> bool {
    match self {
      OpError::Storage(_) => true,
      OpError::Query(QueryError::Storage(_)) => true,
      OpError::Query(QueryError::Search(e)) => matches!(e, ReindexError::Storage(_) | ReindexError::Provider(crate::ProviderError::Storage(_))),
      OpError::Reindex(e) => matches!(e, ReindexError::Storage(_) | ReindexError::Provider(crate::ProviderError::Storage(_))),
      _ => false,
    }
  }
}

impl fmt::Display for OpError {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      OpError::NotAnObject => write!(f, "operation must be a JSON object"),
      OpError::MissingField(name) => write!(f, "missing required field '{}'", name),
      OpError::UnknownModel(m) => write!(f, "unknown model '{}'", m),
      OpError::UnknownAction(a) => write!(f, "unknown action '{}'", a),
      OpError::Parse(e) => write!(f, "{}", e),
      OpError::Insert(e) => write!(f, "{:?}", e),
      OpError::Update(e) => write!(f, "{:?}", e),
      OpError::Delete(e) => write!(f, "{:?}", e),
      OpError::Query(e) => write!(f, "{:?}", e),
      OpError::Reindex(e) => write!(f, "{}", e),
      OpError::Storage(e) => write!(f, "{:?}", e),
    }
  }
}

impl std::error::Error for OpError {}

fn op_field<'a>(obj: &'a serde_json::Map<String, Value>, name: &'static str) -> Result<&'a Value, OpError> {
  obj.get(name).ok_or(OpError::MissingField(name))
}

fn op_parse_err<E: fmt::Debug>(e: E) -> OpError {
  OpError::Parse(format!("{:?}", e))
}

/// Executes a single `{ model, action, ... }` operation against the DB, returning its result as a JSON
/// `Value`. Unlike [`execute_batch`], reads (`findMany`/`findFirst`) go through the search-capable
/// [`MarciDB`] methods, so `$near`/`$search` (`@custom` index) queries work; writes each run in their own
/// transaction (the engine's short-lived write methods). This is the embedding/FFI counterpart of the
/// server's per-route handlers — same command shape as one element of an `execute_batch` array.
///
/// Result format by action matches [`execute_batch`], plus `$reindex` → `{ "ok": true, "indexed": <n> }`.
pub fn execute_op(db: &MarciDB, op: &Value) -> Result<Value, OpError> {
  let obj = op.as_object().ok_or(OpError::NotAnObject)?;
  let model = obj.get("model").and_then(|m| m.as_str()).ok_or(OpError::MissingField("model"))?;
  let action = obj.get("action").and_then(|a| a.as_str()).ok_or(OpError::MissingField("action"))?;

  let entity = db.get_model(model).ok_or_else(|| OpError::UnknownModel(model.to_string()))?;

  match action {
    "insert" => {
      let write_op = parse_insert(&db.schema, entity, op_field(obj, "data")?).map_err(op_parse_err)?;
      let id = db.insert_item(entity, &write_op).map_err(OpError::Insert)?;
      Ok(json_value(decode_id(&id, entity, &db.schema)))
    },
    "update" => {
      let id = parse_id(&db.schema, entity, op_field(obj, "id")?).map_err(op_parse_err)?;
      let update_op = parse_update(&db.schema, entity, op_field(obj, "data")?).map_err(op_parse_err)?;
      db.update_item(entity, &id, &update_op).map_err(OpError::Update)?;
      Ok(Value::Null)
    },
    "updateMany" => {
      let query_json = update_many_query(op_field(obj, "query")?);
      let query = parse_query(&db.schema, entity, &query_json).map_err(op_parse_err)?;
      let update_op = parse_update(&db.schema, entity, op_field(obj, "data")?).map_err(op_parse_err)?;
      let updated = db.update_many(entity, &query, &update_op).map_err(OpError::Update)?;
      Ok(Value::from(updated))
    },
    "delete" => {
      let id = parse_id(&db.schema, entity, op_field(obj, "id")?).map_err(op_parse_err)?;
      let deleted = db.delete_item(entity, &id).map_err(OpError::Delete)?;
      Ok(Value::Bool(deleted))
    },
    "findFirst" => {
      let query = parse_query(&db.schema, entity, op_field(obj, "query")?).map_err(op_parse_err)?;
      let item = db.find_first(&query, |ctx| decode_document(ctx).unwrap()).map_err(OpError::Query)?;
      Ok(item.map(json_value).unwrap_or(Value::Null))
    },
    "findMany" => {
      let query = parse_query(&db.schema, entity, op_field(obj, "query")?).map_err(op_parse_err)?;
      let items = db.find_many(&query, |ctx| decode_document(ctx).unwrap()).map_err(OpError::Query)?;
      Ok(json_value(array_to_json(&items)))
    },
    "count" => {
      let mut agg = parse_aggregate(&db.schema, entity, op_field(obj, "query")?).map_err(op_parse_err)?;
      agg.count = true;
      let result = db.aggregate(&agg).map_err(OpError::Storage)?;
      Ok(Value::from(result.count))
    },
    "aggregate" => {
      let agg = parse_aggregate(&db.schema, entity, op_field(obj, "query")?).map_err(op_parse_err)?;
      let result = db.aggregate(&agg).map_err(OpError::Storage)?;
      Ok(json_value(aggregate_to_json(&agg, &result)))
    },
    "$reindex" => {
      let indexed = db.reindex_entity(entity).map_err(OpError::Reindex)?;
      Ok(serde_json::json!({ "ok": true, "indexed": indexed }))
    },
    other => Err(OpError::UnknownAction(other.to_string())),
  }
}

fn parse_err<E: fmt::Debug>(e: E) -> BatchErrorKind {
  BatchErrorKind::Parse(format!("{:?}", e))
}

/// Parses the string assembled by the json layer back into a `Value`. Decoding always yields valid JSON
fn json_value(s: String) -> Value {
  serde_json::from_str(&s).expect("decoded document must be valid JSON")
}

/// Recursively substitutes `{"$ref":"<i>.<path>"}` with values from the results of previous operations
fn resolve_refs(value: &Value, prior: &[Value]) -> Result<Value, BatchErrorKind> {
  match value {
    Value::Object(map) => {
      if map.len() == 1 && let Some(Value::String(reference)) = map.get("$ref") {
        return resolve_ref_path(reference, prior);
      }
      let mut out = serde_json::Map::with_capacity(map.len());
      for (key, val) in map {
        out.insert(key.clone(), resolve_refs(val, prior)?);
      }
      Ok(Value::Object(out))
    },
    Value::Array(items) => {
      let mut out = Vec::with_capacity(items.len());
      for item in items {
        out.push(resolve_refs(item, prior)?);
      }
      Ok(Value::Array(out))
    },
    other => Ok(other.clone()),
  }
}

/// `"0"` → the result of operation 0; `"0.id"` / `"0.author.id"` — a field within it by dotted path
fn resolve_ref_path(reference: &str, prior: &[Value]) -> Result<Value, BatchErrorKind> {
  let bad = || BatchErrorKind::BadRef(reference.to_string());

  let mut parts = reference.split('.');
  let index: usize = parts.next().and_then(|p| p.parse().ok()).ok_or_else(bad)?;
  let mut current = prior.get(index).ok_or_else(bad)?;
  for key in parts {
    current = current.get(key).ok_or_else(bad)?;
  }
  Ok(current.clone())
}
