//! `upsert`: write the row a key names — an update when it exists, an insert when it does not — in one
//! operation, so an application keeps no check-then-insert of its own (a setting per user and key, one
//! like per user and post, a counter per day). The key is the id, or the fields of ONE unique index: a
//! `@unique` field, or all the fields of a `@unique(<name>)` group — what makes "the row" well defined.
//! The data is in the INSERT form (plain values, `{ id }` for a relation): on the update path it is
//! translated (`$connect`, `$set`), `$`-operators are refused, since an increment has no meaning for a row
//! that is being created. `on_insert` adds fields used only when the row is created (an id the client
//! generates, a `createdAt`).

use canopydb::WriteTransaction;
use serde_json::{Map, Value, json};

use crate::{MarciDB, StorageError, error::RequireTree, index_utils::encode_index, json_parsers::{EncodeError, encode_enum, encode_primitive_value, parse_id, parse_insert, parse_update}, schema::{Entity, Field, FieldLocation, FieldType, Schema}, update_op::{UpdateError, process_update}, write_op::{InsertError, process_write}};

#[derive(Debug)]
pub enum UpsertError {
  /// The key is not the id nor the fields of one unique index, a key value is null, or the data carries an operator.
  BadRequest(String),
  Parse(EncodeError),
  Insert(InsertError),
  Update(UpdateError),
  Storage(StorageError),
}

impl From<canopydb::Error> for UpsertError { fn from(e: canopydb::Error) -> Self { UpsertError::Storage(StorageError::Backend(e)) } }
impl From<StorageError> for UpsertError { fn from(e: StorageError) -> Self { UpsertError::Storage(e) } }

impl std::fmt::Display for UpsertError {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      UpsertError::BadRequest(m) => write!(f, "{}", m),
      UpsertError::Parse(e) => write!(f, "{}", e),
      UpsertError::Insert(e) => write!(f, "{}", e),
      UpsertError::Update(e) => write!(f, "{}", e),
      UpsertError::Storage(e) => write!(f, "{}", e),
    }
  }
}
impl std::error::Error for UpsertError {}

impl UpsertError {
  /// The class of the failure, as clients see it — see `InsertError::kind`.
  pub fn kind(&self) -> &'static str {
    match self {
      UpsertError::BadRequest(_) | UpsertError::Parse(_) => "bad_request",
      UpsertError::Insert(e) => e.kind(),
      UpsertError::Update(e) => e.kind(),
      UpsertError::Storage(_) => "internal",
    }
  }
  /// The field the failure is about (`Model.field`), when it is about one.
  pub fn field(&self) -> Option<&str> {
    match self {
      UpsertError::Insert(e) => e.field(),
      UpsertError::Update(e) => e.field(),
      _ => None,
    }
  }
}

/// The operation, inside an open write transaction. Returns the id of the row written.
pub fn process_upsert(tx: &WriteTransaction, db: &MarciDB, entity: &Entity, key: &Value, data: &Value, on_insert: Option<&Value>) -> Result<Vec<u8>, UpsertError> {
  let schema = &db.schema;
  let Some(key) = key.as_object() else {
    return Err(UpsertError::BadRequest(format!("{}: the upsert key must be an object — the id, or the fields of one unique index", entity.name)));
  };
  let Some(data) = data.as_object() else {
    return Err(UpsertError::BadRequest(format!("{}: the upsert data must be an object", entity.name)));
  };
  refuse_operators(entity, data)?;
  match find_by_key(tx, schema, entity, key)? {
    Some(id) => {
      let update = insert_form_to_update(schema, entity, data)?;
      let op = parse_update(schema, entity, &update).map_err(UpsertError::Parse)?;
      if !process_update(tx, entity, &id, &op, db).map_err(UpsertError::Update)? {
        return Err(UpsertError::Update(UpdateError::ItemNotFound));
      }
      Ok(id)
    }
    None => {
      // the row: the data, what is only for a creation, and the key — which wins over both
      let mut row = data.clone();
      if let Some(extra) = on_insert.and_then(|v| v.as_object()) {
        for (k, v) in extra { row.entry(k.clone()).or_insert(v.clone()); }
      }
      for (k, v) in key { row.insert(k.clone(), v.clone()); }
      let op = parse_insert(schema, entity, &Value::Object(row)).map_err(UpsertError::Parse)?;
      process_write(tx, entity, &op, db, None).map_err(UpsertError::Insert)
    }
  }
}

/// The id of the row the key names, when there is one. The key's fields must be exactly the id's, all
/// the fields of a unique group, or one `@unique` field.
fn find_by_key(tx: &WriteTransaction, schema: &Schema, entity: &Entity, key: &Map<String, Value>) -> Result<Option<Vec<u8>>, UpsertError> {
  let mut names: Vec<&str> = key.keys().map(|k| k.as_str()).collect();
  names.sort_unstable();

  let mut id_names: Vec<&str> = entity.fields.iter().filter(|f| matches!(f.location, FieldLocation::Key { .. })).map(|f| f.name.as_str()).collect();
  id_names.sort_unstable();
  if names == id_names {
    let id = parse_id(schema, entity, &Value::Object(key.clone())).map_err(UpsertError::Parse)?;
    let tree = tx.require_tree(entity.name.as_bytes())?;
    return Ok(tree.get(&id)?.map(|_| id));
  }

  for group in entity.unique_groups.iter() {
    let mut part_names: Vec<&str> = group.parts.iter().map(|(fi, _)| entity.fields[*fi].name.as_str()).collect();
    part_names.sort_unstable();
    if part_names != names { continue }
    let mut prefix = Vec::new();
    for (fi, part) in group.parts.iter() {
      let field = &entity.fields[*fi];
      let stored = stored_value(schema, field, &key[&field.name])?;
      prefix.extend(encode_index(field, part, &stored));
    }
    return lookup(tx, group.tree_name.as_bytes(), &prefix);
  }

  if names.len() == 1
    && let Some(field) = entity.fields.iter().find(|f| f.name == names[0])
    && let Some(index) = field.indexes.iter().find(|i| i.is_unique())
  {
    let stored = stored_value(schema, field, &key[&field.name])?;
    return lookup(tx, index.tree_name(), &encode_index(field, index, &stored));
  }

  Err(UpsertError::BadRequest(format!(
    "{}: the upsert key must be the id ({}) or the fields of one unique index — a @unique field, or all the fields of a @unique(<name>) group; got {}",
    entity.name, id_names.join(", "), names.join(", ")
  )))
}

fn lookup(tx: &WriteTransaction, tree_name: &[u8], prefix: &[u8]) -> Result<Option<Vec<u8>>, UpsertError> {
  let tree = tx.require_tree(tree_name)?;
  match tree.prefix_keys(&prefix)?.next() {
    Some(entry) => Ok(Some(entry?[prefix.len()..].to_vec())),
    None => Ok(None),
  }
}

/// A key field's value as the row stores it — what `get_data` returns, so it can be encoded as an index key.
fn stored_value(schema: &Schema, field: &Field, value: &Value) -> Result<Vec<u8>, UpsertError> {
  if value.is_null() {
    return Err(UpsertError::BadRequest(format!("{}: a value of the upsert key cannot be null", field.full_name)));
  }
  let mut buf = Vec::new();
  match &field.ty {
    FieldType::Primitive(ty) => encode_primitive_value(&mut buf, field, ty, value).map_err(UpsertError::Parse)?,
    FieldType::Enum(info) => encode_enum(&mut buf, field, info, value).map_err(UpsertError::Parse)?,
    FieldType::Ref(ref_info) => buf = parse_id(schema, &schema.models[ref_info.model_index], value).map_err(UpsertError::Parse)?,
    _ => return Err(UpsertError::BadRequest(format!("{}: a list cannot be a field of the upsert key", field.full_name))),
  }
  Ok(buf)
}

/// A `$`-operator in the data is refused on either path: a row may be created by the operation, and an
/// increment of a row that does not exist yet has no meaning.
fn refuse_operators(entity: &Entity, data: &Map<String, Value>) -> Result<(), UpsertError> {
  for (name, value) in data {
    if let Some(obj) = value.as_object() && let Some(op) = obj.keys().find(|k| k.starts_with('$')) {
      return Err(UpsertError::BadRequest(format!(
        "{}.{}: upsert data takes the insert form — plain values, {{ id }} for a relation; {} is not allowed (a row may be created by it)",
        entity.name, name, op
      )));
    }
  }
  Ok(())
}

/// The data, written in the insert form, as the update grammar wants it: a relation `{ id }` becomes
/// `$connect`, a struct or a list becomes `$set`.
fn insert_form_to_update(schema: &Schema, entity: &Entity, data: &Map<String, Value>) -> Result<Value, UpsertError> {
  let mut out = Map::with_capacity(data.len());
  for (name, value) in data {
    let Some(field) = entity.fields.iter().find(|f| &f.name == name) else {
      out.insert(name.clone(), value.clone());   // parse_update names the unknown field
      continue;
    };
    let converted = match &field.ty {
      FieldType::Ref(ref_info) if !value.is_null() => {
        if schema.models[ref_info.model_index].autoinsert { json!({ "$set": value }) } else { json!({ "$connect": value }) }
      }
      FieldType::RefList(_) => json!({ "$set": value }),
      _ => value.clone(),
    };
    out.insert(name.clone(), converted);
  }
  Ok(Value::Object(out))
}
