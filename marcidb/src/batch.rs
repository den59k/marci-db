use std::fmt;

use serde_json::Value;

use crate::{DeleteError, InsertError, MarciDB, MarciTransaction, StorageError, UpdateError, aggregate_to_json, array_to_json, decode_document, decode_id, parse_aggregate, parse_id, parse_insert, parse_query, parse_update};

/// Ошибка batch-транзакции с индексом операции, на которой она произошла.
/// `index == ops.len()` означает ошибку на коммите (после всех операций)
#[derive(Debug)]
pub struct BatchError {
  pub index: usize,
  pub kind: BatchErrorKind,
}

#[derive(Debug)]
pub enum BatchErrorKind {
  /// Операция — не JSON-объект
  NotAnObject,
  /// Не хватает обязательного поля (`model` / `action` / `data` / `id` / `query`)
  MissingField(&'static str),
  UnknownModel(String),
  UnknownAction(String),
  /// Невалидная ссылка `$ref` на результат предыдущей операции
  BadRef(String),
  /// Ошибка разбора входного JSON (encode / parse)
  Parse(String),
  Insert(InsertError),
  Update(UpdateError),
  Delete(DeleteError),
  /// Ошибка хранилища (в т.ч. на коммите)
  Storage(StorageError),
}

impl fmt::Display for BatchError {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "batch op #{}: {:?}", self.index, self.kind)
  }
}

impl std::error::Error for BatchError {}

/// Выполняет список операций в одной атомарной транзакции.
///
/// При успехе возвращает результаты по одному на операцию (в том же порядке) и коммитит.
/// При первой же ошибке вся транзакция откатывается, а в [`BatchError`] возвращается индекс
/// упавшей операции. Поддерживаются ссылки `{"$ref":"<i>.<path>"}` на результат операции `i`
/// (например, сгенерированный id), которые разрешаются перед исполнением операции.
///
/// Формат результата по типам операций:
/// * `insert` — объект id (как у одиночной вставки)
/// * `update` — `null`
/// * `delete` — `true`/`false`
/// * `findFirst` — объект или `null`
/// * `findMany` — массив объектов
/// * `aggregate` — объект агрегатов; `count` — число
pub fn execute_batch(db: &MarciDB, ops: &[Value]) -> Result<Vec<Value>, BatchError> {
  let tx = db.begin_write();

  let mut results: Vec<Value> = Vec::with_capacity(ops.len());
  for (index, op) in ops.iter().enumerate() {
    let result = apply_op(&tx, db, op, &results).map_err(|kind| BatchError { index, kind })?;
    results.push(result);
  }

  // Любой ранний выход выше уронит tx без коммита → откат
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

fn parse_err<E: fmt::Debug>(e: E) -> BatchErrorKind {
  BatchErrorKind::Parse(format!("{:?}", e))
}

/// Разбирает строку, собранную json-слоем, обратно в `Value`. Декод всегда даёт валидный JSON
fn json_value(s: String) -> Value {
  serde_json::from_str(&s).expect("decoded document must be valid JSON")
}

/// Рекурсивно подставляет `{"$ref":"<i>.<path>"}` значениями из результатов предыдущих операций
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

/// `"0"` → результат операции 0; `"0.id"` / `"0.author.id"` — поле в нём по точечному пути
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
