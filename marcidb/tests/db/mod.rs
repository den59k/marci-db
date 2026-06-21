#![allow(dead_code)]

pub mod delete_tests;
pub mod indexes_tests;
pub mod insert_tests;
pub mod query_tests;
pub mod unique_tests;
pub mod update_tests;
pub mod update_ref_tests;
pub mod companion_id_tests;

// ── Существующие модули ────────────────────────────────────────────────────────
pub mod where_ops_tests;       // Операторы $lt, $lte, $gte, $eq, $ne, $in, $notIn, $and, $not, $every
pub mod delete_restrict_tests; // @onDelete(Restrict) → DeleteError::RestrictConstraints
mod persistense_tests;
mod string_ops_tests;
mod enum_list_tests;

// ── Новые модули покрытия ─────────────────────────────────────────────────────
pub mod url_parser_tests;          // url_parser/url_parsers.rs  14.58% → ~85%
pub mod parsers_tests;             // json_parsers/parsers.rs    50.39% + parse_where.rs 49.11%
pub mod num_utils_tests;           // num_utils.rs 44.44% + index_utils.rs 60.74%
pub mod process_where_tests;       // query_op/process_where.rs 50.83% + process_query_one.rs 82.50%
pub mod delete_update_coverage_tests; // process_delete.rs 75.59% + process_update.rs 75.86%
pub mod schema_tests;              // schema_* файлы 70-83%]
pub mod new_tests;
mod where_coverage_tests;

use std::str::FromStr;

use marcidb::{MarciDB, array_to_json, decode_document, decode_id, parse_id, parse_insert, parse_query, parse_update};
use serde_json::Value;


pub fn insert_data(db: &MarciDB, model: &str, data: Value) -> Value {
  let entity = db.get_model(model).unwrap();
  let to_insert = parse_insert(&db.schema, entity, &data).unwrap();
  let item_id = db.insert_item(entity, &to_insert).unwrap();
  Value::from_str(&decode_id(&item_id, entity, &db.schema)).unwrap()
}

pub fn update_data(db: &MarciDB, model: &str, item_id: &Value, data: Value) {
  let entity = db.get_model(model).unwrap();
  let id = parse_id(&db.schema, entity, item_id).unwrap();
  let to_update = parse_update(&db.schema, entity, &data).unwrap();
  db.update_item(entity, &id, &to_update).unwrap();
}

pub fn get_data(db: &MarciDB, model: &str, json_query: Value) -> Value {
  let entity = db.get_model(model).unwrap();
  let query = parse_query(&db.schema, entity, &json_query).unwrap();

  let items = db.find_many(&query, |ctx| decode_document(ctx).unwrap());
  Value::from_str(&array_to_json(&items)).unwrap()
}


pub fn get_data_one(db: &MarciDB, model: &str, json_query: Value) -> Value {
  let entity = db.get_model(model).unwrap();
  let query = parse_query(&db.schema, entity, &json_query).unwrap();

  let item = db.find_first(&query, |ctx| decode_document(ctx).unwrap());
  if let Some(item) = item {
    Value::from_str(&item).unwrap()
  } else {
    Value::Null
  }
}

pub fn delete_data(db: &MarciDB, model: &str, data: Value) {
  let entity = db.get_model(model).unwrap();
  let id = parse_id(&db.schema, entity, &data).unwrap();
  db.delete_item(entity, &id).unwrap();
}