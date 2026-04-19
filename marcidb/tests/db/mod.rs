#![allow(dead_code)]

pub mod delete_tests;
pub mod indexes_tests;
pub mod insert_tests;
pub mod query_tests;
pub mod unique_tests;
pub mod update_tests;
pub mod update_ref_tests;
pub mod companion_id_tests;

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

  // println!("{:#?}", query);

  let items = db.find_many(&query, |ctx| decode_document(ctx).unwrap());
  Value::from_str(&array_to_json(&items)).unwrap()
}


pub fn get_data_one(db: &MarciDB, model: &str, json_query: Value) -> Value {
  let entity = db.get_model(model).unwrap();
  let query = parse_query(&db.schema, entity, &json_query).unwrap();

  // println!("{:#?}", query);

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
