#![allow(dead_code)]

use std::str::FromStr;

use marcidb::{MarciDB, array_to_json, decode_document, decode_id, parse_id, parse_insert, parse_query, parse_update};
use serde_json::Value;


pub fn insert_data(db: &MarciDB, model: &str, data: Value) -> Value {
  let entity = db.get_model(model).unwrap();
  let to_insert = parse_insert(&db.schema, entity, &data).unwrap();
  let item_id = db.insert_item(entity, &to_insert).unwrap();
  Value::from_str(&decode_id(&item_id, entity, &db.schema)).unwrap()
}

pub fn update_data(db: &MarciDB, model: &str, r#where: Value, data: Value) {
  let entity = db.get_model(model).unwrap();
  let id = parse_id(&db.schema, entity, &r#where).unwrap();
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

pub fn delete_data(db: &MarciDB, model: &str, data: Value) {
  let entity = db.get_model(model).unwrap();
  let id = parse_id(&db.schema, entity, &data).unwrap();
  db.delete_item(entity, &id).unwrap();
}
