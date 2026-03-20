#![allow(dead_code)]

use std::str::FromStr;

use marcidb::{MarciDB, array_to_json, decode_document, decode_id, parse_insert, parse_query};
use serde_json::Value;


pub fn insert_data(db: &MarciDB, model: &str, data: Value) -> Value {
  let entity = db.get_model(model).unwrap();
  let to_insert = parse_insert(&db.schema, entity, &data).unwrap();
  let item_id = db.insert_data(&to_insert).unwrap();
  Value::from_str(&decode_id(&item_id, entity, &db.schema)).unwrap()
}

pub fn get_data(db: &MarciDB, model: &str, json_query: Value) -> Value {
  let entity = db.get_model(model).unwrap();
  let query = parse_query(&db.schema, entity, &json_query).unwrap();

  println!("{:#?}", query);

  let items = db.find_many(&query, |ctx| decode_document(ctx).unwrap());
  Value::from_str(&array_to_json(&items)).unwrap()
}
