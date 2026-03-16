
mod common;

use marcidb::{MarciDB};
use serde_json::json;
use tempfile::tempdir;

use crate::common::{get_data, insert_data};

#[test]
fn base_query_test() {

  let schema_str = "
    model User {
      name        String
      age         UInt?
      email       String?
      active      Boolean     @default(true)
    }
  ";

  let dir = tempdir().unwrap();
  let db: MarciDB = MarciDB::new(schema_str, dir.path().to_str().unwrap());  
  
  insert_data(&db, "User", json!({ "name": "Alice", "age": 20, "email": "alice@test.com" }));
  insert_data(&db, "User", json!({ "name": "Bob", "age": 40, "email": "bob@test.com" }));
  insert_data(&db, "User", json!({ "name": "Charlie", "age": 18, "email": null }));
  insert_data(&db, "User", json!({ "name": "Unknown", "age": null, "email": null }));
  
  {
    let resp = get_data(&db, "User", json!({
      "name": true,
      "$where": { "email": "alice@test.com" }
    }));
    assert_eq!(resp, json!([
      { "name": "Alice" }
    ]))
  }

  {
    let resp = get_data(&db, "User", json!({
      "name": true,
      "$where": { "email": { "$not": null } }
    }));
    assert_eq!(resp, json!([
      { "name": "Alice" }, { "name": "Bob" }
    ]))
  }

  {
    let resp = get_data(&db, "User", json!({
      "name": true,
      "$where": { "age": { "$gt": 20 }, "email": { "$not": null } }
    }));
    assert_eq!(resp, json!([
      { "name": "Bob" }
    ]))
  }

}