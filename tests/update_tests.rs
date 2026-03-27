use marcidb::MarciDB;
use serde_json::json;
use tempfile::tempdir;

use crate::common::{get_data, insert_data, update_data};

mod common;

#[test]
fn base_update_test() {
  let schema_str = "
    model User {
      name        String
      age         UInt?
      email       String?     @unique
      active      Boolean     @default(true)
    }
  ";

  let dir = tempdir().unwrap();
  let db: MarciDB = MarciDB::new(schema_str, dir.path().to_str().unwrap());  

  let user_a = insert_data(&db, "User", json!({ "name": "Alice", "age": 20, "email": "alice@test.com" }));
  let user_b = insert_data(&db, "User", json!({ "name": "Bob", "age": 40, "email": "bob@test.com", "active": false }));

  {
    let resp = get_data(&db, "User", json!({ "name": true, "$where": { "active": true } }));
    assert_eq!(resp, json!([ { "name": "Alice" } ]))
  }
  
  {
    update_data(&db, "User", user_b, json!({ "active": true }));

    let resp = get_data(&db, "User", json!({ "name": true, "$where": { "active": true } }));
    assert_eq!(resp, json!([ { "name": "Alice" }, { "name": "Bob" } ]))
  }

  {
    update_data(&db, "User", user_a, json!({ "email": "alice-new@test.com", "age": { "$increment": 5 } }));

    let resp = get_data(&db, "User", json!({ 
      "name": true, "age": true, "active": true, "$where": { "email": "alice-new@test.com" }
    }));
    assert_eq!(resp, json!([ { "name": "Alice", "age": 25, "active": true } ]))
  }
  
}