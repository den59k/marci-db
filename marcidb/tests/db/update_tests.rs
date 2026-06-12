use marcidb::MarciDB;
use serde_json::json;
use tempfile::tempdir;

use crate::db::{get_data, insert_data,update_data};


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
    update_data(&db, "User", &user_b, json!({ "active": true }));

    let resp = get_data(&db, "User", json!({ "name": true, "$where": { "active": true } }));
    assert_eq!(resp, json!([ { "name": "Alice" }, { "name": "Bob" } ]))
  }

  {
    update_data(&db, "User", &user_a, json!({ "email": "alice-new@test.com", "age": { "$increment": 5 } }));

    let resp = get_data(&db, "User", json!({
      "name": true, "age": true, "active": true, "$where": { "email": "alice-new@test.com" }
    }));
    assert_eq!(resp, json!([ { "name": "Alice", "age": 25, "active": true } ]))
  }

}

#[test]
fn enum_variant_update_test() {
  let schema_str = "
    model Account {
      name        String
      type        AccountType
    }

    enum AccountType {
      basic
      pro {
        sign      String
      }
    }
  ";

  let dir = tempdir().unwrap();
  let db: MarciDB = MarciDB::new(schema_str, dir.path().to_str().unwrap());

  let account = insert_data(&db, "Account", json!({ "name": "Alice", "type": "pro", "sign": "alice-sign" }));

  {
    let resp = get_data(&db, "Account", json!({ "name": true, "type": true, "sign": true }));
    assert_eq!(resp, json!([ { "name": "Alice", "type": "pro", "sign": "alice-sign" } ]));
  }

  // Смена варианта enum очищает поля старого варианта
  {
    update_data(&db, "Account", &account, json!({ "type": "basic" }));

    let resp = get_data(&db, "Account", json!({ "name": true, "type": true, "sign": true }));
    assert_eq!(resp, json!([ { "name": "Alice", "type": "basic" } ]));
  }

  // Старое значение sign не "воскресает" при обратной смене варианта
  {
    update_data(&db, "Account", &account, json!({ "type": "pro" }));

    let resp = get_data(&db, "Account", json!({ "name": true, "type": true, "sign": true }));
    assert_eq!(resp, json!([ { "name": "Alice", "type": "pro", "sign": null } ]));
  }

  // Обновление поля чужого варианта игнорируется и не пишет данные в body
  {
    update_data(&db, "Account", &account, json!({ "type": "basic" }));
    update_data(&db, "Account", &account, json!({ "sign": "sneaky" }));

    let resp = get_data(&db, "Account", json!({ "name": true, "type": true, "sign": true }));
    assert_eq!(resp, json!([ { "name": "Alice", "type": "basic" } ]));

    update_data(&db, "Account", &account, json!({ "type": "pro" }));
    let resp = get_data(&db, "Account", json!({ "sign": true }));
    assert_eq!(resp, json!([ { "sign": null } ]));
  }

  // Смена варианта вместе с установкой его полей в одном update
  {
    update_data(&db, "Account", &account, json!({ "type": "pro", "sign": "new-sign" }));

    let resp = get_data(&db, "Account", json!({ "type": true, "sign": true }));
    assert_eq!(resp, json!([ { "type": "pro", "sign": "new-sign" } ]));
  }
}