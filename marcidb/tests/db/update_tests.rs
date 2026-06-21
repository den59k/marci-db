use marcidb::{parse_id, MarciDB};
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

// ========== Тесты на проверку null для not-nullable полей при обновлении ==========

#[test]
fn update_null_not_allowed_test() {
  use marcidb::{parse_update, EncodeError};

  let schema_str = "
        model User {
            name    String
            age     Int?
            email   String
        }
    ";
  let dir = tempdir().unwrap();
  let db = MarciDB::new(schema_str, dir.path().to_str().unwrap());
  let entity = db.get_model("User").unwrap();
  let user = insert_data(&db, "User", json!({ "name": "Alice", "age": 30, "email": "alice@test.com" }));
  let id = parse_id(&db.schema, entity, &user).unwrap();

  // Попытка установить null для поля name (обязательное)
  let res = parse_update(&db.schema, entity, &json!({ "name": null }));
  assert!(
    matches!(res, Err(EncodeError::NullNotAllowed(ref s)) if s.ends_with("name")),
    "Ожидалась NullNotAllowed для поля name, получено: {:?}", res
  );

  // Попытка установить null для поля email (обязательное)
  let res = parse_update(&db.schema, entity, &json!({ "email": null }));
  assert!(
    matches!(res, Err(EncodeError::NullNotAllowed(ref s)) if s.ends_with("email")),
    "Ожидалась NullNotAllowed для поля email, получено: {:?}", res
  );

  // age – nullable, установка в null должна быть разрешена
  let update_op = parse_update(&db.schema, entity, &json!({ "age": null })).unwrap();
  let result = db.update_item(entity, &id, &update_op);
  assert!(result.is_ok(), "Ошибка при обновлении nullable поля на null: {:?}", result);

  // Проверяем, что возраст стал null
  let resp = get_data(&db, "User", json!({ "name": true, "age": true, "$where": { "name": "Alice" } }));
  assert_eq!(resp, json!([ { "name": "Alice", "age": null } ]));
}