mod common;

use marcidb::{InsertError, MarciDB, UpdateError, parse_id, parse_insert, parse_update};
use serde_json::json;
use tempfile::tempdir;

use crate::common::{get_data, insert_data};

#[test]
fn write_unique_test() {

  let schema_str = "
    model User {
        email       String?     @unique
        name        String
        passport    Passport    @bind(Passport.user)
    }

    model Passport {
        id          String      @id
        user        User        @unique
    }
  ";

  let dir = tempdir().unwrap();
  let db: MarciDB = MarciDB::new(schema_str, dir.path().to_str().unwrap());  
  
  let user_a = insert_data(&db, "User", json!({ "name": "Alice", "email": "alice@test.test" }));
  let user_b = insert_data(&db, "User", json!({ "name": "Bob", "email": "bob@test.test" }));
  let _user_c = insert_data(&db, "User", json!({ "name": "Charlie" }));
  
  let passport_a = insert_data(&db, "Passport", json!({ "id": "123123", "user": &user_a }));
  let _passport_b = insert_data(&db, "Passport", json!({ "id": "123124", "user": &user_b }));

  let user_a_id = parse_id(&db.schema, db.get_model("User").unwrap(), &user_a).unwrap();
  let user_b_id = parse_id(&db.schema, db.get_model("User").unwrap(), &user_b).unwrap();

  let passport_a_id = parse_id(&db.schema, db.get_model("Passport").unwrap(), &passport_a).unwrap();

  {
    let entity = db.get_model("User").unwrap();
    let to_insert = parse_insert(&db.schema, entity, &json!({ "name": "Alice New", "email": "alice@test.test" })).unwrap();
    let resp = db.insert_item(entity, &to_insert);
    assert_eq!(resp, Err(InsertError::UniqueViolation("User.email".to_string(), user_a_id.clone())));
  }

  {
    let entity = db.get_model("User").unwrap();
    let to_update = parse_update(&db.schema, entity, &json!({ "email": "bob@test.test" })).unwrap();
    let resp = db.update_item(entity, &user_a_id, &to_update);
    assert_eq!(resp, Err(UpdateError::UniqueViolation("User.email".to_string(), user_b_id.clone())));
  }

  {
    let entity = db.get_model("Passport").unwrap();
    let to_insert = parse_insert(&db.schema, entity, &json!({ "id": "111111", "user": &user_a })).unwrap();
    let resp = db.insert_item(entity, &to_insert);
    assert_eq!(resp, Err(InsertError::UniqueViolation("Passport.user".to_string(), passport_a_id.clone())));
  }

  {
    let resp = get_data(&db, "User", json!({
      "name": true, "passport": true
    }));

    assert_eq!(resp, json!([
      { "name": "Alice", "passport": { "id": "123123" } },
      { "name": "Bob", "passport": { "id": "123124" } },
      { "name": "Charlie", "passport": null }
    ]))
  }
}