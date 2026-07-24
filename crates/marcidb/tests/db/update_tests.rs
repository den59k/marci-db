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

  // Changing the enum variant clears the fields of the old variant
  {
    update_data(&db, "Account", &account, json!({ "type": "basic" }));

    let resp = get_data(&db, "Account", json!({ "name": true, "type": true, "sign": true }));
    assert_eq!(resp, json!([ { "name": "Alice", "type": "basic" } ]));
  }

  // The old sign value does not "come back to life" when switching the variant back
  {
    update_data(&db, "Account", &account, json!({ "type": "pro" }));

    let resp = get_data(&db, "Account", json!({ "name": true, "type": true, "sign": true }));
    assert_eq!(resp, json!([ { "name": "Alice", "type": "pro", "sign": null } ]));
  }

  // Updating a field of a foreign variant is ignored and does not write data into the body
  {
    update_data(&db, "Account", &account, json!({ "type": "basic" }));
    update_data(&db, "Account", &account, json!({ "sign": "sneaky" }));

    let resp = get_data(&db, "Account", json!({ "name": true, "type": true, "sign": true }));
    assert_eq!(resp, json!([ { "name": "Alice", "type": "basic" } ]));

    update_data(&db, "Account", &account, json!({ "type": "pro" }));
    let resp = get_data(&db, "Account", json!({ "sign": true }));
    assert_eq!(resp, json!([ { "sign": null } ]));
  }

  // Changing the variant together with setting its fields in a single update
  {
    update_data(&db, "Account", &account, json!({ "type": "pro", "sign": "new-sign" }));

    let resp = get_data(&db, "Account", json!({ "type": true, "sign": true }));
    assert_eq!(resp, json!([ { "type": "pro", "sign": "new-sign" } ]));
  }
}

#[test]
fn enum_shared_field_update_test() {
  let schema_str = "
    model Account {
      name        String
      type        AccountType
    }

    enum AccountType {
      basic
      pro | business {
        sign      String
      }
      business {
        company   String
      }
    }
  ";

  let dir = tempdir().unwrap();
  let db: MarciDB = MarciDB::new(schema_str, dir.path().to_str().unwrap());

  let account = insert_data(&db, "Account", json!({
    "name": "Alice", "type": "business", "sign": "alice-sign", "company": "ACME"
  }));

  {
    let resp = get_data(&db, "Account", json!({ "type": true, "sign": true, "company": true }));
    assert_eq!(resp, json!([ { "type": "business", "sign": "alice-sign", "company": "ACME" } ]));
  }

  // Changing the variant within a shared-field group: company is cleared, sign is overwritten with the new payload
  {
    update_data(&db, "Account", &account, json!({ "type": "pro", "sign": "pro-sign" }));

    let resp = get_data(&db, "Account", json!({ "type": true, "sign": true, "company": true }));
    assert_eq!(resp, json!([ { "type": "pro", "sign": "pro-sign" } ]));
  }

  // The shared field is not cleared when switching within the group, even if not provided
  {
    update_data(&db, "Account", &account, json!({ "type": "business", "company": "ACME" }));

    let resp = get_data(&db, "Account", json!({ "type": true, "sign": true, "company": true }));
    assert_eq!(resp, json!([ { "type": "business", "sign": "pro-sign", "company": "ACME" } ]));
  }

  // Switching to a variant outside the group clears the shared field
  {
    update_data(&db, "Account", &account, json!({ "type": "basic" }));

    let resp = get_data(&db, "Account", json!({ "type": true, "sign": true, "company": true }));
    assert_eq!(resp, json!([ { "type": "basic" } ]));
  }

  // The shared field does not "come back to life" when returning to the group
  {
    update_data(&db, "Account", &account, json!({ "type": "pro" }));

    let resp = get_data(&db, "Account", json!({ "type": true, "sign": true }));
    assert_eq!(resp, json!([ { "type": "pro", "sign": null } ]));
  }

  // Updating the shared field without changing the variant works from any variant in the group
  {
    update_data(&db, "Account", &account, json!({ "sign": "updated-sign" }));

    let resp = get_data(&db, "Account", json!({ "type": true, "sign": true }));
    assert_eq!(resp, json!([ { "type": "pro", "sign": "updated-sign" } ]));
  }

  // Outside the group, updating the shared field is ignored
  {
    update_data(&db, "Account", &account, json!({ "type": "basic" }));
    update_data(&db, "Account", &account, json!({ "sign": "sneaky" }));

    let resp = get_data(&db, "Account", json!({ "type": true, "sign": true }));
    assert_eq!(resp, json!([ { "type": "basic" } ]));
  }
}