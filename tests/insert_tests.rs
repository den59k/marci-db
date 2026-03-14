use std::str::FromStr;

use marcidb::{MarciDB, array_to_json, decode_document, decode_id, parse_insert, parse_query};
use serde_json::{Value, json};
use tempfile::tempdir;

pub fn insert_data(db: &MarciDB, model: &str, data: Value) -> Value {
  let entity = db.get_model(model).unwrap();
  let to_insert = parse_insert(&db.schema, entity, &data).unwrap();
  let item_id = db.insert_data(&to_insert).unwrap();
  Value::from_str(&decode_id(&item_id, entity, &db.schema)).unwrap()
}

pub fn get_data(db: &MarciDB, model: &str, json_query: Value) -> Value {
  let entity = db.get_model(model).unwrap();
  let query = parse_query(&db.schema, entity, &json_query).unwrap();
  let items = db.find_many(&query, |ctx| decode_document(ctx).unwrap());
  Value::from_str(&array_to_json(&items)).unwrap()
}

#[test]
fn base_insert_test() {
  let schema_str = "
    model User {
        name        String
        info        UserInfo?
        posts       Post[]  @derived(Post.author)
    }
    
    struct UserInfo {
        bio         String
    }

    model Post {
        title       String
        author      User?
    }

    model Project {
        name        String
        users       UserRole[]
    }

    struct UserRole {
        user        User          @id
        role        String
    }
  ";

  let dir = tempdir().unwrap();
  let db: MarciDB = MarciDB::new(schema_str, dir.path().to_str().unwrap());  

  let user_a =  insert_data(&db, "User", json!({ "name": "Alice" }));
  let _user_b = insert_data(&db, "User", json!({ "name": "Bob", "info": { "bio": "Just simple first user" } }));
  
  insert_data(&db, "Project", json!({ "name": "Project A" }));
  insert_data(&db, "Project", json!({ "name": "Project B", "users": [{ "user": user_a, "role": "creator" }] }));

  insert_data(&db, "Post", json!({ "title": "First Alice post", "author": user_a }));
  insert_data(&db, "Post", json!({ "title": "Second Alice post", "author": user_a }));
  insert_data(&db, "Post", json!({ "title": "Unnamed post" }));

  assert_eq!(db.count(db.get_model("User").unwrap()), 2);
  assert_eq!(db.count(db.get_model("Project").unwrap()), 2);
  assert_eq!(db.count(db.get_model("Project.users").unwrap()), 1);
  assert_eq!(db.count(db.get_model("Post").unwrap()), 3);

  {
    let resp = get_data(&db, "User", json!({
      "name": true,
      "info": { "bio": true }
    }));
    assert_eq!(resp, json!([
      { "name": "Alice", "info": null }, 
      { "name": "Bob", "info": { "bio": "Just simple first user" } }
    ]));
  }
  
  {
    let resp = get_data(&db, "Project", json!({
      "name": true,
      "users": { "role": true }
    }));
  
    assert_eq!(resp, json!([
      { "name": "Project A", "users": [] },
      { "name": "Project B", "users": [{ "role": "creator" }] }
    ]));
  }

  {
    let resp = get_data(&db, "Project", json!({
      "name": true,
      "users": { "role": true, "user": { "id": true, "name": true } }
    }));

    assert_eq!(resp, json!([
      { "name": "Project A", "users": [] },
      { "name": "Project B", "users": [{ "user": { "id": user_a.get("id").unwrap(), "name": "Alice" }, "role": "creator" }] }
    ]));
  }

  {
    let resp = get_data(&db, "Post", json!({
      "title": true,
      "author": { "id": true, "name": true }
    }));
    assert_eq!(resp, json!([
      { "title": "First Alice post", "author": { "id": user_a.get("id").unwrap(), "name": "Alice" } },
      { "title": "Second Alice post", "author": { "id": user_a.get("id").unwrap(), "name": "Alice" } },
      { "title": "Unnamed post", "author": null }
    ]));
  }

  {
    let resp = get_data(&db, "User", json!({
      "name": true,
      "posts": { "title": true }
    }));
    assert_eq!(resp, json!([
      { "name": "Alice", "posts": [{ "title": "First Alice post" }, { "title": "Second Alice post" }] }, 
      { "name": "Bob", "posts": [] }
    ]));
  }
}
