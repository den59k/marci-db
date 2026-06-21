use marcidb::{MarciDB};
use serde_json::json;
use tempfile::tempdir;

use crate::db::{get_data, insert_data};

#[test]
fn base_insert_test() {
  let schema_str = "
    model User {
        name        String
        info        UserInfo?
        posts       Post[]  @bind(Post.author)
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

    enum Role {
      creator
      admin {
        sign      String
      }
    }

    struct UserRole {
        user        User          @id
        role        Role
    }
  ";

  let dir = tempdir().unwrap();
  let db: MarciDB = MarciDB::new(schema_str, dir.path().to_str().unwrap());  

  let user_a =  insert_data(&db, "User", json!({ "name": "Alice" }));
  let _user_b = insert_data(&db, "User", json!({ "name": "Bob", "info": { "bio": "Just simple first user" } }));
  
  insert_data(&db, "Project", json!({ "name": "Project A" }));
  insert_data(&db, "Project", json!({ "name": "Project B", "users": [{ "user": user_a, "role": "creator" }] }));
  insert_data(&db, "Project", json!({ "name": "Alice Project", "users": [{ "user": user_a, "role": "admin", "sign": "AliceSign" }] }));

  insert_data(&db, "Post", json!({ "title": "First Alice post", "author": user_a }));
  insert_data(&db, "Post", json!({ "title": "Second Alice post", "author": user_a }));
  insert_data(&db, "Post", json!({ "title": "Unnamed post" }));

  assert_eq!(db.count(db.get_model("User").unwrap()), 2);
  assert_eq!(db.count(db.get_model("Project").unwrap()), 3);
  assert_eq!(db.count(db.get_model("Project.users").unwrap()), 2);
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
      { "name": "Project B", "users": [{ "role": "creator" }] },
      { "name": "Alice Project", "users": [{ "role": "admin" }] },
    ]));
  }

  {
    let resp = get_data(&db, "Project", json!({
      "name": true,
      "users": { "role": true, "sign": true, "user": { "id": true, "name": true } }
    }));

    assert_eq!(resp, json!([
      { "name": "Project A", "users": [] },
      { "name": "Project B", "users": [{ "user": { "id": user_a.get("id").unwrap(), "name": "Alice" }, "role": "creator" }] },
      { "name": "Alice Project", "users": [{ "user": { "id": user_a.get("id").unwrap(), "name": "Alice" }, "role": "admin", "sign": "AliceSign" }] }
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

#[test]
fn insert_null_not_allowed_test() {
  use marcidb::{parse_insert, EncodeError};

  let schema_str = "
        model User {
            name    String
            age     Int
        }
    ";
  let dir = tempdir().unwrap();
  let db = MarciDB::new(schema_str, dir.path().to_str().unwrap());
  let entity = db.get_model("User").unwrap();

  // 1. Попытка вставить с null в поле name (обязательное)
  let res = parse_insert(&db.schema, entity, &json!({ "name": null, "age": 30 }));
  assert!(
    matches!(res, Err(EncodeError::NullNotAllowed(ref s)) if s.ends_with("name")),
    "Ожидалась NullNotAllowed для поля name, получено: {:?}", res
  );

  // 2. Попытка вставить с null в поле age (обязательное)
  let res = parse_insert(&db.schema, entity, &json!({ "name": "Alice", "age": null }));
  assert!(
    matches!(res, Err(EncodeError::NullNotAllowed(ref s)) if s.ends_with("age")),
    "Ожидалась NullNotAllowed для поля age, получено: {:?}", res
  );

  // 3. Успешная вставка с nullable полем = null (email?)
  let schema_str2 = "
        model User {
            name   String
            email  String?
        }
    ";
  let dir2 = tempdir().unwrap(); // отдельная директория для второй БД
  let db2 = MarciDB::new(schema_str2, dir2.path().to_str().unwrap());
  let entity2 = db2.get_model("User").unwrap();
  let res = parse_insert(&db2.schema, entity2, &json!({ "name": "Bob", "email": null }));
  assert!(res.is_ok(), "Ошибка при вставке nullable поля: {:?}", res);
  let res = parse_insert(&db2.schema, entity2, &json!({ "name": "Bob" }));
  assert!(res.is_ok(), "Ошибка при отсутствии nullable поля: {:?}", res);
}

#[test]
fn insert_null_key_field_test() {
  use marcidb::{parse_insert, EncodeError};

  let schema_str = "
        model Passport {
            id      String  @id
            user    User
        }
        model User {
            name    String
        }
    ";
  let dir = tempdir().unwrap();
  let db = MarciDB::new(schema_str, dir.path().to_str().unwrap());
  let entity = db.get_model("Passport").unwrap();

  let res = parse_insert(&db.schema, entity, &json!({ "id": null, "user": { "name": "Alice" } }));
  assert!(
    matches!(res, Err(EncodeError::NullNotAllowed(ref s)) if s.contains("id")),
    "Ожидалась NullNotAllowed для ключевого поля, получено: {:?}", res
  );
}