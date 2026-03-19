
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

#[test]
fn nested_query_test() {

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
  insert_data(&db, "User", json!({ "name": "Bob", "info": { "bio": "Just simple first user" } }));
  
  insert_data(&db, "Project", json!({ "name": "Project A" }));
  insert_data(&db, "Project", json!({ "name": "Project B", "users": [{ "user": user_a, "role": "creator" }] }));
  insert_data(&db, "Project", json!({ "name": "Alice Project", "users": [{ "user": user_a, "role": "admin", "sign": "AliceSign" }] }));

  insert_data(&db, "Post", json!({ "title": "First Alice post", "author": user_a }));
  insert_data(&db, "Post", json!({ "title": "Second Alice post", "author": user_a }));
  insert_data(&db, "Post", json!({ "title": "Unnamed post" }));

  {
    let resp = get_data(&db, "User", json!({
      "name": true,
      "$where": { "posts": { "$some": {} } }
    }));
    assert_eq!(resp, json!([
      { "name": "Alice" }
    ]))
  }

  {
    let resp = get_data(&db, "User", json!({
      "name": true,
      "$where": { "posts": { "$none": {} } }
    }));
    assert_eq!(resp, json!([
      { "name": "Bob" }
    ]))
  }
}