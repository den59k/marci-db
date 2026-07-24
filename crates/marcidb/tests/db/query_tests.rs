use marcidb::{MarciDB};
use serde_json::json;
use tempfile::tempdir;

use crate::db::{get_data, insert_data};

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
        posts       Post[]        @bind(Post.author)
    }
    
    struct UserInfo {
        bio         String
        age         Int
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
  insert_data(&db, "User", json!({ "name": "Bob", "info": { "bio": "Just simple first user", "age": 23 } }));
  
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

  {
    let resp = get_data(&db, "User", json!({
      "name": true,
      "$where": { "info": { "$not": null } }
    }));
    assert_eq!(resp, json!([
      { "name": "Bob" }
    ]))
  }

  {
    let resp = get_data(&db, "User", json!({
      "name": true,
      "$where": { "info": { "age": { "$gt": 24 } } }
    }));
    assert_eq!(resp, json!([]))
  }

  // {
  //   let resp = get_data(&db, "User", json!({
  //     "name": true,
  //     "$where": { "info": { "age": { "$lt": 24 } } }
  //   }));
  //   assert_eq!(resp, json!([
  //     { "name": "Bob" }
  //   ]))
  // }
}



#[test]
fn many_to_many_query_test() {

  let schema_str = "
    model User {
      name        String
      chats       Chat[]
    }

    model Chat {
      name        String
      users       User[]    @bind(User.chats)
    }
  ";

  let dir = tempdir().unwrap();
  let db: MarciDB = MarciDB::new(schema_str, dir.path().to_str().unwrap());  

  let user_a = insert_data(&db, "User", json!({ "name": "Alice" }));
  let user_b = insert_data(&db, "User", json!({ "name": "Bob" }));
  let user_c = insert_data(&db, "User", json!({ "name": "Charlie" }));
  
  insert_data(&db, "Chat", json!({ "name": "Empty chat", "users": [] }));
  insert_data(&db, "Chat", json!({ "name": "Second chat", "users": [ user_a, user_b ] }));
  insert_data(&db, "Chat", json!({ "name": "All chat", "users": [ user_a, user_b, user_c ] }));

  {
    let resp = get_data(&db, "Chat", json!({
      "name": true,
      "users": { "name": true }
    }));
    assert_eq!(resp, json!([
      { "name": "Empty chat", "users": [] },
      { "name": "Second chat", "users": [{ "name": "Alice" }, { "name": "Bob" }] },
      { "name": "All chat", "users": [{ "name": "Alice" }, { "name": "Bob" }, { "name": "Charlie" }] },
    ]))
  }

  {
    let resp = get_data(&db, "User", json!({
      "name": true,
      "chats": { "name": true },
      "$where": { "name": "Alice" }
    }));
    assert_eq!(resp, json!([
      { "name": "Alice", "chats": [
        { "name": "Second chat" },
        { "name": "All chat" },
      ] },
    ]))
  }

}
#[test]
fn include_cache_test() {
  let schema_str = "
    model User {
      name        String
      posts       Post[]      @bind(Post.author)
    }

    model Post {
      title       String
      author      User?
    }
  ";

  let dir = tempdir().unwrap();
  let db: MarciDB = MarciDB::new(schema_str, dir.path().to_str().unwrap());

  let alice = insert_data(&db, "User", json!({ "name": "Alice" }));
  let bob = insert_data(&db, "User", json!({ "name": "Bob" }));

  insert_data(&db, "Post", json!({ "title": "First", "author": { "id": alice["id"] } }));
  insert_data(&db, "Post", json!({ "title": "Second", "author": { "id": alice["id"] } }));
  insert_data(&db, "Post", json!({ "title": "Third", "author": { "id": bob["id"] } }));
  insert_data(&db, "Post", json!({ "title": "Orphan" }));

  // A shared author across several posts is decoded via the cache — the result must not differ
  {
    let resp = get_data(&db, "Post", json!({ "title": true, "author": { "name": true } }));
    assert_eq!(resp, json!([
      { "title": "First", "author": { "name": "Alice" } },
      { "title": "Second", "author": { "name": "Alice" } },
      { "title": "Third", "author": { "name": "Bob" } },
      { "title": "Orphan", "author": null }
    ]));
  }

  // Two different selects of the same entity in one query do not share a cache with each other
  {
    let schema_str2 = "
      model Item {
        title       String
        owner       User?
        editor      User?
      }

      model User {
        name        String
        rank        UInt        @default(1)
      }
    ";
    let dir2 = tempdir().unwrap();
    let db2: MarciDB = MarciDB::new(schema_str2, dir2.path().to_str().unwrap());

    let user = insert_data(&db2, "User", json!({ "name": "Dana", "rank": 7 }));
    insert_data(&db2, "Item", json!({ "title": "A", "owner": { "id": user["id"] }, "editor": { "id": user["id"] } }));
    insert_data(&db2, "Item", json!({ "title": "B", "owner": { "id": user["id"] }, "editor": { "id": user["id"] } }));

    let resp = get_data(&db2, "Item", json!({
      "title": true,
      "owner": { "name": true },
      "editor": { "rank": true }
    }));
    assert_eq!(resp, json!([
      { "title": "A", "owner": { "name": "Dana" }, "editor": { "rank": 7 } },
      { "title": "B", "owner": { "name": "Dana" }, "editor": { "rank": 7 } }
    ]));
  }
}
