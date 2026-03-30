use marcidb::MarciDB;
use serde_json::json;
use tempfile::tempdir;

use crate::db::{get_data, insert_data};

#[test]
fn query_indexes_test() {

  let schema_str = "
    model User {
        email       String  @unique
        name        String
        posts       Post[]  @bind(Post.author)
    }
    
    model Post {
        title       String
        createdAt   DateTime  @index
        author      User?
    }
  ";
  
  let dir = tempdir().unwrap();
  let db: MarciDB = MarciDB::new(schema_str, dir.path().to_str().unwrap());  

  let user_a =  insert_data(&db, "User", json!({ "name": "Alice", "email": "alice@test.test" }));
  insert_data(&db, "User", json!({ "name": "Bob", "email": "bob@test.test" }));

  insert_data(&db, "Post", json!({ "title": "Last Alice post", "createdAt": "2026-03-21T14:16:27.665Z", "author": user_a }));
  insert_data(&db, "Post", json!({ "title": "Second Alice post", "createdAt": "2026-03-20T14:16:27.665Z", "author": user_a }));
  insert_data(&db, "Post", json!({ "title": "First Alice post", "createdAt": "2026-02-20T14:16:27.665Z", "author": user_a }));
  insert_data(&db, "Post", json!({ "title": "Unnamed post", "createdAt": "2026-01-20T14:16:27.665Z" }));

  {
    let data = get_data(&db, "User", json!({
      "name": true,
      "$where": { "email": "alice@test.test" }
    }));
    assert_eq!(data, json!([
      { "name": "Alice" }
    ]))
  }

  {
    let data = get_data(&db, "Post", json!({
      "title": true,
      "$where": { "createdAt": { "$gt": "2026-03-01T00:00:00.0Z" } }
    }));
    assert_eq!(data, json!([
      { "title": "Second Alice post" },
      { "title": "Last Alice post" },
    ]))
  }

}