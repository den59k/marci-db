use marcidb::MarciDB;
use serde_json::json;
use tempfile::tempdir;

use crate::db::{get_data, get_data_one, insert_data};

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
    let data = get_data_one(&db, "User", json!({
      "name": true,
      "$where": user_a
    }));
    assert_eq!(data, json!({ "name": "Alice" }))
  }

  {
    let data = get_data_one(&db, "User", json!({
      "name": true,
      "$where": { "email": "alice@test.test" }
    }));
    assert_eq!(data, json!({ "name": "Alice" }))
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

  {
    let data = get_data_one(&db, "Post", json!({
      "title": true,
      "$where": { "createdAt": { "$gt": "2026-03-01T00:00:00.0Z" } }
    }));
    assert_eq!(data, json!({ "title": "Second Alice post" }))
  }

    {
      let data = get_data(&db, "Post", json!({
        "title": true,
        "$where": { "title": { "$includes": "Alice" } }
      }));
      assert_eq!(data, json!([
        { "title": "Last Alice post" }, { "title": "Second Alice post" }, { "title": "First Alice post" }
      ]))
    }

  {
    let data = get_data_one(&db, "User", json!({
      "name": true,
      "$where": { "email": { "$startsWith": "alice" } }
    }));
    assert_eq!(data, json!({ "name": "Alice" }))
  }

}
/// A value the row got from `@default` must be indexed like a written one: `{ views: 0 }` is an index
/// scan, and a defaulted row that skipped the index was invisible to it (a plain scan still found it).
/// The same holds for `@unique` — two defaulted inserts must collide.
#[test]
fn default_values_are_indexed_test() {
  let schema_str = "
    model Post {
        title       String
        views       Int       @index    @default(0)
        plain       Int                 @default(0)
        slot        String    @unique   @default(\"free\")
        createdAt   DateTime  @index    @default(now())
    }
  ";
  let dir = tempdir().unwrap();
  let db = MarciDB::new(schema_str, dir.path().to_str().unwrap());

  insert_data(&db, "Post", json!({ "title": "a" }));                          // everything defaulted
  insert_data(&db, "Post", json!({ "title": "b", "views": 0, "slot": "s2" })); // written explicitly
  insert_data(&db, "Post", json!({ "title": "c", "views": 3, "slot": "s3" }));

  // index scan on the defaulted field agrees with the plain scan on the unindexed one
  assert_eq!(get_data(&db, "Post", json!({ "title": true, "$where": { "views": 0 } })), json!([{ "title": "a" }, { "title": "b" }]));
  assert_eq!(get_data(&db, "Post", json!({ "title": true, "$where": { "views": { "$lt": 1 } } })), json!([{ "title": "a" }, { "title": "b" }]));
  assert_eq!(get_data(&db, "Post", json!({ "title": true, "$where": { "plain": 0 } })), json!([{ "title": "a" }, { "title": "b" }, { "title": "c" }]));
  assert_eq!(get_data(&db, "Post", json!({ "title": true, "$where": { "createdAt": { "$gt": 0 } } })).as_array().unwrap().len(), 3);

  // the defaulted unique value occupies its slot
  let dup = crate::db::try_insert(&db, "Post", json!({ "title": "d" }));
  assert!(matches!(dup, Err(marcidb::InsertError::UniqueViolation(ref f, _)) if f == "Post.slot"), "expected a unique violation, got {:?}", dup);
}
