use marcidb::{BatchErrorKind, DeleteError, MarciDB, execute_batch, execute_op, parse_query};
use serde_json::json;
use tempfile::tempdir;

use crate::db::{get_data, insert_data};

/// `Post.author` is required → the default `Restrict`; `Comment.post` cascades.
const SCHEMA: &str = "
  model User {
    name    String
    score   UInt    @index
    posts   Post[]  @bind(Post.author)
  }
  model Post {
    title   String
    author  User
    comments Comment[] @bind(Comment.post)
  }
  model Comment {
    text  String
    post  Post @onDelete(Cascade)
  }
";

fn make_db(dir: &tempfile::TempDir) -> MarciDB {
  let db = MarciDB::new(SCHEMA, dir.path().to_str().unwrap());
  for (name, score) in [("a", 10u64), ("b", 20), ("c", 30), ("d", 40)] {
    insert_data(&db, "User", json!({ "name": name, "score": score }));
  }
  db
}

fn names(db: &MarciDB) -> serde_json::Value {
  get_data(db, "User", json!({ "name": true }))
}

fn delete_many(db: &MarciDB, model: &str, query: serde_json::Value) -> Result<u64, DeleteError> {
  let entity = db.get_model(model).unwrap();
  let query_op = parse_query(&db.schema, entity, &query).unwrap();
  db.delete_many(entity, &query_op)
}

#[test]
fn delete_many_by_filter_returns_deleted_count() {
  let dir = tempdir().unwrap();
  let db = make_db(&dir);

  // Index scan on `score`: the scan walks the index the deletes rewrite — the context must drop first
  assert_eq!(delete_many(&db, "User", json!({ "$where": { "score": { "$gte": 30 } } })).unwrap(), 2);
  assert_eq!(names(&db), json!([{ "name": "a" }, { "name": "b" }]));

  // No match → 0, nothing changes; an empty filter deletes everything
  assert_eq!(delete_many(&db, "User", json!({ "$where": { "name": "zzz" } })).unwrap(), 0);
  assert_eq!(delete_many(&db, "User", json!({})).unwrap(), 2);
  assert_eq!(names(&db), json!([]));
}

#[test]
fn delete_many_rejects_bounded_and_search_queries() {
  let dir = tempdir().unwrap();
  let db = make_db(&dir);
  for q in [json!({ "$limit": 1 }), json!({ "$skip": 1 }), json!({ "$cursor": { "id": 1 } })] {
    match delete_many(&db, "User", q) {
      Err(DeleteError::Unsupported(_)) => {},
      other => panic!("expected Unsupported, got {:?}", other),
    }
  }
  assert_eq!(names(&db).as_array().unwrap().len(), 4);
}

/// Each row goes through the single-delete path: cascades apply, and a `Restrict` dependency fails the
/// whole operation — including rows already deleted before it — with nothing committed.
#[test]
fn delete_many_cascades_and_rolls_back_on_restrict() {
  let dir = tempdir().unwrap();
  let db = make_db(&dir);
  let p1 = insert_data(&db, "Post", json!({ "title": "p1", "author": { "id": 1 } }));
  insert_data(&db, "Comment", json!({ "text": "c1", "post": p1 }));
  insert_data(&db, "Comment", json!({ "text": "c2", "post": p1 }));
  insert_data(&db, "Post", json!({ "title": "p2", "author": { "id": 2 } }));

  // Deleting posts cascades into their comments
  assert_eq!(delete_many(&db, "Post", json!({ "$where": { "title": "p1" } })).unwrap(), 1);
  assert_eq!(get_data(&db, "Comment", json!({ "text": true })), json!([]));

  // User 2 still has a post (Restrict): user 1 matches first, then user 2 fails → user 1 must survive
  match delete_many(&db, "User", json!({ "$where": { "score": { "$lte": 20 } } })) {
    Err(DeleteError::RestrictConstraints(field, _)) => assert_eq!(field, "Post.author"),
    other => panic!("expected a Restrict rejection, got {:?}", other),
  }
  assert_eq!(names(&db).as_array().unwrap().len(), 4);
}

/// The batch and single-op dispatch (`$transaction` / embedded FFI) know the action, and only `$`-keys
/// of the query count.
#[test]
fn delete_many_via_batch_and_execute_op() {
  let dir = tempdir().unwrap();
  let db = make_db(&dir);

  let deleted = execute_op(&db, &json!({ "model": "User", "action": "deleteMany", "query": { "name": true, "$where": { "score": 10 } } })).unwrap();
  assert_eq!(deleted, json!(1));

  // The deleteMany succeeds, then a later op fails — the whole batch rolls back
  let err = execute_batch(&db, &[
    json!({ "model": "User", "action": "deleteMany", "query": { "$where": { "score": 20 } } }),
    json!({ "model": "User", "action": "deleteMany", "query": { "$limit": 1 } }),
  ]).unwrap_err();
  assert_eq!(err.index, 1);
  assert!(matches!(err.kind, BatchErrorKind::Delete(DeleteError::Unsupported(_))), "unexpected: {:?}", err.kind);
  assert_eq!(names(&db), json!([{ "name": "b" }, { "name": "c" }, { "name": "d" }]));

  let results = execute_batch(&db, &[
    json!({ "model": "User", "action": "deleteMany", "query": { "$where": { "score": { "$gt": 20 } } } }),
    json!({ "model": "User", "action": "findMany", "query": { "name": true } }),
  ]).unwrap();
  assert_eq!(results, vec![json!(2), json!([{ "name": "b" }])]);
}
