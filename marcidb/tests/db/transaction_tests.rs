use std::str::FromStr;

use marcidb::{InsertError, MarciDB, MarciTransaction, array_to_json, decode_document, decode_id, parse_insert, parse_query};
use serde_json::{Value, json};
use tempfile::tempdir;

const SCHEMA: &str = "
  model User {
      name    String
      posts   Post[]  @bind(Post.author)
  }

  model Post {
      title   String
      author  User?
  }
";

fn make_db() -> (tempfile::TempDir, MarciDB) {
  let dir = tempdir().unwrap();
  let db = MarciDB::new(SCHEMA, dir.path().to_str().unwrap());
  (dir, db)
}

/// Insert inside a transaction returning the decoded id (mirror of insert_data)
fn tx_insert(tx: &MarciTransaction, db: &MarciDB, model: &str, data: Value) -> Value {
  let entity = db.get_model(model).unwrap();
  let op = parse_insert(&db.schema, entity, &data).unwrap();
  let id = tx.insert_item(entity, &op).unwrap();
  Value::from_str(&decode_id(&id, entity, &db.schema)).unwrap()
}

/// Query inside a transaction (sees its uncommitted changes)
fn tx_query(tx: &MarciTransaction, db: &MarciDB, model: &str, q: Value) -> Value {
  let entity = db.get_model(model).unwrap();
  let query = parse_query(&db.schema, entity, &q).unwrap();
  let items = tx.find_many(&query, |ctx| decode_document(ctx).unwrap()).unwrap();
  Value::from_str(&array_to_json(&items)).unwrap()
}

#[test]
fn transaction_commit_test() {
  let (_dir, db) = make_db();
  let user = db.get_model("User").unwrap();
  let post = db.get_model("Post").unwrap();

  let tx = db.begin_write();
  let alice = tx_insert(&tx, &db, "User", json!({ "name": "Alice" }));
  tx_insert(&tx, &db, "Post", json!({ "title": "First", "author": alice }));
  tx_insert(&tx, &db, "Post", json!({ "title": "Second", "author": alice }));

  // From inside the transaction the changes are visible (read-your-writes)...
  assert_eq!(tx.count(user).unwrap(), 1);
  assert_eq!(tx.count(post).unwrap(), 2);
  assert_eq!(
    tx_query(&tx, &db, "User", json!({ "name": true, "posts": { "title": true } })),
    json!([{ "name": "Alice", "posts": [{ "title": "First" }, { "title": "Second" }] }])
  );

  // ...but not to other readers before commit (snapshot isolation)
  assert_eq!(db.count(user).unwrap(), 0);
  assert_eq!(db.count(post).unwrap(), 0);

  tx.commit().unwrap();

  // After commit everything is visible, the author→posts relation is in place too
  assert_eq!(db.count(user).unwrap(), 1);
  assert_eq!(db.count(post).unwrap(), 2);
}

#[test]
fn transaction_rollback_on_drop_test() {
  let (_dir, db) = make_db();
  let user = db.get_model("User").unwrap();

  {
    let tx = db.begin_write();
    tx_insert(&tx, &db, "User", json!({ "name": "Ghost" }));
    assert_eq!(tx.count(user).unwrap(), 1);
    // tx goes out of scope without commit → rollback
  }

  assert_eq!(db.count(user).unwrap(), 0);
}

#[test]
fn transaction_explicit_rollback_test() {
  let (_dir, db) = make_db();
  let user = db.get_model("User").unwrap();

  let tx = db.begin_write();
  tx_insert(&tx, &db, "User", json!({ "name": "Alice" }));
  tx_insert(&tx, &db, "User", json!({ "name": "Bob" }));
  tx.rollback().unwrap();

  assert_eq!(db.count(user).unwrap(), 0);
}

#[test]
fn transaction_closure_commits_on_ok_test() {
  let (_dir, db) = make_db();
  let user = db.get_model("User").unwrap();

  let id = db.transaction(|tx| {
    tx_insert(tx, &db, "User", json!({ "name": "Alice" }));
    tx.insert_item(user, &parse_insert(&db.schema, user, &json!({ "name": "Bob" })).unwrap())
  }).unwrap();

  assert!(!id.is_empty());
  assert_eq!(db.count(user).unwrap(), 2);
}

#[test]
fn transaction_closure_rolls_back_on_err_test() {
  let (_dir, db) = make_db();
  let user = db.get_model("User").unwrap();

  // Several successful inserts, then Err — the whole transaction rolls back atomically.
  // The closure's error type must implement From<StorageError> (to propagate the commit error) —
  // the DB's domain errors do
  let result: Result<(), InsertError> = db.transaction(|tx| {
    tx_insert(tx, &db, "User", json!({ "name": "Alice" }));
    tx_insert(tx, &db, "User", json!({ "name": "Bob" }));
    Err(InsertError::ItemNotFound)
  });

  assert_eq!(result, Err(InsertError::ItemNotFound));
  assert_eq!(db.count(user).unwrap(), 0);
}
