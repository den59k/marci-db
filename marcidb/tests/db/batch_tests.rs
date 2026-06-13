use marcidb::{BatchErrorKind, InsertError, MarciDB, execute_batch};
use serde_json::json;
use tempfile::tempdir;

const SCHEMA: &str = "
  model User {
      name    String
      email   String  @unique
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

fn user_count(db: &MarciDB) -> u64 {
  db.count(db.get_model("User").unwrap()).unwrap()
}

#[test]
fn batch_commit_returns_results_in_order() {
  let (_dir, db) = make_db();

  let results = execute_batch(&db, &[
    json!({ "model": "User", "action": "insert", "data": { "name": "Alice", "email": "a@x.com" } }),
    json!({ "model": "User", "action": "insert", "data": { "name": "Bob",   "email": "b@x.com" } }),
  ]).unwrap();

  // insert возвращает объект id, по одному результату на операцию, в порядке операций
  assert_eq!(results, vec![json!({ "id": 1 }), json!({ "id": 2 })]);
  assert_eq!(user_count(&db), 2);
}

#[test]
fn batch_ref_links_generated_id() {
  let (_dir, db) = make_db();

  // Post в операции #1 ссылается на id пользователя, созданного в операции #0
  let results = execute_batch(&db, &[
    json!({ "model": "User", "action": "insert", "data": { "name": "Alice", "email": "a@x.com" } }),
    json!({ "model": "Post", "action": "insert", "data": { "title": "Hi", "author": { "id": { "$ref": "0.id" } } } }),
    json!({ "model": "User", "action": "findFirst", "query": { "name": true, "posts": { "title": true } } }),
  ]).unwrap();

  // findFirst видит и пользователя, и связанный с ним пост (read-your-writes внутри транзакции)
  assert_eq!(results[2], json!({ "name": "Alice", "posts": [{ "title": "Hi" }] }));
}

#[test]
fn batch_rolls_back_all_on_failing_op() {
  let (_dir, db) = make_db();

  // Операция #0 проходит, #1 нарушает unique email → откатывается ВСЯ транзакция
  let err = execute_batch(&db, &[
    json!({ "model": "User", "action": "insert", "data": { "name": "Alice", "email": "dup@x.com" } }),
    json!({ "model": "User", "action": "insert", "data": { "name": "Bob",   "email": "dup@x.com" } }),
  ]).unwrap_err();

  assert_eq!(err.index, 1);
  assert!(matches!(err.kind, BatchErrorKind::Insert(InsertError::UniqueViolation(..))));
  // Ничего не сохранилось — вставка Alice из операции #0 тоже откачена
  assert_eq!(user_count(&db), 0);
}

#[test]
fn batch_read_your_writes() {
  let (_dir, db) = make_db();

  let results = execute_batch(&db, &[
    json!({ "model": "User", "action": "insert",   "data":  { "name": "Alice", "email": "a@x.com" } }),
    json!({ "model": "User", "action": "findMany", "query": { "name": true } }),
    json!({ "model": "User", "action": "count",    "query": {} }),
  ]).unwrap();

  assert_eq!(results[1], json!([{ "name": "Alice" }]));
  assert_eq!(results[2], json!(1));
}

#[test]
fn batch_delete_returns_bool() {
  let (_dir, db) = make_db();

  execute_batch(&db, &[
    json!({ "model": "User", "action": "insert", "data": { "name": "Alice", "email": "a@x.com" } }),
    json!({ "model": "User", "action": "insert", "data": { "name": "Bob",   "email": "b@x.com" } }),
  ]).unwrap();

  let results = execute_batch(&db, &[
    json!({ "model": "User", "action": "delete", "id": { "id": 1 } }),
    json!({ "model": "User", "action": "delete", "id": { "id": 999 } }),
    json!({ "model": "User", "action": "count",  "query": {} }),
  ]).unwrap();

  assert_eq!(results[0], json!(true));   // существовал — удалён
  assert_eq!(results[1], json!(false));  // не существовал
  assert_eq!(results[2], json!(1));
  assert_eq!(user_count(&db), 1);
}

#[test]
fn batch_unknown_model_reports_index() {
  let (_dir, db) = make_db();

  let err = execute_batch(&db, &[
    json!({ "model": "User",        "action": "insert", "data": { "name": "Alice", "email": "a@x.com" } }),
    json!({ "model": "Nonexistent", "action": "insert", "data": {} }),
  ]).unwrap_err();

  assert_eq!(err.index, 1);
  assert!(matches!(err.kind, BatchErrorKind::UnknownModel(_)));
  assert_eq!(user_count(&db), 0); // операция #0 откачена
}
