use marcidb::{MarciDB, MigrationApplyError, diff};
use serde_json::json;
use tempfile::tempdir;

use crate::db::{get_data, get_data_one, insert_data};

/// add field на существующих данных: старые строки читаются (поле отсутствует),
/// новые строки пишутся с полем — без переписывания старых строк (формат v2)
#[test]
fn migrate_add_field() {
  let old = "model User {\n  name String\n}";
  let new = "model User {\n  name String\n  age  UInt\n}";

  let dir = tempdir().unwrap();
  let mut db = MarciDB::new(old, dir.path().to_str().unwrap());

  insert_data(&db, "User", json!({ "name": "Alice" }));
  insert_data(&db, "User", json!({ "name": "Bob" }));

  db.apply_migration(&diff(old, new).unwrap(), new).unwrap();

  // Старые строки читаются, age отсутствует
  assert_eq!(get_data(&db, "User", json!({ "name": true })), json!([{ "name": "Alice" }, { "name": "Bob" }]));

  // Новая строка с age под новой схемой
  insert_data(&db, "User", json!({ "name": "Carol", "age": 30 }));
  assert_eq!(
    get_data_one(&db, "User", json!({ "name": true, "age": true, "$where": { "name": "Carol" } })),
    json!({ "name": "Carol", "age": 30 })
  );
}

/// add index строит индекс из существующих строк (бэкфилл) — запрос по индексу
/// находит записи, вставленные ДО миграции
#[test]
fn migrate_add_index_backfills_existing_rows() {
  let old = "model User {\n  name  String\n  email String\n}";
  let new = "model User {\n  name  String\n  email String @index\n}";

  let dir = tempdir().unwrap();
  let mut db = MarciDB::new(old, dir.path().to_str().unwrap());

  insert_data(&db, "User", json!({ "name": "Alice", "email": "a@x.com" }));
  insert_data(&db, "User", json!({ "name": "Bob", "email": "b@x.com" }));

  let ops = diff(old, new).unwrap();
  assert_eq!(marcidb::serialize_migration(&ops), "add index User.email");
  db.apply_migration(&ops, new).unwrap();

  // Индекс построен из существующих строк → запрос по email находит ранее вставленного Alice
  assert_eq!(
    get_data_one(&db, "User", json!({ "name": true, "$where": { "email": "a@x.com" } })),
    json!({ "name": "Alice" })
  );
}

/// Вставка поля в СЕРЕДИНУ модели сдвинула бы слоты существующих полей и сломала старые строки —
/// в v1 это отклоняется (гард `check_layout_stable`), а не применяется молча. Добавлять — в конец.
#[test]
fn migrate_insert_field_in_middle_rejected() {
  let old = "model M {\n  a String\n  b String\n}";
  let new = "model M {\n  a String\n  c String\n  b String\n}"; // c вставлено ПЕРЕД b → слот b сдвинулся бы

  let dir = tempdir().unwrap();
  let mut db = MarciDB::new(old, dir.path().to_str().unwrap());
  insert_data(&db, "M", json!({ "a": "a1", "b": "b1" }));

  let result = db.apply_migration(&diff(old, new).unwrap(), new);
  assert!(matches!(result, Err(MigrationApplyError::Unsupported(_))));
}

/// drop field пока не поддержан apply (нужен tombstone слота) — явная ошибка, не молчаливая порча
#[test]
fn migrate_drop_field_unsupported() {
  let old = "model User {\n  name String\n  age  UInt\n}";
  let new = "model User {\n  name String\n}";

  let dir = tempdir().unwrap();
  let mut db = MarciDB::new(old, dir.path().to_str().unwrap());

  let result = db.apply_migration(&diff(old, new).unwrap(), new);
  assert!(matches!(result, Err(MigrationApplyError::Unsupported(_))));
}
