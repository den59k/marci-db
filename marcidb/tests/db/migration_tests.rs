use marcidb::{MarciDB, MigrationApplyError};
use serde_json::json;
use tempfile::tempdir;

use crate::db::{get_data, get_data_one, insert_data};

/// add field на существующих данных: старые строки читаются (поле отсутствует),
/// новые строки пишутся с полем — без переписывания старых строк (формат v2)
#[test]
fn migrate_add_field() {
  let dir = tempdir().unwrap();
  let mut db = MarciDB::new("model User {\n  name String\n}", dir.path().to_str().unwrap());

  insert_data(&db, "User", json!({ "name": "Alice" }));
  insert_data(&db, "User", json!({ "name": "Bob" }));

  db.migrate_to("model User {\n  name String\n  age  UInt\n}").unwrap();

  assert_eq!(get_data(&db, "User", json!({ "name": true })), json!([{ "name": "Alice" }, { "name": "Bob" }]));

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
  let dir = tempdir().unwrap();
  let mut db = MarciDB::new("model User {\n  name  String\n  email String\n}", dir.path().to_str().unwrap());

  insert_data(&db, "User", json!({ "name": "Alice", "email": "a@x.com" }));
  insert_data(&db, "User", json!({ "name": "Bob", "email": "b@x.com" }));

  db.migrate_to("model User {\n  name  String\n  email String @index\n}").unwrap();

  assert_eq!(
    get_data_one(&db, "User", json!({ "name": true, "$where": { "email": "a@x.com" } })),
    json!({ "name": "Alice" })
  );
}

/// Первый push в пустую БД создаёт модель (CreateModel) — БД появляется «из ничего»
#[test]
fn migrate_create_model_on_empty_db() {
  let dir = tempdir().unwrap();
  let mut db = MarciDB::open(dir.path().to_str().unwrap());

  // Пустая БД — моделей ещё нет
  assert!(db.get_model("User").is_none());

  db.migrate_to("model User {\n  name  String\n  email String @index\n}").unwrap();

  insert_data(&db, "User", json!({ "name": "Alice", "email": "a@x.com" }));
  assert_eq!(get_data(&db, "User", json!({ "name": true })), json!([{ "name": "Alice" }]));
  // Индекс работает
  assert_eq!(
    get_data_one(&db, "User", json!({ "name": true, "$where": { "email": "a@x.com" } })),
    json!({ "name": "Alice" })
  );
}

/// Состояние после миграции переживает рестарт: open() реконструирует схему из __marci_meta__
#[test]
fn migrate_persists_across_reopen() {
  let dir = tempdir().unwrap();
  let path = dir.path().to_str().unwrap().to_string();

  {
    let mut db = MarciDB::new("model User {\n  name String\n}", &path);
    insert_data(&db, "User", json!({ "name": "Alice" }));
    db.migrate_to("model User {\n  name String\n  age  UInt\n}").unwrap();
    insert_data(&db, "User", json!({ "name": "Bob", "age": 5 }));
  } // БД закрывается

  // Переоткрытие: схема (с age) реконструирована из БД, без передачи schema.marci
  let db = MarciDB::open(&path);
  assert!(db.get_model("User").is_some());
  assert_eq!(
    get_data_one(&db, "User", json!({ "name": true, "age": true, "$where": { "name": "Bob" } })),
    json!({ "name": "Bob", "age": 5 })
  );
}

/// Вставка поля в СЕРЕДИНУ модели сдвинула бы слоты существующих полей — отклоняется (не молча)
#[test]
fn migrate_insert_field_in_middle_rejected() {
  let dir = tempdir().unwrap();
  let mut db = MarciDB::new("model M {\n  a String\n  b String\n}", dir.path().to_str().unwrap());
  insert_data(&db, "M", json!({ "a": "a1", "b": "b1" }));

  let result = db.migrate_to("model M {\n  a String\n  c String\n  b String\n}");
  assert!(matches!(result, Err(MigrationApplyError::Unsupported(_))));
}

/// drop field пока не поддержан apply (нужен tombstone слота) — явная ошибка, не молчаливая порча
#[test]
fn migrate_drop_field_unsupported() {
  let dir = tempdir().unwrap();
  let mut db = MarciDB::new("model User {\n  name String\n  age  UInt\n}", dir.path().to_str().unwrap());

  let result = db.migrate_to("model User {\n  name String\n}");
  assert!(matches!(result, Err(MigrationApplyError::Unsupported(_))));
}

/// $init (reinit): полный сброс — старые данные стираются, применяется ЛЮБАЯ новая схема,
/// даже несовместимая с прежней (смена типа поля + новая модель — migrate_to бы их отклонил)
#[test]
fn init_resets_data_and_schema() {
  let dir = tempdir().unwrap();
  let mut db = MarciDB::new("model User {\n  name String\n  age  UInt\n}", dir.path().to_str().unwrap());

  insert_data(&db, "User", json!({ "name": "Alice", "age": 30 }));
  insert_data(&db, "User", json!({ "name": "Bob", "age": 40 }));

  // age сменил тип UInt→String, добавлена модель Post — migrate_to отклонил бы смену типа
  db.reinit("model User {\n  name String\n  age  String\n}\nmodel Post {\n  title String\n}").unwrap();

  // Старые данные стёрты
  assert_eq!(get_data(&db, "User", json!({ "name": true })), json!([]));

  // Новая схема работает: age теперь String, появилась модель Post
  insert_data(&db, "User", json!({ "name": "Carol", "age": "old" }));
  assert_eq!(
    get_data_one(&db, "User", json!({ "name": true, "age": true })),
    json!({ "name": "Carol", "age": "old" })
  );
  insert_data(&db, "Post", json!({ "title": "Hi" }));
  assert_eq!(get_data(&db, "Post", json!({ "title": true })), json!([{ "title": "Hi" }]));
}

/// Сброс через reinit переживает рестарт: open() реконструирует НОВую схему из __marci_meta__,
/// а деревья старой модели физически снесены (модель не воскресает)
#[test]
fn init_persists_across_reopen() {
  let dir = tempdir().unwrap();
  let path = dir.path().to_str().unwrap().to_string();

  {
    let mut db = MarciDB::new("model User {\n  name String\n}", &path);
    insert_data(&db, "User", json!({ "name": "Alice" }));
    db.reinit("model Account {\n  email String\n}").unwrap();
    insert_data(&db, "Account", json!({ "email": "a@x.com" }));
  }

  let db = MarciDB::open(&path);
  assert!(db.get_model("User").is_none());     // старая модель ушла
  assert!(db.get_model("Account").is_some());  // новая на месте
  assert_eq!(get_data(&db, "Account", json!({ "email": true })), json!([{ "email": "a@x.com" }]));
}
