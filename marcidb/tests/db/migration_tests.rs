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

// ─────────────────────────────── императивные миграции (ledger + replay) ───────────────────────────────

/// Императивный реплей: применяет .mig по ledger'у. Повторный пуш того же списка — no-op
#[test]
fn migrations_replay_and_idempotent() {
  let dir = tempdir().unwrap();
  let mut db = MarciDB::open(dir.path().to_str().unwrap());

  let m0 = ("0000_init".to_string(), "create model User {\n  name String\n}".to_string());
  let m1 = ("0001_age".to_string(), "add field User.age UInt".to_string());

  let applied = db.apply_migrations(&[m0.clone(), m1.clone()]).unwrap();
  assert_eq!(applied, vec!["0000_init", "0001_age"]);

  insert_data(&db, "User", json!({ "name": "Alice", "age": 30 }));
  assert_eq!(get_data_one(&db, "User", json!({ "name": true, "age": true })), json!({ "name": "Alice", "age": 30 }));

  // Повторный пуш — ничего не применяется, данные целы
  let applied2 = db.apply_migrations(&[m0.clone(), m1.clone()]).unwrap();
  assert!(applied2.is_empty());
  assert_eq!(get_data_one(&db, "User", json!({ "name": true, "age": true })), json!({ "name": "Alice", "age": 30 }));
}

/// Инкрементальный пуш: применяется только новая миграция, старые данные сохраняются
#[test]
fn migrations_incremental_push() {
  let dir = tempdir().unwrap();
  let mut db = MarciDB::open(dir.path().to_str().unwrap());

  let m0 = ("0000_init".to_string(), "create model User {\n  name String\n}".to_string());
  db.apply_migrations(&[m0.clone()]).unwrap();
  insert_data(&db, "User", json!({ "name": "Alice" }));

  let m1 = ("0001_age".to_string(), "add field User.age UInt".to_string());
  let applied = db.apply_migrations(&[m0.clone(), m1.clone()]).unwrap();
  assert_eq!(applied, vec!["0001_age"]);

  // Старая строка читается (age отсутствует), новая пишется с age
  assert_eq!(get_data(&db, "User", json!({ "name": true })), json!([{ "name": "Alice" }]));
  insert_data(&db, "User", json!({ "name": "Bob", "age": 5 }));
  assert_eq!(
    get_data_one(&db, "User", json!({ "name": true, "age": true, "$where": { "name": "Bob" } })),
    json!({ "name": "Bob", "age": 5 })
  );
}

/// Ledger переживает рестарт: повторное открытие видит схему, повторный пуш всё ещё no-op
#[test]
fn migrations_ledger_persists_across_reopen() {
  let dir = tempdir().unwrap();
  let path = dir.path().to_str().unwrap().to_string();
  let m0 = ("0000_init".to_string(), "create model User {\n  name String\n}".to_string());

  {
    let mut db = MarciDB::open(&path);
    db.apply_migrations(&[m0.clone()]).unwrap();
    insert_data(&db, "User", json!({ "name": "Alice" }));
  }

  let mut db = MarciDB::open(&path);
  assert!(db.get_model("User").is_some());
  let applied = db.apply_migrations(&[m0.clone()]).unwrap();
  assert!(applied.is_empty());
  assert_eq!(get_data(&db, "User", json!({ "name": true })), json!([{ "name": "Alice" }]));
}

/// Разошедшаяся история (другой id на уже применённой позиции) отклоняется
#[test]
fn migrations_history_diverged_rejected() {
  let dir = tempdir().unwrap();
  let mut db = MarciDB::open(dir.path().to_str().unwrap());

  db.apply_migrations(&[("0000_init".to_string(), "create model User {\n  name String\n}".to_string())]).unwrap();

  let result = db.apply_migrations(&[("0000_other".to_string(), "create model User {\n  name String\n}".to_string())]);
  assert!(matches!(result, Err(MigrationApplyError::HistoryDiverged { .. })));
}

