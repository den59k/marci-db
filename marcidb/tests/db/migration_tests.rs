use marcidb::{MarciDB, MigrateApplyError, parse_schema, serialize_snapshot};
use serde_json::json;
use tempfile::tempdir;

use crate::db::{get_data, get_data_one, insert_data};

/// Контент императивной миграции = materialized-снапшот версии схемы
fn snap(schema_text: &str) -> String {
  serialize_snapshot(&parse_schema(schema_text))
}

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

/// Первый push в пустую БД создаёт сущность (CreateEntity) — БД появляется «из ничего»
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

/// Состояние после миграции переживает рестарт: open() реконструирует схему из снапшота в __marci_meta__
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

  // Переоткрытие: схема (с age) реконструирована из снапшота, без передачи schema.marci
  let db = MarciDB::open(&path);
  assert!(db.get_model("User").is_some());
  assert_eq!(
    get_data_one(&db, "User", json!({ "name": true, "age": true, "$where": { "name": "Bob" } })),
    json!({ "name": "Bob", "age": 5 })
  );
}

/// Вставка поля в СЕРЕДИНУ модели: reconcile_slots переносит слоты существующих полей,
/// новое поле получает следующий свободный → миграция проходит, старые данные целы (фикс layout-бага)
#[test]
fn migrate_insert_field_in_middle_carries_slots() {
  let dir = tempdir().unwrap();
  let mut db = MarciDB::new("model M {\n  a String\n  b String\n}", dir.path().to_str().unwrap());
  insert_data(&db, "M", json!({ "a": "a1", "b": "b1" }));

  db.migrate_to("model M {\n  a String\n  c String\n  b String\n}").unwrap();

  // Старая строка читается корректно (a/b на своих слотах, c отсутствует → null)
  assert_eq!(get_data(&db, "M", json!({ "a": true, "b": true, "c": true })), json!([{ "a": "a1", "b": "b1", "c": null }]));
  // Новая строка пишет c
  insert_data(&db, "M", json!({ "a": "a2", "b": "b2", "c": "c2" }));
  assert_eq!(
    get_data_one(&db, "M", json!({ "a": true, "b": true, "c": true, "$where": { "a": "a2" } })),
    json!({ "a": "a2", "b": "b2", "c": "c2" })
  );
}

/// drop field пока не поддержан apply (нужен tombstone слота) — явная ошибка, не молчаливая порча
#[test]
fn migrate_drop_field_unsupported() {
  let dir = tempdir().unwrap();
  let mut db = MarciDB::new("model User {\n  name String\n  age  UInt\n}", dir.path().to_str().unwrap());

  let result = db.migrate_to("model User {\n  name String\n}");
  assert!(matches!(result, Err(MigrateApplyError::Unsupported(_))));
}

/// Смена типа поля требует трансформации данных — отклоняется
#[test]
fn migrate_type_change_rejected() {
  let dir = tempdir().unwrap();
  let mut db = MarciDB::new("model User {\n  age UInt\n}", dir.path().to_str().unwrap());
  let result = db.migrate_to("model User {\n  age String\n}");
  assert!(matches!(result, Err(MigrateApplyError::Diff(_))));
}

// ─────────────────────────────── императивные миграции (ledger + replay снапшотов) ───────────────────────────────

/// Императивный реплей: применяет снапшоты по ledger'у. Повторный пуш того же списка — no-op
#[test]
fn migrations_replay_and_idempotent() {
  let dir = tempdir().unwrap();
  let mut db = MarciDB::open(dir.path().to_str().unwrap());

  let m0 = ("0000_init".to_string(), snap("model User {\n  name String\n}"));
  let m1 = ("0001_age".to_string(), snap("model User {\n  name String\n  age UInt\n}"));

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

  let m0 = ("0000_init".to_string(), snap("model User {\n  name String\n}"));
  db.apply_migrations(&[m0.clone()]).unwrap();
  insert_data(&db, "User", json!({ "name": "Alice" }));

  let m1 = ("0001_age".to_string(), snap("model User {\n  name String\n  age UInt\n}"));
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
  let m0 = ("0000_init".to_string(), snap("model User {\n  name String\n}"));

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

/// Невалидная схема через migrate_to ($sync) — ошибка, а не паника
#[test]
fn migrate_to_rejects_invalid_schema() {
  let dir = tempdir().unwrap();
  let mut db = MarciDB::open(dir.path().to_str().unwrap());
  assert!(db.migrate_to("model A {\n  x Undefined\n}").is_err());      // неизвестный тип
  assert!(db.migrate_to("model A {\n  x String @bogus\n}").is_err());  // плохой атрибут
}

/// Невалидный снапшот через apply_migrations ($migrate) — ошибка, а не паника
#[test]
fn apply_migrations_rejects_invalid_snapshot() {
  let dir = tempdir().unwrap();
  let mut db = MarciDB::open(dir.path().to_str().unwrap());
  // битый синтаксис снапшота
  assert!(db.apply_migrations(&[("0000_bad".to_string(), "totally bogus line".to_string())]).is_err());
  // синтаксис ok, но ссылка на неизвестную сущность — ловится при разводке имён
  assert!(db.apply_migrations(&[("0000_x".to_string(), "entity M {\n  ref Nope @slot(4)\n}".to_string())]).is_err());
}

/// Enum end-to-end через императивный путь ($migrate): снапшот со впечатанным enum → apply_migrations
#[test]
fn migrate_enum_end_to_end_via_mig() {
  let dir = tempdir().unwrap();
  let mut db = MarciDB::open(dir.path().to_str().unwrap());

  let schema = "enum ChatType {\n  direct {\n    uniqueId String\n  }\n  group {\n    name String\n  }\n}\n\nmodel Chat {\n  type ChatType\n}";
  let applied = db.apply_migrations(&[("0000_init".to_string(), snap(schema))]).unwrap();
  assert_eq!(applied, vec!["0000_init"]);

  insert_data(&db, "Chat", json!({ "type": "group", "name": "General" }));
  assert_eq!(
    get_data_one(&db, "Chat", json!({ "type": true, "name": true })),
    json!({ "type": "group", "name": "General" })
  );
}

/// Enum end-to-end через декларативный путь ($sync): migrate_to со схемой-с-enum со скретча
#[test]
fn migrate_enum_end_to_end_via_sync() {
  let dir = tempdir().unwrap();
  let mut db = MarciDB::open(dir.path().to_str().unwrap());

  let schema = "enum ChatType {\n  direct {\n    uniqueId String\n  }\n  group {\n    name String\n  }\n}\n\nmodel Chat {\n  type ChatType\n}";
  db.migrate_to(schema).unwrap();

  insert_data(&db, "Chat", json!({ "type": "direct", "uniqueId": "u-1" }));
  assert_eq!(
    get_data_one(&db, "Chat", json!({ "type": true, "uniqueId": true })),
    json!({ "type": "direct", "uniqueId": "u-1" })
  );
}

/// Список enum (`Enum[]`) отклоняется с подсказкой об альтернативе (список модели с enum-полем)
#[test]
fn enum_list_rejected_with_hint() {
  let dir = tempdir().unwrap();
  let mut db = MarciDB::open(dir.path().to_str().unwrap());

  let schema = "enum Role {\n  admin\n  user\n}\n\nmodel User {\n  roles Role[]\n}";
  let err = db.migrate_to(schema).unwrap_err();
  let msg = format!("{}", err);
  assert!(msg.contains("list of enum"), "ожидали объяснение, got: {}", msg);
  assert!(msg.contains("RoleItem"), "ожидали подсказку-альтернативу, got: {}", msg);
}

/// Разошедшаяся история (другой id на уже применённой позиции) отклоняется
#[test]
fn migrations_history_diverged_rejected() {
  let dir = tempdir().unwrap();
  let mut db = MarciDB::open(dir.path().to_str().unwrap());

  db.apply_migrations(&[("0000_init".to_string(), snap("model User {\n  name String\n}"))]).unwrap();

  let result = db.apply_migrations(&[("0000_other".to_string(), snap("model User {\n  name String\n}"))]);
  assert!(matches!(result, Err(MigrateApplyError::HistoryDiverged { .. })));
}
