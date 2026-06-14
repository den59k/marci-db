use marcidb::{MarciDB, MigrateApplyError, parse_schema, parse_snapshot, serialize_snapshot, try_parse_schema};
use marcidb_schema::{diff, evolve, migration_ops, reconcile, serialize_migration};
use serde_json::json;
use tempfile::tempdir;

use crate::db::{get_data, get_data_one, insert_data};

/// Declarative `$sync` against a live DB: parse the `.marci` schema, carry slots/ids from the stored
/// snapshot, diff, then apply through the engine's commit primitive. This is what the server's `$sync`
/// does; the engine no longer has a `migrate_to` method (the smart side moved to `marcidb-schema`).
fn migrate_to(db: &mut MarciDB, schema_text: &str) -> Result<(), MigrateApplyError> {
  let mut new_schema = try_parse_schema(schema_text)?;
  reconcile(&mut new_schema, &db.schema);
  let ops = diff(&db.schema, &new_schema)?;
  db.commit_schema(new_schema, &ops)
}

/// Imperative `$migrate` against a live DB: lay the `.march` actions onto the current snapshot (`evolve`),
/// parse the result, extract the ops, commit. This is what the server's `$migrate` does; the engine no
/// longer has an `apply_migration` method (the `.march` text format moved to `marcidb-schema`).
fn apply_migration(db: &mut MarciDB, migration_text: &str) -> Result<(), MigrateApplyError> {
  let cur = serialize_snapshot(&db.schema);
  let new_text = evolve(&cur, migration_text)?;
  let new_schema = parse_snapshot(&new_text)?;
  let ops = migration_ops(migration_text)?;
  db.commit_schema(new_schema, &ops)
}

/// Migration file text = self-contained actions (diff prev→new). `prev`/`new` are `.marci` schema
/// texts (prev="" for the first migration); slots/variants are inherited from prev via reconcile
fn mig(prev: &str, new: &str) -> String {
  let prev_schema = parse_schema(prev);
  let mut new_schema = parse_schema(new);
  reconcile(&mut new_schema, &prev_schema);
  let ops = diff(&prev_schema, &new_schema).unwrap();
  serialize_migration(&ops, &new_schema)
}

/// add field on existing data: old rows are read (field is absent),
/// new rows are written with the field — without rewriting old rows (v2 format)
#[test]
fn migrate_add_field() {
  let dir = tempdir().unwrap();
  let mut db = MarciDB::new("model User {\n  name String\n}", dir.path().to_str().unwrap());

  insert_data(&db, "User", json!({ "name": "Alice" }));
  insert_data(&db, "User", json!({ "name": "Bob" }));

  migrate_to(&mut db,"model User {\n  name String\n  age  UInt\n}").unwrap();

  assert_eq!(get_data(&db, "User", json!({ "name": true })), json!([{ "name": "Alice" }, { "name": "Bob" }]));

  insert_data(&db, "User", json!({ "name": "Carol", "age": 30 }));
  assert_eq!(
    get_data_one(&db, "User", json!({ "name": true, "age": true, "$where": { "name": "Carol" } })),
    json!({ "name": "Carol", "age": 30 })
  );
}

/// add index builds the index from existing rows (backfill) — a query by index
/// finds records inserted BEFORE the migration
#[test]
fn migrate_add_index_backfills_existing_rows() {
  let dir = tempdir().unwrap();
  let mut db = MarciDB::new("model User {\n  name  String\n  email String\n}", dir.path().to_str().unwrap());

  insert_data(&db, "User", json!({ "name": "Alice", "email": "a@x.com" }));
  insert_data(&db, "User", json!({ "name": "Bob", "email": "b@x.com" }));

  migrate_to(&mut db,"model User {\n  name  String\n  email String @index\n}").unwrap();

  assert_eq!(
    get_data_one(&db, "User", json!({ "name": true, "$where": { "email": "a@x.com" } })),
    json!({ "name": "Alice" })
  );
}

/// The first push into an empty DB creates the entity (CreateEntity) — the DB appears "out of nothing"
#[test]
fn migrate_create_model_on_empty_db() {
  let dir = tempdir().unwrap();
  let mut db = MarciDB::open(dir.path().to_str().unwrap());

  // Empty DB — no models yet
  assert!(db.get_model("User").is_none());

  migrate_to(&mut db,"model User {\n  name  String\n  email String @index\n}").unwrap();

  insert_data(&db, "User", json!({ "name": "Alice", "email": "a@x.com" }));
  assert_eq!(get_data(&db, "User", json!({ "name": true })), json!([{ "name": "Alice" }]));
  // The index works
  assert_eq!(
    get_data_one(&db, "User", json!({ "name": true, "$where": { "email": "a@x.com" } })),
    json!({ "name": "Alice" })
  );
}

/// State after migration survives a restart: open() reconstructs the schema from the snapshot in __marci_meta__
#[test]
fn migrate_persists_across_reopen() {
  let dir = tempdir().unwrap();
  let path = dir.path().to_str().unwrap().to_string();

  {
    let mut db = MarciDB::new("model User {\n  name String\n}", &path);
    insert_data(&db, "User", json!({ "name": "Alice" }));
    migrate_to(&mut db,"model User {\n  name String\n  age  UInt\n}").unwrap();
    insert_data(&db, "User", json!({ "name": "Bob", "age": 5 }));
  } // DB is closed

  // Reopen: schema (with age) is reconstructed from the snapshot, without passing schema.marci
  let db = MarciDB::open(&path);
  assert!(db.get_model("User").is_some());
  assert_eq!(
    get_data_one(&db, "User", json!({ "name": true, "age": true, "$where": { "name": "Bob" } })),
    json!({ "name": "Bob", "age": 5 })
  );
}

/// Inserting a field into the MIDDLE of a model: reconcile_slots carries over slots of existing fields,
/// the new field gets the next free one → migration passes, old data is intact (layout bug fix)
#[test]
fn migrate_insert_field_in_middle_carries_slots() {
  let dir = tempdir().unwrap();
  let mut db = MarciDB::new("model M {\n  a String\n  b String\n}", dir.path().to_str().unwrap());
  insert_data(&db, "M", json!({ "a": "a1", "b": "b1" }));

  migrate_to(&mut db,"model M {\n  a String\n  c String\n  b String\n}").unwrap();

  // The old row reads correctly (a/b on their slots, c absent → null)
  assert_eq!(get_data(&db, "M", json!({ "a": true, "b": true, "c": true })), json!([{ "a": "a1", "b": "b1", "c": null }]));
  // The new row writes c
  insert_data(&db, "M", json!({ "a": "a2", "b": "b2", "c": "c2" }));
  assert_eq!(
    get_data_one(&db, "M", json!({ "a": true, "b": true, "c": true, "$where": { "a": "a2" } })),
    json!({ "a": "a2", "b": "b2", "c": "c2" })
  );
}

/// Dropping a Body field retires its slot ($sync): the field is gone, old rows still read, and a field
/// added later must NOT reuse the retired slot (which would resurrect dead bytes as the new field).
#[test]
fn migrate_drop_field_retires_slot() {
  let dir = tempdir().unwrap();
  let mut db = MarciDB::new("model M {\n  a String\n  b String\n}", dir.path().to_str().unwrap());
  insert_data(&db, "M", json!({ "a": "a1", "b": "b1" }));  // b is at slot 8

  // drop b — it's gone, the old row still reads its other fields
  migrate_to(&mut db, "model M {\n  a String\n}").unwrap();
  assert_eq!(get_data(&db, "M", json!({ "a": true })), json!([{ "a": "a1" }]));
  assert!(marcidb::serialize_snapshot(&db.schema).contains("@retired(8)"), "slot 8 must be retired");

  // add d — it must land ABOVE the retired slot, not reuse slot 8
  migrate_to(&mut db, "model M {\n  a String\n  d String\n}").unwrap();
  // the pre-existing row (which has "b1" sitting at slot 8) must read d as null — proves no slot reuse
  assert_eq!(get_data(&db, "M", json!({ "a": true, "d": true })), json!([{ "a": "a1", "d": null }]));

  // new rows use d normally
  insert_data(&db, "M", json!({ "a": "a2", "d": "d2" }));
  assert_eq!(get_data(&db, "M", json!({ "a": true, "d": true })), json!([
    { "a": "a1", "d": null },
    { "a": "a2", "d": "d2" },
  ]));
}

/// A retired slot survives a reopen (it lives in `__marci_meta__`), so a later add still avoids it.
#[test]
fn migrate_drop_field_retirement_persists_across_reopen() {
  let dir = tempdir().unwrap();
  let path = dir.path().to_str().unwrap().to_string();
  {
    let mut db = MarciDB::new("model M {\n  a String\n  b String\n}", &path);
    insert_data(&db, "M", json!({ "a": "a1", "b": "b1" }));
    migrate_to(&mut db, "model M {\n  a String\n}").unwrap();  // drop b (slot 8)
  }
  let mut db = MarciDB::open(&path);
  assert!(marcidb::serialize_snapshot(&db.schema).contains("@retired(8)"), "retirement must survive reopen");
  migrate_to(&mut db, "model M {\n  a String\n  e String\n}").unwrap();  // add e
  assert_eq!(get_data(&db, "M", json!({ "a": true, "e": true })), json!([{ "a": "a1", "e": null }]));
}

/// Dropping a field via the imperative `$migrate` path (a `.march` `drop field` action) retires the slot too.
#[test]
fn migrate_drop_field_via_migrate_path() {
  let dir = tempdir().unwrap();
  let mut db = MarciDB::new("model M {\n  a String\n  b String\n}", dir.path().to_str().unwrap());
  insert_data(&db, "M", json!({ "a": "a1", "b": "b1" }));

  apply_migration(&mut db, "drop field M.b").unwrap();
  assert_eq!(get_data(&db, "M", json!({ "a": true })), json!([{ "a": "a1" }]));
  assert!(marcidb::serialize_snapshot(&db.schema).contains("@retired(8)"));

  // generate would compute slot 12 here (next free above the retired 8); the action carries it
  apply_migration(&mut db, "add field M.d String @slot(12)").unwrap();
  assert_eq!(get_data(&db, "M", json!({ "a": true, "d": true })), json!([{ "a": "a1", "d": null }]));
}

/// Dropping a relation field is rejected (its trees / reverse side need handling) — an explicit error.
#[test]
fn migrate_drop_relation_field_unsupported() {
  let dir = tempdir().unwrap();
  let schema = "model User {\n  name String\n  posts Post[] @bind(Post.author)\n}\nmodel Post {\n  title String\n  author User?\n}";
  let mut db = MarciDB::new(schema, dir.path().to_str().unwrap());
  let result = migrate_to(&mut db, "model User {\n  name String\n}\nmodel Post {\n  title String\n  author User?\n}");
  assert!(matches!(result, Err(MigrateApplyError::Unsupported(_))), "got {:?}", result);
}

/// Changing a field's type requires data transformation — rejected
#[test]
fn migrate_type_change_rejected() {
  let dir = tempdir().unwrap();
  let mut db = MarciDB::new("model User {\n  age UInt\n}", dir.path().to_str().unwrap());
  let result = migrate_to(&mut db,"model User {\n  age String\n}");
  assert!(matches!(result, Err(MigrateApplyError::Diff(_))));
}

// ─────────── imperative migrations ($migrate): a dumb server applies the actions it is sent ───────────

/// Sequential application: create a model, then add a field. Old rows are intact.
#[test]
fn migrate_apply_sequential() {
  let dir = tempdir().unwrap();
  let mut db = MarciDB::open(dir.path().to_str().unwrap());

  let v0 = "model User {\n  name String\n}";
  let v1 = "model User {\n  name String\n  age UInt\n}";
  apply_migration(&mut db,&mig("", v0)).unwrap();
  insert_data(&db, "User", json!({ "name": "Alice" }));

  apply_migration(&mut db,&mig(v0, v1)).unwrap();   // only adding age
  assert_eq!(get_data(&db, "User", json!({ "name": true })), json!([{ "name": "Alice" }]));
  insert_data(&db, "User", json!({ "name": "Bob", "age": 5 }));
  assert_eq!(
    get_data_one(&db, "User", json!({ "name": true, "age": true, "$where": { "name": "Bob" } })),
    json!({ "name": "Bob", "age": 5 })
  );
}

/// The server is dumb: reapplying the same migration fails (idempotency is the client's concern)
#[test]
fn migrate_reapply_fails_no_ledger() {
  let dir = tempdir().unwrap();
  let mut db = MarciDB::open(dir.path().to_str().unwrap());
  let m0 = mig("", "model User {\n  name String\n}");
  apply_migration(&mut db,&m0).unwrap();
  assert!(apply_migration(&mut db,&m0).is_err()); // create entity User again → error
}

/// State after an imperative migration survives a restart (snapshot in __marci_meta__)
#[test]
fn migrate_imperative_persists_across_reopen() {
  let dir = tempdir().unwrap();
  let path = dir.path().to_str().unwrap().to_string();
  {
    let mut db = MarciDB::open(&path);
    apply_migration(&mut db,&mig("", "model User {\n  name String\n}")).unwrap();
    insert_data(&db, "User", json!({ "name": "Alice" }));
  }
  let db = MarciDB::open(&path);
  assert!(db.get_model("User").is_some());
  assert_eq!(get_data(&db, "User", json!({ "name": true })), json!([{ "name": "Alice" }]));
}

/// Invalid schema via migrate_to ($sync) — an error, not a panic
#[test]
fn migrate_to_rejects_invalid_schema() {
  let dir = tempdir().unwrap();
  let mut db = MarciDB::open(dir.path().to_str().unwrap());
  assert!(migrate_to(&mut db,"model A {\n  x Undefined\n}").is_err());      // unknown type
  assert!(migrate_to(&mut db,"model A {\n  x String @bogus\n}").is_err());  // bad attribute
}

/// Invalid action via apply_migration ($migrate) — an error, not a panic
#[test]
fn apply_migration_rejects_invalid() {
  let dir = tempdir().unwrap();
  let mut db = MarciDB::open(dir.path().to_str().unwrap());
  // unknown action
  assert!(apply_migration(&mut db,"totally bogus line").is_err());
  // actions parse fine, but the field references an unknown model — caught during name resolution
  assert!(apply_migration(&mut db,"create entity M\nadd field M.ref Nope @slot(4)").is_err());
}

/// Enum end-to-end via the imperative path ($migrate): self-contained actions with the enum baked in
#[test]
fn migrate_enum_end_to_end_via_mig() {
  let dir = tempdir().unwrap();
  let mut db = MarciDB::open(dir.path().to_str().unwrap());

  let schema = "enum ChatType {\n  direct {\n    uniqueId String\n  }\n  group {\n    name String\n  }\n}\n\nmodel Chat {\n  type ChatType\n}";
  apply_migration(&mut db,&mig("", schema)).unwrap();

  insert_data(&db, "Chat", json!({ "type": "group", "name": "General" }));
  assert_eq!(
    get_data_one(&db, "Chat", json!({ "type": true, "name": true })),
    json!({ "type": "group", "name": "General" })
  );
}

/// Enum end-to-end via the declarative path ($sync): migrate_to with an enum-bearing schema from scratch
#[test]
fn migrate_enum_end_to_end_via_sync() {
  let dir = tempdir().unwrap();
  let mut db = MarciDB::open(dir.path().to_str().unwrap());

  let schema = "enum ChatType {\n  direct {\n    uniqueId String\n  }\n  group {\n    name String\n  }\n}\n\nmodel Chat {\n  type ChatType\n}";
  migrate_to(&mut db,schema).unwrap();

  insert_data(&db, "Chat", json!({ "type": "direct", "uniqueId": "u-1" }));
  assert_eq!(
    get_data_one(&db, "Chat", json!({ "type": true, "uniqueId": true })),
    json!({ "type": "direct", "uniqueId": "u-1" })
  );
}

/// A list of enums (`Enum[]`) is rejected with a hint about the alternative (a list of a model with an enum field)
#[test]
fn enum_list_rejected_with_hint() {
  let dir = tempdir().unwrap();
  let mut db = MarciDB::open(dir.path().to_str().unwrap());

  let schema = "enum Role {\n  admin\n  user\n}\n\nmodel User {\n  roles Role[]\n}";
  let err = migrate_to(&mut db,schema).unwrap_err();
  let msg = format!("{}", err);
  assert!(msg.contains("list of enum"), "expected an explanation, got: {}", msg);
  assert!(msg.contains("RoleItem"), "expected an alternative hint, got: {}", msg);
}

/// Reordering enum variants in schema.marci: ids are carried over from the old snapshot,
/// so already-written data keeps reading correctly (the discriminant does not "drift")
#[test]
fn migrate_enum_reorder_preserves_data() {
  let dir = tempdir().unwrap();
  let mut db = MarciDB::new(
    "model Account {\n  name String\n  type AccountType\n}\n\nenum AccountType {\n  basic\n  pro {\n    sign String\n  }\n}",
    dir.path().to_str().unwrap(),
  );
  insert_data(&db, "Account", json!({ "name": "Alice", "type": "pro", "sign": "a-sign" }));

  // The variants are swapped — but the pro/basic ids must be preserved
  migrate_to(&mut db,"model Account {\n  name String\n  type AccountType\n}\n\nenum AccountType {\n  pro {\n    sign String\n  }\n  basic\n}").unwrap();

  // The record made before the migration still reads as pro with its sign
  assert_eq!(
    get_data_one(&db, "Account", json!({ "name": true, "type": true, "sign": true })),
    json!({ "name": "Alice", "type": "pro", "sign": "a-sign" })
  );
}
