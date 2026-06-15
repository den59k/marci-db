//! Phase 2: the `@custom` module-index SPI, driven through the public API exactly as an external provider
//! crate would. A tiny "echo" provider stands in for a real module (vector/FTS) so the engine's migration,
//! reindex, search, and snapshot-persistence paths are exercised without any module dependency.

use std::sync::Arc;

use marcidb::{
    Field, FieldType, IndexProvider, IndexTree, MarciDB, MigrateApplyError, PrimitiveFieldType, ProviderError,
    ProviderRegistry, ReindexError, RowScan, SearchHit, try_parse_schema,
};
use serde_json::json;
use std::collections::HashSet;
use tempfile::tempdir;

use crate::db::{delete_data, get_data, insert_data, update_data};

/// Declarative `$sync` against a live DB (parse → reconcile → diff → commit), as the server's `$sync` does.
fn migrate_to(db: &mut MarciDB, schema_text: &str) -> Result<(), MigrateApplyError> {
    let mut new_schema = try_parse_schema(schema_text).unwrap();
    marcidb_schema::reconcile(&mut new_schema, &db.schema);
    let ops = marcidb_schema::diff(&db.schema, &new_schema).unwrap();
    db.commit_schema(new_schema, &ops)
}

/// Trivial provider: stores `id -> field bytes` and answers `{ "contains": <substr> }` by substring match,
/// scored by match position. Enough to prove the build/search/persistence wiring end to end.
struct EchoProvider;

impl IndexProvider for EchoProvider {
    fn name(&self) -> &str { "echo" }

    fn validate(&self, field: &Field, _args: &str) -> Result<(), ProviderError> {
        match field.ty {
            FieldType::Primitive(PrimitiveFieldType::String) => Ok(()),
            _ => Err(ProviderError::Invalid(format!("echo requires a String field: {}", field.full_name))),
        }
    }

    fn rebuild(&self, _field: &Field, _args: &str, scan: RowScan<'_>, store: &mut IndexTree<'_>) -> Result<(), ProviderError> {
        for row in scan {
            let (id, value) = row?;
            if let Some(value) = value {
                store.insert(&id, &value)?;
            }
        }
        Ok(())
    }

    fn search(&self, _field: &Field, _args: &str, payload: &serde_json::Value, store: &IndexTree<'_>) -> Result<Vec<SearchHit>, ProviderError> {
        let needle = payload.get("contains").and_then(|v| v.as_str())
            .ok_or_else(|| ProviderError::Invalid("payload must be { contains: <string> }".into()))?;

        let mut hits = vec![];
        for entry in store.iter()? {
            let (id, value) = entry?;
            if let Ok(s) = std::str::from_utf8(&value) {
                if let Some(pos) = s.find(needle) {
                    hits.push(SearchHit { id, score: pos as f32 });
                }
            }
        }
        hits.sort_by(|a, b| a.score.partial_cmp(&b.score).unwrap());
        Ok(hits)
    }
}

fn registry() -> Arc<ProviderRegistry> {
    let mut reg = ProviderRegistry::new();
    reg.register(Box::new(EchoProvider));
    Arc::new(reg)
}

/// `{ contains: <substr> }` substring search over an `id -> value` index, scored by match position.
fn substring_hits(store: &IndexTree<'_>, payload: &serde_json::Value) -> Result<Vec<SearchHit>, ProviderError> {
    let needle = payload.get("contains").and_then(|v| v.as_str())
        .ok_or_else(|| ProviderError::Invalid("payload must be { contains: <string> }".into()))?;
    let mut hits = vec![];
    for entry in store.iter()? {
        let (id, value) = entry?;
        if let Ok(s) = std::str::from_utf8(&value) {
            if let Some(pos) = s.find(needle) {
                hits.push(SearchHit { id, score: pos as f32 });
            }
        }
    }
    hits.sort_by(|a, b| a.score.partial_cmp(&b.score).unwrap());
    Ok(hits)
}

/// Like `EchoProvider`, but opts into incremental maintenance: the `on_*` hooks keep the `id -> value` map
/// live, so a search reflects writes with no `$reindex`. Proves the engine dispatches the hooks — and only
/// for providers that opt in — through every write path, including nested-write and cascade-delete recursion.
struct LiveEchoProvider;

impl IndexProvider for LiveEchoProvider {
    fn name(&self) -> &str { "live" }

    fn validate(&self, field: &Field, _args: &str) -> Result<(), ProviderError> {
        match field.ty {
            FieldType::Primitive(PrimitiveFieldType::String) => Ok(()),
            _ => Err(ProviderError::Invalid(format!("live requires a String field: {}", field.full_name))),
        }
    }

    fn rebuild(&self, _field: &Field, _args: &str, scan: RowScan<'_>, store: &mut IndexTree<'_>) -> Result<(), ProviderError> {
        for row in scan {
            let (id, value) = row?;
            if let Some(value) = value { store.insert(&id, &value)?; }
        }
        Ok(())
    }

    fn search(&self, _field: &Field, _args: &str, payload: &serde_json::Value, store: &IndexTree<'_>) -> Result<Vec<SearchHit>, ProviderError> {
        substring_hits(store, payload)
    }

    fn maintains_incrementally(&self) -> bool { true }

    fn on_insert(&self, _field: &Field, _args: &str, row: marcidb::RowRef<'_>, new: Option<&[u8]>, store: &mut IndexTree<'_>) -> Result<(), ProviderError> {
        if let Some(new) = new { store.insert(row.id, new)?; }
        Ok(())
    }

    fn on_update(&self, _field: &Field, _args: &str, row: marcidb::RowRef<'_>, _old: Option<&[u8]>, new: Option<&[u8]>, store: &mut IndexTree<'_>) -> Result<(), ProviderError> {
        match new {
            Some(new) => { store.insert(row.id, new)?; }
            None => { store.remove(row.id)?; }
        }
        Ok(())
    }

    fn on_delete(&self, _field: &Field, _args: &str, row: marcidb::RowRef<'_>, _old: Option<&[u8]>, store: &mut IndexTree<'_>) -> Result<(), ProviderError> {
        store.remove(row.id)?;
        Ok(())
    }
}

fn live_registry() -> Arc<ProviderRegistry> {
    let mut reg = ProviderRegistry::new();
    reg.register(Box::new(LiveEchoProvider));
    Arc::new(reg)
}

const SCHEMA: &str = "
    model Doc {
        title String
        tags  String @custom(echo)
    }
";

#[test]
fn custom_index_reindex_search_and_persistence() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_str().unwrap();
    let reg = registry();

    {
        let db = MarciDB::new(SCHEMA, path).with_providers(reg.clone());

        insert_data(&db, "Doc", json!({ "title": "A", "tags": "red green" }));
        insert_data(&db, "Doc", json!({ "title": "B", "tags": "green blue" }));
        insert_data(&db, "Doc", json!({ "title": "C", "tags": "red blue" }));

        let doc = db.get_model("Doc").unwrap();
        let tags = doc.fields.iter().find(|f| f.name == "tags").unwrap();

        // The custom tree exists (created at migration) but is empty until $reindex.
        assert_eq!(db.search_custom(tags, &json!({ "contains": "red" })).unwrap().len(), 0,
            "index must be empty before reindex");

        // Reindex populates it from current rows.
        assert_eq!(db.reindex_entity(doc).unwrap(), 1, "exactly one custom index rebuilt");

        assert_eq!(db.search_custom(tags, &json!({ "contains": "red" })).unwrap().len(), 2);
        assert_eq!(db.search_custom(tags, &json!({ "contains": "blue" })).unwrap().len(), 2);
        assert_eq!(db.search_custom(tags, &json!({ "contains": "green" })).unwrap().len(), 2);
        assert_eq!(db.search_custom(tags, &json!({ "contains": "purple" })).unwrap().len(), 0);
    }

    // Reopen: the @custom attribute survived the snapshot, the schema rebuilt the FieldIndex::Custom,
    // and the index data persisted on disk — search works with no re-reindex.
    let db = MarciDB::open(path).with_providers(reg.clone());
    let doc = db.get_model("Doc").unwrap();
    let tags = doc.fields.iter().find(|f| f.name == "tags").unwrap();
    assert!(tags.indexes.iter().any(|i| matches!(i, marcidb::FieldIndex::Custom { name, .. } if name == "echo")),
        "custom index not reconstructed after reopen");
    assert_eq!(db.search_custom(tags, &json!({ "contains": "red" })).unwrap().len(), 2,
        "index data must persist across reopen");
}

#[test]
fn custom_index_query_via_near() {
    let dir = tempdir().unwrap();
    let db = MarciDB::new(SCHEMA, dir.path().to_str().unwrap()).with_providers(registry());

    insert_data(&db, "Doc", json!({ "title": "A", "tags": "red green" }));
    insert_data(&db, "Doc", json!({ "title": "B", "tags": "green blue" }));
    insert_data(&db, "Doc", json!({ "title": "C", "tags": "red blue" }));

    let doc = db.get_model("Doc").unwrap();
    db.reindex_entity(doc).unwrap();

    // `$near` resolves through the provider, then rows are fetched and decoded normally.
    let res = get_data(&db, "Doc", json!({
        "title": true, "tags": true,
        "$where": { "tags": { "$near": { "contains": "red" } } }
    }));
    let titles: HashSet<&str> = res.as_array().unwrap().iter().map(|d| d["title"].as_str().unwrap()).collect();
    assert_eq!(titles, HashSet::from(["A", "C"]));

    // Residual filter is applied as a post-condition over the ranked candidate set.
    let res2 = get_data(&db, "Doc", json!({
        "title": true, "tags": true,
        "$where": { "tags": { "$near": { "contains": "red" } }, "title": "C" }
    }));
    let arr2 = res2.as_array().unwrap();
    assert_eq!(arr2.len(), 1);
    assert_eq!(arr2[0]["title"].as_str().unwrap(), "C");

    // No match → empty result.
    let res3 = get_data(&db, "Doc", json!({
        "title": true,
        "$where": { "tags": { "$near": { "contains": "purple" } } }
    }));
    assert_eq!(res3.as_array().unwrap().len(), 0);
}

#[test]
fn near_inside_not_is_rejected() {
    let dir = tempdir().unwrap();
    let db = MarciDB::new(SCHEMA, dir.path().to_str().unwrap()).with_providers(registry());
    let entity = db.get_model("Doc").unwrap();

    // A ranked candidate set can't be negated coherently yet — $near under $not is a parse error.
    let parsed = marcidb::parse_query(&db.schema, entity, &json!({
        "$where": { "$not": { "tags": { "$near": { "contains": "x" } } } }
    }));
    assert!(parsed.is_err(), "$near inside $not must be rejected");
}

#[test]
fn migration_validates_custom_index_provider() {
    // @echo on a String field with the provider registered → migration succeeds.
    let dir = tempdir().unwrap();
    let mut db = MarciDB::open(dir.path().to_str().unwrap()).with_providers(registry());
    assert!(migrate_to(&mut db, "model Doc {\n  title String\n  tags String @echo\n}").is_ok());

    // Unregistered provider (a typo, or a module not compiled in) → fails at migration, not deferred to reindex.
    let dir2 = tempdir().unwrap();
    let mut db2 = MarciDB::open(dir2.path().to_str().unwrap()).with_providers(registry());
    match migrate_to(&mut db2, "model Doc {\n  body String @nonexistent\n}") {
        Err(MigrateApplyError::NoProvider { provider, .. }) => assert_eq!(provider, "nonexistent"),
        other => panic!("expected NoProvider, got {:?}", other),
    }

    // Provider rejects the field type (echo requires String) → InvalidIndex at migration.
    let dir3 = tempdir().unwrap();
    let mut db3 = MarciDB::open(dir3.path().to_str().unwrap()).with_providers(registry());
    assert!(matches!(
        migrate_to(&mut db3, "model Doc {\n  n Int @echo\n}"),
        Err(MigrateApplyError::InvalidIndex { .. })
    ));
}

#[test]
fn updates_and_deletes_do_not_touch_custom_index_without_provider() {
    // A DB with a @custom field must open and serve plain CRUD even when no provider is registered.
    let dir = tempdir().unwrap();
    let db = MarciDB::new(SCHEMA, dir.path().to_str().unwrap()); // empty registry

    let id = insert_data(&db, "Doc", json!({ "title": "A", "tags": "red" }));
    crate::db::update_data(&db, "Doc", &id, json!({ "tags": "green" }));
    crate::db::delete_data(&db, "Doc", id);

    // Reindex without a provider is a clear, recoverable error (not a panic).
    let doc = db.get_model("Doc").unwrap();
    match db.reindex_entity(doc) {
        Err(ReindexError::NoProvider(name)) => assert_eq!(name, "echo"),
        other => panic!("expected NoProvider, got {:?}", other),
    }
}

// ─────────────────────────── incremental (live) maintenance ───────────────────────────

const LIVE_SCHEMA: &str = "
    model Doc {
        title String
        tags  String @custom(live)
    }
";

#[test]
fn non_incremental_provider_skips_write_hooks() {
    // EchoProvider keeps the default `maintains_incrementally() == false`, so the engine never opens its
    // index tree on the write path — it stays empty until an explicit `$reindex`.
    let dir = tempdir().unwrap();
    let db = MarciDB::new(SCHEMA, dir.path().to_str().unwrap()).with_providers(registry());
    let doc = db.get_model("Doc").unwrap();
    let tags = doc.fields.iter().find(|f| f.name == "tags").unwrap();

    insert_data(&db, "Doc", json!({ "title": "A", "tags": "red" }));
    assert_eq!(db.search_custom(tags, &json!({ "contains": "red" })).unwrap().len(), 0,
        "a non-incremental provider must not be maintained on the write path");

    db.reindex_entity(doc).unwrap();
    assert_eq!(db.search_custom(tags, &json!({ "contains": "red" })).unwrap().len(), 1);
}

#[test]
fn incremental_provider_maintains_index_live() {
    let dir = tempdir().unwrap();
    let db = MarciDB::new(LIVE_SCHEMA, dir.path().to_str().unwrap()).with_providers(live_registry());
    let doc = db.get_model("Doc").unwrap();
    let tags = doc.fields.iter().find(|f| f.name == "tags").unwrap();
    let count = |needle: &str| db.search_custom(tags, &json!({ "contains": needle })).unwrap().len();

    // Insert — indexed immediately, with no `$reindex`.
    let a = insert_data(&db, "Doc", json!({ "title": "A", "tags": "red green" }));
    insert_data(&db, "Doc", json!({ "title": "B", "tags": "green blue" }));
    assert_eq!(count("red"), 1);
    assert_eq!(count("green"), 2);

    // Update — the old value is removed and the new one indexed (on_update carries both).
    update_data(&db, "Doc", &a, json!({ "tags": "blue sky" }));
    assert_eq!(count("red"), 0, "the old value must be de-indexed");
    assert_eq!(count("blue"), 2);
    assert_eq!(count("sky"), 1);

    // Delete — the row's postings are removed.
    delete_data(&db, "Doc", a);
    assert_eq!(count("blue"), 1);
    assert_eq!(count("sky"), 0);
}

const NESTED_SCHEMA: &str = "
    model Note {
        title  String
        detail Detail?
    }
    struct Detail {
        text String @custom(live)
    }
";

#[test]
fn incremental_index_through_nested_write_and_cascade_delete() {
    let dir = tempdir().unwrap();
    let db = MarciDB::new(NESTED_SCHEMA, dir.path().to_str().unwrap()).with_providers(live_registry());
    // A `struct` is synthesized into a model named after the field path that owns it ("Note.detail").
    let detail = db.get_model("Note.detail").unwrap();
    let text = detail.fields.iter().find(|f| f.name == "text").unwrap();
    let count = |needle: &str| db.search_custom(text, &json!({ "contains": needle })).unwrap().len();

    // A nested insert recurses through `process_write` → `on_insert` fires for the owned child row.
    let note = insert_data(&db, "Note", json!({ "title": "N", "detail": { "text": "hello world" } }));
    assert_eq!(count("hello"), 1, "a nested write must index the owned child");

    // A cascade delete recurses through `process_delete` → `on_delete` fires for the owned child row.
    delete_data(&db, "Note", note);
    assert_eq!(count("hello"), 0, "a cascade delete must de-index the owned child");
}
