//! Phase 2: the `@custom` module-index SPI, driven through the public API exactly as an external provider
//! crate would. A tiny "echo" provider stands in for a real module (vector/FTS) so the engine's migration,
//! reindex, search, and snapshot-persistence paths are exercised without any module dependency.

use std::sync::Arc;

use marcidb::{
    Field, FieldType, IndexProvider, IndexTree, MarciDB, PrimitiveFieldType, ProviderError,
    ProviderRegistry, ReindexError, RowScan, SearchHit,
};
use serde_json::json;
use std::collections::HashSet;
use tempfile::tempdir;

use crate::db::{get_data, insert_data};

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
