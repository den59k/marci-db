//! End-to-end full-text search through MarciDB: a `String @custom(fulltext)` field — migrate, insert,
//! `$reindex`, then `$search`. Covers English + Russian stemming, mixed-language (default `multi`) analysis,
//! and tf·idf ranking, over a real (temp) canopydb.

use std::collections::HashSet;
use std::sync::Arc;

use marcidb::{MarciDB, ProviderRegistry, array_to_json, decode_document, decode_id, parse_id, parse_insert, parse_query, parse_update};
use marci_fulltext_index::FullTextProvider;
use serde_json::{Value, json};
use tempfile::tempdir;

fn registry() -> Arc<ProviderRegistry> {
    let mut reg = ProviderRegistry::new();
    reg.register(Box::new(FullTextProvider::new()));
    Arc::new(reg)
}

fn insert(db: &MarciDB, title: &str, body: &str) {
    let entity = db.get_model("Doc").unwrap();
    let op = parse_insert(&db.schema, entity, &json!({ "title": title, "body": body })).unwrap();
    db.insert_item(entity, &op).unwrap();
}

/// Insert a doc and return its decoded id (so it can later be updated/deleted). `body` is a JSON value so a
/// `null` body can be exercised on a nullable field.
fn insert_doc(db: &MarciDB, title: &str, body: Value) -> Value {
    let entity = db.get_model("Doc").unwrap();
    let op = parse_insert(&db.schema, entity, &json!({ "title": title, "body": body })).unwrap();
    let id = db.insert_item(entity, &op).unwrap();
    serde_json::from_str(&decode_id(&id, entity, &db.schema)).unwrap()
}

fn update_body(db: &MarciDB, id: &Value, body: Value) {
    let entity = db.get_model("Doc").unwrap();
    let pid = parse_id(&db.schema, entity, id).unwrap();
    let op = parse_update(&db.schema, entity, &json!({ "body": body })).unwrap();
    db.update_item(entity, &pid, &op).unwrap();
}

fn delete(db: &MarciDB, id: &Value) {
    let entity = db.get_model("Doc").unwrap();
    let pid = parse_id(&db.schema, entity, id).unwrap();
    db.delete_item(entity, &pid).unwrap();
}

/// Run a `$search` and return matched titles in ranked order.
fn search(db: &MarciDB, payload: Value) -> Vec<String> {
    let entity = db.get_model("Doc").unwrap();
    let q = json!({ "title": true, "$where": { "body": { "$search": payload } } });
    let query = parse_query(&db.schema, entity, &q).unwrap();
    let items = db.find_many(&query, |ctx| decode_document(ctx).unwrap()).unwrap();
    let arr: Value = serde_json::from_str(&array_to_json(&items)).unwrap();
    arr.as_array().unwrap().iter().map(|d| d["title"].as_str().unwrap().to_string()).collect()
}

const SCHEMA: &str = "
    model Doc {
        title String
        body  String @custom(fulltext)
    }
";

fn fresh() -> (tempfile::TempDir, MarciDB) {
    let dir = tempdir().unwrap();
    let db = MarciDB::new(SCHEMA, dir.path().to_str().unwrap()).with_providers(registry());
    (dir, db)
}

#[test]
fn english_and_russian_stemming() {
    let (_d, db) = fresh();
    insert(&db, "A", "The cat runs fast every morning");
    insert(&db, "B", "Кошки быстро бегают по улице");      // cats run fast on the street
    insert(&db, "C", "A dog walks slowly in the park");
    insert(&db, "D", "Эта машина очень быстрая");          // this car is very fast

    let doc = db.get_model("Doc").unwrap();
    assert_eq!(db.reindex_entity(doc).unwrap(), 1);

    // English: "running" and "runs" share the stem "run".
    assert_eq!(search(&db, json!("running")), vec!["A"]);
    assert_eq!(search(&db, json!("walked")), vec!["C"]);

    // Russian: query inflections match indexed inflections via the Snowball stem.
    assert_eq!(search(&db, json!("кошку")), vec!["B"]);    // accusative of кошка ↔ indexed Кошки
    assert_eq!(search(&db, json!("машину")), vec!["D"]);   // accusative of машина ↔ indexed машина

    // No match → empty.
    assert!(search(&db, json!("xyzzy")).is_empty());
}

#[test]
fn mixed_language_field_default_multi() {
    // A single field with both scripts; default `multi` stems each token by script.
    let (_d, db) = fresh();
    insert(&db, "EN", "modern art photos");
    insert(&db, "RU", "современная фотография");              // modern photograph (ru)
    insert(&db, "MIX", "the фотография of modern art");       // mixed

    let doc = db.get_model("Doc").unwrap();
    db.reindex_entity(doc).unwrap();

    // Russian query hits the Russian and the mixed doc (both: фотография/фотографий → "фотограф").
    let ru: HashSet<String> = search(&db, json!("фотографий")).into_iter().collect();
    assert_eq!(ru, HashSet::from(["RU".into(), "MIX".into()]));

    // English query hits the English and the mixed doc (both contain "modern").
    let en: HashSet<String> = search(&db, json!("modern")).into_iter().collect();
    assert_eq!(en, HashSet::from(["EN".into(), "MIX".into()]));
}

#[test]
fn tf_idf_ranks_more_relevant_first() {
    let (_d, db) = fresh();
    insert(&db, "P", "alpha beta");  // matches both query terms
    insert(&db, "Q", "alpha only");  // matches one term (rarer "beta" missing)

    let doc = db.get_model("Doc").unwrap();
    db.reindex_entity(doc).unwrap();

    // OR semantics: both match "alpha"; P also matches the rarer "beta", so P ranks first.
    let ranked = search(&db, json!({ "query": "alpha beta", "limit": 5 }));
    assert_eq!(ranked, vec!["P", "Q"]);

    // A term unique to one doc returns only that doc.
    assert_eq!(search(&db, json!("beta")), vec!["P"]);
}

#[test]
fn forced_language_arg_is_validated() {
    // `@custom(fulltext, russian)` is accepted; the analyzer still indexes/searches.
    let dir = tempdir().unwrap();
    let schema = "model Doc {\n  title String\n  body String @custom(fulltext, russian)\n}";
    let db = MarciDB::new(schema, dir.path().to_str().unwrap()).with_providers(registry());
    insert(&db, "A", "Книга лежит на столе");  // a book lies on the table
    db.reindex_entity(db.get_model("Doc").unwrap()).unwrap();
    assert_eq!(search(&db, json!("книги")), vec!["A"]); // genitive книги ↔ indexed Книга
}

// ─────────────────────────── live (incremental) maintenance ───────────────────────────

const NULLABLE_SCHEMA: &str = "
    model Doc {
        title String
        body  String? @custom(fulltext)
    }
";

#[test]
fn live_insert_update_delete_without_reindex() {
    let (_d, db) = fresh();
    let a = insert_doc(&db, "A", json!("the cat runs fast"));
    insert_doc(&db, "B", json!("a dog walks slowly"));

    // No `$reindex` — the inserts are searchable immediately.
    assert_eq!(search(&db, json!("cat")), vec!["A"]);
    assert_eq!(search(&db, json!("dog")), vec!["B"]);

    // An update removes the old postings and adds the new ones.
    update_body(&db, &a, json!("the bird flies high"));
    assert!(search(&db, json!("cat")).is_empty(), "terms from the old value must be de-indexed");
    assert_eq!(search(&db, json!("bird")), vec!["A"]);

    // A delete removes the doc's postings.
    delete(&db, &a);
    assert!(search(&db, json!("bird")).is_empty());
    assert_eq!(search(&db, json!("dog")), vec!["B"]);
}

#[test]
fn null_and_value_transitions() {
    let dir = tempdir().unwrap();
    let db = MarciDB::new(NULLABLE_SCHEMA, dir.path().to_str().unwrap()).with_providers(registry());

    // A null body is not a document — nothing indexed, doc count unaffected.
    let a = insert_doc(&db, "A", json!(null));
    insert_doc(&db, "B", json!("alpha beta"));
    assert_eq!(search(&db, json!("alpha")), vec!["B"]);

    // null → value: the doc gains its first terms and becomes searchable.
    update_body(&db, &a, json!("alpha gamma"));
    let alpha: HashSet<String> = search(&db, json!("alpha")).into_iter().collect();
    assert_eq!(alpha, HashSet::from(["A".into(), "B".into()]));
    assert_eq!(search(&db, json!("gamma")), vec!["A"]);

    // value → null: the doc loses its last terms and is de-indexed again.
    update_body(&db, &a, json!(null));
    assert_eq!(search(&db, json!("alpha")), vec!["B"]);
    assert!(search(&db, json!("gamma")).is_empty());
}

#[test]
fn live_maintenance_equals_full_rebuild() {
    // After a mix of live ops, the index must be byte-for-byte identical to a from-scratch `$reindex` —
    // this guards both the postings and the document count `N` (the stats key is part of the dump).
    let (_d, db) = fresh();
    let a = insert_doc(&db, "A", json!("the quick brown fox jumps"));
    let b = insert_doc(&db, "B", json!("lazy dogs sleep all day"));
    insert_doc(&db, "C", json!("quick foxes and lazy dogs"));
    update_body(&db, &a, json!("the slow brown bear waits"));
    delete(&db, &b);
    insert_doc(&db, "E", json!("brown bears and quick rivers"));

    let live = db.dump_dev("custom_fulltext_Doc.body");
    db.reindex_entity(db.get_model("Doc").unwrap()).unwrap();
    let rebuilt = db.dump_dev("custom_fulltext_Doc.body");

    assert_eq!(live, rebuilt, "live maintenance must match a full rebuild byte-for-byte (postings + doc count N)");
}

#[test]
fn transaction_commits_index_atomically() {
    // Several writes share one transaction; their index updates commit together with the rows.
    let (_d, db) = fresh();
    db.transaction(|tx| {
        let entity = db.get_model("Doc").unwrap();
        for (t, b) in [("A", "alpha one"), ("B", "beta two"), ("C", "gamma three")] {
            let op = parse_insert(&db.schema, entity, &json!({ "title": t, "body": b })).unwrap();
            tx.insert_item(entity, &op)?;
        }
        Ok::<(), marcidb::InsertError>(())
    }).unwrap();

    assert_eq!(search(&db, json!("alpha")), vec!["A"]);
    assert_eq!(search(&db, json!("beta")), vec!["B"]);
    assert_eq!(search(&db, json!("gamma")), vec!["C"]);
}

#[test]
fn rolled_back_write_leaves_index_unchanged() {
    let (_d, db) = fresh();
    insert_doc(&db, "A", json!("permanent content"));
    assert_eq!(search(&db, json!("permanent")), vec!["A"]);
    let before = db.dump_dev("custom_fulltext_Doc.body");

    // An uncommitted transaction's index writes roll back together with the row.
    {
        let tx = db.begin_write().unwrap();
        let entity = db.get_model("Doc").unwrap();
        let op = parse_insert(&db.schema, entity, &json!({ "title": "B", "body": "ephemeral content" })).unwrap();
        tx.insert_item(entity, &op).unwrap();
        // dropped without `commit()` → rollback
    }

    assert!(search(&db, json!("ephemeral")).is_empty(), "a rolled-back insert must not be indexed");
    assert_eq!(db.dump_dev("custom_fulltext_Doc.body"), before, "the index must be unchanged after a rollback");
}
