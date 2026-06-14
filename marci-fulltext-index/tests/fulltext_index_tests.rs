//! End-to-end full-text search through MarciDB: a `String @custom(fulltext)` field — migrate, insert,
//! `$reindex`, then `$search`. Covers English + Russian stemming, mixed-language (default `multi`) analysis,
//! and tf·idf ranking, over a real (temp) canopydb.

use std::collections::HashSet;
use std::sync::Arc;

use marcidb::{MarciDB, ProviderRegistry, array_to_json, decode_document, parse_insert, parse_query};
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
