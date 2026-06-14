//! End-to-end: a `Float[N] @custom(vector, …)` field driven through MarciDB — migrate, insert, `$reindex`,
//! then `$near` search returns the nearest rows. Exercises the full SPI ↔ marci_vector bridge over a real
//! (temp) canopydb, including the BE-column / LE-index endianness handling.

use std::sync::Arc;

use marcidb::{MarciDB, ProviderRegistry, array_to_json, decode_document, parse_insert, parse_query};
use marci_vector_index::VectorIndexProvider;
use serde_json::{Value, json};
use tempfile::tempdir;

fn registry() -> Arc<ProviderRegistry> {
    let mut reg = ProviderRegistry::new();
    reg.register(Box::new(VectorIndexProvider::new()));
    Arc::new(reg)
}

fn insert(db: &MarciDB, model: &str, data: Value) {
    let entity = db.get_model(model).unwrap();
    let op = parse_insert(&db.schema, entity, &data).unwrap();
    db.insert_item(entity, &op).unwrap();
}

fn near(db: &MarciDB, model: &str, field: &str, payload: Value) -> Vec<String> {
    let entity = db.get_model(model).unwrap();
    let q = json!({ "name": true, "$where": { field: { "$near": payload } } });
    let query = parse_query(&db.schema, entity, &q).unwrap();
    let items = db.find_many(&query, |ctx| decode_document(ctx).unwrap()).unwrap();
    let arr: Value = serde_json::from_str(&array_to_json(&items)).unwrap();
    arr.as_array().unwrap().iter().map(|d| d["name"].as_str().unwrap().to_string()).collect()
}

const SCHEMA: &str = "
    model Place {
        name String
        loc  Float[2] @custom(vector, euclidean)
    }
";

#[test]
fn nearest_neighbour_search_over_marcidb() {
    let dir = tempdir().unwrap();
    let db = MarciDB::new(SCHEMA, dir.path().to_str().unwrap()).with_providers(registry());

    insert(&db, "Place", json!({ "name": "A", "loc": [0.0, 0.0] }));
    insert(&db, "Place", json!({ "name": "B", "loc": [0.0, 2.0] }));
    insert(&db, "Place", json!({ "name": "C", "loc": [0.0, 1.0] }));
    insert(&db, "Place", json!({ "name": "D", "loc": [1.0, 0.0] }));
    insert(&db, "Place", json!({ "name": "E", "loc": [10.0, 10.0] }));

    let place = db.get_model("Place").unwrap();
    assert_eq!(db.reindex_entity(place).unwrap(), 1);

    // Nearest 3 to (0, 0.5): A(0,0) and C(0,1) tie at 0.25, then D(1,0) at 1.25. B and E are far.
    let mut names = near(&db, "Place", "loc", json!({ "vector": [0.0, 0.5], "k": 3 }));
    names.sort();
    assert_eq!(names, vec!["A", "C", "D"]);

    // k=1 → just the closest tie group's first; assert it's one of the two nearest.
    let top1 = near(&db, "Place", "loc", json!({ "vector": [0.0, 0.5], "k": 1 }));
    assert_eq!(top1.len(), 1);
    assert!(top1[0] == "A" || top1[0] == "C", "closest should be A or C, got {:?}", top1);
}

#[test]
fn near_with_wrong_dimensions_is_rejected() {
    let dir = tempdir().unwrap();
    let db = MarciDB::new(SCHEMA, dir.path().to_str().unwrap()).with_providers(registry());
    insert(&db, "Place", json!({ "name": "A", "loc": [0.0, 0.0] }));
    let place = db.get_model("Place").unwrap();
    db.reindex_entity(place).unwrap();

    // 3-element query vector against a Float[2] index → provider Invalid error (surfaced as QueryError).
    let q = json!({ "name": true, "$where": { "loc": { "$near": { "vector": [0.0, 0.0, 0.0] } } } });
    let query = parse_query(&db.schema, place, &q).unwrap();
    let result = db.find_many(&query, |ctx| decode_document(ctx).unwrap());
    assert!(result.is_err(), "mismatched query dimensions must error");
}
