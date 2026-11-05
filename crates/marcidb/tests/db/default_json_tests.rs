//! `Json @default(<JSON literal>)` and `DateTime @default(<epoch ms>)`: a row inserted without the fields
//! reads the literal back decoded and the date as its number, one inserted with them keeps its values — and
//! both hold after a reopen, where the defaults are re-parsed from the stored snapshot.
use marcidb::MarciDB;
use serde_json::json;
use tempfile::tempdir;

use crate::db::{get_data, get_data_one, insert_data};

#[test]
fn json_and_datetime_defaults_fill_a_row_inserted_without_them() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_str().unwrap();
    let db = MarciDB::new(r#"
        model Doc {
            name    String
            meta    Json      @default({"theme": "dark", "to": "a@b.c", "tabs": [1, 2]})
            tags    Json      @default([])
            label   Json      @default("text")
            at      DateTime  @default(1735689600000)
        }
    "#, path);
    insert_data(&db, "Doc", json!({ "name": "bare" }));
    insert_data(&db, "Doc", json!({ "name": "given", "meta": { "x": null }, "tags": [3], "label": 0, "at": 5 }));

    let fields = json!({ "name": true, "meta": true, "tags": true, "label": true, "at": true });
    let bare = json!({ "meta": { "theme": "dark", "to": "a@b.c", "tabs": [1, 2] }, "tags": [], "label": "text", "at": 1735689600000i64 });
    let mut expected_bare = bare.clone();
    expected_bare["name"] = json!("bare");
    assert_eq!(get_data(&db, "Doc", fields.clone()), json!([
        expected_bare,
        { "name": "given", "meta": { "x": null }, "tags": [3], "label": 0, "at": 5 },
    ]));
    // A defaulted blob is filtered by path like an inserted one
    assert_eq!(get_data(&db, "Doc", json!({ "name": true, "$where": { "meta": { "tabs.1": 2 } } })), json!([{ "name": "bare" }]));

    drop(db);
    let db = MarciDB::open(path);
    insert_data(&db, "Doc", json!({ "name": "reopened" }));
    let mut expected_reopened = bare;
    expected_reopened["name"] = json!("reopened");
    let mut query = fields;
    query["$where"] = json!({ "name": "reopened" });
    assert_eq!(get_data_one(&db, "Doc", query), expected_reopened);
}
