//! `@default([])` on a list is the empty list (it read as `[""]`: one empty string), `@default([1, 2])` with
//! spaces parses — a row inserted without the field gets the default, one with it keeps its value.
use marcidb::MarciDB;
use serde_json::json;
use tempfile::tempdir;

use crate::db::{get_data, insert_data};

#[test]
fn a_list_default_fills_a_row_inserted_without_the_field() {
    let dir = tempdir().unwrap();
    let db = MarciDB::new("
        model Post {
            title   String
            tags    String[]  @default([])
            marks   Int[]     @default([1, 2])
            names   String[]  @default([ \"a\", \"b c\" ])
        }
    ", dir.path().to_str().unwrap());
    insert_data(&db, "Post", json!({ "title": "bare" }));
    insert_data(&db, "Post", json!({ "title": "given", "tags": ["x"], "marks": [], "names": [] }));
    assert_eq!(
        get_data(&db, "Post", json!({ "title": true, "tags": true, "marks": true, "names": true })),
        json!([
            { "title": "bare", "tags": [], "marks": [1, 2], "names": ["a", "b c"] },
            { "title": "given", "tags": ["x"], "marks": [], "names": [] },
        ])
    );
}
