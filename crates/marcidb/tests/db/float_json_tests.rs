use marcidb::{MarciDB, array_to_json, decode_document, parse_query};
use serde_json::json;
use tempfile::tempdir;

use crate::db::{get_data, insert_data};

fn create_db(dir: &tempfile::TempDir) -> MarciDB {
  MarciDB::new("
    model M {
      name  String
      d     Double
      f     Float
    }
  ", dir.path().to_str().unwrap())
}

/// Float output has to be re-parseable JSON at every magnitude. Rust's `Display` for floats never uses
/// exponent notation, so a large `Double` used to serialize as a 300+ digit decimal literal — valid JSON
/// grammatically, but past what a strict parser accepts as a number, so the row could not be read back.
#[test]
fn large_floats_roundtrip_test() {
  let dir = tempdir().unwrap();
  let db = create_db(&dir);

  let cases = [
    ("max",   f64::MAX,   f32::MAX),
    ("min",   f64::MIN,   f32::MIN),
    ("big",   1e20,       1e20),
    ("tiny",  5e-324,     1e-38),
    ("plain", -2.25,      0.5),
    ("whole", 1.0,        1.0),
    ("zero",  0.0,        0.0),
  ];
  for (name, d, f) in cases {
    insert_data(&db, "M", json!({ "name": name, "d": d, "f": f }));
  }

  // Go through the raw encoder output rather than a helper, so the assertion is about the bytes on the wire.
  let entity = db.get_model("M").unwrap();
  let query = parse_query(&db.schema, entity, &json!({ "name": true, "d": true, "f": true })).unwrap();
  let items = db.find_many(&query, |ctx| decode_document(ctx).unwrap()).unwrap();
  let raw = array_to_json(&items);

  let parsed: serde_json::Value = serde_json::from_str(&raw)
    .unwrap_or_else(|e| panic!("encoder emitted unparseable JSON: {}\n{}", e, raw));

  // Every value survives the round trip exactly.
  for (i, (name, d, f)) in cases.iter().enumerate() {
    assert_eq!(parsed[i]["name"], json!(name));
    assert_eq!(parsed[i]["d"].as_f64().unwrap(), *d, "double mismatch for {}", name);
    assert_eq!(parsed[i]["f"].as_f64().unwrap() as f32, *f, "float mismatch for {}", name);
  }
}

/// A whole-numbered float keeps its fractional form (`1.0`, not `1`), so a `Double` column does not read
/// back as an integer.
#[test]
fn whole_floats_keep_float_form_test() {
  let dir = tempdir().unwrap();
  let db = create_db(&dir);
  insert_data(&db, "M", json!({ "name": "a", "d": 1.0, "f": 2.0 }));

  let data = get_data(&db, "M", json!({ "d": true, "f": true }));
  assert!(data[0]["d"].is_f64(), "expected a float, got {}", data[0]["d"]);
  assert!(data[0]["f"].is_f64(), "expected a float, got {}", data[0]["f"]);
  assert_eq!(data[0]["d"].as_f64().unwrap(), 1.0);
  assert_eq!(data[0]["f"].as_f64().unwrap(), 2.0);
}
