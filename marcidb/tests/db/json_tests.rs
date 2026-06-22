use marcidb::{MarciDB, parse_query, query_binary_many, shape_supported};
use serde_json::{Value, json};
use tempfile::tempdir;

use crate::db::{get_data, get_data_one, insert_data, update_data};

fn create_db(dir: &tempfile::TempDir) -> MarciDB {
  let schema_str = "
    model Doc {
      name        String
      data        Json
      meta        Json?
    }
  ";
  MarciDB::new(schema_str, dir.path().to_str().unwrap())
}

/// Insert a spread of JSON shapes (object, array, nested, every scalar, an explicit nested null) and read
/// them back through the full storage path. Object comparison is order-independent (serde_json `Value`),
/// so the canonical sorted-key encoding doesn't affect equality.
#[test]
fn json_round_trip_through_db() {
  let dir = tempdir().unwrap();
  let db = create_db(&dir);

  insert_data(&db, "Doc", json!({
    "name": "obj",
    "data": { "city": "Tokyo", "tags": ["a", "b"], "active": true, "score": 4.5, "note": null },
    "meta": { "v": 1 }
  }));
  insert_data(&db, "Doc", json!({
    "name": "arr",
    "data": [1, "two", false, null, { "nested": [3, 4] }]
  }));
  insert_data(&db, "Doc", json!({
    "name": "scalar",
    "data": "just a string"
  }));

  let resp = get_data(&db, "Doc", json!({ "name": true, "data": true, "meta": true }));
  assert_eq!(resp, json!([
    { "name": "obj",    "data": { "city": "Tokyo", "tags": ["a", "b"], "active": true, "score": 4.5, "note": null }, "meta": { "v": 1 } },
    // An omitted nullable Json field reads back as null (offset 0), like every other type.
    { "name": "arr",    "data": [1, "two", false, null, { "nested": [3, 4] }], "meta": null },
    { "name": "scalar", "data": "just a string", "meta": null },
  ]));
}

/// Selecting only the JSON field returns just that field, and a JSON field can be overwritten by an update
/// (it is variable-length, so the row is rewritten — same path as `String`).
#[test]
fn json_select_only_and_update() {
  let dir = tempdir().unwrap();
  let db = create_db(&dir);

  let id = insert_data(&db, "Doc", json!({ "name": "x", "data": { "a": 1 } }));

  let only = get_data_one(&db, "Doc", json!({ "data": true, "$where": { "name": "x" } }));
  assert_eq!(only, json!({ "data": { "a": 1 } }));

  update_data(&db, "Doc", &id, json!({ "data": { "a": 2, "b": [true, false] } }));
  let after = get_data_one(&db, "Doc", json!({ "name": true, "data": true, "$where": { "name": "x" } }));
  assert_eq!(after, json!({ "name": "x", "data": { "a": 2, "b": [true, false] } }));
}

/// JSON survives reopening the database. `MarciDB::open` reconstructs the schema from the stored snapshot,
/// so this also exercises the snapshot round-trip of the `Json` type (serialize/parse "Json").
#[test]
fn json_persists_across_reopen() {
  let dir = tempdir().unwrap();
  let payload = json!({ "k": [1, 2, { "deep": "value" }], "flag": true });

  {
    let db = create_db(&dir);
    insert_data(&db, "Doc", json!({ "name": "persist", "data": payload }));
  }

  let db = MarciDB::open(dir.path().to_str().unwrap());
  let resp = get_data_one(&db, "Doc", json!({ "data": true, "$where": { "name": "persist" } }));
  assert_eq!(resp, json!({ "data": { "k": [1, 2, { "deep": "value" }], "flag": true } }));
}

/// Phase 3: a query selecting a Json field is now binary-eligible (it was gated out in Phase 1), and the
/// JSON field rides the wire as decoded JSON text. We decode the buffer the way the TS client does and
/// compare to the JSON read path — the two transports must agree.
#[test]
fn json_rides_the_binary_transport() {
  let dir = tempdir().unwrap();
  let db = create_db(&dir);

  insert_data(&db, "Doc", json!({ "name": "a", "data": { "x": [1, 2], "y": "hi" } }));
  insert_data(&db, "Doc", json!({ "name": "b", "data": [true, null, 3.5] }));

  let entity = db.get_model("Doc").unwrap();
  let select = json!({ "id": true, "name": true, "data": true });
  let query = parse_query(&db.schema, entity, &select).unwrap();

  // The Phase 1 gate is lifted: the shape is binary-decodable.
  assert!(shape_supported(&query));

  let bytes = query_binary_many(&db, &query).unwrap();
  assert_eq!(decode_rows(&bytes), vec![
    json!({ "id": 1, "name": "a", "data": { "x": [1, 2], "y": "hi" } }),
    json!({ "id": 2, "name": "b", "data": [true, null, 3.5] }),
  ]);
}

// ── Minimal reader for the binary result buffer, mirroring the TS client decoder (all ints little-endian).
// Shape: [u8 version][u32 count]{ id:u64 | name: tag+str | data: tag+str }. A Json field is a length-prefixed
// UTF-8 string that the client JSON.parses — here we do the same with serde_json::from_str.

fn read_u32(buf: &[u8], o: &mut usize) -> usize {
  let v = u32::from_le_bytes(buf[*o..*o + 4].try_into().unwrap()) as usize;
  *o += 4;
  v
}

fn read_str(buf: &[u8], o: &mut usize) -> String {
  let len = read_u32(buf, o);
  let s = String::from_utf8(buf[*o..*o + len].to_vec()).unwrap();
  *o += len;
  s
}

fn decode_rows(buf: &[u8]) -> Vec<Value> {
  let mut o = 0;
  assert_eq!(buf[o], 1, "binary version");
  o += 1;
  let count = read_u32(buf, &mut o);
  let mut out = Vec::with_capacity(count);
  for _ in 0..count {
    let id = u64::from_le_bytes(buf[o..o + 8].try_into().unwrap()); // key field — no presence tag
    o += 8;
    let name = read_tagged_str(buf, &mut o).map_or(Value::Null, Value::String);
    let data = read_tagged_str(buf, &mut o)
      .map_or(Value::Null, |t| serde_json::from_str(&t).expect("json field is valid JSON text"));
    out.push(json!({ "id": id, "name": name, "data": data }));
  }
  out
}

/// A body field: 1-byte presence tag, then (if present) a length-prefixed UTF-8 string.
fn read_tagged_str(buf: &[u8], o: &mut usize) -> Option<String> {
  let present = buf[*o] != 0;
  *o += 1;
  present.then(|| read_str(buf, o))
}
