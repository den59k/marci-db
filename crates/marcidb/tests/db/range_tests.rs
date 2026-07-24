use marcidb::MarciDB;
use serde_json::json;
use tempfile::tempdir;

use crate::db::{get_aggregate, get_data, insert_data};

/// `score` is indexed (queries go through an index range), `plain` holds the same value without an
/// index (queries fall back to a residual row scan). Every range assertion is made against both so
/// the planned access path is checked against the straightforward evaluation.
fn create_db(dir: &tempfile::TempDir) -> MarciDB {
  let schema_str = "
    model Item {
      name    String
      score   UInt    @index
      plain   UInt
    }
  ";

  let db = MarciDB::new(schema_str, dir.path().to_str().unwrap());

  // Inserted in ascending order, so id order matches score order and both paths return the same sequence.
  for score in [10u64, 20, 29, 30, 40] {
    insert_data(&db, "Item", json!({ "name": format!("i{}", score), "score": score, "plain": score }));
  }

  db
}

/// Runs the same operator against the indexed and the unindexed field and asserts both return `expected`.
fn assert_both(db: &MarciDB, cond: serde_json::Value, expected: &[&str]) {
  let expected: serde_json::Value = expected.iter().map(|n| json!({ "name": n })).collect();

  for field in ["score", "plain"] {
    let data = get_data(db, "Item", json!({
      "name": true,
      "$where": { field: cond }
    }));
    assert_eq!(data, expected, "mismatch on field `{}` for condition {}", field, cond);
  }
}

/// Two bounds on one field. On the indexed field these fuse into a single bounded index scan; on the
/// unindexed one they are two residual predicates. Both must agree.
#[test]
fn two_sided_range_test() {
  let dir = tempdir().unwrap();
  let db = create_db(&dir);

  // Every combination of an inclusive/exclusive lower and upper bound
  assert_both(&db, json!({ "$gte": 20, "$lte": 30 }), &["i20", "i29", "i30"]);
  assert_both(&db, json!({ "$gte": 20, "$lt": 30 }), &["i20", "i29"]);
  assert_both(&db, json!({ "$gt": 20, "$lte": 30 }), &["i29", "i30"]);
  assert_both(&db, json!({ "$gt": 20, "$lt": 30 }), &["i29"]);

  // Bounds that don't land on a stored value
  assert_both(&db, json!({ "$gte": 21, "$lte": 29 }), &["i29"]);

  // Degenerate range — a single value
  assert_both(&db, json!({ "$gte": 30, "$lte": 30 }), &["i30"]);

  // Crossed bounds match nothing
  assert_both(&db, json!({ "$gte": 30, "$lte": 20 }), &[]);

  // Covering everything, and covering nothing
  assert_both(&db, json!({ "$gte": 0, "$lte": 100 }), &["i10", "i20", "i29", "i30", "i40"]);
  assert_both(&db, json!({ "$gte": 41, "$lte": 100 }), &[]);

  // The operators need not be a range pair: a third condition narrows the same field further
  assert_both(&db, json!({ "$gte": 20, "$lte": 30, "$ne": 29 }), &["i20", "i30"]);
  assert_both(&db, json!({ "$gte": 20, "$notIn": [29, 30] }), &["i20", "i40"]);
}

/// A fused range is a plain `IndexRange`, so ordering reuses it rather than post-sorting.
#[test]
fn two_sided_range_with_order_test() {
  let dir = tempdir().unwrap();
  let db = create_db(&dir);

  let data = get_data(&db, "Item", json!({
    "name": true,
    "$where": { "score": { "$gte": 20, "$lte": 30 } },
    "$order": { "score": "desc" }
  }));
  assert_eq!(data, json!([{ "name": "i30" }, { "name": "i29" }, { "name": "i20" }]));

  let data = get_data(&db, "Item", json!({
    "name": true,
    "$where": { "score": { "$gte": 20, "$lte": 30 } },
    "$order": { "score": "asc" },
    "$limit": 2
  }));
  assert_eq!(data, json!([{ "name": "i20" }, { "name": "i29" }]));

  // Same range, ordered by an unindexed field — falls back to post-sorting the filtered rows
  let data = get_data(&db, "Item", json!({
    "name": true,
    "$where": { "score": { "$gte": 20, "$lte": 30 } },
    "$order": { "plain": "desc" }
  }));
  assert_eq!(data, json!([{ "name": "i30" }, { "name": "i29" }, { "name": "i20" }]));
}

#[test]
fn two_sided_range_count_test() {
  let dir = tempdir().unwrap();
  let db = create_db(&dir);

  for field in ["score", "plain"] {
    let resp = get_aggregate(&db, "Item", json!({ "$count": true, "$where": { field: { "$gte": 20, "$lte": 30 } } }));
    assert_eq!(resp, json!({ "count": 3 }), "field {}", field);

    let resp = get_aggregate(&db, "Item", json!({ "$count": true, "$where": { field: { "$gte": 30, "$lte": 30 } } }));
    assert_eq!(resp, json!({ "count": 1 }), "field {}", field);

    let resp = get_aggregate(&db, "Item", json!({ "$count": true, "$where": { field: { "$gt": 100, "$lt": 200 } } }));
    assert_eq!(resp, json!({ "count": 0 }), "field {}", field);
  }
}

/// The explicit `$and` spelling stays available and must agree with the compact form.
#[test]
fn two_sided_range_via_and_test() {
  let dir = tempdir().unwrap();
  let db = create_db(&dir);

  let data = get_data(&db, "Item", json!({
    "name": true,
    "$where": { "$and": [ { "score": { "$gte": 20 } }, { "score": { "$lt": 30 } } ] }
  }));
  assert_eq!(data, json!([{ "name": "i20" }, { "name": "i29" }]));
}

/// Index keys are `encoded_value ++ id` and the range end is exclusive, so an inclusive upper bound
/// must step past every key sharing the boundary value. These cases all sit exactly on a boundary,
/// where an off-by-one upper bound silently drops rows rather than erroring.
#[test]
fn upper_bound_boundary_test() {
  let dir = tempdir().unwrap();
  let db = create_db(&dir);

  // $lte must keep rows equal to the bound
  assert_both(&db, json!({ "$lte": 30 }), &["i10", "i20", "i29", "i30"]);

  // $lt must keep rows just under the bound (29 is the value an off-by-one end would drop)
  assert_both(&db, json!({ "$lt": 30 }), &["i10", "i20", "i29"]);

  // A fused two-sided range uses the same upper-bound machinery
  assert_both(&db, json!({ "$gte": 10, "$lte": 29 }), &["i10", "i20", "i29"]);

  // Lower bounds, for symmetry
  assert_both(&db, json!({ "$gte": 30 }), &["i30", "i40"]);
  assert_both(&db, json!({ "$gt": 29 }), &["i30", "i40"]);
}

/// `$lt: 0` on an unsigned field: the encoding is all-zero, which has no representable predecessor.
/// The upper bound must stay an empty range instead of degrading into an unbounded one.
#[test]
fn lt_zero_test() {
  let dir = tempdir().unwrap();
  let db = create_db(&dir);

  assert_both(&db, json!({ "$lt": 0 }), &[]);

  // A count over the indexed field reads the range directly, with no residual filter to correct it
  let resp = get_aggregate(&db, "Item", json!({ "$count": true, "$where": { "score": { "$lt": 0 } } }));
  assert_eq!(resp, json!({ "count": 0 }));

  let resp = get_aggregate(&db, "Item", json!({ "$count": true, "$where": { "score": { "$lte": 30 } } }));
  assert_eq!(resp, json!({ "count": 4 }));
}
