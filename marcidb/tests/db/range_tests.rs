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

#[test]
fn between_test() {
  let dir = tempdir().unwrap();
  let db = create_db(&dir);

  // Both bounds are inclusive
  assert_both(&db, json!({ "$between": [20, 30] }), &["i20", "i29", "i30"]);

  // Bounds that don't land on a stored value
  assert_both(&db, json!({ "$between": [21, 29] }), &["i29"]);

  // Degenerate range — a single value
  assert_both(&db, json!({ "$between": [30, 30] }), &["i30"]);

  // Reversed bounds match nothing (same as a crossed $gte + $lte)
  assert_both(&db, json!({ "$between": [30, 20] }), &[]);

  // Range covering everything, and one covering nothing
  assert_both(&db, json!({ "$between": [0, 100] }), &["i10", "i20", "i29", "i30", "i40"]);
  assert_both(&db, json!({ "$between": [41, 100] }), &[]);
}

/// `$between` produces a plain `IndexRange`, so ordering reuses the same range rather than post-sorting.
#[test]
fn between_with_order_test() {
  let dir = tempdir().unwrap();
  let db = create_db(&dir);

  let data = get_data(&db, "Item", json!({
    "name": true,
    "$where": { "score": { "$between": [20, 30] } },
    "$order": { "score": "desc" }
  }));
  assert_eq!(data, json!([{ "name": "i30" }, { "name": "i29" }, { "name": "i20" }]));

  let data = get_data(&db, "Item", json!({
    "name": true,
    "$where": { "score": { "$between": [20, 30] } },
    "$order": { "score": "asc" },
    "$limit": 2
  }));
  assert_eq!(data, json!([{ "name": "i20" }, { "name": "i29" }]));

  // Same range, ordered by an unindexed field — falls back to post-sorting the filtered rows
  let data = get_data(&db, "Item", json!({
    "name": true,
    "$where": { "score": { "$between": [20, 30] } },
    "$order": { "plain": "desc" }
  }));
  assert_eq!(data, json!([{ "name": "i30" }, { "name": "i29" }, { "name": "i20" }]));
}

#[test]
fn between_count_test() {
  let dir = tempdir().unwrap();
  let db = create_db(&dir);

  // On the indexed field a lone $between is fully covered by the range, so rows are counted straight
  // off the index keys — the bounds have to be exact, there is no residual recheck to fall back on.
  let resp = get_aggregate(&db, "Item", json!({ "$count": true, "$where": { "score": { "$between": [20, 30] } } }));
  assert_eq!(resp, json!({ "count": 3 }));

  let resp = get_aggregate(&db, "Item", json!({ "$count": true, "$where": { "plain": { "$between": [20, 30] } } }));
  assert_eq!(resp, json!({ "count": 3 }));

  let resp = get_aggregate(&db, "Item", json!({ "$count": true, "$where": { "score": { "$between": [30, 30] } } }));
  assert_eq!(resp, json!({ "count": 1 }));
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

  // $between's upper bound is the same machinery
  assert_both(&db, json!({ "$between": [10, 29] }), &["i10", "i20", "i29"]);

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
