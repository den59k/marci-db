use marcidb::MarciDB;
use serde_json::json;
use tempfile::tempdir;

use crate::db::{get_aggregate, get_data, get_data_one, insert_data};

fn create_db(dir: &tempfile::TempDir) -> MarciDB {
  let schema_str = "
    model User {
      name        String
      age         UInt        @index
      rating      Int?
      weight      Double?
      city        String?
    }
  ";

  let db = MarciDB::new(schema_str, dir.path().to_str().unwrap());

  insert_data(&db, "User", json!({ "name": "Alice", "age": 30, "rating": 5, "weight": 60.5, "city": "Tokyo" }));
  insert_data(&db, "User", json!({ "name": "Bob", "age": 20, "rating": -3 }));
  insert_data(&db, "User", json!({ "name": "Carol", "age": 40, "weight": 70.0, "city": "Oslo" }));
  insert_data(&db, "User", json!({ "name": "Dave", "age": 25, "rating": 10, "city": "Lima" }));

  db
}

#[test]
fn count_test() {
  let dir = tempdir().unwrap();
  let db = create_db(&dir);

  // Without a filter — tree.len()
  {
    let resp = get_aggregate(&db, "User", json!({ "$count": true }));
    assert_eq!(resp, json!({ "count": 4 }));
  }

  // The filter is fully covered by the index range — count by index keys
  {
    let resp = get_aggregate(&db, "User", json!({ "$count": true, "$where": { "age": { "$gte": 25 } } }));
    assert_eq!(resp, json!({ "count": 3 }));
  }

  // Residual filter on an unindexed field — row scan
  {
    let resp = get_aggregate(&db, "User", json!({ "$count": true, "$where": { "city": { "$not": null } } }));
    assert_eq!(resp, json!({ "count": 3 }));
  }

  // Compound filter: index range + residual condition (only Alice and Carol have weight)
  {
    let resp = get_aggregate(&db, "User", json!({
      "$count": true, "$where": { "age": { "$gte": 25 }, "weight": { "$not": null } }
    }));
    assert_eq!(resp, json!({ "count": 2 }));
  }

  // Empty result
  {
    let resp = get_aggregate(&db, "User", json!({ "$count": true, "$where": { "age": { "$gt": 100 } } }));
    assert_eq!(resp, json!({ "count": 0 }));
  }

  // null / not-null on an indexed nullable field — difference of tree sizes, no scan
  {
    let resp = get_aggregate(&db, "User", json!({ "$count": true, "$where": { "rating": { "$not": null } } }));
    assert_eq!(resp, json!({ "count": 3 }));

    let resp = get_aggregate(&db, "User", json!({ "$count": true, "$where": { "rating": null } }));
    assert_eq!(resp, json!({ "count": 1 }));
  }
}

#[test]
fn min_max_index_fast_path_test() {
  let schema_str = "
    model Reading {
      sensor      String      @index
      celsius     Int         @index
      voltage     Double      @index
    }
  ";

  let dir = tempdir().unwrap();
  let db = MarciDB::new(schema_str, dir.path().to_str().unwrap());

  // Empty table: min/max — null, count — 0
  {
    let resp = get_aggregate(&db, "Reading", json!({ "$count": true, "$min": "celsius", "$max": "celsius" }));
    assert_eq!(resp, json!({ "count": 0, "min": null, "max": null }));
  }

  insert_data(&db, "Reading", json!({ "sensor": "north", "celsius": -25, "voltage": 3.7 }));
  insert_data(&db, "Reading", json!({ "sensor": "south", "celsius": 14, "voltage": -0.5 }));
  insert_data(&db, "Reading", json!({ "sensor": "east", "celsius": -3, "voltage": 12.25 }));

  // Int with negative values: reverse sign-flip decode from the index key
  {
    let resp = get_aggregate(&db, "Reading", json!({ "$count": true, "$min": "celsius", "$max": "celsius" }));
    assert_eq!(resp, json!({ "count": 3, "min": -25, "max": 14 }));
  }

  // Double: negatives are fully inverted
  {
    let resp = get_aggregate(&db, "Reading", json!({ "$min": "voltage", "$max": "voltage" }));
    assert_eq!(resp, json!({ "min": -0.5, "max": 12.25 }));
  }

  // String Value index: the value up to the null terminator
  {
    let resp = get_aggregate(&db, "Reading", json!({ "$min": "sensor", "$max": "sensor" }));
    assert_eq!(resp, json!({ "min": "east", "max": "south" }));
  }

  // Mixed query: min by index + sum — falls back to a scan, same result
  {
    let resp = get_aggregate(&db, "Reading", json!({ "$min": "celsius", "$sum": "celsius" }));
    assert_eq!(resp, json!({ "min": -25, "sum": -14 }));
  }
}

#[test]
fn sum_avg_test() {
  let dir = tempdir().unwrap();
  let db = create_db(&dir);

  {
    let resp = get_aggregate(&db, "User", json!({ "$sum": "age", "$avg": "age", "$count": true }));
    assert_eq!(resp, json!({ "count": 4, "sum": 115, "avg": 28.75 }));
  }

  // null values are excluded from both the sum and the average's denominator: (5 - 3 + 10) / 3
  {
    let resp = get_aggregate(&db, "User", json!({ "$sum": "rating", "$avg": "rating" }));
    assert_eq!(resp, json!({ "sum": 12, "avg": 4 }));
  }

  // Fractional type
  {
    let resp = get_aggregate(&db, "User", json!({ "$sum": "weight" }));
    assert_eq!(resp, json!({ "sum": 130.5 }));
  }

  // With a filter
  {
    let resp = get_aggregate(&db, "User", json!({ "$sum": "age", "$where": { "age": { "$lt": 30 } } }));
    assert_eq!(resp, json!({ "sum": 45 }));
  }

  // Empty set — null (as in SQL)
  {
    let resp = get_aggregate(&db, "User", json!({ "$sum": "age", "$avg": "age", "$where": { "age": { "$gt": 100 } } }));
    assert_eq!(resp, json!({ "sum": null, "avg": null }));
  }
}

#[test]
fn min_max_test() {
  let dir = tempdir().unwrap();
  let db = create_db(&dir);

  {
    let resp = get_aggregate(&db, "User", json!({ "$min": "age", "$max": "age" }));
    assert_eq!(resp, json!({ "min": 20, "max": 40 }));
  }

  // Negative values (sign-flip encoding)
  {
    let resp = get_aggregate(&db, "User", json!({ "$min": "rating", "$max": "rating" }));
    assert_eq!(resp, json!({ "min": -3, "max": 10 }));
  }

  // Strings are compared lexicographically, null does not participate
  {
    let resp = get_aggregate(&db, "User", json!({ "$min": "city", "$max": "city" }));
    assert_eq!(resp, json!({ "min": "Lima", "max": "Tokyo" }));
  }

  // With a filter
  {
    let resp = get_aggregate(&db, "User", json!({ "$max": "age", "$where": { "city": { "$not": null } } }));
    assert_eq!(resp, json!({ "max": 40 }));
  }

  // Empty set
  {
    let resp = get_aggregate(&db, "User", json!({ "$min": "age", "$where": { "age": { "$gt": 100 } } }));
    assert_eq!(resp, json!({ "min": null }));
  }
}

#[test]
fn nested_aggregate_test() {
  let schema_str = "
    model User {
      name        String
      posts       Post[]      @bind(Post.author)
    }

    model Post {
      title       String
      views       UInt
      author      User?
    }
  ";

  let dir = tempdir().unwrap();
  let db = MarciDB::new(schema_str, dir.path().to_str().unwrap());

  let alice = insert_data(&db, "User", json!({ "name": "Alice" }));
  insert_data(&db, "User", json!({ "name": "Bob" }));

  insert_data(&db, "Post", json!({ "title": "First", "views": 10, "author": { "id": alice["id"] } }));
  insert_data(&db, "Post", json!({ "title": "Second", "views": 25, "author": { "id": alice["id"] } }));
  insert_data(&db, "Post", json!({ "title": "Third", "views": 5, "author": { "id": alice["id"] } }));

  // count over a relation: count of index keys, Bob has zero children
  {
    let resp = get_data(&db, "User", json!({ "name": true, "posts": { "$count": true } }));
    assert_eq!(resp, json!([
      { "name": "Alice", "posts": { "count": 3 } },
      { "name": "Bob", "posts": { "count": 0 } }
    ]));
  }

  // Several aggregates over a relation
  {
    let resp = get_data_one(&db, "User", json!({
      "posts": { "$count": true, "$sum": "views", "$max": "views", "$min": "title" },
      "$where": { "name": "Alice" }
    }));
    assert_eq!(resp, json!({
      "posts": { "count": 3, "sum": 40, "max": 25, "min": "First" }
    }));
  }

  // Aggregate with $where over children (residual filter)
  {
    let resp = get_data_one(&db, "User", json!({
      "posts": { "$count": true, "$sum": "views", "$where": { "views": { "$gte": 10 } } },
      "$where": { "name": "Alice" }
    }));
    assert_eq!(resp, json!({ "posts": { "count": 2, "sum": 35 } }));
  }

  // Empty relation: aggregates over zero rows
  {
    let resp = get_data_one(&db, "User", json!({
      "posts": { "$count": true, "$max": "views" },
      "$where": { "name": "Bob" }
    }));
    assert_eq!(resp, json!({ "posts": { "count": 0, "max": null } }));
  }
}

#[test]
fn nested_aggregate_struct_test() {
  let schema_str = "
    model Project {
      name        String
      tasks       Task[]
    }

    struct Task {
      title       String
      hours       UInt
    }
  ";

  let dir = tempdir().unwrap();
  let db = MarciDB::new(schema_str, dir.path().to_str().unwrap());

  insert_data(&db, "Project", json!({
    "name": "Apollo",
    "tasks": [
      { "title": "Design", "hours": 8 },
      { "title": "Build", "hours": 20 }
    ]
  }));

  // Plain select for cross-checking
  {
    let resp = get_data_one(&db, "Project", json!({ "name": true, "tasks": { "title": true } }));
    assert_eq!(resp, json!({ "name": "Apollo", "tasks": [ { "title": "Design" }, { "title": "Build" } ] }));
  }

  // Struct children live under the parent's prefix in the main tree
  let resp = get_data_one(&db, "Project", json!({
    "name": true,
    "tasks": { "$count": true, "$sum": "hours" }
  }));
  assert_eq!(resp, json!({ "name": "Apollo", "tasks": { "count": 2, "sum": 28 } }));
}
