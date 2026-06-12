use marcidb::MarciDB;
use serde_json::json;
use tempfile::tempdir;

use crate::db::{get_aggregate, insert_data};

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

  // Без фильтра — tree.len()
  {
    let resp = get_aggregate(&db, "User", json!({ "$count": true }));
    assert_eq!(resp, json!({ "count": 4 }));
  }

  // Фильтр полностью покрыт индексным диапазоном — подсчёт по ключам индекса
  {
    let resp = get_aggregate(&db, "User", json!({ "$count": true, "$where": { "age": { "$gte": 25 } } }));
    assert_eq!(resp, json!({ "count": 3 }));
  }

  // Residual-фильтр по неиндексированному полю — скан строк
  {
    let resp = get_aggregate(&db, "User", json!({ "$count": true, "$where": { "city": { "$not": null } } }));
    assert_eq!(resp, json!({ "count": 3 }));
  }

  // Составной фильтр: индексный диапазон + residual-условие (weight есть только у Alice и Carol)
  {
    let resp = get_aggregate(&db, "User", json!({
      "$count": true, "$where": { "age": { "$gte": 25 }, "weight": { "$not": null } }
    }));
    assert_eq!(resp, json!({ "count": 2 }));
  }

  // Пустой результат
  {
    let resp = get_aggregate(&db, "User", json!({ "$count": true, "$where": { "age": { "$gt": 100 } } }));
    assert_eq!(resp, json!({ "count": 0 }));
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

  // null-значения не входят ни в сумму, ни в знаменатель среднего: (5 - 3 + 10) / 3
  {
    let resp = get_aggregate(&db, "User", json!({ "$sum": "rating", "$avg": "rating" }));
    assert_eq!(resp, json!({ "sum": 12, "avg": 4 }));
  }

  // Дробный тип
  {
    let resp = get_aggregate(&db, "User", json!({ "$sum": "weight" }));
    assert_eq!(resp, json!({ "sum": 130.5 }));
  }

  // С фильтром
  {
    let resp = get_aggregate(&db, "User", json!({ "$sum": "age", "$where": { "age": { "$lt": 30 } } }));
    assert_eq!(resp, json!({ "sum": 45 }));
  }

  // Пустое множество — null (как в SQL)
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

  // Отрицательные значения (sign-flip кодировка)
  {
    let resp = get_aggregate(&db, "User", json!({ "$min": "rating", "$max": "rating" }));
    assert_eq!(resp, json!({ "min": -3, "max": 10 }));
  }

  // Строки сравниваются лексикографически, null не участвует
  {
    let resp = get_aggregate(&db, "User", json!({ "$min": "city", "$max": "city" }));
    assert_eq!(resp, json!({ "min": "Lima", "max": "Tokyo" }));
  }

  // С фильтром
  {
    let resp = get_aggregate(&db, "User", json!({ "$max": "age", "$where": { "city": { "$not": null } } }));
    assert_eq!(resp, json!({ "max": 40 }));
  }

  // Пустое множество
  {
    let resp = get_aggregate(&db, "User", json!({ "$min": "age", "$where": { "age": { "$gt": 100 } } }));
    assert_eq!(resp, json!({ "min": null }));
  }
}
