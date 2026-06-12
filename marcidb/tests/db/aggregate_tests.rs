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

  // null / not-null по индексированному nullable-полю — разность размеров деревьев, без скана
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

  // Пустая таблица: min/max — null, count — 0
  {
    let resp = get_aggregate(&db, "Reading", json!({ "$count": true, "$min": "celsius", "$max": "celsius" }));
    assert_eq!(resp, json!({ "count": 0, "min": null, "max": null }));
  }

  insert_data(&db, "Reading", json!({ "sensor": "north", "celsius": -25, "voltage": 3.7 }));
  insert_data(&db, "Reading", json!({ "sensor": "south", "celsius": 14, "voltage": -0.5 }));
  insert_data(&db, "Reading", json!({ "sensor": "east", "celsius": -3, "voltage": 12.25 }));

  // Int с отрицательными значениями: обратный sign-flip декод из ключа индекса
  {
    let resp = get_aggregate(&db, "Reading", json!({ "$count": true, "$min": "celsius", "$max": "celsius" }));
    assert_eq!(resp, json!({ "count": 3, "min": -25, "max": 14 }));
  }

  // Double: отрицательные инвертируются полностью
  {
    let resp = get_aggregate(&db, "Reading", json!({ "$min": "voltage", "$max": "voltage" }));
    assert_eq!(resp, json!({ "min": -0.5, "max": 12.25 }));
  }

  // Строковый Value-индекс: значение до нуль-терминатора
  {
    let resp = get_aggregate(&db, "Reading", json!({ "$min": "sensor", "$max": "sensor" }));
    assert_eq!(resp, json!({ "min": "east", "max": "south" }));
  }

  // Смешанный запрос: min по индексу + sum — уходит в скан, результат тот же
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

  // count по связи: подсчёт ключей индекса, у Bob — ноль детей
  {
    let resp = get_data(&db, "User", json!({ "name": true, "posts": { "$count": true } }));
    assert_eq!(resp, json!([
      { "name": "Alice", "posts": { "count": 3 } },
      { "name": "Bob", "posts": { "count": 0 } }
    ]));
  }

  // Несколько агрегатов по связи
  {
    let resp = get_data_one(&db, "User", json!({
      "posts": { "$count": true, "$sum": "views", "$max": "views", "$min": "title" },
      "$where": { "name": "Alice" }
    }));
    assert_eq!(resp, json!({
      "posts": { "count": 3, "sum": 40, "max": 25, "min": "First" }
    }));
  }

  // Агрегат с $where по детям (residual-фильтр)
  {
    let resp = get_data_one(&db, "User", json!({
      "posts": { "$count": true, "$sum": "views", "$where": { "views": { "$gte": 10 } } },
      "$where": { "name": "Alice" }
    }));
    assert_eq!(resp, json!({ "posts": { "count": 2, "sum": 35 } }));
  }

  // Пустая связь: агрегаты по нулю строк
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

  // Обычный select для сверки
  {
    let resp = get_data_one(&db, "Project", json!({ "name": true, "tasks": { "title": true } }));
    assert_eq!(resp, json!({ "name": "Apollo", "tasks": [ { "title": "Design" }, { "title": "Build" } ] }));
  }

  // Struct-дети живут под префиксом родителя в основном дереве
  let resp = get_data_one(&db, "Project", json!({
    "name": true,
    "tasks": { "$count": true, "$sum": "hours" }
  }));
  assert_eq!(resp, json!({ "name": "Apollo", "tasks": { "count": 2, "sum": 28 } }));
}
