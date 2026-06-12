use marcidb::MarciDB;
use serde_json::json;
use tempfile::tempdir;

use crate::db::{get_data, get_data_one, insert_data};

fn create_db(dir: &tempfile::TempDir) -> MarciDB {
  let schema_str = "
    model User {
      name        String
      age         UInt        @index
      rating      Int?        @index
      city        String?
      posts       Post[]      @bind(Post.author)
    }

    model Post {
      title       String
      author      User?
    }
  ";

  let db = MarciDB::new(schema_str, dir.path().to_str().unwrap());

  insert_data(&db, "User", json!({ "name": "Alice", "age": 30, "rating": 5, "city": "Tokyo" }));
  insert_data(&db, "User", json!({ "name": "Bob", "age": 20, "rating": -3 }));
  insert_data(&db, "User", json!({ "name": "Carol", "age": 40, "city": "Oslo" }));
  insert_data(&db, "User", json!({ "name": "Dave", "age": 25, "rating": 10, "city": "Lima" }));

  db
}

#[test]
fn limit_skip_test() {
  let dir = tempdir().unwrap();
  let db = create_db(&dir);

  {
    let resp = get_data(&db, "User", json!({ "name": true, "$limit": 2 }));
    assert_eq!(resp, json!([ { "name": "Alice" }, { "name": "Bob" } ]));
  }

  {
    let resp = get_data(&db, "User", json!({ "name": true, "$skip": 1, "$limit": 2 }));
    assert_eq!(resp, json!([ { "name": "Bob" }, { "name": "Carol" } ]));
  }

  // limit/skip применяются после фильтра. Без $order порядок задаёт скан:
  // здесь это индексный диапазон по age (Dave 25, Alice 30, Carol 40)
  {
    let resp = get_data(&db, "User", json!({ "name": true, "$where": { "age": { "$gte": 25 } }, "$skip": 1, "$limit": 1 }));
    assert_eq!(resp, json!([ { "name": "Alice" } ]));
  }

  {
    let resp = get_data(&db, "User", json!({ "name": true, "$limit": 0 }));
    assert_eq!(resp, json!([]));
  }
}

#[test]
fn order_by_id_test() {
  let dir = tempdir().unwrap();
  let db = create_db(&dir);

  {
    let resp = get_data(&db, "User", json!({ "name": true, "$order": { "id": "desc" }, "$limit": 2 }));
    assert_eq!(resp, json!([ { "name": "Dave" }, { "name": "Carol" } ]));
  }

  // desc + фильтр без индексного диапазона
  {
    let resp = get_data(&db, "User", json!({ "name": true, "$order": { "id": "desc" }, "$where": { "city": { "$not": null } } }));
    assert_eq!(resp, json!([ { "name": "Dave" }, { "name": "Carol" }, { "name": "Alice" } ]));
  }
}

#[test]
fn order_by_indexed_field_test() {
  let dir = tempdir().unwrap();
  let db = create_db(&dir);

  // Скан индекса сортировки
  {
    let resp = get_data(&db, "User", json!({ "name": true, "age": true, "$order": { "age": "asc" } }));
    assert_eq!(resp, json!([
      { "name": "Bob", "age": 20 },
      { "name": "Dave", "age": 25 },
      { "name": "Alice", "age": 30 },
      { "name": "Carol", "age": 40 }
    ]));
  }

  {
    let resp = get_data(&db, "User", json!({ "name": true, "$order": { "age": "desc" }, "$limit": 2 }));
    assert_eq!(resp, json!([ { "name": "Carol" }, { "name": "Alice" } ]));
  }

  // Residual-фильтр по другому полю при скане индекса сортировки
  {
    let resp = get_data(&db, "User", json!({
      "name": true, "$order": { "age": "asc" }, "$where": { "city": { "$not": null } }, "$limit": 2
    }));
    assert_eq!(resp, json!([ { "name": "Dave" }, { "name": "Alice" } ]));
  }

  // Диапазон $where по тому же полю, что и сортировка — переиспользуем диапазон
  {
    let resp = get_data(&db, "User", json!({
      "name": true, "$where": { "age": { "$gte": 25 } }, "$order": { "age": "desc" }
    }));
    assert_eq!(resp, json!([ { "name": "Carol" }, { "name": "Alice" }, { "name": "Dave" } ]));
  }
}

#[test]
fn order_by_nullable_field_test() {
  let dir = tempdir().unwrap();
  let db = create_db(&dir);

  // Nullable поле с индексом: sparse-индекс не используется, сортировка в памяти,
  // null-строки не теряются. asc — null в конце
  {
    let resp = get_data(&db, "User", json!({ "name": true, "$order": { "rating": "asc" } }));
    assert_eq!(resp, json!([
      { "name": "Bob" },
      { "name": "Alice" },
      { "name": "Dave" },
      { "name": "Carol" }
    ]));
  }

  // desc — null в начале
  {
    let resp = get_data(&db, "User", json!({ "name": true, "$order": { "rating": "desc" } }));
    assert_eq!(resp, json!([
      { "name": "Carol" },
      { "name": "Dave" },
      { "name": "Alice" },
      { "name": "Bob" }
    ]));
  }
}

#[test]
fn order_by_unindexed_field_test() {
  let dir = tempdir().unwrap();
  let db = create_db(&dir);

  // Строковое поле без индекса: сортировка в памяти, null в конце
  {
    let resp = get_data(&db, "User", json!({ "name": true, "$order": { "city": "asc" }, "$limit": 3 }));
    assert_eq!(resp, json!([ { "name": "Dave" }, { "name": "Carol" }, { "name": "Alice" } ]));
  }

  {
    let resp = get_data(&db, "User", json!({ "name": true, "$order": { "name": "desc" } }));
    assert_eq!(resp, json!([ { "name": "Dave" }, { "name": "Carol" }, { "name": "Bob" }, { "name": "Alice" } ]));
  }
}

#[test]
fn order_in_includes_test() {
  let dir = tempdir().unwrap();
  let db = create_db(&dir);

  let user_id = get_data_one(&db, "User", json!({ "id": true, "$where": { "name": "Alice" } }));

  for title in ["First", "Second", "Third"] {
    insert_data(&db, "Post", json!({ "title": title, "author": { "id": user_id["id"] } }));
  }

  {
    let resp = get_data_one(&db, "User", json!({
      "name": true,
      "posts": { "title": true, "$order": { "id": "desc" }, "$limit": 2 },
      "$where": { "name": "Alice" }
    }));
    assert_eq!(resp, json!({
      "name": "Alice",
      "posts": [ { "title": "Third" }, { "title": "Second" } ]
    }));
  }

  // Сортировка по полю внутри include — в памяти
  {
    let resp = get_data_one(&db, "User", json!({
      "name": true,
      "posts": { "title": true, "$order": { "title": "asc" }, "$limit": 2 },
      "$where": { "name": "Alice" }
    }));
    assert_eq!(resp, json!({
      "name": "Alice",
      "posts": [ { "title": "First" }, { "title": "Second" } ]
    }));
  }
}

#[test]
fn find_first_order_test() {
  let dir = tempdir().unwrap();
  let db = create_db(&dir);

  // findFirst + desc по индексу = максимум
  {
    let resp = get_data_one(&db, "User", json!({ "name": true, "age": true, "$order": { "age": "desc" } }));
    assert_eq!(resp, json!({ "name": "Carol", "age": 40 }));
  }

  // findFirst + сортировка в памяти
  {
    let resp = get_data_one(&db, "User", json!({ "name": true, "$order": { "rating": "desc" } }));
    assert_eq!(resp, json!({ "name": "Carol" }));
  }

  // findFirst + $skip
  {
    let resp = get_data_one(&db, "User", json!({ "name": true, "$order": { "age": "asc" }, "$skip": 1 }));
    assert_eq!(resp, json!({ "name": "Dave" }));
  }
}
