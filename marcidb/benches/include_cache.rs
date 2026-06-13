// Бенчмарк include-кэша: вложенный select связанной записи, разделяемой между строками.
// Запуск: cargo bench -p marcidb

use std::time::Instant;

use marcidb::{MarciDB, decode_document, parse_insert, parse_query};
use serde_json::{Value, json};

const SCHEMA: &str = "
  model User {
    name        String
    posts       Post[]      @bind(Post.author)
  }

  model Post {
    title       String
    author      User?
  }
";

fn insert(db: &MarciDB, model: &str, data: Value) {
  let entity = db.get_model(model).unwrap();
  let op = parse_insert(&db.schema, entity, &data).unwrap();
  db.insert_item(entity, &op).unwrap();
}

/// Создаёт БД: posts постов, равномерно распределённых по users авторам
fn create_db(dir: &tempfile::TempDir, users: usize, posts: usize) -> MarciDB {
  let db = MarciDB::new(SCHEMA, dir.path().to_str().unwrap());

  for i in 0..users {
    insert(&db, "User", json!({ "name": format!("User {}", i) }));
  }
  for i in 0..posts {
    // id пользователей — счётчик, начинается с 1
    let author_id = (i % users) + 1;
    insert(&db, "Post", json!({ "title": format!("Post {}", i), "author": { "id": author_id } }));
  }

  db
}

fn bench(name: &str, db: &MarciDB, expected: usize) {
  let entity = db.get_model("Post").unwrap();
  let query = parse_query(&db.schema, entity, &json!({
    "title": true,
    "author": { "id": true, "name": true }
  })).unwrap();

  // Прогрев
  for _ in 0..3 {
    let items = db.find_many(&query, |ctx| decode_document(ctx).unwrap()).unwrap();
    assert_eq!(items.len(), expected);
  }

  const ITERS: u32 = 30;
  let start = Instant::now();
  for _ in 0..ITERS {
    let items = db.find_many(&query, |ctx| decode_document(ctx).unwrap()).unwrap();
    assert_eq!(items.len(), expected);
  }
  let elapsed = start.elapsed() / ITERS;
  println!("{:<40} {:>12.3?}/iter", name, elapsed);
}

fn main() {
  const POSTS: usize = 10_000;

  // Высокое разделение: 100 авторов на 10000 постов — кэш должен сильно помочь
  {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(&dir, 100, POSTS);
    bench("shared authors (100 users, 10k posts)", &db, POSTS);
  }

  // Без разделения: у каждого поста свой автор — худший случай для кэша
  {
    let dir = tempfile::tempdir().unwrap();
    let db = create_db(&dir, POSTS, POSTS);
    bench("unique authors (10k users, 10k posts)", &db, POSTS);
  }
}
