// tests/db/persistence_tests.rs

use marcidb::MarciDB;
use serde_json::json;
use tempfile::TempDir;

use crate::db::{get_data, insert_data};

#[test]
fn persistence_across_restarts() {
    // ---- Этап 1: Первый запуск ----
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().to_str().unwrap();
    let schema_str = "
        model User {
            name    String
            age     Int
        }
    ";

    // Создаём БД, наполняем данными
    {
        let db = MarciDB::new(schema_str, &db_path);

        // Вставляем несколько пользователей
        let alice = insert_data(&db, "User", json!({"name": "Alice", "age": 30}));
        let bob   = insert_data(&db, "User", json!({"name": "Bob", "age": 25}));

        // Проверяем, что данные на месте (опционально)
        let users = get_data(&db, "User", json!({"name": true, "age": true}));
        assert_eq!(users, json!([
            {"name": "Alice", "age": 30},
            {"name": "Bob",   "age": 25}
        ]));
        // `db` выходит из области видимости → соединение закрыто, файлы на диске
    }

    // ---- Этап 2: Перезапуск (новый экземпляр MarciDB) ----
    {
        let db = MarciDB::new(schema_str, &db_path);

        // Читаем заново – данные должны сохраниться
        let users = get_data(&db, "User", json!({"name": true, "age": true}));
        assert_eq!(users, json!([
            {"name": "Alice", "age": 30},
            {"name": "Bob",   "age": 25}
        ]));
    }
}