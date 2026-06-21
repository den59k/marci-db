use marcidb::{DeleteError, MarciDB, parse_id};
use serde_json::json;
use tempfile::tempdir;

use crate::db::insert_data;

// ─────────────────────────────────────────────────────────────────────────────
// @onDelete(Restrict) должен блокировать удаление, если существуют зависимые записи
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn delete_restrict_test() {
    let schema_str = "
        model User {
            name    String
            posts   Post[]  @bind(Post.author)
        }

        model Post {
            title   String
            author  User?   @onDelete(Restrict)
        }
    ";

    let dir = tempdir().unwrap();
    let db = MarciDB::new(schema_str, dir.path().to_str().unwrap());

    let user_a = insert_data(&db, "User", json!({ "name": "Alice" }));
    let user_b = insert_data(&db, "User", json!({ "name": "Bob"   }));

    // Создаём посты, привязанные к Alice
    insert_data(&db, "Post", json!({ "title": "Alice post 1", "author": user_a }));
    insert_data(&db, "Post", json!({ "title": "Alice post 2", "author": user_a }));

    // Попытка удалить Alice, у которой есть посты с @onDelete(Restrict),
    // должна вернуть ошибку RestrictConstraints
    {
        let entity = db.get_model("User").unwrap();
        let id = parse_id(&db.schema, entity, &user_a).unwrap();
        let result = db.delete_item(entity, &id);

        assert!(
            matches!(result, Err(DeleteError::RestrictConstraints(..))),
            "Ожидалась RestrictConstraints, получено: {:?}",
            result
        );

        // Alice должна остаться в базе
        assert_eq!(db.count(db.get_model("User").unwrap()), 2);
        assert_eq!(db.count(db.get_model("Post").unwrap()), 2);
    }

    // Bob не имеет постов — его удаление должно пройти успешно
    {
        let entity = db.get_model("User").unwrap();
        let id = parse_id(&db.schema, entity, &user_b).unwrap();
        let result = db.delete_item(entity, &id);

        assert!(result.is_ok(), "Удаление Bob должно было пройти успешно");
        assert_eq!(db.count(db.get_model("User").unwrap()), 1);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// После удаления всех зависимых записей Restrict больше не мешает
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn delete_restrict_after_posts_removed_test() {
    let schema_str = "
        model User {
            name    String
            posts   Post[]  @bind(Post.author)
        }

        model Post {
            title   String
            author  User?   @onDelete(Restrict)
        }
    ";

    let dir = tempdir().unwrap();
    let db = MarciDB::new(schema_str, dir.path().to_str().unwrap());

    let user_a = insert_data(&db, "User", json!({ "name": "Alice" }));
    let post   = insert_data(&db, "Post", json!({ "title": "My post", "author": user_a }));

    // Сначала удаляем пост
    {
        let entity = db.get_model("Post").unwrap();
        let id = parse_id(&db.schema, entity, &post).unwrap();
        db.delete_item(entity, &id).unwrap();
    }

    // Теперь Alice не имеет постов — удаление должно пройти
    {
        let entity = db.get_model("User").unwrap();
        let id = parse_id(&db.schema, entity, &user_a).unwrap();
        let result = db.delete_item(entity, &id);

        assert!(result.is_ok());
        assert_eq!(db.count(db.get_model("User").unwrap()), 0);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Restrict совместно с другими типами onDelete в одной схеме
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn delete_restrict_mixed_constraints_test() {
    let schema_str = "
        model User {
            name        String
            posts       Post[]       @bind(Post.author)
            comments    Comment[]    @bind(Comment.author)
        }

        model Post {
            title   String
            author  User?   @onDelete(Restrict)
        }

        model Comment {
            text    String
            author  User?   @onDelete(SetNull)
        }
    ";

    let dir = tempdir().unwrap();
    let db = MarciDB::new(schema_str, dir.path().to_str().unwrap());

    let user_a = insert_data(&db, "User", json!({ "name": "Alice" }));

    insert_data(&db, "Post",    json!({ "title": "Post",    "author": user_a }));
    insert_data(&db, "Comment", json!({ "text":  "Comment", "author": user_a }));

    // Удаление заблокировано из-за Post (@onDelete(Restrict))
    {
        let entity = db.get_model("User").unwrap();
        let id = parse_id(&db.schema, entity, &user_a).unwrap();
        let result = db.delete_item(entity, &id);
        assert!(matches!(result, Err(DeleteError::RestrictConstraints(..))));
    }

    // Удалим пост
    {
        let post_entity = db.get_model("Post").unwrap();
        let posts = db.find_many(
            &marcidb::parse_query(&db.schema, post_entity, &json!({ "title": true })).unwrap(),
            |ctx| marcidb::decode_document(ctx).unwrap()
        );
        // Получаем первый пост и удаляем его
        let post_query = marcidb::parse_query(
            &db.schema, post_entity,
            &json!({ "$where": { "title": "Post" } })
        ).unwrap();
        let post_id_json = db.find_first(
            &post_query,
            |ctx| marcidb::decode_id(ctx.id, post_entity, &db.schema)
        ).unwrap();
        let post_id_val: serde_json::Value = std::str::FromStr::from_str(&post_id_json).unwrap();
        let post_id = parse_id(&db.schema, post_entity, &post_id_val).unwrap();
        db.delete_item(post_entity, &post_id).unwrap();
    }

    // Теперь Alice не имеет постов — удаление проходит
    // Comment.author обнуляется (@onDelete(SetNull))
    {
        let entity = db.get_model("User").unwrap();
        let id = parse_id(&db.schema, entity, &user_a).unwrap();
        let result = db.delete_item(entity, &id);
        assert!(result.is_ok());

        assert_eq!(db.count(db.get_model("User").unwrap()),   0);
        assert_eq!(db.count(db.get_model("Comment").unwrap()), 1); // комментарий остался
    }
}
