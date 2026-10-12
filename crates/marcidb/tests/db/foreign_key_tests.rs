//! A relation points at a row that exists. Before 0.14.2 a link was written into the index trees
//! without a look at the target model, so `{ author: { id: 999 } }` was accepted and read back as null;
//! every path that links rows (insert, `$connect`, `$set` of links, `@list`, a ref in a composite key)
//! goes through `write_ref_indexes`, which is where the check lives.
use marcidb::{InsertError, MarciDB, UpdateError, parse_insert};
use serde_json::json;
use tempfile::tempdir;

use crate::db::{get_data, insert_data, try_insert, try_update, update_data};

const SCHEMA: &str = "
    model Author {
        name    String
        posts   Post[]  @bind(Post.author)
    }

    model Post {
        title   String
        author  Author?
        tags    Tag[]
    }

    model Tag {
        name    String
    }

    model Dish {
        id      String  @id
        name    String
    }

    model Order {
        dish    Dish
        qty     Int
    }

    model Chat {
        title   String
    }

    model ChatUser {
        chat    Chat    @id   @onDelete(Cascade)
        user    Author  @id   @onDelete(Cascade)
    }
";

fn make_db() -> (tempfile::TempDir, MarciDB) {
    let dir = tempdir().unwrap();
    let db = MarciDB::new(SCHEMA, dir.path().to_str().unwrap());
    (dir, db)
}

#[test]
fn insert_refuses_a_pointer_at_nothing() {
    let (_dir, db) = make_db();
    let alice = insert_data(&db, "Author", json!({ "name": "Alice" }));

    // a numeric key
    match try_insert(&db, "Post", json!({ "title": "Lost", "author": { "id": 999 } })) {
        Err(InsertError::ForeignKeyViolation(field, _)) => assert_eq!(field, "Post.author"),
        other => panic!("expected a foreign key violation, got {:?}", other),
    }
    assert_eq!(get_data(&db, "Post", json!({})), json!([]), "the refused row left nothing behind");

    // a string key
    insert_data(&db, "Dish", json!({ "id": "latte", "name": "Latte" }));
    match try_insert(&db, "Order", json!({ "dish": { "id": "nothing" }, "qty": 1 })) {
        Err(InsertError::ForeignKeyViolation(field, _)) => assert_eq!(field, "Order.dish"),
        other => panic!("expected a foreign key violation, got {:?}", other),
    }
    assert_eq!(get_data(&db, "Order", json!({})), json!([]));

    // the rows that exist link as before
    let post = insert_data(&db, "Post", json!({ "title": "Found", "author": alice.clone() }));
    insert_data(&db, "Order", json!({ "dish": { "id": "latte" }, "qty": 2 }));
    assert_eq!(
        get_data(&db, "Post", json!({ "title": true, "author": { "name": true } })),
        json!([{ "title": "Found", "author": { "name": "Alice" } }])
    );
    assert_eq!(db.count_dev("Author.posts->Post"), 1);
}

#[test]
fn insert_checks_every_id_of_a_list() {
    let (_dir, db) = make_db();
    let tag = insert_data(&db, "Tag", json!({ "name": "rust" }));

    match try_insert(&db, "Post", json!({ "title": "Tagged", "tags": [ tag.clone(), { "id": 999 } ] })) {
        Err(InsertError::ForeignKeyViolation(field, _)) => assert_eq!(field, "Post.tags"),
        other => panic!("expected a foreign key violation, got {:?}", other),
    }
    assert_eq!(get_data(&db, "Post", json!({})), json!([]));
    assert_eq!(db.count_dev("Post.tags->Tag"), 0, "no link of the refused row was written");

    insert_data(&db, "Post", json!({ "title": "Tagged", "tags": [ tag ] }));
    assert_eq!(db.count_dev("Post.tags->Tag"), 1);
}

#[test]
fn insert_checks_a_ref_in_the_key() {
    let (_dir, db) = make_db();
    let alice = insert_data(&db, "Author", json!({ "name": "Alice" }));
    let chat = insert_data(&db, "Chat", json!({ "title": "General" }));

    match try_insert(&db, "ChatUser", json!({ "chat": { "id": 999 }, "user": alice.clone() })) {
        Err(InsertError::ForeignKeyViolation(field, _)) => assert_eq!(field, "ChatUser.chat"),
        other => panic!("expected a foreign key violation, got {:?}", other),
    }
    assert_eq!(get_data(&db, "ChatUser", json!({})), json!([]));

    insert_data(&db, "ChatUser", json!({ "chat": chat, "user": alice }));
    assert_eq!(get_data(&db, "ChatUser", json!({})).as_array().unwrap().len(), 1);
}

#[test]
fn update_refuses_a_pointer_at_nothing() {
    let (_dir, db) = make_db();
    let alice = insert_data(&db, "Author", json!({ "name": "Alice" }));
    let tag = insert_data(&db, "Tag", json!({ "name": "rust" }));
    let post = insert_data(&db, "Post", json!({ "title": "Draft", "author": alice.clone(), "tags": [ tag.clone() ] }));

    // a single relation
    match try_update(&db, "Post", &post, json!({ "author": { "$connect": { "id": 999 } } })) {
        Err(UpdateError::ForeignKeyViolation(field, _)) => assert_eq!(field, "Post.author"),
        other => panic!("expected a foreign key violation, got {:?}", other),
    }
    // a list: $connect and $set
    match try_update(&db, "Post", &post, json!({ "tags": { "$connect": [ { "id": 999 } ] } })) {
        Err(UpdateError::ForeignKeyViolation(field, _)) => assert_eq!(field, "Post.tags"),
        other => panic!("expected a foreign key violation, got {:?}", other),
    }
    match try_update(&db, "Post", &post, json!({ "tags": { "$set": [ { "id": 999 } ] } })) {
        Err(UpdateError::ForeignKeyViolation(field, _)) => assert_eq!(field, "Post.tags"),
        other => panic!("expected a foreign key violation, got {:?}", other),
    }

    // the row kept its links through the refused updates
    assert_eq!(
        get_data(&db, "Post", json!({ "author": { "name": true }, "tags": { "name": true } })),
        json!([{ "author": { "name": "Alice" }, "tags": [{ "name": "rust" }] }])
    );

    let bob = insert_data(&db, "Author", json!({ "name": "Bob" }));
    update_data(&db, "Post", &post, json!({ "author": { "$connect": bob.clone() } }));
    assert_eq!(
        get_data(&db, "Post", json!({ "author": { "name": true } })),
        json!([{ "author": { "name": "Bob" } }])
    );
}

#[test]
fn a_transaction_sees_the_row_it_inserted_first() {
    let (_dir, db) = make_db();
    let author = db.get_model("Author").unwrap();
    let post = db.get_model("Post").unwrap();

    let tx = db.begin_write().unwrap();
    let alice_op = parse_insert(&db.schema, author, &json!({ "name": "Alice" })).unwrap();
    let alice_id = tx.insert_item(author, &alice_op).unwrap();
    let alice = serde_json::from_str::<serde_json::Value>(&marcidb::decode_id(&alice_id, author, &db.schema)).unwrap();
    let post_op = parse_insert(&db.schema, post, &json!({ "title": "In the same transaction", "author": alice })).unwrap();
    tx.insert_item(post, &post_op).unwrap();
    tx.commit().unwrap();

    assert_eq!(db.count_dev("Author.posts->Post"), 1);
}
