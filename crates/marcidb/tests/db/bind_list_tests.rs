//! Linking through the `@bind` side of a one-to-many (`User.posts Post[] @bind(Post.author)`). The list
//! stores nothing itself: the link is the child's `author`, so `$connect` / `$remove` / `$set` on `posts`
//! and `posts: [{ id }]` on insert write the children's field — before, they wrote the index tree alone
//! and `Post.author` stayed null while `User.posts` listed the post.
use marcidb::{InsertError, MarciDB, UpdateError};
use serde_json::json;
use tempfile::tempdir;

use crate::db::{get_data, insert_data, try_insert, try_update, update_data};

const SCHEMA: &str = "
    model User {
        name      String
        posts     Post[]      @bind(Post.author)
        notes     Note[]      @bind(Note.owner)
        chats     ChatUser[]  @bind(ChatUser.user)
        passport  Passport?   @bind(Passport.user)
    }

    model Passport {
        number  String
        user    User?   @unique
    }

    model Post {
        title   String
        author  User?
    }

    model Note {
        text    String
        owner   User
    }

    model Chat {
        title   String
        users   ChatUser[]  @bind(ChatUser.chat)
    }

    model ChatUser {
        chat    Chat    @id   @onDelete(Cascade)
        user    User    @id   @onDelete(Cascade)
    }
";

fn make_db() -> (tempfile::TempDir, MarciDB) {
    let dir = tempdir().unwrap();
    let db = MarciDB::new(SCHEMA, dir.path().to_str().unwrap());
    (dir, db)
}

fn posts_of(db: &MarciDB, name: &str) -> serde_json::Value {
    get_data(db, "User", json!({ "posts": { "title": true }, "$where": { "name": name } }))[0]["posts"].clone()
}
fn author_of(db: &MarciDB, title: &str) -> serde_json::Value {
    get_data(db, "Post", json!({ "author": { "name": true }, "$where": { "title": title } }))[0]["author"].clone()
}

#[test]
fn connect_through_the_list_writes_the_child() {
    let (_dir, db) = make_db();
    let alice = insert_data(&db, "User", json!({ "name": "Alice" }));
    let bob = insert_data(&db, "User", json!({ "name": "Bob" }));
    let orphan = insert_data(&db, "Post", json!({ "title": "Orphan" }));
    let alices = insert_data(&db, "Post", json!({ "title": "Alice's", "author": alice.clone() }));

    // $connect: the post gets its author; both sides agree
    update_data(&db, "User", &alice, json!({ "posts": { "$connect": [ orphan.clone() ] } }));
    assert_eq!(author_of(&db, "Orphan"), json!({ "name": "Alice" }));
    assert_eq!(posts_of(&db, "Alice"), json!([{ "title": "Orphan" }, { "title": "Alice's" }]));
    assert_eq!(db.count_dev("User.posts->Post"), 2);

    // connecting from Bob MOVES the post: Alice's list loses it, one tree entry per post
    update_data(&db, "User", &bob, json!({ "posts": { "$connect": [ orphan.clone() ] } }));
    assert_eq!(author_of(&db, "Orphan"), json!({ "name": "Bob" }));
    assert_eq!(posts_of(&db, "Alice"), json!([{ "title": "Alice's" }]));
    assert_eq!(posts_of(&db, "Bob"), json!([{ "title": "Orphan" }]));
    assert_eq!(db.count_dev("User.posts->Post"), 2);

    // $remove: the optional author goes null
    update_data(&db, "User", &alice, json!({ "posts": { "$remove": [ alices.clone() ] } }));
    assert_eq!(author_of(&db, "Alice's"), json!(null));
    assert_eq!(posts_of(&db, "Alice"), json!([]));
    assert_eq!(db.count_dev("User.posts->Post"), 1);

    // $set: exactly these
    update_data(&db, "User", &alice, json!({ "posts": { "$set": [ orphan.clone(), alices.clone() ] } }));
    assert_eq!(author_of(&db, "Orphan"), json!({ "name": "Alice" }));
    assert_eq!(author_of(&db, "Alice's"), json!({ "name": "Alice" }));
    assert_eq!(posts_of(&db, "Bob"), json!([]));
    update_data(&db, "User", &alice, json!({ "posts": { "$set": [ alices.clone() ] } }));
    assert_eq!(author_of(&db, "Orphan"), json!(null));
    assert_eq!(posts_of(&db, "Alice"), json!([{ "title": "Alice's" }]));
    assert_eq!(db.count_dev("User.posts->Post"), 1);
}

#[test]
fn connect_on_insert_writes_the_children() {
    let (_dir, db) = make_db();
    let a = insert_data(&db, "Post", json!({ "title": "A" }));
    let b = insert_data(&db, "Post", json!({ "title": "B" }));
    insert_data(&db, "User", json!({ "name": "Carol", "posts": [ a, b ] }));
    assert_eq!(author_of(&db, "A"), json!({ "name": "Carol" }));
    assert_eq!(author_of(&db, "B"), json!({ "name": "Carol" }));
    assert_eq!(posts_of(&db, "Carol"), json!([{ "title": "A" }, { "title": "B" }]));

    // a child that does not exist: the list field's foreign key, and no user is left behind
    let e = try_insert(&db, "User", json!({ "name": "Dan", "posts": [ { "id": 999 } ] })).unwrap_err();
    assert_eq!((e.kind(), e.field()), ("foreign_key", Some("User.posts")));
    assert_eq!(e.to_string(), "User.posts: no row with id 999 to point at");
    assert!(matches!(e, InsertError::Relation(_)));
    assert_eq!(get_data(&db, "User", json!({ "name": true, "$where": { "name": "Dan" } })), json!([]));
}

#[test]
fn connect_through_a_one_to_one_writes_the_holder() {
    let (_dir, db) = make_db();
    let alice = insert_data(&db, "User", json!({ "name": "Alice" }));
    let p1 = insert_data(&db, "Passport", json!({ "number": "P1" }));
    let p2 = insert_data(&db, "Passport", json!({ "number": "P2" }));
    let holder = |number: &str| get_data(&db, "Passport", json!({ "user": { "name": true }, "$where": { "number": number } }))[0]["user"].clone();
    let passport = |name: &str| get_data(&db, "User", json!({ "passport": { "number": true }, "$where": { "name": name } }))[0]["passport"].clone();

    update_data(&db, "User", &alice, json!({ "passport": { "$connect": p1.clone() } }));
    assert_eq!(holder("P1"), json!({ "name": "Alice" }));
    assert_eq!(passport("Alice"), json!({ "number": "P1" }));

    // another passport: the old holder is released, never left pointing at Alice
    update_data(&db, "User", &alice, json!({ "passport": { "$connect": p2.clone() } }));
    assert_eq!(holder("P1"), json!(null));
    assert_eq!(holder("P2"), json!({ "name": "Alice" }));
    assert_eq!(passport("Alice"), json!({ "number": "P2" }));

    update_data(&db, "User", &alice, json!({ "passport": null }));
    assert_eq!(holder("P2"), json!(null));
    assert_eq!(passport("Alice"), json!(null));

    // on insert
    insert_data(&db, "User", json!({ "name": "Bob", "passport": p1 }));
    assert_eq!(holder("P1"), json!({ "name": "Bob" }));
    assert_eq!(passport("Bob"), json!({ "number": "P1" }));
}

#[test]
fn a_required_child_field_cannot_be_unlinked_and_a_key_cannot_move() {
    let (_dir, db) = make_db();
    let alice = insert_data(&db, "User", json!({ "name": "Alice" }));
    let bob = insert_data(&db, "User", json!({ "name": "Bob" }));
    let note = insert_data(&db, "Note", json!({ "text": "todo", "owner": alice.clone() }));

    let e = try_update(&db, "User", &alice, json!({ "notes": { "$remove": [ note.clone() ] } })).unwrap_err();
    assert!(matches!(e, UpdateError::RequiredRelation(..)), "{:?}", e);
    assert_eq!(e.to_string(), "User.notes: cannot unlink from User.notes — Note.owner is required; delete the row or point it at another");
    assert_eq!((e.kind(), e.field()), ("bad_request", Some("User.notes")));

    // moving it to Bob is fine: the required field changes, never goes null
    update_data(&db, "User", &bob, json!({ "notes": { "$connect": [ note ] } }));
    assert_eq!(get_data(&db, "Note", json!({ "owner": { "name": true } }))[0]["owner"], json!({ "name": "Bob" }));

    // a missing child on update
    let e = try_update(&db, "User", &bob, json!({ "posts": { "$connect": [ { "id": 999 } ] } })).unwrap_err();
    assert_eq!(e.to_string(), "User.posts: no row with id 999 to point at");
    assert_eq!(e.kind(), "foreign_key");

    // the relation in a composite key: the row is its key, so it is inserted or deleted, never relinked
    let chat = insert_data(&db, "Chat", json!({ "title": "General" }));
    insert_data(&db, "ChatUser", json!({ "chat": chat.clone(), "user": alice.clone() }));
    let e = try_update(&db, "Chat", &chat, json!({ "users": { "$connect": [ { "chat": chat.clone(), "user": bob.clone() } ] } })).unwrap_err();
    assert_eq!(e.to_string(), "ChatUser.chat: a key field cannot be updated");
}
