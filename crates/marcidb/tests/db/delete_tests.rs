use marcidb::{DeleteError, MarciDB};
use serde_json::json;
use tempfile::tempdir;

use crate::db::{delete_data, get_data, insert_data, try_delete};

#[test]
fn delete_cascade_test() {
    let schema_str = "
        model User {
            name        String
            info        UserInfo?
            posts       Post[]  @bind(Post.author)
        }
        
        struct UserInfo {
            bio         String
        }

        model Post {
            title       String
            author      User?
        }

        model Project {
            name        String
            users       UserRole[]
        }

        enum Role {
            creator
            admin {
                sign      String
            }
        }

        struct UserRole {
            user        User       @id    @onDelete(Cascade)
            role        Role
        }
    ";

    let dir = tempdir().unwrap();
    let db: MarciDB = MarciDB::new(schema_str, dir.path().to_str().unwrap());  

    // println!("{:#?}", db.schema);

    let user_a =  insert_data(&db, "User", json!({ "name": "Alice" }));
    let user_b = insert_data(&db, "User", json!({ "name": "Bob", "info": { "bio": "Just simple first user" } }));
    
    insert_data(&db, "Project", json!({ "name": "Project A" }));
    insert_data(&db, "Project", json!({ "name": "Project B", "users": [{ "user": user_a, "role": "creator" }] }));
    insert_data(&db, "Project", json!({ "name": "Alice Project", "users": [{ "user": user_a, "role": "admin", "sign": "AliceSign" }] }));

    insert_data(&db, "Post", json!({ "title": "First Alice post", "author": user_a }));
    insert_data(&db, "Post", json!({ "title": "Second Alice post", "author": user_a }));
    insert_data(&db, "Post", json!({ "title": "Unnamed post" }));

    assert_eq!(db.count(db.get_model("User").unwrap()).unwrap(), 2);
    assert_eq!(db.count(db.get_model("Project").unwrap()).unwrap(), 3);
    assert_eq!(db.count(db.get_model("Project.users").unwrap()).unwrap(), 2);
    assert_eq!(db.count(db.get_model("Post").unwrap()).unwrap(), 3);
    assert_eq!(db.count(db.get_model("User.info").unwrap()).unwrap(), 1);

    assert_eq!(db.count_dev("User.posts->Post"), 2);

    {
        delete_data(&db, "User", user_a);
    
        assert_eq!(db.count(db.get_model("User").unwrap()).unwrap(), 1);
        assert_eq!(db.count(db.get_model("Project").unwrap()).unwrap(), 3);
        assert_eq!(db.count(db.get_model("Project.users").unwrap()).unwrap(), 0);
        assert_eq!(db.count(db.get_model("Post").unwrap()).unwrap(), 3);
        assert_eq!(db.count_dev("User.posts->Post"), 0);

        assert_eq!(get_data(&db, "Post", json!({ "title": true, "author": true })), json!([
            { "title": "First Alice post", "author": null },
            { "title": "Second Alice post", "author": null },
            { "title": "Unnamed post", "author": null },
        ]));
    }

    {
        delete_data(&db, "User", user_b);
        assert_eq!(db.count(db.get_model("User").unwrap()).unwrap(), 0);

        assert_eq!(db.count(db.get_model("User.info").unwrap()).unwrap(), 0);
    }
}


#[test]
fn delete_many_to_many() {

    let schema_str = "
        model User {
            name        String
            chats       Chat[]
        }

        model Chat {
            name        String
            users       User[]      @bind(User.chats)
            messages    Message[]   @bind(Message.chat)
        }

        model Message {
            text        String
            chat        Chat        @onDelete(Cascade)
            author      User?       @onDelete(SetNull)
        }
    ";

    let dir = tempdir().unwrap();
    let db: MarciDB = MarciDB::new(schema_str, dir.path().to_str().unwrap());  

    let user_a =  insert_data(&db, "User", json!({ "name": "Alice" }));
    let user_b = insert_data(&db, "User", json!({ "name": "Bob" }));
    let user_c = insert_data(&db, "User", json!({ "name": "Charlie" }));
    
    let chat_a = insert_data(&db, "Chat", json!({ "name": "Empty chat", "users": [] }));
    let chat_b = insert_data(&db, "Chat", json!({ "name": "Alice & Bob", "users": [ user_a, user_b ] }));
    let chat_c = insert_data(&db, "Chat", json!({ "name": "Common chat", "users": [ user_a, user_b, user_c ] }));

    insert_data(&db, "Message", json!({ "text": "Hello", "chat": chat_a, "author": null }));
    insert_data(&db, "Message", json!({ "text": "Hello, I am Alice", "chat": chat_b, "author": user_a }));
    insert_data(&db, "Message", json!({ "text": "Hi! How are you?", "chat": chat_b, "author": user_b }));

    insert_data(&db, "Message", json!({ "text": "I am Alice", "chat": chat_c, "author": user_a }));
    insert_data(&db, "Message", json!({ "text": "I am Bob", "chat": chat_c, "author": user_b }));
    insert_data(&db, "Message", json!({ "text": "I am Charlie", "chat": chat_c, "author": user_c }));

    assert_eq!(db.count_dev("User.chats->Chat"), 5);
    assert_eq!(db.count_dev("Chat.users->User"), 5);
    
    {
        delete_data(&db, "User", user_a);
        assert_eq!(db.count(db.get_model("User").unwrap()).unwrap(), 2);
        assert_eq!(db.count_dev("User.chats->Chat"), 3);
        assert_eq!(db.count_dev("Chat.users->User"), 3);

        let resp = get_data(&db, "Chat", json!({ 
            "name": true, 
            "users": { "name": true }
        }));
        
        assert_eq!(resp, json!([
            { "name": "Empty chat", "users": [] },
            { "name": "Alice & Bob", "users": [{ "name": "Bob" }] },
            { "name": "Common chat", "users": [{ "name": "Bob" }, { "name": "Charlie" }] },
        ]));        
    }

    {
        delete_data(&db, "Chat", chat_c);
        assert_eq!(db.count_dev("User.chats->Chat"), 1);
        assert_eq!(db.count_dev("Chat.users->User"), 1);

        assert_eq!(db.count_dev("Message"), 3);

        let resp = get_data(&db, "Chat", json!({ 
            "name": true, 
            "messages": { "text": true, "author": { "name": true } }
        }));
        assert_eq!(resp, json!([
            { "name": "Empty chat", "messages": [
                { "text": "Hello", "author": null }
            ] },
            { "name": "Alice & Bob", "messages": [
                { "text": "Hello, I am Alice", "author": null },
                { "text": "Hi! How are you?", "author": { "name": "Bob" } }
            ] }
        ]));        
    }
}
/// Regression: a `SetNull` dependency must leave the REST of the referencing row untouched.
///
/// Nulling a foreign key removes its bytes from the row body, so every offset after the hole has to
/// move with them. Dropping the bytes without shifting the offsets left the following fields pointing
/// past their own data — `status` decoded raw offset bytes and `chargedRub` read zero. That is silent
/// on-disk corruption of a neighbouring row, not a dangling reference, and it is invisible unless the
/// foreign key happens to be the last field in the model (which is what the older tests all did).
#[test]
fn set_null_does_not_corrupt_the_fields_after_it() {
    let schema_str = "
        model Class {
            title       String
        }

        model Enrollment {
            class       Class?
            status      String
            chargedRub  Int
            note        String?
        }
    ";

    let dir = tempdir().unwrap();
    let db: MarciDB = MarciDB::new(schema_str, dir.path().to_str().unwrap());

    let class_a = insert_data(&db, "Class", json!({ "title": "Yoga" }));
    let class_b = insert_data(&db, "Class", json!({ "title": "Boxing" }));

    insert_data(&db, "Enrollment", json!({ "class": class_a, "status": "active", "chargedRub": 800, "note": "paid" }));
    insert_data(&db, "Enrollment", json!({ "class": class_b, "status": "pending", "chargedRub": 1200, "note": null }));

    let selection = json!({ "class": { "title": true }, "status": true, "chargedRub": true, "note": true });
    assert_eq!(get_data(&db, "Enrollment", selection.clone()), json!([
        { "class": { "title": "Yoga" },   "status": "active",  "chargedRub": 800,  "note": "paid" },
        { "class": { "title": "Boxing" }, "status": "pending", "chargedRub": 1200, "note": null },
    ]));

    delete_data(&db, "Class", class_a);

    // Only the relation is cleared; every scalar after it still reads back byte-for-byte
    assert_eq!(get_data(&db, "Enrollment", selection.clone()), json!([
        { "class": null,                  "status": "active",  "chargedRub": 800,  "note": "paid" },
        { "class": { "title": "Boxing" }, "status": "pending", "chargedRub": 1200, "note": null },
    ]));

    // ...and the row survives a reopen — the damage was persisted, so an in-memory-only check would miss it
    drop(db);
    let db: MarciDB = MarciDB::open(dir.path().to_str().unwrap());
    assert_eq!(get_data(&db, "Enrollment", selection), json!([
        { "class": null,                  "status": "active",  "chargedRub": 800,  "note": "paid" },
        { "class": { "title": "Boxing" }, "status": "pending", "chargedRub": 1200, "note": null },
    ]));
}

/// A REQUIRED relation refuses the delete by default (Prisma parity). Without this, deleting a
/// referenced row silently nulled a foreign key its own schema declares non-null — the delete reported
/// success and left the database describing something that cannot exist.
#[test]
fn required_relation_restricts_delete_by_default() {
    let schema_str = "
        model Class {
            title       String
        }

        model Enrollment {
            class       Class
            status      String
            chargedRub  Int
        }
    ";

    let dir = tempdir().unwrap();
    let db: MarciDB = MarciDB::new(schema_str, dir.path().to_str().unwrap());

    let class_a = insert_data(&db, "Class", json!({ "title": "Yoga" }));
    let class_b = insert_data(&db, "Class", json!({ "title": "Boxing" }));
    insert_data(&db, "Enrollment", json!({ "class": class_a.clone(), "status": "active", "chargedRub": 800 }));

    match try_delete(&db, "Class", class_a.clone()) {
        Err(DeleteError::RestrictConstraints(field, _)) => assert_eq!(field, "Enrollment.class"),
        other => panic!("expected a Restrict rejection, got {:?}", other),
    }

    // The rejection rolls everything back: the class is still there and the enrollment is intact
    assert_eq!(db.count(db.get_model("Class").unwrap()).unwrap(), 2);
    assert_eq!(get_data(&db, "Enrollment", json!({ "class": { "title": true }, "status": true, "chargedRub": true })), json!([
        { "class": { "title": "Yoga" }, "status": "active", "chargedRub": 800 },
    ]));

    // An unreferenced row still deletes
    assert_eq!(try_delete(&db, "Class", class_b).unwrap(), true);
    assert_eq!(db.count(db.get_model("Class").unwrap()).unwrap(), 1);
}

/// The counterpart to the above: an OPTIONAL relation keeps the SetNull default, so an unconstrained
/// delete still works and simply clears the link.
#[test]
fn optional_relation_sets_null_by_default() {
    let schema_str = "
        model Class {
            title       String
        }

        model Enrollment {
            class       Class?
            status      String
        }
    ";

    let dir = tempdir().unwrap();
    let db: MarciDB = MarciDB::new(schema_str, dir.path().to_str().unwrap());

    let class_a = insert_data(&db, "Class", json!({ "title": "Yoga" }));
    insert_data(&db, "Enrollment", json!({ "class": class_a.clone(), "status": "active" }));

    assert_eq!(try_delete(&db, "Class", class_a).unwrap(), true);
    assert_eq!(get_data(&db, "Enrollment", json!({ "class": { "title": true }, "status": true })), json!([
        { "class": null, "status": "active" },
    ]));
}
