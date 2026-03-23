mod common;

use marcidb::{MarciDB};
use serde_json::json;
use tempfile::tempdir;

use crate::common::{delete_data, get_data, insert_data};

#[test]
fn delete_cascade_test() {
    let schema_str = "
        model User {
            name        String
            info        UserInfo?
            posts       Post[]  @derived(Post.author)
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

    assert_eq!(db.count(db.get_model("User").unwrap()), 2);
    assert_eq!(db.count(db.get_model("Project").unwrap()), 3);
    assert_eq!(db.count(db.get_model("Project.users").unwrap()), 2);
    assert_eq!(db.count(db.get_model("Post").unwrap()), 3);
    assert_eq!(db.count(db.get_model("User.info").unwrap()), 1);

    assert_eq!(db.count_dev("User.posts->Post"), 2);

    {
        delete_data(&db, "User", user_a);
    
        assert_eq!(db.count(db.get_model("User").unwrap()), 1);
        assert_eq!(db.count(db.get_model("Project").unwrap()), 3);
        assert_eq!(db.count(db.get_model("Project.users").unwrap()), 0);
        assert_eq!(db.count(db.get_model("Post").unwrap()), 3);
        assert_eq!(db.count_dev("User.posts->Post"), 0);

        assert_eq!(get_data(&db, "Post", json!({ "title": true, "author": true })), json!([
            { "title": "First Alice post", "author": null },
            { "title": "Second Alice post", "author": null },
            { "title": "Unnamed post", "author": null },
        ]));
    }

    {
        delete_data(&db, "User", user_b);
        assert_eq!(db.count(db.get_model("User").unwrap()), 0);

        assert_eq!(db.count(db.get_model("User.info").unwrap()), 0);
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
            users       User[]      @derived(User.chats)
            messages    Message[]   @derived(Message.chat)
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
        assert_eq!(db.count(db.get_model("User").unwrap()), 2);
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

    println!("Step 2");
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