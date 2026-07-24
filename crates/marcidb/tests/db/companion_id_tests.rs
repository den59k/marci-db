use marcidb::MarciDB;
use serde_json::json;
use tempfile::tempdir;

use crate::db::{get_data, insert_data};

#[test]
fn companion_id_test() {

  let schema_str = "
    model User {
      name        String
      chats         ChatUser[]    @bind(ChatUser.user)
    }

    model Chat {
      name        String
      users         ChatUser[]    @bind(ChatUser.chat)
    }

    model ChatUser {
      chat          Chat          @id
      user          User          @id
    }
  ";

  let dir = tempdir().unwrap();
  let db: MarciDB = MarciDB::new(schema_str, dir.path().to_str().unwrap());  

  let user_a = insert_data(&db, "User", json!({ "name": "Alice" }));
  let user_b = insert_data(&db, "User", json!({ "name": "Bob" }));
  let user_c = insert_data(&db, "User", json!({ "name": "Charlie" }));

  let chat_a = insert_data(&db, "Chat", json!({ "name": "First chat" }));
  let chat_b = insert_data(&db, "Chat", json!({ "name": "Second chat" }));
  let chat_c = insert_data(&db, "Chat", json!({ "name": "Third chat" }));

  insert_data(&db, "ChatUser", json!({ "chat": chat_a, "user": user_a }));
  insert_data(&db, "ChatUser", json!({ "chat": chat_b, "user": user_a }));
  insert_data(&db, "ChatUser", json!({ "chat": chat_c, "user": user_b }));

  {
    let resp = get_data(&db, "ChatUser", json!({
      "chat": { "name": true }, "user": { "name": true }
    }));
    assert_eq!(resp, json!([
      { "chat": { "name": "First chat" }, "user": { "name": "Alice" } },
      { "chat": { "name": "Second chat" }, "user": { "name": "Alice" } },
      { "chat": { "name": "Third chat" }, "user": { "name": "Bob" } },
    ]))
  }

  {
    let resp = get_data(&db, "ChatUser", json!({
      "chat": { "name": true },
      "$where": { "user": user_a }
    }));
    assert_eq!(resp, json!([
      { "chat": { "name": "First chat" } },
      { "chat": { "name": "Second chat" } }
    ]))
  }

  insert_data(&db, "ChatUser", json!({ "chat": chat_c, "user": user_c }));
}