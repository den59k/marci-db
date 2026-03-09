use marcidb::{MarciDB, MarciDocument, array_to_json, decode_document, parse_schema, parse_select};
use serde_json::{Value, json};
use tempfile::{TempDir, tempdir};

fn init_db(dir: TempDir) -> MarciDB {
  let base_schema = parse_schema("

model User {
  name        String
  surname     String
  email       String        @index @unique
  age         Int           @index
  info        UserInfo
  posts       Post[]        @derived(Post.author)
  projects    Project[]     @derived(Project.users.user) @inject(Project.users { role as user_role, sign as admin_sign })
}

struct UserInfo {
  bio         String
}

model Post {
  title       String
  createdAt   DateTime
  author      User          @onDelete(SetNull)
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
  user        User          @id   @onDelete(Cascade)
  role        Role
}

model File {
  name        String
}

  ");

  let db = MarciDB::new(base_schema, dir.path().to_str().unwrap());

  return db;
}

#[test]
fn full_test() {

  let dir = tempdir().unwrap();
  let db = init_db(dir);  

  let model_user = db.get_model("User").unwrap();

  let user_a = json!({
    "name": "Alice",
    "surname": "Swift",
    "email": "alice@mail.test",
    "age": 20,
    "info": { "bio": "Just test user" }
  });
  let user_b = json!({
    "name": "Bob",
    "surname": "Marley",
    "email": "bob@mail.test",
    "age": 38
  });
  let user_c = json!({
    "name": "Charlie",
    "age": 16
  });


  db.insert_data(&MarciDocument::from_json(&db.schema, model_user, &user_a).unwrap()).unwrap();
  db.insert_data(&MarciDocument::from_json(&db.schema, model_user, &user_b).unwrap()).unwrap();
  db.insert_data(&MarciDocument::from_json(&db.schema, model_user, &user_c).unwrap()).unwrap();
  
  let select_one = json!({ "id": true, "name": true, "surname": true, "email": true, "info": true });
  let select = parse_select(model_user, &select_one, &db.schema, None).unwrap();
  let all_users = db.get_all(&select, |ctx | decode_document(ctx).unwrap());

  assert_eq!(serde_json::from_slice::<Value>(array_to_json(&all_users).as_bytes()).unwrap(), json!([
    {
      "id": 1,
      "name": "Alice",
      "surname": "Swift",
      "email": "alice@mail.test",
      "info": { "bio": "Just test user" }
    },
    {
      "id": 2,
      "name": "Bob",
      "surname": "Marley",
      "email": "bob@mail.test",
      "info": null
    },
    {
      "id": 3,
      "name": "Charlie",
      "surname": null,
      "email": null,
      "info": null
    }
  ]));



  // db.insert_data(model, id, data, structs)

}