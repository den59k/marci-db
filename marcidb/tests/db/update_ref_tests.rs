use marcidb::MarciDB;
use serde_json::json;
use tempfile::tempdir;

use crate::db::{get_data, insert_data, update_data};

#[test]
fn update_struct_one_test() {

  let schema_str = "
    model User {
      name        String
      info        UserInfo?
    }

    struct UserInfo {
      bio         String
      age         Int?
    }
  ";

  let dir = tempdir().unwrap();
  let db: MarciDB = MarciDB::new(schema_str, dir.path().to_str().unwrap());  

  let user_a = insert_data(&db, "User", json!({ "name": "Alice" }));
  let user_b = insert_data(&db, "User", json!({ "name": "Bob", "info": { "bio": "First user", "age": 30 } }));
  insert_data(&db, "User", json!({ "name": "Charlie" }));

  {
    update_data(&db, "User", &user_a, json!({ "info": { "$set": { "bio": "Second user" } } }));
    assert_eq!(db.count_dev("User.info"), 2);
    
    assert_eq!(
      get_data(&db, "User", json!({ "name": true, "info": true, "$where": { "name": "Alice" } })),
      json!([ { "name": "Alice", "info": { "bio": "Second user", "age": null } } ])
    )
  }

  {
    update_data(&db, "User", &user_a, json!({ "info": { "$update": { "bio": "Alice bio" } } }));
    assert_eq!(db.count_dev("User.info"), 2);
    
    assert_eq!(
      get_data(&db, "User", json!({ "name": true, "info": true, "$where": { "name": "Alice" } })),
      json!([ { "name": "Alice", "info": { "bio": "Alice bio", "age": null } } ])
    )
  }

  {
    update_data(&db, "User", &user_b, json!({ "info": null }));
    assert_eq!(db.count_dev("User.info"), 1);
    assert_eq!(
      get_data(&db, "User", json!({ "name": true, "info": true, "$where": { "name": "Bob" } })),
      json!([ { "name": "Bob", "info": null } ])
    )
  }

}


#[test]
fn update_struct_many_test() {

  let schema_str = "
    model User {
      name        String
      projects    Project[]           @bind(Project.users.user)
    }

    model Project {
      name        String
      users       ProjectUser[]
    }
    
    struct ProjectUser {
      user        User                @id
      role        ProjectUserRole
    }

    enum ProjectUserRole {
      viewer
      editor
      owner
    }
  ";

  let dir = tempdir().unwrap();
  let db: MarciDB = MarciDB::new(schema_str, dir.path().to_str().unwrap());  

  let user_a = insert_data(&db, "User", json!({ "name": "Alice" }));
  let user_b = insert_data(&db, "User", json!({ "name": "Bob", "info": { "bio": "First user", "age": 30 } }));
  let user_c = insert_data(&db, "User", json!({ "name": "Charlie" }));

  let project_a = insert_data(&db, "Project", json!({
    "name": "New Project",
    "users": [ { "user": user_a, "role": "owner" }, { "user": user_b, "role": "viewer" }, { "user": user_c, "role": "viewer" },  ]
  }));

  let project_b = insert_data(&db, "Project", json!({
    "name": "Empty Project"
  }));

  assert_eq!(db.count_dev("Project.users"), 3);

  {
    update_data(&db, "Project", &project_a, json!({
      "users": { "$set": [] }
    }));
  
    assert_eq!(db.count_dev("Project.users"), 0);

    assert_eq!(
      get_data(&db, "User", json!({ "name": true, "projects": true })),
      json!([ { "name": "Alice", "projects": [] }, { "name": "Bob", "projects": [] }, { "name": "Charlie", "projects": [] } ])
    );

    assert_eq!(
      get_data(&db, "Project", json!({ "name": true, "users": true })),
      json!([ { "name": "New Project", "users": [] }, { "name": "Empty Project", "users": [] } ])
    );
  }

  {
    update_data(&db, "Project", &project_b, json!({
      "name": "Maintained project",
      "users": { "$push": [ { "user": user_a, "role": "owner" } ] }
    }));

    assert_eq!(db.count_dev("Project.users"), 1);
  }

}


#[test]
fn update_ref_connect_test() {

  let schema_str = "
    model User {
      name        String
      posts       Post[]           @bind(Post.author)
      postsView   Post[]           @bind(Post.viewers)
    }

    model Post {
      title        String
      author       User?
      viewers      User[]
    }

  ";

  let dir = tempdir().unwrap();
  let db: MarciDB = MarciDB::new(schema_str, dir.path().to_str().unwrap());  

  let user_a = insert_data(&db, "User", json!({ "name": "Alice" }));
  let user_b = insert_data(&db, "User", json!({ "name": "Bob" }));
  let user_c = insert_data(&db, "User", json!({ "name": "Charlie" }));
  
  let post_a = insert_data(&db, "Post", json!({ "title": "About Alice's life", "author": user_a }));
  let _post_b = insert_data(&db, "Post", json!({ "title": "Bob's Post", "author": user_b }));

  {
    update_data(&db, "Post", &post_a, json!({
      "viewers": { "$connect": [ user_c ] }
    }));

    assert_eq!(db.count_dev("Post.viewers->User"), 1);


  }

}


/// Not-yet-implemented relation operations → a typed `UpdateError::Unsupported`,
/// not a `todo!()` panic. The transaction rolls back, so the cases are independent
#[test]
fn update_unsupported_relation_ops() {
  use marcidb::{UpdateError, parse_id, parse_update};

  let schema_str = "
    model User {
      name        String
      posts       Post[]           @bind(Post.author)
      postsView   Post[]           @bind(Post.viewers)
    }

    model Post {
      title        String
      author       User?
      viewers      User[]
    }
  ";

  let dir = tempdir().unwrap();
  let db: MarciDB = MarciDB::new(schema_str, dir.path().to_str().unwrap());

  let user_a = insert_data(&db, "User", json!({ "name": "Alice" }));
  let post = insert_data(&db, "Post", json!({ "title": "Hi", "author": &user_a, "viewers": [ &user_a ] }));

  let entity = db.get_model("Post").unwrap();
  let post_id = parse_id(&db.schema, entity, &post).unwrap();

  let cases = [
    json!({ "author": { "$connect": &user_a } }), // DisconnectAll
    json!({ "author": null }),                     // DisconnectAll
    json!({ "viewers": { "$remove": [ &user_a ] } }), // Disconnect
  ];
  for upd in cases {
    let op = parse_update(&db.schema, entity, &upd).unwrap();
    let res = db.update_item(entity, &post_id, &op);
    assert!(matches!(res, Err(UpdateError::Unsupported(_))), "upd {} -> {:?}", upd, res);
  }
}