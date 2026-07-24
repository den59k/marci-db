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


/// `DisconnectAll` (set-null / `$connect` on a single ref) and `Disconnect` (`$remove` on a ref list)
/// break relations without deleting the related objects, tearing down the index entries on both sides.
#[test]
fn update_disconnect_relation_ops() {
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
  let post = insert_data(&db, "Post", json!({ "title": "Hi", "author": &user_a, "viewers": [ &user_a, &user_b ] }));

  assert_eq!(db.count_dev("User.posts->Post"), 1);
  assert_eq!(db.count_dev("Post.viewers->User"), 2);
  assert_eq!(db.count_dev("User.postsView->Post"), 2);

  // `$connect` on a single ref: DisconnectAll tears down the old author entry, Connect writes the new one.
  {
    update_data(&db, "Post", &post, json!({ "author": { "$connect": &user_b } }));
    assert_eq!(db.count_dev("User.posts->Post"), 1);
    assert_eq!(
      get_data(&db, "Post", json!({ "title": true, "author": { "name": true } })),
      json!([ { "title": "Hi", "author": { "name": "Bob" } } ])
    );
    assert_eq!(
      get_data(&db, "User", json!({ "name": true, "posts": { "title": true }, "$where": { "name": "Alice" } })),
      json!([ { "name": "Alice", "posts": [] } ])
    );
  }

  // Set-null on a single ref: DisconnectAll removes the reverse index entry, the FK body field goes null.
  {
    update_data(&db, "Post", &post, json!({ "author": null }));
    assert_eq!(db.count_dev("User.posts->Post"), 0);
    assert_eq!(
      get_data(&db, "Post", json!({ "title": true, "author": { "name": true } })),
      json!([ { "title": "Hi", "author": null } ])
    );
  }

  // `$remove` on a ref list: Disconnect drops both sides of the index for the given item, leaving Bob.
  {
    update_data(&db, "Post", &post, json!({ "viewers": { "$remove": [ &user_a ] } }));
    assert_eq!(db.count_dev("Post.viewers->User"), 1);
    assert_eq!(db.count_dev("User.postsView->Post"), 1);
    assert_eq!(
      get_data(&db, "Post", json!({ "title": true, "viewers": { "name": true } })),
      json!([ { "title": "Hi", "viewers": [ { "name": "Bob" } ] } ])
    );
  }
}