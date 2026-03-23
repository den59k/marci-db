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
    let _user_b = insert_data(&db, "User", json!({ "name": "Bob", "info": { "bio": "Just simple first user" } }));
    
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
}

