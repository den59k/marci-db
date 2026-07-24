use marcidb::{EncodeError, MarciDB, UpdateError, parse_update};
use serde_json::json;
use tempfile::tempdir;

use crate::db::{get_data, get_data_one, insert_data, try_insert, try_update, update_data};

/// Primitive arrays + every relation kind of the operator matrix in one schema
const BLOG_SCHEMA: &str = "
    model Post {
        title    String
        tags     String[]
        scores   Int[]
        author   User?
        viewers  User[]
    }

    model User {
        name       String
        posts      Post[]  @bind(Post.author)
        postsView  Post[]  @bind(Post.viewers)
    }
";

const BOARD_SCHEMA: &str = "
    model Board {
        name   String
        tasks  Task[]
    }

    struct Task {
        title  String
        done   Int?
    }
";

#[test]
fn primitive_list_operators() {
    let dir = tempdir().unwrap();
    let db: MarciDB = MarciDB::new(BLOG_SCHEMA, dir.path().to_str().unwrap());

    let post = insert_data(&db, "Post", json!({ "title": "Hi", "tags": ["a", "b"] }));
    let tags_of = |db: &MarciDB| get_data_one(db, "Post", json!({ "tags": true }));

    // $push appends; a single value and an array are both accepted
    {
        update_data(&db, "Post", &post, json!({ "tags": { "$push": "c" } }));
        assert_eq!(tags_of(&db), json!({ "tags": ["a", "b", "c"] }));

        // The array is a sequence: pushing an existing value keeps the duplicate
        update_data(&db, "Post", &post, json!({ "tags": { "$push": ["b", "d"] } }));
        assert_eq!(tags_of(&db), json!({ "tags": ["a", "b", "c", "b", "d"] }));
    }

    // $pushUnique appends only absent values (and dedupes within the batch)
    {
        update_data(&db, "Post", &post, json!({ "tags": { "$pushUnique": ["b", "e", "e"] } }));
        assert_eq!(tags_of(&db), json!({ "tags": ["a", "b", "c", "b", "d", "e"] }));

        // All values already present — no change
        update_data(&db, "Post", &post, json!({ "tags": { "$pushUnique": ["a", "e"] } }));
        assert_eq!(tags_of(&db), json!({ "tags": ["a", "b", "c", "b", "d", "e"] }));
    }

    // $remove removes every occurrence of the given values
    {
        update_data(&db, "Post", &post, json!({ "tags": { "$remove": ["b", "d"] } }));
        assert_eq!(tags_of(&db), json!({ "tags": ["a", "c", "e"] }));
    }

    // $set (and the bare-array form) replaces the whole array — also the positional-edit path
    {
        update_data(&db, "Post", &post, json!({ "tags": { "$set": ["z", "a", "z"] } }));
        assert_eq!(tags_of(&db), json!({ "tags": ["z", "a", "z"] }));

        update_data(&db, "Post", &post, json!({ "tags": ["one"] }));
        assert_eq!(tags_of(&db), json!({ "tags": ["one"] }));

        update_data(&db, "Post", &post, json!({ "tags": { "$remove": "one" } }));
        assert_eq!(tags_of(&db), json!({ "tags": [] }));
    }

    // Fixed-size elements (Int) go through the static layout; $push onto an absent list starts one
    {
        update_data(&db, "Post", &post, json!({ "scores": { "$push": [3, 1, 3] } }));
        assert_eq!(get_data_one(&db, "Post", json!({ "scores": true })), json!({ "scores": [3, 1, 3] }));

        update_data(&db, "Post", &post, json!({ "scores": { "$remove": 3 } }));
        assert_eq!(get_data_one(&db, "Post", json!({ "scores": true })), json!({ "scores": [1] }));

        update_data(&db, "Post", &post, json!({ "scores": { "$pushUnique": [1, 2] } }));
        assert_eq!(get_data_one(&db, "Post", json!({ "scores": true })), json!({ "scores": [1, 2] }));
    }

    // Scalar fields and relations in the same update as a list operator still apply
    {
        update_data(&db, "Post", &post, json!({ "title": "Hello", "tags": { "$push": "x" } }));
        assert_eq!(
            get_data_one(&db, "Post", json!({ "title": true, "tags": true })),
            json!({ "title": "Hello", "tags": ["x"] })
        );
    }
}

#[test]
fn primitive_list_rejects_invalid_operators() {
    let schema = marcidb::try_parse_schema("
        model Device {
            name         String
            fingerprint  Byte[8]
            tags         String[]
        }
    ").unwrap();
    let device = &schema.models[0];

    // A fixed-size list cannot change length — in-place operators are rejected
    let err = parse_update(&schema, device, &json!({ "fingerprint": { "$push": 1 } })).unwrap_err();
    assert!(matches!(err, EncodeError::FixedSizeList(f) if f == "Device.fingerprint"));

    // Unknown operator
    let err = parse_update(&schema, device, &json!({ "tags": { "$prepend": "a" } })).unwrap_err();
    assert!(matches!(err, EncodeError::UnsupportedOperation(op) if op == "$prepend"));

    // One operator per update — combined ops would have no defined order
    let err = parse_update(&schema, device, &json!({ "tags": { "$push": "a", "$remove": "b" } })).unwrap_err();
    assert!(matches!(err, EncodeError::OnlyOneKeyExpected(_, _)));
}

#[test]
fn set_links_replaces_membership() {
    let dir = tempdir().unwrap();
    let db: MarciDB = MarciDB::new(BLOG_SCHEMA, dir.path().to_str().unwrap());

    let user_a = insert_data(&db, "User", json!({ "name": "Alice" }));
    let user_b = insert_data(&db, "User", json!({ "name": "Bob" }));
    let user_c = insert_data(&db, "User", json!({ "name": "Charlie" }));
    let post = insert_data(&db, "Post", json!({ "title": "Hi", "viewers": [ &user_a, &user_b ] }));

    // $set replaces link membership: a is disconnected, c connected, b untouched
    {
        update_data(&db, "Post", &post, json!({ "viewers": { "$set": [ &user_b, &user_c ] } }));
        assert_eq!(db.count_dev("Post.viewers->User"), 2);
        assert_eq!(db.count_dev("User.postsView->Post"), 2);
        assert_eq!(
            get_data_one(&db, "Post", json!({ "viewers": { "name": true } })),
            json!({ "viewers": [ { "name": "Bob" }, { "name": "Charlie" } ] })
        );
    }

    // The rows themselves are never touched — all three users still exist
    {
        update_data(&db, "Post", &post, json!({ "viewers": { "$set": [] } }));
        assert_eq!(db.count_dev("Post.viewers->User"), 0);
        assert_eq!(
            get_data(&db, "User", json!({ "name": true })),
            json!([ { "name": "Alice" }, { "name": "Bob" }, { "name": "Charlie" } ])
        );
    }
}

#[test]
fn struct_list_update_items() {
    let dir = tempdir().unwrap();
    let db: MarciDB = MarciDB::new(BOARD_SCHEMA, dir.path().to_str().unwrap());

    let board_a = insert_data(&db, "Board", json!({
        "name": "Main",
        "tasks": [ { "title": "One" }, { "title": "Two" } ]
    }));
    let _board_b = insert_data(&db, "Board", json!({
        "name": "Other",
        "tasks": [ { "title": "Foreign" } ]
    }));

    let task_ids = |db: &MarciDB, name: &str| get_data_one(db, "Board", json!({
        "tasks": { "id": true, "title": true, "done": true }, "$where": { "name": name }
    }));

    let tasks = task_ids(&db, "Main");
    let task_one = tasks["tasks"][0]["id"].clone();
    let task_two = tasks["tasks"][1]["id"].clone();

    // $update edits one child in place; the single and array forms are both accepted
    {
        update_data(&db, "Board", &board_a, json!({
            "tasks": { "$update": { "id": task_one, "data": { "done": 1 } } }
        }));
        update_data(&db, "Board", &board_a, json!({
            "tasks": { "$update": [
                { "id": task_one, "data": { "title": "One!" } },
                { "id": task_two, "data": { "title": "Two!", "done": 2 } }
            ] }
        }));
        assert_eq!(task_ids(&db, "Main")["tasks"], json!([
            { "id": task_one, "title": "One!", "done": 1 },
            { "id": task_two, "title": "Two!", "done": 2 }
        ]));
        // Sibling boards are untouched
        assert_eq!(task_ids(&db, "Other")["tasks"][0]["title"], json!("Foreign"));
    }

    // A missing child id is an error, and the whole update rolls back
    {
        let err = try_update(&db, "Board", &board_a, json!({
            "tasks": { "$update": [
                { "id": task_one, "data": { "title": "Rolled back" } },
                { "id": 9999, "data": { "title": "Ghost" } }
            ] }
        })).unwrap_err();
        assert!(matches!(err, UpdateError::ItemNotFound));
        assert_eq!(task_ids(&db, "Main")["tasks"][0]["title"], json!("One!"));
    }

    // The child key is parent-prefixed, so another parent's child is unreachable by construction:
    // addressing board A's task through board B resolves to a key that does not exist there
    {
        let foreign = task_ids(&db, "Other")["tasks"][0]["id"].clone();
        let err = try_update(&db, "Board", &board_a, json!({
            "tasks": { "$update": { "id": foreign, "data": { "title": "Stolen" } } }
        })).unwrap_err();
        assert!(matches!(err, UpdateError::ItemNotFound));
        assert_eq!(task_ids(&db, "Other")["tasks"][0]["title"], json!("Foreign"));
    }
}

#[test]
fn struct_list_remove_items() {
    let dir = tempdir().unwrap();
    let db: MarciDB = MarciDB::new(BOARD_SCHEMA, dir.path().to_str().unwrap());

    let board = insert_data(&db, "Board", json!({
        "name": "Main",
        "tasks": [ { "title": "One" }, { "title": "Two" }, { "title": "Three" } ]
    }));
    assert_eq!(db.count_dev("Board.tasks"), 3);

    let tasks = get_data_one(&db, "Board", json!({ "tasks": { "id": true } }));
    let task_two = tasks["tasks"][1]["id"].clone();

    // $remove deletes the named children (owned elements have no unlink)
    update_data(&db, "Board", &board, json!({ "tasks": { "$remove": { "id": task_two } } }));
    assert_eq!(db.count_dev("Board.tasks"), 2);
    assert_eq!(
        get_data_one(&db, "Board", json!({ "tasks": { "title": true } })),
        json!({ "tasks": [ { "title": "One" }, { "title": "Three" } ] })
    );

    // Removing an absent child is a no-op — the requested end state already holds
    update_data(&db, "Board", &board, json!({ "tasks": { "$remove": { "id": 9999 } } }));
    assert_eq!(db.count_dev("Board.tasks"), 2);
}

/// Regression: `$set` with items on an owned list must delete EVERY existing child before
/// creating the new ones (it previously prepared a to-one `Remove`, which resolved no child ids)
#[test]
fn struct_list_set_replaces_all_children() {
    let dir = tempdir().unwrap();
    let db: MarciDB = MarciDB::new(BOARD_SCHEMA, dir.path().to_str().unwrap());

    let board = insert_data(&db, "Board", json!({
        "name": "Main",
        "tasks": [ { "title": "Old1" }, { "title": "Old2" }, { "title": "Old3" } ]
    }));
    insert_data(&db, "Board", json!({ "name": "Other", "tasks": [ { "title": "Keep" } ] }));

    update_data(&db, "Board", &board, json!({
        "tasks": { "$set": [ { "title": "New1" }, { "title": "New2" } ] }
    }));

    assert_eq!(db.count_dev("Board.tasks"), 3); // 2 new + 1 on the other board
    assert_eq!(
        get_data(&db, "Board", json!({ "name": true, "tasks": { "title": true } })),
        json!([
            { "name": "Main", "tasks": [ { "title": "New1" }, { "title": "New2" } ] },
            { "name": "Other", "tasks": [ { "title": "Keep" } ] }
        ])
    );
}

/// Regression: deleting an owned child through the parent (`field: null`) must prepare the
/// delete from the CHILD entity (it previously used the parent, leaving the child's own index
/// entries behind)
#[test]
fn struct_delete_cleans_child_indexes() {
    let schema_str = "
        model User {
            name  String
            info  UserInfo?
        }

        struct UserInfo {
            email  String  @unique
        }
    ";
    let dir = tempdir().unwrap();
    let db: MarciDB = MarciDB::new(schema_str, dir.path().to_str().unwrap());

    let user_a = insert_data(&db, "User", json!({ "name": "Alice", "info": { "email": "x@y.z" } }));
    update_data(&db, "User", &user_a, json!({ "info": null }));

    // A stale unique-index entry would make this insert collide
    let resp = try_insert(&db, "User", json!({ "name": "Bob", "info": { "email": "x@y.z" } }));
    assert!(resp.is_ok(), "stale unique index entry after child delete: {:?}", resp.err());
}

#[test]
fn relation_op_matrix_rejections() {
    let schema = marcidb::try_parse_schema(BLOG_SCHEMA).unwrap();
    let post = schema.models.iter().find(|m| m.name == "Post").unwrap();

    let board_schema = marcidb::try_parse_schema("
        model User {
            name  String
            info  UserInfo?
        }
        struct UserInfo {
            bio  String
        }
        model Board {
            name   String
            tasks  Task[]
        }
        struct Task {
            title  String
        }
    ").unwrap();
    let user = board_schema.models.iter().find(|m| m.name == "User").unwrap();
    let board = board_schema.models.iter().find(|m| m.name == "Board").unwrap();

    // Content ops on an independent model: the row is updated through its own collection
    for op in ["$update", "$ensure", "$set"] {
        let err = parse_update(&schema, post, &json!({ "author": { op: { "name": "X" } } })).unwrap_err();
        assert!(matches!(err, EncodeError::OwnedRelationOnly { ref field, op: ref o } if field == "Post.author" && o == op), "{op}: {err:?}");
    }
    for op in ["$push", "$update"] {
        let err = parse_update(&schema, post, &json!({ "viewers": { op: { "name": "X" } } })).unwrap_err();
        assert!(matches!(err, EncodeError::OwnedRelationOnly { ref field, op: ref o } if field == "Post.viewers" && o == op), "{op}: {err:?}");
    }

    // Link ops on an owned (struct) relation: a child cannot be connected, only created
    let err = parse_update(&board_schema, user, &json!({ "info": { "$connect": { "id": 1 } } })).unwrap_err();
    assert!(matches!(err, EncodeError::LinkedRelationOnly { ref field, .. } if field == "User.info"), "{err:?}");

    let err = parse_update(&board_schema, board, &json!({ "tasks": { "$connect": { "id": 1 } } })).unwrap_err();
    assert!(matches!(err, EncodeError::LinkedRelationOnly { ref field, .. } if field == "Board.tasks"), "{err:?}");
}
