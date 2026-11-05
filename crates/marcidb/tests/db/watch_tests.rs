//! Watches (watch.rs): a signal that a read's result may have changed, raised by the commit of a write
//! that touched it. What must hold: a row is checked by its new body (insert), its old one (delete) and
//! both (update); a write outside the filter, and one that rolled back, fire nothing; a relation is
//! watched coarsely; a model nobody watches is not checked; a schema change fires everything; only reads
//! can be watched; a closed database detaches its watchers.

use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};

use marcidb::{MarciDB, Watcher, execute_batch, parse_insert, try_parse_schema};
use marcidb_schema::{diff, reconcile};
use serde_json::{Value, json};
use tempfile::{TempDir, tempdir};

use crate::db::{delete_data, insert_data, update_data};

const SCHEMA: &str = "
model User {
    name   String
    age    Int
    posts  Post[]  @bind(Post.author)
}
model Post {
    title  String
    author User    @onDelete(Cascade)
}
";

fn open() -> (TempDir, MarciDB) {
    let dir = tempdir().unwrap();
    let db = MarciDB::new(SCHEMA, dir.path().to_str().unwrap());
    (dir, db)
}

/// A watcher that counts its wakes.
fn watcher() -> (Arc<Watcher>, Arc<AtomicUsize>) {
    let wakes = Arc::new(AtomicUsize::new(0));
    let counter = wakes.clone();
    (Watcher::new(move || { counter.fetch_add(1, Ordering::SeqCst); }), wakes)
}

fn find_many(model: &str, query: Value) -> Value {
    json!({ "model": model, "action": "findMany", "query": query })
}

fn sync(db: &mut MarciDB, schema: &str) {
    let mut new_schema = try_parse_schema(schema).unwrap();
    reconcile(&mut new_schema, &db.schema);
    let ops = diff(&db.schema, &new_schema).unwrap();
    db.commit_schema(new_schema, &ops).unwrap();
}

#[test]
fn insert_update_and_delete_fire_by_the_filter() {
    let (_dir, db) = open();
    let (w, wakes) = watcher();
    db.watch(&w, 7, &find_many("User", json!({ "$where": { "age": { "$gt": 30 } }, "$order": { "name": "asc" }, "$limit": 1 }))).unwrap();

    let ann = insert_data(&db, "User", json!({ "name": "ann", "age": 40 }));
    assert_eq!(w.take_changed(), vec![7], "an insert of a matching row");
    assert_eq!(wakes.load(Ordering::SeqCst), 1);

    let bob = insert_data(&db, "User", json!({ "name": "bob", "age": 10 }));
    assert_eq!(w.take_changed(), Vec::<u64>::new(), "a row outside the filter");

    update_data(&db, "User", &bob, json!({ "age": 50 }));
    assert_eq!(w.take_changed(), vec![7], "an update into the filter");

    update_data(&db, "User", &bob, json!({ "name": "bobby" }));
    assert_eq!(w.take_changed(), vec![7], "an update of a matching row");

    delete_data(&db, "User", ann);
    assert_eq!(w.take_changed(), vec![7], "a delete of a matching row");

    let cid = insert_data(&db, "User", json!({ "name": "cid", "age": 1 }));
    w.take_changed();
    update_data(&db, "User", &cid, json!({ "age": 2 }));
    delete_data(&db, "User", cid);
    assert_eq!(w.take_changed(), Vec::<u64>::new(), "neither before nor after in the filter");
    assert_eq!(wakes.load(Ordering::SeqCst), 4);
}

#[test]
fn an_update_fires_when_only_the_old_row_matched() {
    let (_dir, db) = open();
    let (w, _) = watcher();
    let ann = insert_data(&db, "User", json!({ "name": "ann", "age": 40 }));
    db.watch(&w, 1, &json!({ "model": "User", "action": "count", "query": { "$where": { "age": { "$gte": 18 } } } })).unwrap();

    update_data(&db, "User", &ann, json!({ "age": 12 }));
    assert_eq!(w.take_changed(), vec![1]);
}

#[test]
fn a_transaction_that_does_not_commit_fires_nothing() {
    let (_dir, db) = open();
    let author = insert_data(&db, "User", json!({ "name": "author", "age": 30 }));
    let (w, wakes) = watcher();
    db.watch(&w, 1, &find_many("User", json!({ "$where": { "name": "ghost" } }))).unwrap();
    let user = db.get_model("User").unwrap();
    let ghost = || parse_insert(&db.schema, user, &json!({ "name": "ghost", "age": 1 })).unwrap();

    let tx = db.begin_write().unwrap();
    tx.insert_item(user, &ghost()).unwrap();
    tx.rollback().unwrap();

    {
        let tx = db.begin_write().unwrap();
        tx.insert_item(user, &ghost()).unwrap();
        // dropped without a commit
    }

    // A batch that fails part-way rolls back, its first insert included.
    let failed = execute_batch(&db, &[
        json!({ "model": "User", "action": "insert", "data": { "name": "ghost", "age": 1 } }),
        json!({ "model": "User", "action": "insert", "data": { "nope": 1 } }),
    ]);
    assert!(failed.is_err());

    // The next commit does not carry what they hit.
    insert_data(&db, "Post", json!({ "title": "t", "author": author }));
    assert_eq!(w.take_changed(), Vec::<u64>::new());
    assert_eq!(wakes.load(Ordering::SeqCst), 0);

    insert_data(&db, "User", json!({ "name": "ghost", "age": 1 }));
    assert_eq!(w.take_changed(), vec![1]);
}

#[test]
fn a_model_nobody_watches_is_not_checked() {
    let (_dir, db) = open();
    let ann = insert_data(&db, "User", json!({ "name": "ann", "age": 40 }));
    update_data(&db, "User", &ann, json!({ "age": 41 }));
    assert_eq!(db.watch_checks_dev(), 0, "no watches: no check");

    let (w, _) = watcher();
    db.watch(&w, 1, &find_many("Post", json!({ "$where": { "title": "hi" } }))).unwrap();
    update_data(&db, "User", &ann, json!({ "age": 42 }));
    assert_eq!(db.watch_checks_dev(), 0, "a watch on another model: no check");

    insert_data(&db, "Post", json!({ "title": "hi", "author": ann }));
    assert_eq!(db.watch_checks_dev(), 1);
    assert_eq!(w.take_changed(), vec![1]);
}

#[test]
fn a_relation_is_watched_coarsely_and_a_relation_condition_may_match() {
    let (_dir, db) = open();
    let ann = insert_data(&db, "User", json!({ "name": "ann", "age": 40 }));
    let (w, _) = watcher();
    // A User row with its posts: any Post write may change it.
    db.watch(&w, 1, &find_many("User", json!({ "$where": { "name": "ann" }, "name": true, "posts": { "title": true } }))).unwrap();
    // Posts titled "x" by ann: the title is checked, the author condition is "may match".
    db.watch(&w, 2, &find_many("Post", json!({ "$where": { "title": "x", "author": { "name": "ann" } } }))).unwrap();

    insert_data(&db, "Post", json!({ "title": "y", "author": ann }));
    assert_eq!(w.take_changed(), vec![1]);

    insert_data(&db, "Post", json!({ "title": "x", "author": ann }));
    assert_eq!(w.take_changed(), vec![1, 2]);

    // A User write reaches the Post watch through its relation condition.
    let bob = insert_data(&db, "User", json!({ "name": "bob", "age": 1 }));
    assert_eq!(w.take_changed(), vec![2], "bob is not ann: watch 1 checks the row, watch 2 cannot");

    // `$not` of a relation condition: still "may match".
    db.unwatch(&w, 2);
    db.watch(&w, 3, &find_many("Post", json!({ "$where": { "$not": { "author": { "name": "ann" } } } }))).unwrap();
    insert_data(&db, "Post", json!({ "title": "z", "author": bob }));
    assert_eq!(w.take_changed(), vec![1, 3]);
}

#[test]
fn a_cascade_fires_the_watches_of_the_rows_it_removes() {
    let (_dir, db) = open();
    let ann = insert_data(&db, "User", json!({ "name": "ann", "age": 40 }));
    insert_data(&db, "Post", json!({ "title": "x", "author": ann }));
    let (w, _) = watcher();
    db.watch(&w, 1, &json!({ "model": "Post", "action": "aggregate", "query": { "$where": { "title": "x" }, "$count": true } })).unwrap();

    delete_data(&db, "User", ann);
    assert_eq!(w.take_changed(), vec![1]);
}

#[test]
fn one_commit_hands_each_id_once_and_unwatch_stops_it() {
    let (_dir, db) = open();
    let (w, wakes) = watcher();
    db.watch(&w, 1, &find_many("User", json!({}))).unwrap();
    db.watch(&w, 2, &json!({ "model": "User", "action": "findFirst", "query": { "$where": { "name": "b" } } })).unwrap();

    execute_batch(&db, &[
        json!({ "model": "User", "action": "insert", "data": { "name": "a", "age": 1 } }),
        json!({ "model": "User", "action": "insert", "data": { "name": "b", "age": 2 } }),
        json!({ "model": "User", "action": "insert", "data": { "name": "c", "age": 3 } }),
    ]).unwrap();
    assert_eq!(w.take_changed(), vec![1, 2]);
    assert_eq!(wakes.load(Ordering::SeqCst), 1, "one wake per commit");

    // Two commits before the watcher looks: still each id once.
    insert_data(&db, "User", json!({ "name": "b", "age": 4 }));
    insert_data(&db, "User", json!({ "name": "b", "age": 5 }));
    assert_eq!(w.take_changed(), vec![1, 2]);

    assert!(db.unwatch(&w, 1));
    assert!(!db.unwatch(&w, 1));
    insert_data(&db, "User", json!({ "name": "b", "age": 6 }));
    assert_eq!(w.take_changed(), vec![2]);

    // Another watcher's ids are its own.
    let (other, _) = watcher();
    db.watch(&other, 2, &find_many("User", json!({ "$where": { "name": "zed" } }))).unwrap();
    insert_data(&db, "User", json!({ "name": "b", "age": 7 }));
    assert_eq!(w.take_changed(), vec![2]);
    assert_eq!(other.take_changed(), Vec::<u64>::new());

    db.unwatch_all(&w);
    insert_data(&db, "User", json!({ "name": "b", "age": 8 }));
    assert_eq!(w.take_changed(), Vec::<u64>::new());
}

#[test]
fn a_migration_fires_every_watch() {
    let (_dir, mut db) = open();
    let (w, _) = watcher();
    db.watch(&w, 1, &find_many("User", json!({ "$where": { "name": "nobody" } }))).unwrap();
    db.watch(&w, 2, &find_many("Post", json!({ "$where": { "title": "nothing" } }))).unwrap();

    sync(&mut db, &SCHEMA.replace("age    Int", "age    Int\n    email  String?"));
    assert_eq!(w.take_changed(), vec![1, 2]);

    // A watch whose op no longer parses fires at the change, then waits for the next one.
    db.watch(&w, 3, &find_many("User", json!({ "$where": { "name": "nobody" }, "$order": { "email": "asc" } }))).unwrap();
    sync(&mut db, SCHEMA);
    assert_eq!(w.take_changed(), vec![1, 2, 3]);
    insert_data(&db, "User", json!({ "name": "nobody", "age": 1 }));
    assert_eq!(w.take_changed(), vec![1], "watch 3 orders by a dropped field: it does not fire");

    // The ops are read again on the new schema: the watches still check their rows.
    insert_data(&db, "User", json!({ "name": "somebody", "age": 1 }));
    assert_eq!(w.take_changed(), Vec::<u64>::new());
}

#[test]
fn only_reads_are_watched() {
    let (_dir, db) = open();
    let (w, _) = watcher();
    let write = db.watch(&w, 1, &json!({ "model": "User", "action": "insert", "query": {} })).unwrap_err();
    assert_eq!(write.kind(), "bad_request");
    assert!(write.to_string().contains("only findMany, findFirst, count and aggregate"), "{}", write);

    assert_eq!(db.watch(&w, 1, &find_many("Nope", json!({}))).unwrap_err().kind(), "not_found");
    assert_eq!(db.watch(&w, 1, &json!({ "model": "User", "action": "aggregate", "query": { "$sum": "nope" } })).unwrap_err().kind(), "bad_request");
    assert_eq!(db.watch(&w, 1, &json!({ "model": "User", "action": "count" })).unwrap_err().kind(), "bad_request");

    // Nothing was registered.
    insert_data(&db, "User", json!({ "name": "ann", "age": 1 }));
    assert_eq!(w.take_changed(), Vec::<u64>::new());
    assert_eq!(db.watch_checks_dev(), 0);
}

#[test]
fn a_closed_database_detaches_its_watchers() {
    let (_dir, db) = open();
    let (w, wakes) = watcher();
    db.watch(&w, 1, &find_many("User", json!({}))).unwrap();
    assert!(!w.is_detached());
    drop(db);
    assert!(w.is_detached());
    assert_eq!(wakes.load(Ordering::SeqCst), 1);
}
