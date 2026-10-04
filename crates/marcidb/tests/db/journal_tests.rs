//! Journals (journal.rs): a named log of a model's deleted rows, written in the deleting transaction.
//! What must hold: a cascade is journaled like a delete by name, an entry survives until the reader
//! confirms it, a seq is never handed out twice (not across a reopen, not after the journal was
//! emptied), journals are independent, and a model nobody journals is untouched.

use std::{str::FromStr, time::{Duration, Instant}};

use marcidb::{JournalError, MarciDB, execute_batch, parse_query, try_parse_schema};
use marcidb_schema::{diff, reconcile};
use serde_json::{Value, json};
use tempfile::tempdir;

use crate::db::{delete_data, insert_data, update_data};

const SCHEMA: &str = "
model User {
    name   String
    posts  Post[]  @bind(Post.author)
    avatar File[]  @bind(File.user)
}
model Post {
    text   String
    author User    @onDelete(Cascade)
    files  File[]  @bind(File.post)
}
model File {
    blob   String
    size   Int
    post   Post?   @onDelete(Cascade)
    user   User?   @onDelete(Cascade)
}
";

fn on_delete() -> Vec<String> { vec!["delete".to_string()] }

fn open(db: &MarciDB, model: &str, name: &str) -> bool {
    db.journal_open(db.get_model(model).unwrap(), name, &on_delete()).unwrap()
}

fn read(db: &MarciDB, model: &str, name: &str, after: Option<u64>) -> Vec<Value> {
    db.journal_read(db.get_model(model).unwrap(), name, after, 100).unwrap()
        .iter().map(|entry| Value::from_str(entry).unwrap()).collect()
}

fn blobs(entries: &[Value]) -> Vec<&str> {
    let mut blobs: Vec<&str> = entries.iter().map(|e| e["row"]["blob"].as_str().unwrap()).collect();
    blobs.sort();
    blobs
}

fn file(db: &MarciDB, blob: &str, owner: Value) -> Value {
    let mut data = json!({ "blob": blob, "size": 10 });
    data.as_object_mut().unwrap().extend(owner.as_object().unwrap().clone());
    insert_data(db, "File", data)
}

#[test]
fn a_delete_by_name_is_journaled_with_the_row() {
    let dir = tempdir().unwrap();
    let db = MarciDB::new(SCHEMA, dir.path().to_str().unwrap());
    let user = insert_data(&db, "User", json!({ "name": "ann" }));
    let before = file(&db, "before", json!({ "user": user }));

    // Deleted before the journal exists: not in it.
    delete_data(&db, "File", before);
    assert!(open(&db, "File", "files"));
    assert!(!open(&db, "File", "files"), "opening an existing journal again changes nothing");

    let kept = file(&db, "kept", json!({ "user": user }));
    let gone = file(&db, "gone", json!({ "user": user }));
    delete_data(&db, "File", gone.clone());

    let entries = read(&db, "File", "files", None);
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0], json!({ "seq": 1, "op": "delete", "row": { "id": gone["id"], "blob": "gone", "size": 10 } }));
    let _ = kept;
}

#[test]
fn a_cascade_is_journaled_through_every_level_and_every_owner() {
    let dir = tempdir().unwrap();
    let db = MarciDB::new(SCHEMA, dir.path().to_str().unwrap());
    open(&db, "File", "files");

    let ann = insert_data(&db, "User", json!({ "name": "ann" }));
    let bob = insert_data(&db, "User", json!({ "name": "bob" }));
    let post = insert_data(&db, "Post", json!({ "text": "hi", "author": ann }));
    let other = insert_data(&db, "Post", json!({ "text": "yo", "author": bob }));
    file(&db, "ann-avatar", json!({ "user": ann }));
    file(&db, "ann-post-1", json!({ "post": post }));
    file(&db, "ann-post-2", json!({ "post": post }));
    file(&db, "bob-avatar", json!({ "user": bob }));
    file(&db, "bob-post", json!({ "post": other }));

    // User → its files directly, and → its posts → their files: one delete, three rows two ways.
    delete_data(&db, "User", ann);

    let entries = read(&db, "File", "files", None);
    assert_eq!(blobs(&entries), vec!["ann-avatar", "ann-post-1", "ann-post-2"]);
    assert_eq!(db.count(db.get_model("File").unwrap()).unwrap(), 2, "the other user's files stay");
}

#[test]
fn delete_many_a_batch_and_a_rollback() {
    let dir = tempdir().unwrap();
    let db = MarciDB::new(SCHEMA, dir.path().to_str().unwrap());
    open(&db, "File", "files");
    let user = insert_data(&db, "User", json!({ "name": "ann" }));
    for blob in ["a", "b", "c", "d"] { file(&db, blob, json!({ "user": user })); }

    let entity = db.get_model("File").unwrap();
    let query = parse_query(&db.schema, entity, &json!({ "$where": { "blob": { "$in": ["a", "b"] } } })).unwrap();
    assert_eq!(db.delete_many(entity, &query).unwrap(), 2);
    assert_eq!(blobs(&read(&db, "File", "files", None)), vec!["a", "b"]);

    // A transaction that fails takes its entries with it.
    let failed = execute_batch(&db, &[
        json!({ "model": "File", "action": "deleteMany", "query": { "$where": { "blob": "c" } } }),
        json!({ "model": "Nope", "action": "delete", "id": 1 }),
    ]);
    assert!(failed.is_err());
    assert_eq!(read(&db, "File", "files", None).len(), 2);

    execute_batch(&db, &[json!({ "model": "File", "action": "deleteMany", "query": { "$where": { "blob": "c" } } })]).unwrap();
    assert_eq!(blobs(&read(&db, "File", "files", None)), vec!["a", "b", "c"]);
}

#[test]
fn entries_stay_until_confirmed_and_a_seq_is_never_reused() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    let user;
    {
        let db = MarciDB::new(SCHEMA, &path);
        open(&db, "File", "files");
        user = insert_data(&db, "User", json!({ "name": "ann" }));
        for blob in ["a", "b", "c"] {
            let id = file(&db, blob, json!({ "user": user }));
            delete_data(&db, "File", id);
        }

        // Read twice without confirming: the same entries.
        assert_eq!(read(&db, "File", "files", None).len(), 3);
        assert_eq!(read(&db, "File", "files", None).len(), 3);

        // Confirm the first two; the third is what is left, however often it is asked for.
        let rest = read(&db, "File", "files", Some(2));
        assert_eq!(rest.len(), 1);
        assert_eq!(rest[0]["seq"], 3);
        assert_eq!(read(&db, "File", "files", Some(2)).len(), 1);

        // Confirm everything: the journal is empty.
        assert!(read(&db, "File", "files", Some(3)).is_empty());
    }

    // Reopened with nothing stored: the next entry still continues after the confirmed seq.
    let db = MarciDB::open(&path);
    let id = file(&db, "d", json!({ "user": user }));
    delete_data(&db, "File", id);
    let entries = read(&db, "File", "files", Some(3));
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0]["seq"], 4);

    // A seq that was never handed out confirms nothing beyond the last one.
    assert!(read(&db, "File", "files", Some(1000)).is_empty());
    let id = file(&db, "e", json!({ "user": user }));
    delete_data(&db, "File", id);
    assert_eq!(read(&db, "File", "files", Some(4))[0]["seq"], 5);
}

#[test]
fn journals_are_independent_and_a_dropped_one_is_gone() {
    let dir = tempdir().unwrap();
    let db = MarciDB::new(SCHEMA, dir.path().to_str().unwrap());
    open(&db, "File", "one");
    open(&db, "File", "two");
    let user = insert_data(&db, "User", json!({ "name": "ann" }));
    let id = file(&db, "a", json!({ "user": user }));
    delete_data(&db, "File", id);

    assert!(read(&db, "File", "one", Some(1)).is_empty());
    assert_eq!(read(&db, "File", "two", None).len(), 1, "confirming in one journal leaves the other");

    let entity = db.get_model("File").unwrap();
    assert!(db.journal_drop(entity, "one").unwrap());
    assert!(!db.journal_drop(entity, "one").unwrap());
    assert_eq!(db.journal_read(entity, "one", None, 10), Err(JournalError::NotFound { name: "one".into() }));

    // Created again, it starts empty and from seq 1.
    open(&db, "File", "one");
    let id = file(&db, "b", json!({ "user": user }));
    delete_data(&db, "File", id);
    assert_eq!(read(&db, "File", "one", None)[0]["seq"], 1);
    assert_eq!(read(&db, "File", "two", None).len(), 2);
}

#[test]
fn what_a_journal_refuses() {
    let dir = tempdir().unwrap();
    let db = MarciDB::new(SCHEMA, dir.path().to_str().unwrap());
    let entity = db.get_model("File").unwrap();

    assert_eq!(db.journal_open(entity, "bad name", &on_delete()), Err(JournalError::InvalidName("bad name".into())));
    assert_eq!(db.journal_open(entity, "j", &["update".to_string()]), Err(JournalError::UnsupportedOp("update".into())));
    assert_eq!(db.journal_open(entity, "j", &[]), Err(JournalError::UnsupportedOp(String::new())));
}

#[test]
fn a_model_without_a_journal_and_an_update_write_nothing() {
    let dir = tempdir().unwrap();
    let db = MarciDB::new(SCHEMA, dir.path().to_str().unwrap());
    open(&db, "File", "files");
    let user = insert_data(&db, "User", json!({ "name": "ann" }));
    let post = insert_data(&db, "Post", json!({ "text": "hi", "author": user }));
    let id = file(&db, "a", json!({ "user": user }));

    update_data(&db, "File", &id, json!({ "blob": "b" }));
    delete_data(&db, "Post", post);
    assert!(read(&db, "File", "files", None).is_empty());
}

#[test]
fn a_dropped_model_takes_its_journals() {
    let with_note = "
model User {
    name   String
}
model Note {
    text   String
}
";
    let without_note = "
model User {
    name   String
}
";
    let dir = tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    {
        let mut db = MarciDB::new(with_note, &path);
        open(&db, "Note", "notes");
        let note = insert_data(&db, "Note", json!({ "text": "a" }));
        delete_data(&db, "Note", note);

        let mut new_schema = try_parse_schema(without_note).unwrap();
        reconcile(&mut new_schema, &db.schema);
        let ops = diff(&db.schema, &new_schema).unwrap();
        db.commit_schema(new_schema, &ops).unwrap();

        // The model comes back: its old journal does not.
        let mut new_schema = try_parse_schema(with_note).unwrap();
        reconcile(&mut new_schema, &db.schema);
        let ops = diff(&db.schema, &new_schema).unwrap();
        db.commit_schema(new_schema, &ops).unwrap();
        let entity = db.get_model("Note").unwrap();
        assert_eq!(db.journal_read(entity, "notes", None, 10), Err(JournalError::NotFound { name: "notes".into() }));
    }
    // Neither does a reopen find one.
    let db = MarciDB::open(&path);
    let entity = db.get_model("Note").unwrap();
    assert_eq!(db.journal_read(entity, "notes", None, 10), Err(JournalError::NotFound { name: "notes".into() }));
    assert!(open(&db, "Note", "notes"));
    assert!(read(&db, "Note", "notes", None).is_empty());
}

#[test]
fn a_waiting_reader_is_woken_by_a_commit() {
    let dir = tempdir().unwrap();
    let db = MarciDB::new(SCHEMA, dir.path().to_str().unwrap());
    open(&db, "File", "files");
    let user = insert_data(&db, "User", json!({ "name": "ann" }));
    let id = file(&db, "a", json!({ "user": user }));

    let signal = db.journal_signal();
    let seen = signal.generation();
    assert!(read(&db, "File", "files", None).is_empty());

    // Nothing journaled: the wait runs out.
    let started = Instant::now();
    assert_eq!(signal.wait(seen, Duration::from_millis(50)), seen);
    assert!(started.elapsed() >= Duration::from_millis(50));

    std::thread::scope(|scope| {
        let waiter = scope.spawn(|| signal.wait(seen, Duration::from_secs(10)));
        std::thread::sleep(Duration::from_millis(50));
        delete_data(&db, "File", id);
        assert_ne!(waiter.join().unwrap(), seen);
    });
    assert_eq!(read(&db, "File", "files", None).len(), 1);
}
