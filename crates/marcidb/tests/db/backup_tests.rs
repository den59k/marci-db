//! Backup and restore (backup.rs): a `.marcibin` dump of every tree, taken in one read transaction.
//! What must hold: a restored database is the original byte for byte (rows, indexes, binding trees,
//! journals, the schema and its version), ids continue where they stopped, a later migration applies,
//! the snapshot point is fixed by `backup_begin`, a damaged file is refused without leaving anything
//! behind, and an existing database is replaced only when asked to.

use std::{fs, path::Path, str::FromStr};

use marcidb::{BACKUP_FORMAT_VERSION, BackupError, BackupReader, MarciDB, OpenOptions, RestoreOptions, decode_row_json, parse_id, serialize_snapshot, try_parse_schema};
use marcidb_schema::{diff, reconcile};
use serde_json::{Value, json};
use tempfile::tempdir;

use crate::db::{delete_data, get_data, insert_data};

const SCHEMA: &str = "
model User {
    name    String
    email   String   @unique
    nick    String?  @index
    plan    Plan
    meta    Json?
    posts   Post[]   @bind(Post.author)
}

enum Plan {
    free
    paid {
        seats   UInt
    }
}

model Post {
    title   String
    author  User     @onDelete(Cascade)
    tags    Tag[]    @list
}

model Tag {
    id      Byte[16] @id  @format(uuid)
    label   String
}

model Like {
    user    User     @unique(pair)
    post    Post     @unique(pair)
}
";

const TAG_A: &str = "0f8fad5b-d9cb-469f-a165-70867728950e";
const TAG_B: &str = "7c9e6679-7425-40de-944b-e07fc1f90ae7";

fn path(dir: &Path) -> &str { dir.to_str().unwrap() }

/// A database in a directory that does not exist yet (canopydb needs it created).
fn create_at(dir: &Path, schema: &str) -> MarciDB {
    fs::create_dir_all(dir).unwrap();
    MarciDB::new(schema, path(dir))
}

/// A database with every kind of tree: rows with autoincrement and uuid ids, a unique index, a nullable
/// index, a unique group, enum payloads, Json, a many-to-one binding, a `@list` reverse tree and a
/// journal holding unconfirmed entries.
fn populated(dir: &Path) -> MarciDB {
    let db = MarciDB::new(SCHEMA, path(dir));
    let ann = insert_data(&db, "User", json!({ "name": "Ann", "email": "ann@x.io", "nick": "annie", "plan": "paid", "seats": 5, "meta": { "theme": "dark", "langs": ["en", "fr"] } }));
    let bob = insert_data(&db, "User", json!({ "name": "Bob", "email": "bob@x.io", "plan": "free" }));
    let cid = insert_data(&db, "User", json!({ "name": "Zebediah", "email": "zeb@x.io", "nick": "z", "plan": "free", "meta": [1, 2, 3] }));
    insert_data(&db, "Tag", json!({ "id": TAG_A, "label": "rust" }));
    insert_data(&db, "Tag", json!({ "id": TAG_B, "label": "db" }));
    let p1 = insert_data(&db, "Post", json!({ "title": "hello", "author": ann, "tags": [{ "id": TAG_B }, { "id": TAG_A }] }));
    let p2 = insert_data(&db, "Post", json!({ "title": "second", "author": bob, "tags": [{ "id": TAG_A }] }));
    let p3 = insert_data(&db, "Post", json!({ "title": "gone soon", "author": cid }));
    insert_data(&db, "Like", json!({ "user": ann, "post": p2 }));
    insert_data(&db, "Like", json!({ "user": bob, "post": p1 }));

    let post = db.get_model("Post").unwrap();
    assert!(db.journal_open(post, "removed", &["delete".to_string()]).unwrap());
    delete_data(&db, "Post", p3);
    db
}

/// Every row of every model as the API reads it, relations included.
fn everything(db: &MarciDB) -> Value {
    json!({
        "User": get_data(db, "User", json!({ "name": true, "email": true, "nick": true, "plan": true, "seats": true, "meta": true, "posts": { "title": true } })),
        "Post": get_data(db, "Post", json!({ "title": true, "author": { "name": true }, "tags": { "label": true } })),
        "Tag": get_data(db, "Tag", json!({})),
        "Like": get_data(db, "Like", json!({ "user": { "name": true }, "post": { "title": true } })),
    })
}

fn journal(db: &MarciDB) -> Vec<Value> {
    db.journal_read(db.get_model("Post").unwrap(), "removed", None, 100).unwrap()
        .iter().map(|e| Value::from_str(e).unwrap()).collect()
}

fn meta_version(db: &MarciDB) -> Vec<u8> {
    db.dump_dev("__marci_meta__").into_iter().find(|(k, _)| k == b"version").unwrap().1
}

fn backup_bytes(db: &MarciDB) -> Vec<u8> {
    let mut out = Vec::new();
    db.backup(&mut out).unwrap();
    out
}

fn restore_bytes(dir: &Path, bytes: &[u8], overwrite: bool) -> Result<(MarciDB, marcidb::BackupInfo), BackupError> {
    MarciDB::restore(path(dir), bytes, &RestoreOptions { overwrite, open: OpenOptions::default() })
}

/// Names in `parent` that a restore leaves only while it runs.
fn leftovers(parent: &Path) -> Vec<String> {
    fs::read_dir(parent).unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|n| n.contains(".restoring") || n.contains(".old-") || n.ends_with(".tmp"))
        .collect()
}

#[test]
fn a_restored_database_is_the_original() {
    let src = tempdir().unwrap();
    let db = populated(src.path());
    let bytes = backup_bytes(&db);

    let parent = tempdir().unwrap();
    let target = parent.path().join("restored");
    let (restored, info) = restore_bytes(&target, &bytes, false).unwrap();

    let trees = db.list_trees().unwrap();
    assert_eq!(restored.list_trees().unwrap(), trees);
    assert_eq!(info.bytes, bytes.len() as u64);
    assert_eq!(info.trees, trees.len());
    assert_eq!(info.entries, trees.iter().map(|t| t.entries).sum::<u64>());
    for kind in ["index_User.email", "index_User.nick", "unique_Like.pair", "User.posts->Post", "Post.tags<-Tag", "__marci_journals__"] {
        assert!(trees.iter().any(|t| t.name == kind), "expected a tree '{}' in {:?}", kind, trees);
    }
    assert!(trees.iter().any(|t| t.name.starts_with("__journal__/")), "the journal's entry tree is backed up");

    for tree in trees.iter() {
        assert_eq!(restored.dump_dev(&tree.name), db.dump_dev(&tree.name), "tree '{}' differs", tree.name);
    }
    assert_eq!(everything(&restored), everything(&db));
    assert_eq!(journal(&restored), journal(&db));
    assert_eq!(journal(&restored).len(), 1, "the unconfirmed delete is still pending");
    assert_eq!(serialize_snapshot(&restored.schema), serialize_snapshot(&db.schema));
    assert_eq!(meta_version(&restored), meta_version(&db));
    assert_eq!(info.schema_version, u64::from_be_bytes(meta_version(&db).try_into().unwrap()));

    // Counters are rebuilt from the last key on open: the next user is 4; post 3 was deleted, so the last
    // post key is 2 and the next post is 3 (as after reopening the original).
    assert_eq!(insert_data(&restored, "User", json!({ "name": "Dee", "email": "dee@x.io", "plan": "free" })), json!({ "id": 4 }));
    assert_eq!(insert_data(&restored, "Post", json!({ "title": "new", "author": { "id": 4 } })), json!({ "id": 3 }));

    // Survives a reopen.
    drop(restored);
    let reopened = MarciDB::open(target.to_str().unwrap());
    assert_eq!(get_data(&reopened, "User", json!({ "name": true })).as_array().unwrap().len(), 4);
    assert!(leftovers(parent.path()).is_empty());
}

#[test]
fn a_restored_database_accepts_a_later_migration() {
    let src = tempdir().unwrap();
    let db = populated(src.path());
    let bytes = backup_bytes(&db);
    let version_before = u64::from_be_bytes(meta_version(&db).try_into().unwrap());

    let parent = tempdir().unwrap();
    let (mut restored, _) = restore_bytes(&parent.path().join("db"), &bytes, false).unwrap();

    let mut next = try_parse_schema(&SCHEMA.replace("    nick    String?  @index\n", "    nick    String?  @index\n    age     UInt?\n")).unwrap();
    reconcile(&mut next, &restored.schema);
    let ops = diff(&restored.schema, &next).unwrap();
    assert!(!ops.is_empty());
    restored.commit_schema(next, &ops).unwrap();

    assert_eq!(u64::from_be_bytes(meta_version(&restored).try_into().unwrap()), version_before + 1);
    insert_data(&restored, "User", json!({ "name": "Eve", "email": "eve@x.io", "plan": "free", "age": 30 }));
    assert_eq!(get_data(&restored, "User", json!({ "name": true, "age": true, "$where": { "email": "ann@x.io" } })), json!([{ "name": "Ann", "age": null }]));
    assert_eq!(get_data(&restored, "User", json!({ "name": true, "age": true, "$where": { "email": "eve@x.io" } })), json!([{ "name": "Eve", "age": 30 }]));
}

#[test]
fn a_backup_holds_the_snapshot_of_backup_begin() {
    let src = tempdir().unwrap();
    let db = populated(src.path());
    let users_before = db.count(db.get_model("User").unwrap()).unwrap();

    let snapshot = db.backup_begin().unwrap();
    // Committed after the snapshot point: must not be in the backup.
    insert_data(&db, "User", json!({ "name": "Late", "email": "late@x.io", "plan": "free" }));
    let user_tree = snapshot.header().trees.iter().find(|t| t.name == "User").unwrap().entries;
    assert_eq!(user_tree, users_before);

    let mut bytes = Vec::new();
    let info = snapshot.write_to(&mut bytes).unwrap();
    assert_eq!(info.bytes, bytes.len() as u64);

    let parent = tempdir().unwrap();
    let (restored, restored_info) = restore_bytes(&parent.path().join("db"), &bytes, false).unwrap();
    assert_eq!(restored_info.entries, info.entries);
    assert_eq!(restored.count(restored.get_model("User").unwrap()).unwrap(), users_before);
    assert_eq!(get_data(&restored, "User", json!({ "$where": { "name": "Late" } })), json!([]));
    assert_eq!(db.count(db.get_model("User").unwrap()).unwrap(), users_before + 1);
}

#[test]
fn a_migration_may_commit_while_a_backup_runs() {
    let src = tempdir().unwrap();
    let mut db = populated(src.path());
    let schema_before = serialize_snapshot(&db.schema);
    let before = everything(&db);

    // The snapshot does not borrow the database: a schema change commits while it is open and the backup
    // still holds the schema and the data of its snapshot point.
    let snapshot = db.backup_begin().unwrap();
    let mut next = try_parse_schema(&SCHEMA.replace("    nick    String?  @index
", "    nick    String?  @index
    age     UInt?
").replace("model Like {
    user    User     @unique(pair)
    post    Post     @unique(pair)
}
", "")).unwrap();
    reconcile(&mut next, &db.schema);
    let ops = diff(&db.schema, &next).unwrap();
    db.commit_schema(next, &ops).unwrap();
    insert_data(&db, "User", json!({ "name": "Late", "email": "late@x.io", "plan": "free", "age": 1 }));
    assert!(db.get_model("Like").is_none());

    let mut bytes = Vec::new();
    snapshot.write_to(&mut bytes).unwrap();

    let parent = tempdir().unwrap();
    let (restored, _) = restore_bytes(&parent.path().join("db"), &bytes, false).unwrap();
    assert_eq!(serialize_snapshot(&restored.schema), schema_before);
    assert_eq!(everything(&restored), before);
}

#[test]
fn a_damaged_backup_is_refused_and_leaves_nothing_behind() {
    let src = tempdir().unwrap();
    let db = populated(src.path());
    let good = backup_bytes(&db);

    let flipped = {
        let mut b = good.clone();
        let at = b.windows(8).position(|w| w == b"Zebediah").expect("the row text is in the dump");
        b[at + 3] ^= 0x20;
        b
    };
    let truncated = good[..good.len() - 20].to_vec();
    let cut_mid_section = good[..good.len() / 2].to_vec();
    let wrong_magic = { let mut b = good.clone(); b[0] = b'X'; b };
    let newer = { let mut b = good.clone(); b[8..10].copy_from_slice(&(BACKUP_FORMAT_VERSION + 1).to_le_bytes()); b };
    let trailer_flip = { let mut b = good.clone(); let n = b.len(); b[n - 1] ^= 0xff; b };

    let cases: [(&str, Vec<u8>, &str); 6] = [
        ("flipped byte", flipped, "checksum mismatch in tree 'User'"),
        ("truncated", truncated, "truncated"),
        ("cut mid-section", cut_mid_section, "truncated"),
        ("wrong magic", wrong_magic, "MARCIBIN"),
        ("newer format", newer, "newer than this engine supports"),
        ("trailer checksum", trailer_flip, "whole-file checksum mismatch"),
    ];

    for (case, bytes, expected) in cases {
        // A fresh target, and one holding a database with `overwrite`: neither may be touched.
        let parent = tempdir().unwrap();
        let fresh = parent.path().join("fresh");
        match restore_bytes(&fresh, &bytes, false) {
            Err(BackupError::Format(msg)) => assert!(msg.contains(expected), "{}: '{}' does not mention '{}'", case, msg, expected),
            Err(e) => panic!("{}: expected a format error, got {:?}", case, e),
            Ok(_) => panic!("{}: a damaged backup was restored", case),
        }
        assert!(!fresh.exists(), "{}: the target was created", case);

        let existing = parent.path().join("existing");
        {
            let db = create_at(&existing, "model Keep {\n  v String\n}");
            insert_data(&db, "Keep", json!({ "v": "still here" }));
        }
        assert!(matches!(restore_bytes(&existing, &bytes, true), Err(BackupError::Format(_))), "{}", case);
        let kept = MarciDB::open(existing.to_str().unwrap());
        assert_eq!(get_data(&kept, "Keep", json!({ "v": true })), json!([{ "v": "still here" }]), "{}", case);
        drop(kept);

        assert!(leftovers(parent.path()).is_empty(), "{}: left {:?}", case, leftovers(parent.path()));
    }
}

#[test]
fn an_existing_database_is_replaced_only_with_overwrite() {
    let src = tempdir().unwrap();
    let db = populated(src.path());
    let bytes = backup_bytes(&db);

    let parent = tempdir().unwrap();
    let target = parent.path().join("db");
    {
        let old = create_at(&target, "model Old {\n  v String\n}");
        insert_data(&old, "Old", json!({ "v": "old" }));
    }

    let options = RestoreOptions { overwrite: false, open: OpenOptions::default() };
    match MarciDB::restore_prepare(target.to_str().unwrap(), &bytes[..], &options) {
        Err(BackupError::Target(msg)) => assert!(msg.contains("overwrite"), "{}", msg),
        Err(e) => panic!("expected a target error, got {:?}", e),
        Ok(_) => panic!("a non-empty target was accepted without overwrite"),
    }
    assert!(leftovers(parent.path()).is_empty());

    let (restored, _) = restore_bytes(&target, &bytes, true).unwrap();
    assert!(restored.get_model("Old").is_none());
    assert_eq!(everything(&restored), everything(&db));
    assert!(leftovers(parent.path()).is_empty(), "left {:?}", leftovers(parent.path()));

    // An empty directory (the server creates one before opening) is no database: no overwrite needed.
    let empty = parent.path().join("empty");
    fs::create_dir(&empty).unwrap();
    restore_bytes(&empty, &bytes, false).unwrap();
}

#[test]
fn a_database_that_is_open_is_not_replaced() {
    let src = tempdir().unwrap();
    let db = populated(src.path());
    let bytes = backup_bytes(&db);

    let parent = tempdir().unwrap();
    let target = parent.path().join("live");
    let live = create_at(&target, "model Live {\n  v String\n}");
    insert_data(&live, "Live", json!({ "v": "in use" }));

    match restore_bytes(&target, &bytes, true) {
        Err(BackupError::Target(msg)) => assert!(msg.contains("open"), "{}", msg),
        Err(e) => panic!("expected a target error, got {:?}", e),
        Ok(_) => panic!("an open database was replaced"),
    }
    // The two-phase path: the copy succeeds, the swap is refused, the staging directory goes away.
    let options = RestoreOptions { overwrite: true, open: OpenOptions::default() };
    let staged = MarciDB::restore_prepare(target.to_str().unwrap(), &bytes[..], &options).unwrap();
    assert!(matches!(staged.commit(), Err(BackupError::Target(_))));
    assert!(leftovers(parent.path()).is_empty(), "left {:?}", leftovers(parent.path()));
    assert_eq!(get_data(&live, "Live", json!({ "v": true })), json!([{ "v": "in use" }]));
}

#[test]
fn backup_to_file_leaves_only_the_finished_file() {
    let src = tempdir().unwrap();
    let db = populated(src.path());

    let out = tempdir().unwrap();
    let file = out.path().join("nested").join("db-20261012-120000.marcibin");
    let info = db.backup_to_file(&file).unwrap();
    assert_eq!(fs::metadata(&file).unwrap().len(), info.bytes);
    assert_eq!(leftovers(file.parent().unwrap()), Vec::<String>::new());

    let (restored, _) = MarciDB::restore_from_file(out.path().join("r").to_str().unwrap(), &file, &RestoreOptions::default()).unwrap();
    assert_eq!(everything(&restored), everything(&db));

    // The parent is a file: nothing can be written.
    let blocker = out.path().join("blocker");
    fs::write(&blocker, b"not a directory").unwrap();
    assert!(db.backup_to_file(blocker.join("x.marcibin")).is_err());
    // The path is a directory: the copy completes, the final rename fails, the temporary file goes away.
    let as_dir = out.path().join("taken.marcibin");
    fs::create_dir(&as_dir).unwrap();
    assert!(db.backup_to_file(&as_dir).is_err());
    assert!(leftovers(out.path()).is_empty(), "left {:?}", leftovers(out.path()));
}

#[test]
fn the_reader_streams_a_backup_without_a_database() {
    let src = tempdir().unwrap();
    let db = populated(src.path());
    let bytes = backup_bytes(&db);

    let mut reader = BackupReader::open(&bytes[..]).unwrap();
    let header = reader.header().clone();
    assert_eq!(header.format, BACKUP_FORMAT_VERSION);
    assert_eq!(header.engine, env!("CARGO_PKG_VERSION"));
    assert_eq!(header.schema, serialize_snapshot(&db.schema));
    assert_eq!(header.trees, db.list_trees().unwrap());

    // Decode the users with nothing but the header's schema; skip the other sections unread.
    let schema = marcidb::parse_snapshot(&header.schema).unwrap();
    let user = schema.models.iter().find(|m| m.name == "User").unwrap();
    let mut names = Vec::new();
    while let Some(mut section) = reader.next_section().unwrap() {
        if section.name() != "User" { continue; }
        while let Some((id, body)) = section.next_entry().unwrap() {
            let row: Value = serde_json::from_str(&decode_row_json(&schema, user, id, body).unwrap()).unwrap();
            names.push(row["name"].as_str().unwrap().to_string());
        }
    }
    assert_eq!(names, ["Ann", "Bob", "Zebediah"]);
    assert_eq!(reader.finish().unwrap(), bytes.len() as u64);

    // `--id` in the inspector: the key a JSON id names.
    assert_eq!(parse_id(&schema, user, &json!({ "id": 2 })).unwrap(), 2u64.to_be_bytes());
}

#[test]
fn an_empty_database_backs_up_and_restores() {
    // No schema at all (a directory opened before any migration): no trees.
    let bare = tempdir().unwrap();
    let db = MarciDB::try_open(path(bare.path())).unwrap();
    let bytes = backup_bytes(&db);
    let mut reader = BackupReader::open(&bytes[..]).unwrap();
    assert!(reader.header().trees.is_empty());
    assert_eq!(reader.header().schema_version, 0);
    assert!(reader.next_section().unwrap().is_none());
    reader.finish().unwrap();
    let parent = tempdir().unwrap();
    let (restored, info) = restore_bytes(&parent.path().join("bare"), &bytes, false).unwrap();
    assert_eq!((info.trees, info.entries), (0, 0));
    assert!(restored.schema.models.is_empty());

    // An empty schema: only the meta tree.
    let meta_only = tempdir().unwrap();
    let db = MarciDB::new("", path(meta_only.path()));
    let bytes = backup_bytes(&db);
    let reader = BackupReader::open(&bytes[..]).unwrap();
    assert_eq!(reader.header().trees.iter().map(|t| t.name.as_str()).collect::<Vec<_>>(), ["__marci_meta__"]);
    assert_eq!(reader.header().schema_version, 1);
    reader.finish().unwrap();
    let (restored, _) = restore_bytes(&parent.path().join("meta"), &bytes, false).unwrap();
    assert_eq!(restored.list_trees().unwrap(), db.list_trees().unwrap());
}
