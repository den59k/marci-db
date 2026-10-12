//! Boundary tests: drive the actual `extern "C"` entry points with C strings, asserting the JSON result
//! envelopes and that handles/strings are freed. This is the same surface the Bun/Node loaders call.

use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::ptr;

use marcidb_ffi::{
    DbHandle, marci_backup_poll, marci_backup_start, marci_close, marci_exec, marci_free_string, marci_last_error,
    marci_listen, marci_listen_stop, marci_migrate_apply, marci_open, marci_restore, marci_snapshot, marci_sync,
};
use serde_json::{Value, json};
use tempfile::tempdir;

const SCHEMA: &str = "
    model User {
        name String
        age  Int
    }
";

/// Reads, parses, and frees a `char*` envelope returned by an FFI call.
fn take(ptr: *mut c_char) -> Value {
    assert!(!ptr.is_null(), "FFI returned a null string");
    let s = unsafe { CStr::from_ptr(ptr) }.to_str().expect("envelope is valid UTF-8").to_owned();
    marci_free_string(ptr);
    serde_json::from_str(&s).expect("envelope is valid JSON")
}

/// Runs an op JSON against the handle and returns the parsed envelope.
fn exec(handle: *mut DbHandle, op: &Value) -> Value {
    let input = CString::new(op.to_string()).unwrap();
    take(marci_exec(handle, input.as_ptr()))
}

/// Asserts the envelope is `{ ok: true }` and returns its `data`.
fn data(env: Value) -> Value {
    assert_eq!(env["ok"], json!(true), "expected ok envelope, got {}", env);
    env["data"].clone()
}

/// Opens an ephemeral DB (fsync off) at a fresh temp dir and applies `SCHEMA`.
fn open_synced(path: &str) -> *mut DbHandle {
    let path_c = CString::new(path).unwrap();
    let opts_c = CString::new(r#"{"disableFsync":true}"#).unwrap();
    let handle = marci_open(path_c.as_ptr(), opts_c.as_ptr());
    assert!(!handle.is_null(), "open failed");

    let schema_c = CString::new(SCHEMA).unwrap();
    let env = take(marci_sync(handle, schema_c.as_ptr()));
    assert_eq!(env["ok"], json!(true), "sync failed: {}", env);
    handle
}

#[test]
fn crud_lifecycle() {
    let dir = tempdir().unwrap();
    let handle = open_synced(dir.path().to_str().unwrap());

    // insert → data is the generated id
    let id = data(exec(handle, &json!({
        "model": "User", "action": "insert", "data": { "name": "Alice", "age": 30 }
    })));
    assert!(!id.is_null(), "insert should return an id, got {}", id);

    data(exec(handle, &json!({
        "model": "User", "action": "insert", "data": { "name": "Bob", "age": 25 }
    })));

    // findMany
    let rows = data(exec(handle, &json!({
        "model": "User", "action": "findMany", "query": { "name": true, "age": true }
    })));
    assert_eq!(rows.as_array().unwrap().len(), 2);

    // count
    let n = data(exec(handle, &json!({ "model": "User", "action": "count", "query": {} })));
    assert_eq!(n, json!(2));

    // update the first row by id, then read it back
    data(exec(handle, &json!({
        "model": "User", "action": "update", "id": id, "data": { "age": 31 }
    })));
    let alice = data(exec(handle, &json!({
        "model": "User", "action": "findFirst",
        "query": { "name": true, "age": true, "$where": { "name": "Alice" } }
    })));
    assert_eq!(alice["age"], json!(31));

    // delete → null, then count drops
    let deleted = data(exec(handle, &json!({ "model": "User", "action": "delete", "id": id })));
    assert_eq!(deleted, json!(null));
    let n = data(exec(handle, &json!({ "model": "User", "action": "count", "query": {} })));
    assert_eq!(n, json!(1));

    // snapshot returns the schema text
    let snap = data(take(marci_snapshot(handle)));
    assert!(snap.as_str().unwrap().contains("User"), "snapshot should mention the model");

    marci_close(handle);
}

#[test]
fn atomic_transaction_with_ref() {
    let dir = tempdir().unwrap();
    let handle = open_synced(dir.path().to_str().unwrap());

    // Two inserts in one atomic transaction; the array form returns one result per op.
    let results = data(exec(handle, &json!([
        { "model": "User", "action": "insert", "data": { "name": "C", "age": 1 } },
        { "model": "User", "action": "insert", "data": { "name": "D", "age": 2 } }
    ])));
    assert_eq!(results.as_array().unwrap().len(), 2);

    let n = data(exec(handle, &json!({ "model": "User", "action": "count", "query": {} })));
    assert_eq!(n, json!(2));

    marci_close(handle);
}

#[test]
fn error_envelopes() {
    let dir = tempdir().unwrap();
    let handle = open_synced(dir.path().to_str().unwrap());

    // unknown model → not_found
    let env = exec(handle, &json!({ "model": "Ghost", "action": "findMany", "query": {} }));
    assert_eq!(env["ok"], json!(false));
    assert_eq!(env["kind"], json!("not_found"));

    // malformed JSON → bad_request
    let bad = CString::new("{ not json").unwrap();
    let env = take(marci_exec(handle, bad.as_ptr()));
    assert_eq!(env["ok"], json!(false));
    assert_eq!(env["kind"], json!("bad_request"));

    marci_close(handle);
}

#[test]
fn null_handle_and_null_args_are_safe() {
    // A null handle is a clean error envelope, not a crash.
    let op = CString::new(r#"{"model":"User","action":"count","query":{}}"#).unwrap();
    let env = take(marci_exec(ptr::null_mut(), op.as_ptr()));
    assert_eq!(env["ok"], json!(false));
    assert_eq!(env["kind"], json!("bad_request"));

    // Null op json against a real handle is also a clean error.
    let dir = tempdir().unwrap();
    let handle = open_synced(dir.path().to_str().unwrap());
    let env = take(marci_exec(handle, ptr::null()));
    assert_eq!(env["ok"], json!(false));
    marci_close(handle);

    // Closing null is a no-op (must not crash).
    marci_close(ptr::null_mut());
}

#[test]
fn open_failure_reports_last_error() {
    // Point the DB path at an existing *file* so create_dir_all fails → open returns null + sets last_error.
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("not_a_dir");
    std::fs::write(&file_path, b"x").unwrap();

    let path_c = CString::new(file_path.to_str().unwrap()).unwrap();
    let handle = marci_open(path_c.as_ptr(), ptr::null());
    assert!(handle.is_null(), "open onto a file should fail");

    let err = marci_last_error();
    assert!(!err.is_null(), "last_error should be set after a failed open");
    let msg = unsafe { CStr::from_ptr(err) }.to_str().unwrap();
    assert!(!msg.is_empty());
}

/// Applies a `.march` migration history via the idempotent migrator; returns the parsed envelope.
fn migrate(handle: *mut DbHandle, migrations: &[&str]) -> Value {
    let json = serde_json::to_string(migrations).unwrap();
    let c = CString::new(json).unwrap();
    take(marci_migrate_apply(handle, c.as_ptr()))
}

const M0: &str = "create entity User\nadd field User.id UInt @id @default(autoincrement())\nadd field User.name String @slot(4)\nadd field User.age Int @slot(8)";
const M1: &str = "add field User.email String @nullable @slot(12)";

#[test]
fn migrate_apply_idempotent_and_incremental() {
    let dir = tempdir().unwrap();
    let path_c = CString::new(dir.path().to_str().unwrap()).unwrap();
    let handle = marci_open(path_c.as_ptr(), ptr::null());
    assert!(!handle.is_null());

    // Fresh DB → applies the whole history.
    let d = data(migrate(handle, &[M0, M1]));
    assert_eq!(d["applied"], json!(2));
    assert_eq!(d["total"], json!(2));

    // The email column from M1 is usable.
    data(exec(handle, &json!({
        "model": "User", "action": "insert", "data": { "name": "A", "email": "a@x", "age": 1 }
    })));

    // Re-running with the same history is a no-op (idempotent).
    assert_eq!(data(migrate(handle, &[M0, M1]))["applied"], json!(0));

    // Re-running with a *truncated* history (DB is ahead of it) is drift, not a silent no-op.
    let env = migrate(handle, &[M0]);
    assert_eq!(env["ok"], json!(false));
    assert_eq!(env["kind"], json!("bad_request"));

    marci_close(handle);
}

#[test]
fn migrate_apply_incremental_after_restart() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_str().unwrap();

    // First run with only M0.
    let h1 = marci_open(CString::new(path).unwrap().as_ptr(), ptr::null());
    assert_eq!(data(migrate(h1, &[M0]))["applied"], json!(1));
    marci_close(h1);

    // Reopen, now the history has grown by M1 → exactly one new migration applies.
    let h2 = marci_open(CString::new(path).unwrap().as_ptr(), ptr::null());
    let d = data(migrate(h2, &[M0, M1]));
    assert_eq!(d["applied"], json!(1));
    assert_eq!(d["total"], json!(2));
    marci_close(h2);
}

#[test]
fn migrate_apply_detects_drift() {
    let dir = tempdir().unwrap();
    let handle = marci_open(CString::new(dir.path().to_str().unwrap()).unwrap().as_ptr(), ptr::null());

    // Bring the DB to the User schema.
    data(migrate(handle, &[M0]));

    // A different history (creates Post, not User) matches no point in the DB's state → drift error.
    let other = "create entity Post\nadd field Post.id UInt @id @default(autoincrement())\nadd field Post.title String @slot(4)";
    let env = migrate(handle, &[other]);
    assert_eq!(env["ok"], json!(false));
    assert_eq!(env["kind"], json!("bad_request"));

    marci_close(handle);
}

#[cfg(feature = "fulltext")]
#[test]
fn fulltext_custom_index_search() {
    const FT_SCHEMA: &str = "
        model Doc {
            title String
            body  String @custom(fulltext)
        }
    ";

    let dir = tempdir().unwrap();
    let path_c = CString::new(dir.path().to_str().unwrap()).unwrap();
    let handle = marci_open(path_c.as_ptr(), ptr::null());
    assert!(!handle.is_null());

    let schema_c = CString::new(FT_SCHEMA).unwrap();
    assert_eq!(take(marci_sync(handle, schema_c.as_ptr()))["ok"], json!(true));

    // fulltext is live-maintained, so a $search reflects inserts with no explicit reindex.
    data(exec(handle, &json!({ "model": "Doc", "action": "insert", "data": { "title": "A", "body": "the quick brown fox" } })));
    data(exec(handle, &json!({ "model": "Doc", "action": "insert", "data": { "title": "B", "body": "lazy dogs sleeping" } })));

    let hits = data(exec(handle, &json!({
        "model": "Doc", "action": "findMany",
        "query": { "title": true, "$where": { "body": { "$search": "quick fox" } } }
    })));
    let arr = hits.as_array().unwrap();
    assert_eq!(arr.len(), 1, "expected exactly one fulltext hit, got {:?}", arr);
    assert_eq!(arr[0]["title"], json!("A"));

    marci_close(handle);
}

// ───────────────────────────────── backup / restore ─────────────────────────────────

fn insert_users(handle: *mut DbHandle, from: usize, n: usize) {
    let ops: Vec<Value> = (from..from + n)
        .map(|i| json!({ "model": "User", "action": "insert", "data": { "name": format!("user{}", i), "age": i as i64 } }))
        .collect();
    data(exec(handle, &Value::Array(ops)));
}

fn count_users(handle: *mut DbHandle) -> Value {
    data(exec(handle, &json!({ "model": "User", "action": "count", "query": {} })))
}

fn backup_start(handle: *mut DbHandle, path: &std::path::Path) -> u64 {
    let path_c = CString::new(path.to_str().unwrap()).unwrap();
    data(take(marci_backup_start(handle, path_c.as_ptr())))["job"].as_u64().expect("a job id")
}

/// Polls until the job is terminal; returns the final envelope.
fn backup_wait(handle: *mut DbHandle, job: u64) -> Value {
    loop {
        let env = take(marci_backup_poll(handle, job));
        if env["ok"] == json!(true) && env["data"]["state"] == json!("running") {
            std::thread::sleep(std::time::Duration::from_millis(2));
            continue;
        }
        return env;
    }
}

fn restore(dir: &std::path::Path, file: &std::path::Path, options: &str) -> *mut DbHandle {
    let dir_c = CString::new(dir.to_str().unwrap()).unwrap();
    let file_c = CString::new(file.to_str().unwrap()).unwrap();
    let opts_c = CString::new(options).unwrap();
    marci_restore(dir_c.as_ptr(), file_c.as_ptr(), opts_c.as_ptr())
}

fn last_error() -> String {
    let err = marci_last_error();
    assert!(!err.is_null(), "last_error should be set");
    unsafe { CStr::from_ptr(err) }.to_str().unwrap().to_string()
}

#[test]
fn backup_runs_beside_writes_and_restores() {
    let dir = tempdir().unwrap();
    let handle = open_synced(dir.path().join("db").to_str().unwrap());
    insert_users(handle, 0, 2000);

    let out = tempdir().unwrap();
    let file = out.path().join("db-20261012-120000.marcibin");
    let job = backup_start(handle, &file);
    // The snapshot point is fixed: writes while the copy runs succeed and stay out of the backup.
    insert_users(handle, 2000, 50);
    assert_eq!(count_users(handle), json!(2050));

    let done = data(backup_wait(handle, job));
    assert_eq!(done["state"], json!("done"));
    assert_eq!(done["bytes"].as_u64().unwrap(), std::fs::metadata(&file).unwrap().len());
    assert!(done["entries"].as_u64().unwrap() >= 2000);
    assert!(done["createdAt"].as_str().unwrap().ends_with('Z'));
    // A terminal poll forgets the job.
    let again = take(marci_backup_poll(handle, job));
    assert_eq!(again["kind"], json!("bad_request"), "{}", again);

    let target = out.path().join("restored");
    let restored = restore(&target, &file, r#"{"disableFsync":true}"#);
    assert!(!restored.is_null(), "restore failed: {}", last_error());
    assert_eq!(count_users(restored), json!(2000));
    let row = data(exec(restored, &json!({ "model": "User", "action": "findFirst", "query": { "name": true, "$where": { "age": 1999 } } })));
    assert_eq!(row, json!({ "name": "user1999" }));
    // The restored handle is a full handle: writes, schema snapshot, a backup of its own.
    insert_users(restored, 5000, 1);
    let job = backup_start(restored, &out.path().join("again.marcibin"));
    assert_eq!(data(backup_wait(restored, job))["state"], json!("done"));

    marci_close(restored);
    marci_close(handle);
}

#[test]
fn restore_refuses_an_open_or_occupied_directory() {
    let dir = tempdir().unwrap();
    let live_dir = dir.path().join("live");
    let handle = open_synced(live_dir.to_str().unwrap());
    insert_users(handle, 0, 3);
    let file = dir.path().join("b.marcibin");
    let job = backup_start(handle, &file);
    data(backup_wait(handle, job));

    // Occupied, no overwrite.
    assert!(restore(&live_dir, &file, "{}").is_null());
    assert!(last_error().contains("overwrite"), "{}", last_error());
    // Occupied and open: overwrite does not help.
    assert!(restore(&live_dir, &file, r#"{"overwrite":true}"#).is_null());
    assert!(last_error().contains("is open"), "{}", last_error());
    // The live database is untouched and still usable.
    assert_eq!(count_users(handle), json!(3));
    marci_close(handle);

    // Closed: overwrite replaces it.
    let restored = restore(&live_dir, &file, r#"{"overwrite":true,"disableFsync":true}"#);
    assert!(!restored.is_null(), "restore failed: {}", last_error());
    assert_eq!(count_users(restored), json!(3));
    marci_close(restored);

    // Not a backup.
    let junk = dir.path().join("junk.marcibin");
    std::fs::write(&junk, b"definitely not a backup").unwrap();
    assert!(restore(&dir.path().join("other"), &junk, "{}").is_null());
    assert!(last_error().contains("not a valid backup"), "{}", last_error());
}

#[test]
fn close_waits_for_a_running_backup() {
    let dir = tempdir().unwrap();
    let handle = open_synced(dir.path().join("db").to_str().unwrap());
    insert_users(handle, 0, 3000);

    let out = tempdir().unwrap();
    let file = out.path().join("closing.marcibin");
    backup_start(handle, &file);
    marci_close(handle);

    // The file is complete (the trailer checksum verifies) and no temporary file is left.
    let reader = marcidb::BackupReader::open(std::fs::File::open(&file).unwrap()).unwrap();
    assert!(reader.header().trees.iter().any(|t| t.name == "User" && t.entries == 3000));
    reader.finish().unwrap();
    let names: Vec<_> = std::fs::read_dir(out.path()).unwrap().map(|e| e.unwrap().file_name()).collect();
    assert_eq!(names, vec![std::ffi::OsString::from("closing.marcibin")]);

    // The storage lock is released: the directory opens again at once.
    let reopened = open_synced(dir.path().join("db").to_str().unwrap());
    assert_eq!(count_users(reopened), json!(3000));
    marci_close(reopened);
}

#[test]
fn backup_errors_are_envelopes() {
    let dir = tempdir().unwrap();
    let handle = open_synced(dir.path().join("db").to_str().unwrap());

    let unknown = take(marci_backup_poll(handle, 42));
    assert_eq!(unknown["kind"], json!("bad_request"));
    assert_eq!(take(marci_backup_poll(ptr::null_mut(), 1))["kind"], json!("bad_request"));
    assert_eq!(take(marci_backup_start(handle, ptr::null()))["kind"], json!("bad_request"));

    // The copy fails (the parent is a file): reported by the poll, nothing left behind.
    let blocker = dir.path().join("blocker");
    std::fs::write(&blocker, b"x").unwrap();
    let job = backup_start(handle, &blocker.join("b.marcibin"));
    let failed = backup_wait(handle, job);
    assert_eq!(failed["ok"], json!(false), "{}", failed);
    assert_eq!(failed["kind"], json!("internal"));

    marci_close(handle);
}

// ───────────────────────────────── maintenance port ─────────────────────────────────

fn listen(handle: *mut DbHandle, options: Value) -> Value {
    let c = CString::new(options.to_string()).unwrap();
    take(marci_listen(handle, c.as_ptr()))
}

/// One HTTP/1.1 exchange over `std::net` (`Connection: close`; a chunked body is decoded). Errors when
/// nothing listens on the port.
fn http(port: u16, method: &str, path: &str, body: &[u8], token: Option<&str>) -> std::io::Result<(u16, Vec<u8>)> {
    use std::io::{Read, Write};
    let mut stream = std::net::TcpStream::connect(("127.0.0.1", port))?;
    let mut head = format!("{} {} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Length: {}\r\n", method, path, body.len());
    if let Some(token) = token {
        head.push_str(&format!("Authorization: Bearer {}\r\n", token));
    }
    head.push_str("\r\n");
    stream.write_all(head.as_bytes())?;
    stream.write_all(body)?;
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw)?;

    let split = raw.windows(4).position(|w| w == b"\r\n\r\n").expect("a response head");
    let head = String::from_utf8_lossy(&raw[..split]).to_ascii_lowercase();
    let status = head.split(' ').nth(1).and_then(|s| s.parse().ok()).expect("a status code");
    let mut body = raw[split + 4..].to_vec();
    if head.contains("transfer-encoding: chunked") {
        let (mut rest, mut plain) = (&body[..], Vec::new());
        loop {
            let eol = rest.windows(2).position(|w| w == b"\r\n").expect("a chunk size line");
            let size = usize::from_str_radix(std::str::from_utf8(&rest[..eol]).unwrap().trim(), 16).unwrap();
            rest = &rest[eol + 2..];
            if size == 0 { break; }
            plain.extend_from_slice(&rest[..size]);
            rest = &rest[size + 2..];
        }
        body = plain;
    }
    Ok((status, body))
}

fn http_json(port: u16, method: &str, path: &str, body: &Value) -> (u16, Value) {
    let (status, bytes) = http(port, method, path, body.to_string().as_bytes(), None).unwrap();
    (status, serde_json::from_slice(&bytes).unwrap_or(Value::Null))
}

#[test]
fn a_maintenance_port_serves_the_open_database() {
    let dir = tempdir().unwrap();
    let handle = open_synced(dir.path().join("db").to_str().unwrap());

    let started = data(listen(handle, json!({ "port": 0, "name": "app" })));
    let port = started["port"].as_u64().unwrap() as u16;
    assert!(port > 0, "port 0 picks a free one: {}", started);
    assert_eq!(started["host"], json!("127.0.0.1"));
    assert_eq!(http(port, "GET", "/$health", b"", None).unwrap().0, 200);

    // Written through FFI, read over HTTP — and the other way round.
    data(exec(handle, &json!({ "model": "User", "action": "insert", "data": { "name": "ffi", "age": 1 } })));
    assert_eq!(http_json(port, "POST", "/app/User/insert", &json!({ "name": "http", "age": 2 })).0, 200);
    assert_eq!(http_json(port, "POST", "/app/User/count", &json!({})), (200, json!(2)));
    assert_eq!(count_users(handle), json!(2));
    let (_, names) = http_json(port, "POST", "/app/User/findMany", &json!({ "name": true }));
    assert_eq!(names, json!([{ "name": "ffi" }, { "name": "http" }]));
    assert_eq!(http_json(port, "POST", "/other/User/count", &json!({})).0, 404);
    assert_eq!(http(port, "POST", "/app/$restore", b"x", None).unwrap().0, 400);

    // A migration over the port is the app's schema too.
    let (status, _) = http(port, "POST", "/app/$sync", b"model User {\n  name String\n  age  Int\n  email String?\n}", None).unwrap();
    assert_eq!(status, 200);
    assert!(data(take(marci_snapshot(handle))).as_str().unwrap().contains("email"));

    // A backup over HTTP restores through FFI.
    let (status, backup) = http(port, "GET", "/app/$backup", b"", None).unwrap();
    assert_eq!(status, 200);
    let file = dir.path().join("port.marcibin");
    std::fs::write(&file, &backup).unwrap();
    let restored = restore(&dir.path().join("restored"), &file, r#"{"disableFsync":true}"#);
    assert!(!restored.is_null(), "restore failed: {}", last_error());
    assert_eq!(count_users(restored), json!(2));
    marci_close(restored);

    // One port per database.
    let again = listen(handle, json!({ "port": 0, "name": "app" }));
    assert_eq!(again["kind"], json!("bad_request"), "{}", again);
    assert!(again["error"].as_str().unwrap().contains("already listens"), "{}", again);

    // Stopped: the port is closed, a second stop finds nothing, a new listen works.
    assert_eq!(data(take(marci_listen_stop(handle))), json!({ "stopped": true }));
    assert!(http(port, "GET", "/$health", b"", None).is_err(), "the port is closed after stop");
    assert_eq!(data(take(marci_listen_stop(handle))), json!({ "stopped": false }));
    let port = data(listen(handle, json!({ "port": 0, "name": "app" })))["port"].as_u64().unwrap() as u16;
    assert_eq!(http(port, "GET", "/$health", b"", None).unwrap().0, 200);

    // Closing the database stops its port, and releases the directory.
    marci_close(handle);
    assert!(http(port, "GET", "/$health", b"", None).is_err(), "the port is closed after marci_close");
    let reopened = open_synced(dir.path().join("db").to_str().unwrap());
    assert_eq!(count_users(reopened), json!(2));
    marci_close(reopened);
}

#[test]
fn a_maintenance_port_checks_its_options_and_the_token() {
    let dir = tempdir().unwrap();
    let handle = open_synced(dir.path().join("db").to_str().unwrap());

    for (options, expected) in [
        (json!({ "port": 0, "host": "0.0.0.0", "name": "app" }), "without authentication"),
        (json!({ "port": 0, "name": "no/slash" }), "invalid database name"),
        (json!({ "port": 0 }), "invalid database name"),
        (json!({ "name": "app" }), "port"),
        (json!({ "port": 70000, "name": "app" }), "port"),
        (json!({ "port": 0, "host": "nowhere", "name": "app" }), "invalid host"),
    ] {
        let refused = listen(handle, options.clone());
        assert_eq!(refused["kind"], json!("bad_request"), "{} → {}", options, refused);
        assert!(refused["error"].as_str().unwrap().contains(expected), "{} → {}", options, refused);
    }

    // A port in use: the OS message comes back.
    let taken = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let busy = listen(handle, json!({ "port": taken.local_addr().unwrap().port(), "name": "app" }));
    assert_eq!(busy["kind"], json!("bad_request"), "{}", busy);
    assert!(busy["error"].as_str().unwrap().contains("cannot listen on"), "{}", busy);

    // With a token, every route but the health check wants it.
    let port = data(listen(handle, json!({ "port": 0, "name": "app", "token": "s3cret" })))["port"].as_u64().unwrap() as u16;
    assert_eq!(http(port, "GET", "/$health", b"", None).unwrap().0, 200);
    assert_eq!(http(port, "POST", "/app/User/count", b"{}", None).unwrap().0, 401);
    assert_eq!(http(port, "POST", "/app/User/count", b"{}", Some("wrong")).unwrap().0, 401);
    assert_eq!(http(port, "POST", "/app/User/count", b"{}", Some("s3cret")).unwrap(), (200, b"0".to_vec()));
    marci_close(handle);
}
