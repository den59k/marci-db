//! Boundary tests: drive the actual `extern "C"` entry points with C strings, asserting the JSON result
//! envelopes and that handles/strings are freed. This is the same surface the Bun/Node loaders call.

use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::ptr;

use marcidb_ffi::{
    DbHandle, marci_close, marci_exec, marci_free_string, marci_last_error, marci_open, marci_snapshot,
    marci_sync,
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

    // delete → true, then count drops
    let deleted = data(exec(handle, &json!({ "model": "User", "action": "delete", "id": id })));
    assert_eq!(deleted, json!(true));
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
