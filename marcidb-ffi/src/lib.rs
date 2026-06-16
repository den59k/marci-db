//! C ABI over the MarciDB engine for in-process embedding (Bun `bun:ffi`, Node `koffi`).
//!
//! Contract:
//! * Every entry point catches panics ([`catch_unwind`]) — unwinding across the C ABI is UB.
//! * Calls that return a `char*` hand back a Rust-allocated, NUL-terminated UTF-8 JSON **result
//!   envelope**; the caller copies it and must release it with [`marci_free_string`].
//!   Envelope shape: `{"ok":true,"data":<value>}` or `{"ok":false,"error":"…","kind":"…"}`,
//!   where `kind` is `bad_request` | `not_found` | `internal` (mirrors the server's `ApiError`).
//! * A [`DbHandle`] is **not** thread-safe: use one from a single thread at a time. FFI calls from a
//!   single JS runtime are serialized by its event loop, which satisfies this.

use std::cell::RefCell;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::ptr;
use std::sync::Arc;

use marcidb::{
    BatchErrorKind, MarciDB, MigrateApplyError, OpError, OpenOptions, ProviderError, ProviderRegistry,
    ReindexError, execute_batch, execute_op, parse_snapshot, serialize_snapshot, try_parse_schema,
};
use serde_json::{Value, json};

/// Opaque handle to an open embedded database. Created by [`marci_open`], freed by [`marci_close`].
pub struct DbHandle {
    db: MarciDB,
}

thread_local! {
    /// Holds the message for the most recent [`marci_open`] that returned NULL, retrievable via
    /// [`marci_last_error`]. Valid until the next FFI call on the same thread — copy it immediately.
    static LAST_ERROR: RefCell<Option<CString>> = const { RefCell::new(None) };
}

// ───────────────────────────────── error / envelope plumbing ─────────────────────────────────

/// A failure carrying the message and its error class (the envelope `kind`).
struct Fault {
    kind: &'static str,
    message: String,
}

impl Fault {
    fn bad_request(m: impl Into<String>) -> Self { Fault { kind: "bad_request", message: m.into() } }
    fn not_found(m: impl Into<String>) -> Self { Fault { kind: "not_found", message: m.into() } }
    fn internal(m: impl Into<String>) -> Self { Fault { kind: "internal", message: m.into() } }
}

fn ok_envelope(data: Value) -> String {
    json!({ "ok": true, "data": data }).to_string()
}

fn err_envelope(f: &Fault) -> String {
    json!({ "ok": false, "error": f.message, "kind": f.kind }).to_string()
}

/// Best-effort message out of a caught panic payload.
fn panic_message(p: Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = p.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = p.downcast_ref::<String>() {
        s.clone()
    } else {
        "panic in marcidb-ffi".to_string()
    }
}

/// Runs `f`, catching panics, and serializes the outcome into a freshly-allocated envelope C string.
fn run<F: FnOnce() -> Result<Value, Fault>>(f: F) -> *mut c_char {
    let envelope = match catch_unwind(AssertUnwindSafe(f)) {
        Ok(Ok(value)) => ok_envelope(value),
        Ok(Err(fault)) => err_envelope(&fault),
        Err(panic) => err_envelope(&Fault::internal(panic_message(panic))),
    };
    into_c_string(envelope)
}

/// Moves a Rust `String` into a heap C string owned by the caller (freed via [`marci_free_string`]).
fn into_c_string(s: String) -> *mut c_char {
    // JSON never contains interior NULs, so this conversion does not fail in practice.
    CString::new(s)
        .unwrap_or_else(|_| CString::new(r#"{"ok":false,"error":"result contained NUL","kind":"internal"}"#).unwrap())
        .into_raw()
}

fn set_last_error(msg: String) {
    let c = CString::new(msg).unwrap_or_else(|_| CString::new("error contained NUL").unwrap());
    LAST_ERROR.with(|e| *e.borrow_mut() = Some(c));
}

// ───────────────────────────────── argument helpers ─────────────────────────────────

/// Borrows an optional UTF-8 string from a C pointer (NULL → `None`). The returned slice is valid for the
/// duration of the call (it aliases the caller's buffer).
unsafe fn opt_str<'a>(ptr: *const c_char) -> Result<Option<&'a str>, Fault> {
    if ptr.is_null() {
        return Ok(None);
    }
    unsafe { CStr::from_ptr(ptr) }
        .to_str()
        .map(Some)
        .map_err(|_| Fault::bad_request("argument was not valid UTF-8"))
}

/// Like [`opt_str`] but NULL is an error.
unsafe fn req_str<'a>(ptr: *const c_char, name: &str) -> Result<&'a str, Fault> {
    unsafe { opt_str(ptr) }?.ok_or_else(|| Fault::bad_request(format!("argument '{}' must not be null", name)))
}

unsafe fn handle_mut<'a>(ptr: *mut DbHandle) -> Result<&'a mut DbHandle, Fault> {
    unsafe { ptr.as_mut() }.ok_or_else(|| Fault::bad_request("database handle is null"))
}

// ───────────────────────────────── providers ─────────────────────────────────

/// Builds the `@custom` index-provider registry. Features mirror the server's `build_providers`.
fn build_providers() -> Arc<ProviderRegistry> {
    #[allow(unused_mut)]
    let mut reg = ProviderRegistry::new();
    #[cfg(feature = "vector")]
    reg.register(Box::new(marci_vector_index::VectorIndexProvider::new()));
    #[cfg(feature = "fulltext")]
    reg.register(Box::new(marci_fulltext_index::FullTextProvider::new()));
    Arc::new(reg)
}

// ───────────────────────────────── lifecycle ─────────────────────────────────

/// Opens (creating the directory if needed) an embedded database at `path`. `options_json` may be NULL or
/// `{}`; recognized keys: `{ "disableFsync": bool }` (fast, durability-unsafe — for ephemeral/test DBs).
///
/// Returns an opaque handle, or NULL on failure (call [`marci_last_error`] for the message). A freshly
/// created DB has an empty schema — apply one via [`marci_sync`] / [`marci_migrate`].
#[unsafe(no_mangle)]
pub extern "C" fn marci_open(path: *const c_char, options_json: *const c_char) -> *mut DbHandle {
    match catch_unwind(AssertUnwindSafe(|| open_inner(path, options_json))) {
        Ok(Ok(handle)) => Box::into_raw(Box::new(handle)),
        Ok(Err(fault)) => {
            set_last_error(fault.message);
            ptr::null_mut()
        }
        Err(panic) => {
            set_last_error(panic_message(panic));
            ptr::null_mut()
        }
    }
}

fn open_inner(path: *const c_char, options_json: *const c_char) -> Result<DbHandle, Fault> {
    let path = unsafe { req_str(path, "path") }?;
    let options = match unsafe { opt_str(options_json) }? {
        Some(s) if !s.trim().is_empty() => parse_options(s)?,
        _ => OpenOptions::default(),
    };

    std::fs::create_dir_all(path)
        .map_err(|e| Fault::internal(format!("cannot create data dir '{}': {}", path, e)))?;

    let db = MarciDB::try_open_with_options(path, &options)
        .map_err(|e| Fault::internal(format!("failed to open database '{}': {:?}", path, e)))?
        .with_providers(build_providers());

    Ok(DbHandle { db })
}

fn parse_options(s: &str) -> Result<OpenOptions, Fault> {
    let v: Value = serde_json::from_str(s).map_err(|e| Fault::bad_request(format!("invalid options JSON: {}", e)))?;
    let mut opts = OpenOptions::default();
    if let Some(b) = v.get("disableFsync").and_then(|x| x.as_bool()) {
        opts.disable_fsync = b;
    }
    Ok(opts)
}

/// Closes and frees a handle (releasing the underlying storage lock). NULL is a no-op.
#[unsafe(no_mangle)]
pub extern "C" fn marci_close(handle: *mut DbHandle) {
    if handle.is_null() {
        return;
    }
    let _ = catch_unwind(AssertUnwindSafe(|| unsafe { drop(Box::from_raw(handle)) }));
}

/// Message of the most recent [`marci_open`] that returned NULL (NULL if none). Owned by the library —
/// **do not** free it, and copy it before the next FFI call on this thread.
#[unsafe(no_mangle)]
pub extern "C" fn marci_last_error() -> *const c_char {
    LAST_ERROR.with(|e| e.borrow().as_ref().map_or(ptr::null(), |c| c.as_ptr()))
}

/// Frees a string returned by any `marci_*` call. NULL is a no-op.
#[unsafe(no_mangle)]
pub extern "C" fn marci_free_string(ptr: *mut c_char) {
    if ptr.is_null() {
        return;
    }
    unsafe { drop(CString::from_raw(ptr)) };
}

// ───────────────────────────────── data ops ─────────────────────────────────

/// Executes one operation. `op_json` is either a single `{ model, action, … }` object (search-capable,
/// own transaction) or an array of such objects (atomic transaction — same semantics as the server's
/// `$transaction`; no `$near`/`$search` inside). Returns the result envelope.
#[unsafe(no_mangle)]
pub extern "C" fn marci_exec(handle: *mut DbHandle, op_json: *const c_char) -> *mut c_char {
    run(|| {
        let h = unsafe { handle_mut(handle) }?;
        let input = unsafe { req_str(op_json, "op_json") }?;
        let value: Value =
            serde_json::from_str(input).map_err(|e| Fault::bad_request(format!("invalid op JSON: {}", e)))?;
        exec_value(&h.db, value)
    })
}

fn exec_value(db: &MarciDB, value: Value) -> Result<Value, Fault> {
    match value {
        Value::Array(ops) => match execute_batch(db, &ops) {
            Ok(results) => Ok(Value::Array(results)),
            Err(e) if matches!(e.kind, BatchErrorKind::Storage(_)) => Err(Fault::internal(e.to_string())),
            Err(e) => Err(Fault::bad_request(e.to_string())),
        },
        obj @ Value::Object(_) => execute_op(db, &obj).map_err(op_fault),
        _ => Err(Fault::bad_request("operation must be a JSON object or an array of operations")),
    }
}

fn op_fault(e: OpError) -> Fault {
    if e.is_storage() {
        return Fault::internal(e.to_string());
    }
    match &e {
        OpError::UnknownModel(_) | OpError::UnknownAction(_) => Fault::not_found(e.to_string()),
        _ => Fault::bad_request(e.to_string()),
    }
}

// ───────────────────────────────── schema / admin ─────────────────────────────────

/// Declarative schema sync (`$sync`): `schema_text` is `.marci` source; the engine diffs it against the
/// stored schema and applies the difference. Creates models on first sync. Mirrors the server's `$sync`.
#[unsafe(no_mangle)]
pub extern "C" fn marci_sync(handle: *mut DbHandle, schema_text: *const c_char) -> *mut c_char {
    run(|| {
        let h = unsafe { handle_mut(handle) }?;
        let schema_text = unsafe { req_str(schema_text, "schema_text") }?;

        let mut new_schema = try_parse_schema(schema_text).map_err(|e| Fault::bad_request(e.to_string()))?;
        marcidb_schema::reconcile(&mut new_schema, &h.db.schema);
        let ops = marcidb_schema::diff(&h.db.schema, &new_schema).map_err(|e| Fault::bad_request(e.to_string()))?;
        h.db.commit_schema(new_schema, &ops).map_err(migrate_fault)?;
        Ok(Value::Null)
    })
}

/// Imperative migration (`$migrate`): `migration_text` is `.march` action source; laid onto the current
/// snapshot and applied. Mirrors the server's `$migrate`.
#[unsafe(no_mangle)]
pub extern "C" fn marci_migrate(handle: *mut DbHandle, migration_text: *const c_char) -> *mut c_char {
    run(|| {
        let h = unsafe { handle_mut(handle) }?;
        let migration_text = unsafe { req_str(migration_text, "migration_text") }?;

        let cur = serialize_snapshot(&h.db.schema);
        let new_text = marcidb_schema::evolve(&cur, migration_text).map_err(|e| Fault::bad_request(e.to_string()))?;
        let new_schema = parse_snapshot(&new_text).map_err(|e| Fault::bad_request(e.to_string()))?;
        let ops = marcidb_schema::migration_ops(migration_text).map_err(|e| Fault::bad_request(e.to_string()))?;
        h.db.commit_schema(new_schema, &ops).map_err(migrate_fault)?;
        Ok(Value::Null)
    })
}

/// Returns the current materialized schema snapshot text (the `data` field is the snapshot string).
#[unsafe(no_mangle)]
pub extern "C" fn marci_snapshot(handle: *mut DbHandle) -> *mut c_char {
    run(|| {
        let h = unsafe { handle_mut(handle) }?;
        Ok(Value::String(serialize_snapshot(&h.db.schema)))
    })
}

/// Rebuilds every model's `@custom` indexes from current data. Returns `{ ok, indexed: <tree count> }`.
#[unsafe(no_mangle)]
pub extern "C" fn marci_reindex_all(handle: *mut DbHandle) -> *mut c_char {
    run(|| {
        let h = unsafe { handle_mut(handle) }?;
        let mut total = 0usize;
        for entity in h.db.schema.models.iter() {
            total += h.db.reindex_entity(entity).map_err(reindex_fault)?;
        }
        Ok(json!({ "ok": true, "indexed": total }))
    })
}

fn migrate_fault(e: MigrateApplyError) -> Fault {
    match e {
        MigrateApplyError::Storage(_) => Fault::internal(format!("{:?}", e)),
        _ => Fault::bad_request(format!("{}", e)),
    }
}

fn reindex_fault(e: ReindexError) -> Fault {
    match &e {
        ReindexError::Storage(_) | ReindexError::Provider(ProviderError::Storage(_)) => Fault::internal(e.to_string()),
        _ => Fault::bad_request(e.to_string()),
    }
}
