//! The HTTP API of MarciDB: routing, handlers, errors and the backup routes, over a [`ServerContext`] that
//! holds the databases, and [`serve`], the accept loop. Two hosts use it: the `marcidb-server` binary (many
//! databases under a directory) and `marcidb-ffi` — an embedded app's maintenance port over the one
//! database the app has open.

use std::{collections::{HashMap, HashSet}, convert::Infallible, future::Future, path::{Path, PathBuf}, sync::{Arc, Mutex, MutexGuard, RwLock, atomic::{AtomicBool, Ordering}}, time::{Duration, Instant}};

use http_body_util::Full;
use hyper::{Method, Request, Response, StatusCode, body::Bytes, header, server::conn::http1, service::service_fn};
use hyper_util::{rt::TokioIo, server::graceful::GracefulShutdown};
use marcidb::{BackupInfo, MarciDB, ProviderRegistry, RestoreStaged};
use tokio::{net::TcpListener, task::JoinSet};

use crate::{backup::{handle_backup, handle_restore}, errors::ApiError, handlers::{handle_upsert, handle_aggregate, handle_count, handle_delete, handle_delete_many, handle_find_first, handle_find_many, handle_insert, handle_journal_drop, handle_journal_open, handle_journal_read, handle_migrate, handle_reindex, handle_reindex_all, handle_snapshot, handle_sync, handle_sync_plan, handle_transaction, handle_update, handle_update_many}, helpers::{ResponseBody, boxed}};

mod backup;
mod handlers;
mod errors;
mod helpers;
#[cfg(test)]
mod tests;

/// How long a `$restore` waits for in-flight requests on the database it replaces before giving up (503).
const RESTORE_EVICT_TIMEOUT: Duration = Duration::from_secs(30);

/// How long [`serve`] lets in-flight requests finish once its shutdown future fires; the rest are aborted.
pub const SHUTDOWN_GRACE: Duration = Duration::from_secs(5);

/// The databases a context serves. Each has its own `RwLock` (migration is exclusive, data is shared).
pub struct ServerContext {
    databases: Databases,
    /// Bearer token every request must carry (`MARCI_TOKEN` / `--token`); `None` = open server (local dev).
    token: Option<String>,
    /// One stdout line per request: on in the server, off on a maintenance port (an app's stdout is not ours).
    log_requests: bool,
    /// Set by [`serve`] once it stops accepting: a waiting journal read answers early instead of holding
    /// the shutdown for its whole wait.
    stopping: AtomicBool,
}

enum Databases {
    /// Many databases under one root, opened lazily; `$migrate` / `$sync` create one that doesn't exist yet.
    Directory(Directory),
    /// One database an embedded app has open, under a fixed name: the app's maintenance port. The app owns
    /// the handle, so nothing here creates, drops or replaces it.
    Single { name: String, db: Arc<RwLock<MarciDB>> },
}

struct Directory {
    root: PathBuf,
    dbs: Mutex<HashMap<String, Slot>>,
    /// Names a `$restore` is writing to (upload → staging → swap). A second restore of the same name is
    /// refused: both would use the same staging directory.
    restores: Mutex<HashSet<String>>,
    /// `@custom` index providers, shared into every DB opened here. Built once at startup.
    providers: Arc<ProviderRegistry>,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// The answer to a restore on a maintenance port.
fn restore_refused() -> ApiError {
    ApiError::BadRequest("restoring is not available on an embedded maintenance port — the app process owns this database; restore it at startup with restoreDatabase(dir, file) instead".to_string())
}

/// The answer to `DELETE /:db` on a maintenance port.
fn drop_refused() -> ApiError {
    ApiError::BadRequest("dropping a database is not available on an embedded maintenance port — the app process owns this database; close it in the app and remove its directory instead".to_string())
}

/// A database the server holds by name.
enum Slot {
    Open(Arc<RwLock<MarciDB>>),
    /// A `$restore` is swapping its directory: requests for it are answered 503 instead of reopening the
    /// directory that is being replaced.
    Swapping,
}

/// Marks a name as being restored for as long as it lives (see [`ServerContext::begin_restore`]).
pub struct RestoreGuard {
    ctx: Arc<ServerContext>,
    name: String,
}

impl Drop for RestoreGuard {
    fn drop(&mut self) {
        if let Databases::Directory(dir) = &self.ctx.databases {
            lock(&dir.restores).remove(&self.name);
        }
    }
}

impl ServerContext {
    /// Many databases under `root` (the server): each opens on first use, `$migrate` / `$sync` create one.
    pub fn directory(root: PathBuf, token: Option<String>) -> Self {
        let dir = Directory { root, dbs: Mutex::new(HashMap::new()), restores: Mutex::new(HashSet::new()), providers: build_providers() };
        Self::with(Databases::Directory(dir), token)
    }

    /// One open database under `name` (an embedded maintenance port). Requests share the handle with the
    /// app — the same `RwLock` — so a `$sync` here changes the schema the app sees. Other names are a 404;
    /// restoring and dropping are refused (400): the app owns the database.
    pub fn single(name: String, db: Arc<RwLock<MarciDB>>, token: Option<String>) -> Self {
        Self::with(Databases::Single { name, db }, token)
    }

    fn with(databases: Databases, token: Option<String>) -> Self {
        ServerContext { databases, token, log_requests: true, stopping: AtomicBool::new(false) }
    }

    /// Whether every request is logged to stdout (on by default).
    pub fn log_requests(mut self, on: bool) -> Self {
        self.log_requests = on;
        self
    }

    /// Whether [`serve`] has stopped accepting and is letting the last requests finish.
    pub fn is_stopping(&self) -> bool {
        self.stopping.load(Ordering::SeqCst)
    }

    /// The directory-mode state, or `refused` on a maintenance port.
    fn directory_or(&self, refused: fn() -> ApiError) -> Result<&Directory, ApiError> {
        match &self.databases {
            Databases::Directory(dir) => Ok(dir),
            Databases::Single { .. } => Err(refused()),
        }
    }

    /// Where uploads and restores go: the data directory (refused on a maintenance port).
    pub(crate) fn restore_root(&self) -> Result<&Path, ApiError> {
        Ok(&self.directory_or(restore_refused)?.root)
    }

    /// A 404 for a name a maintenance port does not serve (whatever the route would do with it).
    pub(crate) fn ensure_served(&self, name: &str) -> Result<(), ApiError> {
        match &self.databases {
            Databases::Single { name: served, .. } if served != name => Err(Self::not_served(served)),
            _ => Ok(()),
        }
    }

    fn not_served(served: &str) -> ApiError {
        ApiError::NotFound(format!("this port serves only database '{}'", served))
    }

    #[cfg(test)]
    fn lock_dbs(&self) -> MutexGuard<'_, HashMap<String, Slot>> {
        lock(&self.directory_or(drop_refused).expect("a directory context").dbs)
    }

    fn swapping(name: &str) -> ApiError {
        ApiError::Unavailable(format!("database '{}' is being restored — retry shortly", name))
    }

    /// Claims `name` for a restore; a restore of that name already running is a 503.
    pub fn begin_restore(self: &Arc<Self>, name: &str) -> Result<RestoreGuard, ApiError> {
        if !lock(&self.directory_or(restore_refused)?.restores).insert(name.to_string()) {
            return Err(ApiError::Unavailable(format!("a restore of '{}' is already running", name)));
        }
        Ok(RestoreGuard { ctx: self.clone(), name: name.to_string() })
    }

    /// Puts a staged restore in place of the database `name` — open or not, existing or not — and opens
    /// it. Requests for `name` get a 503 during the swap; requests already running finish first (up to
    /// [`RESTORE_EVICT_TIMEOUT`], then the restore is abandoned with a 503 and the old database stays).
    pub fn replace_db(&self, name: &str, staged: RestoreStaged) -> Result<BackupInfo, ApiError> {
        self.replace_db_within(name, staged, RESTORE_EVICT_TIMEOUT)
    }

    fn replace_db_within(&self, name: &str, staged: RestoreStaged, timeout: Duration) -> Result<BackupInfo, ApiError> {
        let dir = match self.directory_or(restore_refused) {
            Ok(dir) => dir,
            Err(refused) => {
                staged.abandon();
                return Err(refused);
            }
        };
        let old = match lock(&dir.dbs).insert(name.to_string(), Slot::Swapping) {
            Some(Slot::Open(db)) => Some(db),
            Some(Slot::Swapping) => return Err(Self::swapping(name)),
            None => None,
        };

        if let Some(db) = old {
            // In-flight requests hold clones of the handle; the swap needs the last one.
            let deadline = Instant::now() + timeout;
            while Arc::strong_count(&db) > 1 {
                if Instant::now() >= deadline {
                    lock(&dir.dbs).insert(name.to_string(), Slot::Open(db));
                    staged.abandon();
                    return Err(ApiError::Unavailable(format!("database '{}' stayed busy — the restore was not applied, retry later", name)));
                }
                std::thread::sleep(Duration::from_millis(20));
            }
            drop(db); // closes the database and releases its directory lock
        }

        let committed = staged.commit();
        let mut dbs = lock(&dir.dbs);
        match committed {
            Ok((db, info)) => {
                dbs.insert(name.to_string(), Slot::Open(Arc::new(RwLock::new(db.with_providers(dir.providers.clone())))));
                Ok(info)
            }
            Err(e) => {
                // A failed swap puts the old directory back; the next request reopens whatever is there.
                dbs.remove(name);
                Err(ApiError::from(e))
            }
        }
    }

    /// Drops a database: closes the open handle (if any) and removes its directory. A missing DB → NotFound.
    /// Used by hosts that own many databases (a draft DB is recreated when its schema can't be migrated).
    pub fn drop_db(&self, name: &str) -> Result<(), ApiError> {
        if !is_valid_db_name(name) {
            return Err(ApiError::BadRequest(format!("invalid database name '{}'", name)));
        }
        let dir = self.directory_or(drop_refused)?;
        let path = dir.root.join(name);
        {
            let mut dbs = lock(&dir.dbs);
            if matches!(dbs.get(name), Some(Slot::Swapping)) {
                return Err(Self::swapping(name));
            }
            let existed = dbs.remove(name).is_some();
            if !existed && !path.exists() {
                return Err(ApiError::NotFound(format!("database '{}' not found", name)));
            }
        }
        // The Arc handed out to in-flight requests keeps the engine alive until they finish; new requests
        // no longer find the entry and would reopen from disk — which is gone after this.
        if path.exists() {
            std::fs::remove_dir_all(&path).map_err(|e| ApiError::Internal(format!("failed to remove database '{}': {}", name, e)))?;
        }
        Ok(())
    }

    /// DB by name. `allow_create` (for `$migrate`) creates an empty DB; otherwise a nonexistent one → NotFound.
    /// A maintenance port has its one database under its name, whatever `allow_create` says, and no other.
    pub fn get_db(&self, name: &str, allow_create: bool) -> Result<Arc<RwLock<MarciDB>>, ApiError> {
        if !is_valid_db_name(name) {
            return Err(ApiError::BadRequest(format!("invalid database name '{}'", name)));
        }

        let dir = match &self.databases {
            Databases::Directory(dir) => dir,
            Databases::Single { name: served, db } => {
                return if served == name { Ok(db.clone()) } else { Err(Self::not_served(served)) };
            }
        };
        let mut dbs = lock(&dir.dbs);
        match dbs.get(name) {
            Some(Slot::Open(db)) => return Ok(db.clone()),
            Some(Slot::Swapping) => return Err(Self::swapping(name)),
            None => {}
        }

        let path = dir.root.join(name);
        if !allow_create && !path.exists() {
            return Err(ApiError::NotFound(format!("database '{}' not found", name)));
        }

        // canopydb requires an existing DB directory
        std::fs::create_dir_all(&path).map_err(|e| ApiError::Internal(e.to_string()))?;

        let db = MarciDB::try_open(path.to_str().unwrap())
            .map_err(|e| ApiError::Internal(format!("failed to open database '{}': {}", name, e)))?
            .with_providers(dir.providers.clone());
        let db = Arc::new(RwLock::new(db));
        dbs.insert(name.to_string(), Slot::Open(db.clone()));
        Ok(db)
    }
}

/// Builds the `@custom` index-provider registry. Modules are compiled in behind cargo features so the
/// default build stays dependency-light (and stable-toolchain friendly): `--features vector` adds the
/// vector index (`@custom(vector, …)`); future modules (full-text, …) register here the same way.
fn build_providers() -> Arc<ProviderRegistry> {
    #[allow(unused_mut)]
    let mut reg = ProviderRegistry::new();
    #[cfg(feature = "vector")]
    reg.register(Box::new(marci_vector_index::VectorIndexProvider::new()));
    #[cfg(feature = "fulltext")]
    reg.register(Box::new(marci_fulltext_index::FullTextProvider::new()));
    Arc::new(reg)
}

/// The DB name is a path segment, so no slashes/dots (protection against path traversal)
pub fn is_valid_db_name(name: &str) -> bool {
    !name.is_empty() && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

// Public handler — catches all errors (hyper only ever sees Ok) and logs the request
pub async fn handle(
    req: Request<hyper::body::Incoming>,
    ctx: Arc<ServerContext>,
) -> Result<Response<ResponseBody>, Infallible> {
    let method = req.method().clone();
    let path = req.uri().path().to_string();
    let start = std::time::Instant::now();

    let log = ctx.log_requests;
    let response = handle_inner(req, ctx)
        .await
        .unwrap_or_else(|e| boxed(e.into_response()));

    if log {
        log_request(&method, &path, response.status(), start.elapsed());
    }
    Ok(response)
}

/// Serves `ctx` on `listener` until `shutdown` completes. Then it stops accepting (the port is free at
/// once), lets in-flight requests finish for up to [`SHUTDOWN_GRACE`] — idle keep-alive connections close
/// right away — and aborts what is left.
pub async fn serve(listener: TcpListener, ctx: Arc<ServerContext>, shutdown: impl Future<Output = ()>) {
    serve_with_grace(listener, ctx, shutdown, SHUTDOWN_GRACE).await
}

/// [`serve`] with an explicit grace period.
pub async fn serve_with_grace(listener: TcpListener, ctx: Arc<ServerContext>, shutdown: impl Future<Output = ()>, grace: Duration) {
    let graceful = GracefulShutdown::new();
    let mut connections = JoinSet::new();
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, _) = match accepted {
                    Ok(conn) => conn,
                    Err(e) => { eprintln!("  accept error: {}", e); continue; }
                };
                let log = ctx.log_requests;
                let service_ctx = ctx.clone();
                let conn = http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), service_fn(move |req| handle(req, service_ctx.clone())));
                let conn = graceful.watch(conn);
                connections.spawn(async move {
                    if let Err(err) = conn.await && log {
                        eprintln!("  connection error: {}", err);
                    }
                });
            }
            // Reap finished connections, so the set does not grow with every client served.
            Some(_) = connections.join_next(), if !connections.is_empty() => {}
            _ = &mut shutdown => break,
        }
    }

    drop(listener);
    ctx.stopping.store(true, Ordering::SeqCst);
    if tokio::time::timeout(grace, graceful.shutdown()).await.is_err() {
        connections.abort_all();
    }
    while connections.join_next().await.is_some() {}
}

/// `?plan=1` on `$sync` — dry run (see `handle_sync_plan`)
fn plan_requested(req: &Request<hyper::body::Incoming>) -> bool {
    req.uri().query().is_some_and(|q| q.split('&').any(|kv| matches!(kv, "plan" | "plan=1" | "plan=true")))
}

/// Health and auth, then the one streamed route (`$backup`), then every buffered route ([`route`]).
async fn handle_inner(
    req: Request<hyper::body::Incoming>,
    ctx: Arc<ServerContext>,
) -> Result<Response<ResponseBody>, ApiError> {
    let method = req.method().clone();
    let path = req.uri().path().trim_matches('/').to_string();

    // Liveness probe (docker healthcheck / a host waiting for the container) — before auth on purpose
    if method == Method::GET && path == "$health" {
        return Ok(boxed(Response::new(Full::new(Bytes::from(format!("{{\"ok\":true,\"version\":\"{}\"}}", env!("CARGO_PKG_VERSION")))))));
    }

    authorize(&req, &ctx)?;

    // A backup streams its body (see backup.rs)
    if method == Method::GET && let Some((db_name, "$backup")) = path.split_once('/') {
        return handle_backup(ctx, db_name.to_string()).await;
    }

    route(req, ctx, method, path).await.map(boxed)
}

/// `Authorization: Bearer <token>` when the server was started with one.
fn authorize(req: &Request<hyper::body::Incoming>, ctx: &ServerContext) -> Result<(), ApiError> {
    if let Some(expected) = &ctx.token {
        let given = req.headers().get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .map(str::trim);
        if given != Some(expected.as_str()) {
            return Err(ApiError::Unauthorized("missing or invalid bearer token".to_string()));
        }
    }
    Ok(())
}

// "/{db}/$migrate" | "/{db}/$sync" | "/{db}/$transaction" | "/{db}/$restore" | "/{db}/{model}/{action}[/{id}]"
// Actions taking an {id}: update, delete. The rest read their arguments from the body.
async fn route(
    req: Request<hyper::body::Incoming>,
    ctx: Arc<ServerContext>,
    method: Method,
    path: String,
) -> Result<Response<Full<Bytes>>, ApiError> {
    // Drop a whole database (closes it and removes its directory)
    if method == Method::DELETE && !path.contains('/') && !path.is_empty() {
        ctx.drop_db(&path)?;
        return Ok(Response::new(Full::new(Bytes::new())));
    }

    let (db_name, rest) = path.split_once('/')
        .ok_or_else(|| ApiError::BadRequest("expected path /<db>/...".to_string()))?;
    let db_name = db_name.to_string();

    // Current server snapshot (for client-side reconciliation) — a read, hence GET
    if method == Method::GET && rest == "$snapshot" {
        return handle_snapshot(ctx, db_name).await;
    }

    if method == Method::POST {
        match rest {
            "$migrate" => return handle_migrate(req, ctx, db_name).await,
            "$sync" => return if plan_requested(&req) { handle_sync_plan(req, ctx, db_name).await } else { handle_sync(req, ctx, db_name).await },
            "$transaction" => return handle_transaction(req, ctx, db_name).await,
            // Rebuild every model's @custom indexes
            "$reindex" => return handle_reindex_all(ctx, db_name).await,
            // Replace the database (or `?into=<name>`) with an uploaded backup
            "$restore" => return handle_restore(req, ctx, db_name).await,
            _ => {}
        }
    }

    let parts: Vec<&str> = rest.splitn(3, '/').collect();
    let (model, action, id) = match parts.as_slice() {
        [m, a, id] if !m.is_empty() && !a.is_empty() => (m.to_string(), a.to_string(), Some(id.to_string())),
        [m, a] if !m.is_empty() && !a.is_empty() => (m.to_string(), a.to_string(), None),
        _ => return Err(ApiError::BadRequest(format!("Invalid path: '/{}'", path))),
    };

    match (&method, action.as_str()) {
        (&Method::POST, "insert") => handle_insert(req, ctx, db_name, model).await,
        (&Method::POST, "findMany") => handle_find_many(req, ctx, db_name, model).await,
        (&Method::POST, "findFirst") => handle_find_first(req, ctx, db_name, model).await,
        (&Method::POST, "count") => handle_count(req, ctx, db_name, model).await,
        (&Method::POST, "aggregate") => handle_aggregate(req, ctx, db_name, model).await,
        (&Method::POST, "upsert") => handle_upsert(req, ctx, db_name, model).await,
        (&Method::POST, "updateMany") => handle_update_many(req, ctx, db_name, model).await,
        (&Method::POST, "deleteMany") => handle_delete_many(req, ctx, db_name, model).await,
        (&Method::POST, "update") => {
            let Some(id) = id else { return Err(ApiError::BadRequest("Param :itemId required".to_string())) };
            handle_update(req, id, ctx, db_name, model).await
        },
        (&Method::POST, "delete") => {
            let Some(id) = id else { return Err(ApiError::BadRequest("Param :itemId required".to_string())) };
            handle_delete(id, ctx, db_name, model).await
        },
        // Rebuild this model's @custom indexes
        (&Method::POST, "$reindex") => handle_reindex(ctx, db_name, model).await,
        // A journal of this model's changes: create, read (and confirm), drop
        (_, "$journal") => {
            let Some(name) = id else { return Err(ApiError::BadRequest("Param :journal required".to_string())) };
            match method {
                Method::POST => handle_journal_open(req, ctx, db_name, model, name).await,
                Method::GET => handle_journal_read(req, ctx, db_name, model, name).await,
                Method::DELETE => handle_journal_drop(ctx, db_name, model, name).await,
                _ => Err(ApiError::NotFound(format!("Route {} /{} not found", method, path))),
            }
        },
        _ => Err(ApiError::NotFound(format!("Route {} /{} not found", method, path))),
    }
}

/// One line per request: `<time>  <method>  <status>  <ms>  <path>`
fn log_request(method: &Method, path: &str, status: StatusCode, elapsed: std::time::Duration) {
    let ts = chrono::Local::now().format("%Y-%m-%d %H:%M:%S");
    let ms = elapsed.as_secs_f64() * 1000.0;
    println!("  {ts}  {m:<6} {code:>3}  {ms:>6.1} ms  {path}", m = method.as_str(), code = status.as_u16());
}

/// Names of databases already present in the data directory (each DB is a sub-directory; a restore's
/// staging or set-aside directory has a `.` in its name, which no database name has)
pub fn list_databases(root: &std::path::Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(root)
        .into_iter()
        .flatten()
        .flatten()
        .filter(|e| e.path().is_dir())
        .filter_map(|e| e.file_name().into_string().ok())
        .filter(|n| is_valid_db_name(n))
        .collect();
    names.sort();
    names
}
