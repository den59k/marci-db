//! The HTTP API of MarciDB: routing, handlers, errors and the backup routes, over a [`ServerContext`] that
//! holds the databases. The `marcidb-server` binary is a thin shell around it (config, banner, accept loop).

use std::{collections::{HashMap, HashSet}, convert::Infallible, path::PathBuf, sync::{Arc, Mutex, MutexGuard, RwLock}, time::{Duration, Instant}};

use http_body_util::Full;
use hyper::{Method, Request, Response, StatusCode, body::Bytes, header};
use marcidb::{BackupInfo, MarciDB, ProviderRegistry, RestoreStaged};

use crate::{backup::{handle_backup, handle_restore}, errors::ApiError, handlers::{handle_upsert, handle_aggregate, handle_count, handle_delete, handle_delete_many, handle_find_first, handle_find_many, handle_insert, handle_journal_drop, handle_journal_open, handle_journal_read, handle_migrate, handle_reindex, handle_reindex_all, handle_snapshot, handle_sync, handle_sync_plan, handle_transaction, handle_update, handle_update_many}, helpers::{ResponseBody, boxed}};

mod backup;
mod handlers;
mod errors;
mod helpers;
#[cfg(test)]
mod tests;

/// How long a `$restore` waits for in-flight requests on the database it replaces before giving up (503).
const RESTORE_EVICT_TIMEOUT: Duration = Duration::from_secs(30);

/// Hosts multiple DBs from a single directory. A DB is opened lazily on first access;
/// `$migrate` creates it if it doesn't exist yet. Each DB has its own `RwLock` (migration is exclusive, data is shared)
pub struct ServerContext {
    root: PathBuf,
    dbs: Mutex<HashMap<String, Slot>>,
    /// Names a `$restore` is writing to (upload → staging → swap). A second restore of the same name is
    /// refused: both would use the same staging directory.
    restores: Mutex<HashSet<String>>,
    /// `@custom` index providers, shared into every DB this server opens. Built once at startup.
    providers: Arc<ProviderRegistry>,
    /// Bearer token every request must carry (`MARCI_TOKEN` / `--token`); `None` = open server (local dev).
    token: Option<String>,
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
        self.ctx.restores.lock().unwrap_or_else(|e| e.into_inner()).remove(&self.name);
    }
}

impl ServerContext {
    pub fn new(root: PathBuf, token: Option<String>) -> Self {
        ServerContext { root, dbs: Mutex::new(HashMap::new()), restores: Mutex::new(HashSet::new()), providers: build_providers(), token }
    }

    fn lock_dbs(&self) -> MutexGuard<'_, HashMap<String, Slot>> {
        self.dbs.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn swapping(name: &str) -> ApiError {
        ApiError::Unavailable(format!("database '{}' is being restored — retry shortly", name))
    }

    /// Claims `name` for a restore; a restore of that name already running is a 503.
    pub fn begin_restore(self: &Arc<Self>, name: &str) -> Result<RestoreGuard, ApiError> {
        if !self.restores.lock().unwrap_or_else(|e| e.into_inner()).insert(name.to_string()) {
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
        let old = match self.lock_dbs().insert(name.to_string(), Slot::Swapping) {
            Some(Slot::Open(db)) => Some(db),
            Some(Slot::Swapping) => return Err(Self::swapping(name)),
            None => None,
        };

        if let Some(db) = old {
            // In-flight requests hold clones of the handle; the swap needs the last one.
            let deadline = Instant::now() + timeout;
            while Arc::strong_count(&db) > 1 {
                if Instant::now() >= deadline {
                    self.lock_dbs().insert(name.to_string(), Slot::Open(db));
                    staged.abandon();
                    return Err(ApiError::Unavailable(format!("database '{}' stayed busy — the restore was not applied, retry later", name)));
                }
                std::thread::sleep(Duration::from_millis(20));
            }
            drop(db); // closes the database and releases its directory lock
        }

        let committed = staged.commit();
        let mut dbs = self.lock_dbs();
        match committed {
            Ok((db, info)) => {
                dbs.insert(name.to_string(), Slot::Open(Arc::new(RwLock::new(db.with_providers(self.providers.clone())))));
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
        let path = self.root.join(name);
        {
            let mut dbs = self.lock_dbs();
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

    /// DB by name. `allow_create` (for `$migrate`) creates an empty DB; otherwise a nonexistent one → NotFound
    pub fn get_db(&self, name: &str, allow_create: bool) -> Result<Arc<RwLock<MarciDB>>, ApiError> {
        if !is_valid_db_name(name) {
            return Err(ApiError::BadRequest(format!("invalid database name '{}'", name)));
        }

        let mut dbs = self.lock_dbs();
        match dbs.get(name) {
            Some(Slot::Open(db)) => return Ok(db.clone()),
            Some(Slot::Swapping) => return Err(Self::swapping(name)),
            None => {}
        }

        let path = self.root.join(name);
        if !allow_create && !path.exists() {
            return Err(ApiError::NotFound(format!("database '{}' not found", name)));
        }

        // canopydb requires an existing DB directory
        std::fs::create_dir_all(&path).map_err(|e| ApiError::Internal(e.to_string()))?;

        let db = MarciDB::try_open(path.to_str().unwrap())
            .map_err(|e| ApiError::Internal(format!("failed to open database '{}': {}", name, e)))?
            .with_providers(self.providers.clone());
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

    let response = handle_inner(req, ctx)
        .await
        .unwrap_or_else(|e| boxed(e.into_response()));

    log_request(&method, &path, response.status(), start.elapsed());
    Ok(response)
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
