use std::{collections::HashMap, convert::Infallible, net::{IpAddr, SocketAddr}, path::PathBuf, sync::{Arc, Mutex, RwLock}};

use http_body_util::Full;
use hyper::{Method, Request, Response, StatusCode, body::Bytes, server::conn::http1, service::service_fn};
use hyper_util::rt::TokioIo;
use marcidb::{MarciDB, ProviderRegistry};
use tokio::{fs, net::TcpListener};

use crate::{errors::ApiError, handlers::{handle_aggregate, handle_count, handle_delete, handle_find_first, handle_find_many, handle_insert, handle_migrate, handle_reindex, handle_reindex_all, handle_snapshot, handle_sync, handle_transaction, handle_update}};

mod handlers;
mod errors;
mod helpers;

/// Hosts multiple DBs from a single directory. A DB is opened lazily on first access;
/// `$migrate` creates it if it doesn't exist yet. Each DB has its own `RwLock` (migration is exclusive, data is shared)
pub struct ServerContext {
    root: PathBuf,
    dbs: Mutex<HashMap<String, Arc<RwLock<MarciDB>>>>,
    /// `@custom` index providers, shared into every DB this server opens. Built once at startup.
    providers: Arc<ProviderRegistry>,
}

impl ServerContext {
    fn new(root: PathBuf) -> Self {
        ServerContext { root, dbs: Mutex::new(HashMap::new()), providers: build_providers() }
    }

    /// DB by name. `allow_create` (for `$migrate`) creates an empty DB; otherwise a nonexistent one → NotFound
    pub fn get_db(&self, name: &str, allow_create: bool) -> Result<Arc<RwLock<MarciDB>>, ApiError> {
        if !is_valid_db_name(name) {
            return Err(ApiError::BadRequest(format!("invalid database name '{}'", name)));
        }

        let mut dbs = self.dbs.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(db) = dbs.get(name) {
            return Ok(db.clone());
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
        dbs.insert(name.to_string(), db.clone());
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
fn is_valid_db_name(name: &str) -> bool {
    !name.is_empty() && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

// Public handler — catches all errors (hyper only ever sees Ok) and logs the request
pub async fn handle(
    req: Request<hyper::body::Incoming>,
    ctx: Arc<ServerContext>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let method = req.method().clone();
    let path = req.uri().path().to_string();
    let start = std::time::Instant::now();

    let response = handle_inner(req, ctx)
        .await
        .unwrap_or_else(|e| e.into_response());

    log_request(&method, &path, response.status(), start.elapsed());
    Ok(response)
}

// "/{db}/$migrate" | "/{db}/$sync" | "/{db}/$transaction" | "/{db}/{model}/{action}[/{id}]"
async fn handle_inner(
    req: Request<hyper::body::Incoming>,
    ctx: Arc<ServerContext>,
) -> Result<Response<Full<Bytes>>, ApiError> {
    let method = req.method().clone();
    let path = req.uri().path().trim_matches('/').to_string();

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
            "$sync" => return handle_sync(req, ctx, db_name).await,
            "$transaction" => return handle_transaction(req, ctx, db_name).await,
            // Rebuild every model's @custom indexes
            "$reindex" => return handle_reindex_all(ctx, db_name).await,
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
        _ => Err(ApiError::NotFound(format!("Route {} /{} not found", method, path))),
    }
}

/// Resolved server configuration. Precedence: CLI flag > environment variable > built-in default.
struct Config {
    host: IpAddr,
    port: u16,
    data_dir: String,
}

/// Parses CLI flags layered over environment variables over defaults. Accepts `--flag value` and
/// `--flag=value`. `--help`/`--version` print and exit; an unknown flag or a bad value exits with code 2.
fn parse_config() -> Config {
    // seed from the environment; CLI flags override below
    let mut host = std::env::var("MARCI_HOST").ok();
    let mut port = std::env::var("PORT").ok();
    let mut data = std::env::var("MARCI_DATA").ok();

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        let (flag, inline) = match arg.split_once('=') {
            Some((f, v)) => (f.to_string(), Some(v.to_string())),
            None => (arg.clone(), None),
        };
        match flag.as_str() {
            "-h" | "--help" => { print_help(); std::process::exit(0); }
            "-v" | "--version" => { println!("marcidb-server {}", env!("CARGO_PKG_VERSION")); std::process::exit(0); }
            "--host" => host = Some(take_value(&flag, inline, &mut args)),
            "--port" => port = Some(take_value(&flag, inline, &mut args)),
            "--data" | "--data-dir" => data = Some(take_value(&flag, inline, &mut args)),
            other => {
                eprintln!("marcidb-server: unknown argument '{}'. Try '--help'.", other);
                std::process::exit(2);
            }
        }
    }

    let host = parse_host(host.as_deref().unwrap_or("0.0.0.0"));
    let port = match port {
        Some(p) => p.parse::<u16>().unwrap_or_else(|_| {
            eprintln!("marcidb-server: invalid port '{}' (expected 0-65535)", p);
            std::process::exit(2);
        }),
        None => 3000,
    };
    let data_dir = data.unwrap_or_else(|| "./data".to_string());

    Config { host, port, data_dir }
}

/// Pulls a flag's value from the inline `=value` or the next argument; exits 2 if absent.
fn take_value(flag: &str, inline: Option<String>, args: &mut impl Iterator<Item = String>) -> String {
    inline.or_else(|| args.next()).unwrap_or_else(|| {
        eprintln!("marcidb-server: missing value for '{}'. Try '--help'.", flag);
        std::process::exit(2);
    })
}

/// Bind host as an IP. `localhost` is accepted as a convenience for `127.0.0.1`.
fn parse_host(s: &str) -> IpAddr {
    if s.eq_ignore_ascii_case("localhost") {
        return IpAddr::from([127, 0, 0, 1]);
    }
    s.parse::<IpAddr>().unwrap_or_else(|_| {
        eprintln!("marcidb-server: invalid host '{}' (expected an IP such as 0.0.0.0, 127.0.0.1, or ::)", s);
        std::process::exit(2);
    })
}

fn print_help() {
    println!("MarciDB Server v{}", env!("CARGO_PKG_VERSION"));
    println!();
    println!("USAGE:");
    println!("  marcidb-server [OPTIONS]");
    println!();
    println!("OPTIONS:");
    println!("  --host <HOST>   Address to bind     [default: 0.0.0.0]  [env: MARCI_HOST]");
    println!("  --port <PORT>   Port to listen on   [default: 3000]     [env: PORT]");
    println!("  --data <DIR>    Data directory      [default: ./data]   [env: MARCI_DATA]");
    println!("  -h, --help      Print help and exit");
    println!("  -v, --version   Print version and exit");
    println!();
    println!("CLI flags take precedence over environment variables.");
}

#[tokio::main]
async fn main() {
    let cfg = parse_config();

    if let Err(e) = fs::create_dir_all(&cfg.data_dir).await {
        eprintln!("MarciDB Server: cannot create data dir '{}': {}", cfg.data_dir, e);
        std::process::exit(1);
    }
    let root = PathBuf::from(&cfg.data_dir);
    let ctx: Arc<ServerContext> = Arc::new(ServerContext::new(root.clone()));

    let addr = SocketAddr::new(cfg.host, cfg.port);
    let listener = match TcpListener::bind(addr).await {
        Ok(listener) => listener,
        Err(e) => {
            eprintln!("MarciDB Server: failed to bind {} ({}). Is the port already in use?", addr, e);
            std::process::exit(1);
        }
    };

    print_banner(&addr, &root, &list_databases(&root));

    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, _) = match accepted {
                    Ok(conn) => conn,
                    Err(e) => { eprintln!("  accept error: {}", e); continue; }
                };
                let io = TokioIo::new(stream);
                let ctx = ctx.clone();

                tokio::task::spawn(async move {
                    let resp = http1::Builder::new()
                        .serve_connection(io, service_fn(move |req| { handle(req, ctx.clone()) }))
                        .await;

                    if let Err(err) = resp {
                        eprintln!("  connection error: {}", err);
                    }
                });
            }
            _ = tokio::signal::ctrl_c() => {
                println!("\n  Shutting down MarciDB Server. Bye!");
                break;
            }
        }
    }
}

/// Startup banner: version, where to reach it, and what's already on disk
fn print_banner(addr: &SocketAddr, data_dir: &std::path::Path, dbs: &[String]) {
    println!();
    println!("  MarciDB Server v{}", env!("CARGO_PKG_VERSION"));
    println!("  ----------------------------------------");
    // 0.0.0.0 / :: bind to every interface — show the friendly localhost URL plus the wildcard
    if addr.ip().is_unspecified() {
        println!("  Local      http://localhost:{}", addr.port());
        println!("  Network    http://{}", addr);
    } else {
        println!("  Listening  http://{}", addr);
    }
    println!("  Data dir   {}", display_path(data_dir));
    if dbs.is_empty() {
        println!("  Databases  none yet (created on first migration)");
    } else {
        println!("  Databases  {} ({})", dbs.len(), dbs.join(", "));
    }
    println!();
    println!("  Ready. Press Ctrl+C to stop.");
    println!();
}

/// One line per request: `<time>  <method>  <status>  <ms>  <path>`
fn log_request(method: &Method, path: &str, status: StatusCode, elapsed: std::time::Duration) {
    let ts = chrono::Local::now().format("%Y-%m-%d %H:%M:%S");
    let ms = elapsed.as_secs_f64() * 1000.0;
    println!("  {ts}  {m:<6} {code:>3}  {ms:>6.1} ms  {path}", m = method.as_str(), code = status.as_u16());
}

/// Names of databases already present in the data directory (each DB is a sub-directory)
fn list_databases(root: &std::path::Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(root)
        .into_iter()
        .flatten()
        .flatten()
        .filter(|e| e.path().is_dir())
        .filter_map(|e| e.file_name().into_string().ok())
        .collect();
    names.sort();
    names
}

/// Absolute path for display, stripped of Windows' `\\?\` extended-length prefix
fn display_path(p: &std::path::Path) -> String {
    let s = std::fs::canonicalize(p).unwrap_or_else(|_| p.to_path_buf()).display().to_string();
    s.strip_prefix(r"\\?\").map(str::to_string).unwrap_or(s)
}
