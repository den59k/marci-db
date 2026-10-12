//! The maintenance port: the server's HTTP API (`marcidb-http`) served from inside the app process, over the
//! database this handle has open. Maintenance scripts, cron backups and `marcidb` CLI commands reach a live
//! embedded database through it — canopydb's directory lock keeps every other process out.
//!
//! Each listener has its own tokio runtime (two worker threads; request work goes to its blocking pool),
//! so requests run beside the app's own FFI calls. They share the handle's `RwLock`, as backups do.

use std::net::{IpAddr, SocketAddr, TcpListener};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use marcidb::MarciDB;
use marcidb_http::{ServerContext, is_valid_db_name, serve};
use serde_json::Value;
use tokio::runtime::Runtime;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::Fault;

/// What `marci_listen` was asked for.
pub(crate) struct ListenOptions {
    port: u16,
    pub(crate) host: IpAddr,
    token: Option<String>,
    name: String,
    log_requests: bool,
}

/// `{ "port": u16, "host"?: string, "token"?: string, "name": string, "logRequests"?: bool }`. The host
/// defaults to `127.0.0.1`; any other than a loopback one needs a token.
pub(crate) fn parse_options(text: &str) -> Result<ListenOptions, Fault> {
    let v: Value = serde_json::from_str(text).map_err(|e| Fault::bad_request(format!("invalid listen options JSON: {}", e)))?;

    let port = v.get("port").and_then(Value::as_u64).filter(|p| *p <= u16::MAX as u64)
        .ok_or_else(|| Fault::bad_request("listen needs a 'port' (0–65535; 0 picks a free one)"))? as u16;
    let host = match v.get("host").and_then(Value::as_str).unwrap_or("127.0.0.1") {
        h if h.eq_ignore_ascii_case("localhost") => IpAddr::from([127, 0, 0, 1]),
        h => h.parse::<IpAddr>().map_err(|_| Fault::bad_request(format!("invalid host '{}' (expected an IP such as 127.0.0.1 or 0.0.0.0)", h)))?,
    };
    let token = v.get("token").and_then(Value::as_str).filter(|t| !t.is_empty()).map(str::to_string);
    let name = v.get("name").and_then(Value::as_str).unwrap_or("");
    if !is_valid_db_name(name) {
        return Err(Fault::bad_request(format!("invalid database name '{}' for the port: 1+ characters of A-Z a-z 0-9 _ -", name)));
    }
    if !host.is_loopback() && token.is_none() {
        return Err(Fault::bad_request(format!(
            "listening on {} would open the database beyond this machine without authentication — pass a token, or use 127.0.0.1",
            host
        )));
    }
    let log_requests = v.get("logRequests").and_then(Value::as_bool).unwrap_or(false);
    Ok(ListenOptions { port, host, token, name: name.to_string(), log_requests })
}

/// A running maintenance port.
pub(crate) struct Listener {
    pub(crate) port: u16,
    runtime: Runtime,
    shutdown: oneshot::Sender<()>,
    served: JoinHandle<()>,
}

impl Listener {
    /// Binds on the calling thread — so the caller gets the real port, or the bind error, from this call —
    /// then serves on a runtime of its own.
    pub(crate) fn start(db: Arc<RwLock<MarciDB>>, options: ListenOptions) -> Result<Listener, Fault> {
        let addr = SocketAddr::new(options.host, options.port);
        let bound = TcpListener::bind(addr).map_err(|e| Fault::bad_request(format!("cannot listen on {}: {}", addr, e)))?;
        bound.set_nonblocking(true).map_err(|e| Fault::internal(format!("cannot listen on {}: {}", addr, e)))?;
        let port = bound.local_addr().map_err(|e| Fault::internal(e.to_string()))?.port();

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("marci-http")
            .enable_all()
            .build()
            .map_err(|e| Fault::internal(format!("cannot start the HTTP runtime: {}", e)))?;
        let listener = {
            let _entered = runtime.enter();
            tokio::net::TcpListener::from_std(bound).map_err(|e| Fault::internal(e.to_string()))?
        };

        let ctx = ServerContext::single(options.name, db, options.token).log_requests(options.log_requests);
        let (shutdown, stopped) = oneshot::channel::<()>();
        let served = runtime.spawn(serve(listener, Arc::new(ctx), async {
            let _ = stopped.await;
        }));
        Ok(Listener { port, runtime, shutdown, served })
    }

    /// Stops accepting (the port is free at once), lets in-flight requests finish for the grace period and
    /// aborts the rest, then stops the runtime. Blocks the calling thread meanwhile — at most the grace
    /// period plus a second for blocking work to wind down. Must not be called from a runtime thread.
    pub(crate) fn stop(self) {
        let Listener { runtime, shutdown, served, .. } = self;
        let _ = shutdown.send(());
        let _ = runtime.block_on(served);
        runtime.shutdown_timeout(Duration::from_secs(1));
    }
}
