//! HTTP-level tests: the server runs on an ephemeral port in-process and is driven with hyper's client.

use std::{net::SocketAddr, sync::{Arc, RwLock}, time::{Duration, Instant}};

use http_body_util::{BodyExt, Full};
use hyper::{HeaderMap, Method, Request, StatusCode, body::Bytes, server::conn::http1, service::service_fn};
use hyper_util::rt::TokioIo;
use marcidb::{BackupReader, MarciDB, OpenOptions, RestoreOptions};
use serde_json::{Value, json};
use tokio::{net::{TcpListener, TcpStream}, sync::oneshot};

use crate::{SHUTDOWN_GRACE, ServerContext, Slot, errors::ApiError, serve, serve_with_grace};

struct TestServer {
    addr: SocketAddr,
    dir: tempfile::TempDir,
}

/// Serves `ctx` on an ephemeral port until the test ends.
async fn listen(ctx: ServerContext) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(serve(listener, Arc::new(ctx.log_requests(false)), std::future::pending()));
    addr
}

async fn start(token: Option<&str>) -> TestServer {
    let dir = tempfile::tempdir().unwrap();
    let addr = listen(ServerContext::directory(dir.path().to_path_buf(), token.map(str::to_string))).await;
    TestServer { addr, dir }
}

struct Reply {
    status: StatusCode,
    headers: HeaderMap,
    body: Bytes,
}

impl Reply {
    fn json(&self) -> Value {
        serde_json::from_slice(&self.body).unwrap_or_else(|e| panic!("not JSON ({}): {:?}", e, String::from_utf8_lossy(&self.body)))
    }
}

async fn request(server: &TestServer, method: Method, path: &str, body: impl Into<Bytes>, token: Option<&str>) -> Reply {
    try_request(server.addr, method, path, body, token).await.unwrap_or_else(|e| panic!("{}", e))
}

/// A request whose connection may be cut (the server aborting it).
async fn try_request(addr: SocketAddr, method: Method, path: &str, body: impl Into<Bytes>, token: Option<&str>) -> Result<Reply, hyper::Error> {
    let stream = TcpStream::connect(addr).await.unwrap();
    let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(stream)).await?;
    tokio::spawn(async move {
        let _ = conn.await;
    });
    let mut req = Request::builder().method(method).uri(path).header("host", "localhost");
    if let Some(token) = token {
        req = req.header("authorization", format!("Bearer {}", token));
    }
    let res = sender.send_request(req.body(Full::new(body.into())).unwrap()).await?;
    let (parts, body) = res.into_parts();
    Ok(Reply { status: parts.status, headers: parts.headers, body: body.collect().await?.to_bytes() })
}

async fn post(server: &TestServer, path: &str, body: impl Into<Bytes>) -> Reply {
    request(server, Method::POST, path, body, None).await
}

async fn count(server: &TestServer, db: &str) -> Value {
    let reply = post(server, &format!("/{}/User/count", db), "{}").await;
    assert_eq!(reply.status, StatusCode::OK, "{:?}", reply.body);
    reply.json()
}

/// Names in the data directory left by a restore (uploads, staging, set-aside directories).
fn leftovers(server: &TestServer) -> Vec<String> {
    std::fs::read_dir(server.dir.path()).unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|n| n.contains('.'))
        .collect()
}

async fn seeded(token: Option<&str>) -> TestServer {
    let server = start(token).await;
    let reply = request(&server, Method::POST, "/app/$sync", "model User {\n  name String\n}", token).await;
    assert_eq!(reply.status, StatusCode::OK, "{:?}", reply.body);
    for name in ["ann", "bob", "cid"] {
        let reply = request(&server, Method::POST, "/app/User/insert", json!({ "name": name }).to_string(), token).await;
        assert_eq!(reply.status, StatusCode::OK, "{:?}", reply.body);
    }
    server
}

#[tokio::test(flavor = "multi_thread")]
async fn a_backup_downloads_and_restores_in_place_or_under_another_name() {
    let server = seeded(None).await;

    let backup = request(&server, Method::GET, "/app/$backup", Bytes::new(), None).await;
    assert_eq!(backup.status, StatusCode::OK);
    assert_eq!(backup.headers["content-type"], "application/x-marcidb-backup");
    let disposition = backup.headers["content-disposition"].to_str().unwrap();
    assert!(disposition.starts_with("attachment; filename=\"app-") && disposition.ends_with(".marcibin\""), "{}", disposition);
    assert_eq!(disposition.len(), "attachment; filename=\"app-YYYYMMDD-HHMMSS.marcibin\"".len(), "{}", disposition);
    let reader = BackupReader::open(&backup.body[..]).unwrap();
    assert!(reader.header().trees.iter().any(|t| t.name == "User" && t.entries == 3));
    assert_eq!(reader.finish().unwrap(), backup.body.len() as u64);

    post(&server, "/app/User/insert", json!({ "name": "late" }).to_string()).await;
    assert_eq!(count(&server, "app").await, json!(4));

    // In place: the live database is replaced.
    let restored = post(&server, "/app/$restore", backup.body.clone()).await;
    assert_eq!(restored.status, StatusCode::OK, "{:?}", restored.body);
    let info = restored.json();
    assert_eq!(info["restored"], json!("app"));
    assert_eq!(info["bytes"], json!(backup.body.len()));
    assert_eq!(count(&server, "app").await, json!(3));
    // Still writable, ids continue.
    let id = post(&server, "/app/User/insert", json!({ "name": "dee" }).to_string()).await.json();
    assert_eq!(id, json!({ "id": 4 }));

    // Under another name: created.
    let copy = post(&server, "/app/$restore?into=copy", backup.body.clone()).await;
    assert_eq!(copy.status, StatusCode::OK, "{:?}", copy.body);
    assert_eq!(copy.json()["restored"], json!("copy"));
    assert_eq!(count(&server, "copy").await, json!(3));
    assert_eq!(count(&server, "app").await, json!(4));

    assert!(leftovers(&server).is_empty(), "left {:?}", leftovers(&server));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_large_backup_streams_in_many_chunks() {
    let server = seeded(None).await;
    let long = "x".repeat(200);
    let ops: Vec<Value> = (0..4000).map(|i| json!({ "model": "User", "action": "insert", "data": { "name": format!("{}{}", long, i) } })).collect();
    assert_eq!(post(&server, "/app/$transaction", Value::Array(ops).to_string()).await.status, StatusCode::OK);

    let backup = request(&server, Method::GET, "/app/$backup", Bytes::new(), None).await;
    assert_eq!(backup.status, StatusCode::OK);
    assert!(backup.body.len() > 10 * 64 * 1024, "{} bytes", backup.body.len());
    let reader = BackupReader::open(&backup.body[..]).unwrap();
    assert!(reader.header().trees.iter().any(|t| t.name == "User" && t.entries == 4003));
    reader.finish().unwrap();

    let restored = post(&server, "/app/$restore?into=big", backup.body).await;
    assert_eq!(restored.status, StatusCode::OK, "{:?}", restored.body);
    assert_eq!(count(&server, "big").await, json!(4003));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_bad_restore_is_refused_and_changes_nothing() {
    let server = seeded(None).await;
    let backup = request(&server, Method::GET, "/app/$backup", Bytes::new(), None).await.body;

    let truncated = post(&server, "/app/$restore", backup.slice(..backup.len() / 2)).await;
    assert_eq!(truncated.status, StatusCode::BAD_REQUEST);
    assert_eq!(truncated.json()["kind"], json!("bad_request"));
    assert!(truncated.json()["error"].as_str().unwrap().contains("truncated"));

    let junk = post(&server, "/app/$restore?into=fresh", "not a backup").await;
    assert_eq!(junk.status, StatusCode::BAD_REQUEST);
    assert_eq!(post(&server, "/fresh/User/count", "{}").await.status, StatusCode::NOT_FOUND);

    let bad_name = post(&server, "/app/$restore?into=..", backup.clone()).await;
    assert_eq!(bad_name.status, StatusCode::BAD_REQUEST);

    assert_eq!(count(&server, "app").await, json!(3));
    assert!(leftovers(&server).is_empty(), "left {:?}", leftovers(&server));

    let missing = request(&server, Method::GET, "/nothing/$backup", Bytes::new(), None).await;
    assert_eq!(missing.status, StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn backup_routes_require_the_token() {
    let server = seeded(Some("s3cret")).await;
    assert_eq!(request(&server, Method::GET, "/app/$backup", Bytes::new(), None).await.status, StatusCode::UNAUTHORIZED);
    assert_eq!(request(&server, Method::POST, "/app/$restore", "x", None).await.status, StatusCode::UNAUTHORIZED);
    let backup = request(&server, Method::GET, "/app/$backup", Bytes::new(), Some("s3cret")).await;
    assert_eq!(backup.status, StatusCode::OK);
    let restored = request(&server, Method::POST, "/app/$restore", backup.body, Some("s3cret")).await;
    assert_eq!(restored.status, StatusCode::OK);
}

/// The guarantee behind `$backup` streaming: a copy that fails after the headers went out aborts the
/// connection — the client gets an error, never a body that ends cleanly but short.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_stream_aborts_the_response() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let service = service_fn(|_req| async {
            let (tx, rx) = tokio::sync::mpsc::channel(4);
            tokio::spawn(async move {
                tx.send(Ok(Bytes::from_static(b"MARCIBIN partial"))).await.unwrap();
                // Let the head and the first chunk reach the client, then fail.
                tokio::time::sleep(Duration::from_millis(100)).await;
                tx.send(Err("disk on fire".into())).await.unwrap();
            });
            Ok::<_, std::convert::Infallible>(hyper::Response::new(crate::backup::ChannelBody { rx }))
        });
        let _ = http1::Builder::new().serve_connection(TokioIo::new(stream), service).await;
    });

    let stream = TcpStream::connect(addr).await.unwrap();
    let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(stream)).await.unwrap();
    tokio::spawn(async move {
        let _ = conn.await;
    });
    let req = Request::builder().uri("/").header("host", "localhost").body(Full::new(Bytes::new())).unwrap();
    let res = sender.send_request(req).await.expect("the head was sent before the failure");
    assert_eq!(res.status(), StatusCode::OK);
    assert!(res.into_body().collect().await.is_err(), "a failed copy must not look like a complete body");
}

#[test]
fn a_restore_gives_up_on_a_database_that_stays_busy() {
    let dir = tempfile::tempdir().unwrap();
    let ctx = Arc::new(ServerContext::directory(dir.path().to_path_buf(), None));
    let db = ctx.get_db("app", true).unwrap();
    {
        let mut db = db.write().unwrap();
        let mut schema = marcidb::parse_schema("model User {\n  name String\n}");
        marcidb_schema::reconcile(&mut schema, &db.schema);
        let ops = marcidb_schema::diff(&db.schema, &schema).unwrap();
        db.commit_schema(schema, &ops).unwrap();
    }
    let mut bytes = Vec::new();
    db.read().unwrap().backup(&mut bytes).unwrap();

    let stage = || {
        let options = RestoreOptions { overwrite: true, open: OpenOptions::default() };
        MarciDB::restore_prepare(dir.path().join("app").to_str().unwrap(), &bytes[..], &options).unwrap()
    };

    // `db` is an in-flight request's handle: the swap waits for it, then gives up.
    let refused = ctx.replace_db_within("app", stage(), Duration::from_millis(100));
    assert!(matches!(refused, Err(ApiError::Unavailable(_))), "{:?}", refused.err());
    assert!(Arc::ptr_eq(&ctx.get_db("app", false).unwrap(), &db), "the old database stays in service");
    assert!(!dir.path().join("app.restoring").exists());

    drop(db);
    ctx.replace_db_within("app", stage(), Duration::from_millis(100)).unwrap();
    assert!(ctx.get_db("app", false).unwrap().read().unwrap().get_model("User").is_some());

    // While a swap runs, the name answers 503 instead of reopening the directory.
    ctx.lock_dbs().insert("app".to_string(), Slot::Swapping);
    assert!(matches!(ctx.get_db("app", false), Err(ApiError::Unavailable(_))));
    assert!(matches!(ctx.drop_db("app"), Err(ApiError::Unavailable(_))));

    // One restore per name at a time.
    let first = ctx.begin_restore("app").unwrap();
    assert!(matches!(ctx.begin_restore("app"), Err(ApiError::Unavailable(_))));
    drop(first);
    ctx.begin_restore("app").unwrap();
}

// ───────────────────────────────── maintenance port (`ServerContext::single`) ─────────────────────────────────

const USER: &str = "model User {\n  name String\n}";

/// An embedded app's database: open in-process, the handle shared with the port.
fn app_db(dir: &tempfile::TempDir) -> Arc<RwLock<MarciDB>> {
    Arc::new(RwLock::new(MarciDB::new(USER, dir.path().to_str().unwrap())))
}

fn app_count(db: &RwLock<MarciDB>) -> u64 {
    let db = db.read().unwrap();
    db.count(db.get_model("User").unwrap()).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_maintenance_port_serves_its_one_database() {
    let dir = tempfile::tempdir().unwrap();
    let db = app_db(&dir);
    let addr = listen(ServerContext::single("app".to_string(), db.clone(), None)).await;
    let server = TestServer { addr, dir };

    // What is written over HTTP is the app's data, and the other way round.
    assert_eq!(post(&server, "/app/User/insert", json!({ "name": "ann" }).to_string()).await.status, StatusCode::OK);
    assert_eq!(app_count(&db), 1);
    {
        let db = db.read().unwrap();
        let user = db.get_model("User").unwrap();
        db.insert_item(user, &marcidb::parse_insert(&db.schema, user, &json!({ "name": "bob" })).unwrap()).unwrap();
    }
    assert_eq!(count(&server, "app").await, json!(2));

    let backup = request(&server, Method::GET, "/app/$backup", Bytes::new(), None).await;
    assert_eq!(backup.status, StatusCode::OK);
    BackupReader::open(&backup.body[..]).unwrap().finish().unwrap();
    assert_eq!(request(&server, Method::GET, "/$health", Bytes::new(), None).await.status, StatusCode::OK);

    // No other database is here — not even to be created.
    for (method, path, body) in [
        (Method::POST, "/other/User/count", "{}"),
        (Method::POST, "/other/$sync", USER),
        (Method::POST, "/other/$sync?plan=1", USER),
        (Method::POST, "/other/$migrate", "create entity Thing"),
        (Method::GET, "/other/$snapshot", ""),
        (Method::GET, "/other/$backup", ""),
    ] {
        let reply = request(&server, method.clone(), path, body, None).await;
        assert_eq!(reply.status, StatusCode::NOT_FOUND, "{} {}", method, path);
        assert!(reply.json()["error"].as_str().unwrap().contains("serves only database 'app'"), "{} {}: {:?}", method, path, reply.body);
    }

    // Replacing or dropping the database is the app's business, not the port's.
    for (method, path) in [
        (Method::POST, "/app/$restore"),
        (Method::POST, "/app/$restore?into=copy"),
        (Method::POST, "/other/$restore"),
        (Method::DELETE, "/app"),
        (Method::DELETE, "/other"),
    ] {
        let reply = request(&server, method.clone(), path, backup.body.clone(), None).await;
        assert_eq!(reply.status, StatusCode::BAD_REQUEST, "{} {}", method, path);
        let error = reply.json()["error"].as_str().unwrap().to_string();
        assert!(error.contains("not available on an embedded maintenance port"), "{} {}: {}", method, path, error);
        if path.contains("$restore") {
            assert!(error.contains("restoreDatabase"), "{}", error);
        }
    }
    assert_eq!(app_count(&db), 2);

    // A migration over the port changes the schema the app sees: it is the same handle.
    let synced = post(&server, "/app/$sync", "model User {\n  name String\n  age  UInt?\n}").await;
    assert_eq!(synced.status, StatusCode::OK, "{:?}", synced.body);
    assert!(db.read().unwrap().get_model("User").unwrap().fields.iter().any(|f| f.name == "age"));
    assert_eq!(app_count(&db), 2);
}

/// Holds `db`'s write lock on another thread for `hold`: a request against the database waits that long.
fn hold_write(db: Arc<RwLock<MarciDB>>, hold: Duration) -> std::thread::JoinHandle<()> {
    let (held_tx, held_rx) = std::sync::mpsc::channel();
    let holder = std::thread::spawn(move || {
        let _guard = db.write().unwrap();
        held_tx.send(()).unwrap();
        std::thread::sleep(hold);
    });
    held_rx.recv().unwrap();
    holder
}

#[tokio::test(flavor = "multi_thread")]
async fn a_request_in_flight_finishes_then_the_port_closes() {
    let dir = tempfile::tempdir().unwrap();
    let db = app_db(&dir);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = oneshot::channel::<()>();
    let ctx = Arc::new(ServerContext::single("app".to_string(), db.clone(), None).log_requests(false));
    let served = tokio::spawn(serve(listener, ctx, async { let _ = stopped.await; }));

    let holder = hold_write(db.clone(), Duration::from_millis(500));
    let slow = tokio::spawn(try_request(addr, Method::POST, "/app/User/insert", json!({ "name": "slow" }).to_string(), None));
    tokio::time::sleep(Duration::from_millis(150)).await; // the insert is in, waiting for the lock

    let started = Instant::now();
    stop.send(()).unwrap();
    served.await.unwrap();
    assert!(started.elapsed() < SHUTDOWN_GRACE, "took {:?}", started.elapsed());
    assert_eq!(slow.await.unwrap().expect("the request in flight is answered").status, StatusCode::OK);
    assert!(TcpStream::connect(addr).await.is_err(), "the port is closed once serve returns");
    holder.join().unwrap();
    assert_eq!(app_count(&db), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_request_that_outlives_the_grace_period_is_aborted() {
    let dir = tempfile::tempdir().unwrap();
    let db = app_db(&dir);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = oneshot::channel::<()>();
    let ctx = Arc::new(ServerContext::single("app".to_string(), db.clone(), None).log_requests(false));
    let served = tokio::spawn(serve_with_grace(listener, ctx, async { let _ = stopped.await; }, Duration::from_millis(200)));

    let holder = hold_write(db.clone(), Duration::from_millis(1500));
    let slow = tokio::spawn(try_request(addr, Method::POST, "/app/User/insert", json!({ "name": "slow" }).to_string(), None));
    tokio::time::sleep(Duration::from_millis(150)).await;

    let started = Instant::now();
    stop.send(()).unwrap();
    served.await.unwrap();
    assert!(started.elapsed() < Duration::from_millis(1000), "took {:?}", started.elapsed());
    assert!(slow.await.unwrap().is_err(), "the aborted request gets no answer");
    holder.join().unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_waiting_journal_read_does_not_hold_the_shutdown() {
    let dir = tempfile::tempdir().unwrap();
    let db = app_db(&dir);
    {
        let db = db.read().unwrap();
        db.journal_open(db.get_model("User").unwrap(), "j", &["delete".to_string()]).unwrap();
    }
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = oneshot::channel::<()>();
    let ctx = Arc::new(ServerContext::single("app".to_string(), db, None).log_requests(false));
    let served = tokio::spawn(serve(listener, ctx, async { let _ = stopped.await; }));

    let waiting = tokio::spawn(try_request(addr, Method::GET, "/app/User/$journal/j?wait=30", Bytes::new(), None));
    tokio::time::sleep(Duration::from_millis(300)).await;

    let started = Instant::now();
    stop.send(()).unwrap();
    served.await.unwrap();
    assert!(started.elapsed() < Duration::from_secs(2), "took {:?}", started.elapsed());
    let reply = waiting.await.unwrap().expect("the waiting read is answered");
    assert_eq!((reply.status, reply.json()), (StatusCode::OK, json!([])));
}

// ─────────────────────────────── watch sockets (`GET /:db/$watch`) ───────────────────────────────

type Socket = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>;

async fn socket(addr: SocketAddr, db: &str, token: Option<&str>) -> Result<Socket, tokio_tungstenite::tungstenite::Error> {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    let mut req = format!("ws://{}/{}/$watch", addr, db).into_client_request().unwrap();
    if let Some(token) = token {
        req.headers_mut().insert("authorization", format!("Bearer {}", token).parse().unwrap());
    }
    tokio_tungstenite::connect_async(req).await.map(|(ws, _)| ws)
}

async fn send_text(ws: &mut Socket, text: String) {
    use futures_util::SinkExt;
    ws.send(tokio_tungstenite::tungstenite::Message::text(text)).await.unwrap();
}

async fn send(ws: &mut Socket, frame: Value) {
    send_text(ws, frame.to_string()).await;
}

/// The next frame as JSON (`{ "closed": <code> }` for a close), or `None` when nothing comes within `wait`.
async fn next(ws: &mut Socket, wait: Duration) -> Option<Value> {
    use futures_util::StreamExt;
    use tokio_tungstenite::tungstenite::Message;
    loop {
        match tokio::time::timeout(wait, ws.next()).await {
            Err(_) => return None,
            Ok(Some(Ok(Message::Text(text)))) => return Some(serde_json::from_str(text.as_str()).unwrap()),
            Ok(Some(Ok(Message::Close(frame)))) => return Some(json!({ "closed": frame.map(|f| u16::from(f.code)) })),
            Ok(Some(Ok(_))) => continue,
            Ok(end) => return Some(json!({ "closed": null, "end": format!("{:?}", end.map(|r| r.map(|_| ()))) })),
        }
    }
}

const QUIET: Duration = Duration::from_millis(300);
const SOON: Duration = Duration::from_secs(5);

fn watch_frame(id: u64, action: &str, query: Value) -> Value {
    json!({ "watch": id, "op": { "model": "User", "action": action, "query": query } })
}

async fn insert(server: &TestServer, name: &str) {
    let reply = post(server, "/app/User/insert", json!({ "name": name }).to_string()).await;
    assert_eq!(reply.status, StatusCode::OK, "{:?}", reply.body);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_watch_socket_signals_the_commits_that_touch_its_reads() {
    let server = seeded(None).await;
    let mut ws = socket(server.addr, "app", None).await.unwrap();

    send(&mut ws, watch_frame(7, "findMany", json!({ "$where": { "name": "dee" } }))).await;
    assert_eq!(next(&mut ws, SOON).await, Some(json!({ "ok": 7 })));
    send(&mut ws, watch_frame(9, "count", json!({}))).await;
    assert_eq!(next(&mut ws, SOON).await, Some(json!({ "ok": 9 })));

    insert(&server, "dee").await;
    assert_eq!(next(&mut ws, SOON).await, Some(json!({ "changed": [7, 9] })));
    insert(&server, "eve").await;
    assert_eq!(next(&mut ws, SOON).await, Some(json!({ "changed": [9] })), "eve is not in watch 7");

    // One commit, one frame, each id once.
    let ops: Vec<Value> = ["dee", "dee", "fay"].iter().map(|n| json!({ "model": "User", "action": "insert", "data": { "name": n } })).collect();
    assert_eq!(post(&server, "/app/$transaction", Value::Array(ops).to_string()).await.status, StatusCode::OK);
    assert_eq!(next(&mut ws, SOON).await, Some(json!({ "changed": [7, 9] })));
    assert_eq!(next(&mut ws, QUIET).await, None);

    // Frames are answered in order: once watch 11 is confirmed, the unwatch before it is done.
    send(&mut ws, json!({ "unwatch": 9 })).await;
    send(&mut ws, watch_frame(11, "findFirst", json!({ "$where": { "name": "zed" } }))).await;
    assert_eq!(next(&mut ws, SOON).await, Some(json!({ "ok": 11 })));
    insert(&server, "gus").await;
    assert_eq!(next(&mut ws, QUIET).await, None, "9 is gone, 7 and 11 do not match");

    // Refusals name the id.
    send(&mut ws, watch_frame(1, "insert", json!({}))).await;
    let refused = next(&mut ws, SOON).await.unwrap();
    assert_eq!((refused["err"].clone(), refused["kind"].clone()), (json!(1), json!("bad_request")));
    assert!(refused["message"].as_str().unwrap().contains("is not a read"), "{}", refused);
    send(&mut ws, json!({ "watch": 2, "op": { "model": "Nope", "action": "findMany", "query": {} } })).await;
    assert_eq!(next(&mut ws, SOON).await.unwrap()["kind"], json!("not_found"));
    send_text(&mut ws, "not json".to_string()).await;
    assert_eq!(next(&mut ws, SOON).await.unwrap()["err"], Value::Null);
}

#[tokio::test(flavor = "multi_thread")]
async fn the_watch_route_is_a_websocket_behind_the_token() {
    use tokio_tungstenite::tungstenite::Error;
    let server = seeded(Some("secret")).await;
    let status = |result: Result<Socket, Error>| match result {
        Err(Error::Http(res)) => res.status().as_u16(),
        Err(e) => panic!("{}", e),
        Ok(_) => 101,
    };
    assert_eq!(status(socket(server.addr, "app", None).await), 401);
    assert_eq!(status(socket(server.addr, "app", Some("wrong")).await), 401);
    assert_eq!(status(socket(server.addr, "nope", Some("secret")).await), 404);
    assert_eq!(status(socket(server.addr, "app", Some("secret")).await), 101);

    let plain = request(&server, Method::GET, "/app/$watch", Bytes::new(), Some("secret")).await;
    assert_eq!(plain.status, StatusCode::BAD_REQUEST);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_restore_closes_the_socket_without_waiting_for_it() {
    let server = seeded(None).await;
    let backup = request(&server, Method::GET, "/app/$backup", Bytes::new(), None).await;
    let mut ws = socket(server.addr, "app", None).await.unwrap();
    send(&mut ws, watch_frame(1, "count", json!({}))).await;
    assert_eq!(next(&mut ws, SOON).await, Some(json!({ "ok": 1 })));

    let started = Instant::now();
    let restored = post(&server, "/app/$restore", backup.body.clone()).await;
    assert_eq!(restored.status, StatusCode::OK, "{:?}", restored.body);
    assert!(started.elapsed() < Duration::from_secs(5), "the socket held the database: {:?}", started.elapsed());
    // 1012: the database it watched is gone — reconnect.
    assert_eq!(next(&mut ws, SOON).await, Some(json!({ "closed": 1012 })));

    // A new socket watches the restored database.
    let mut ws = socket(server.addr, "app", None).await.unwrap();
    send(&mut ws, watch_frame(1, "count", json!({}))).await;
    assert_eq!(next(&mut ws, SOON).await, Some(json!({ "ok": 1 })));
    insert(&server, "dee").await;
    assert_eq!(next(&mut ws, SOON).await, Some(json!({ "changed": [1] })));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_stopping_server_closes_its_watch_sockets() {
    let dir = tempfile::tempdir().unwrap();
    let db = app_db(&dir);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = oneshot::channel::<()>();
    let ctx = Arc::new(ServerContext::single("app".to_string(), db, None).log_requests(false));
    let served = tokio::spawn(serve(listener, ctx, async { let _ = stopped.await; }));

    let mut ws = socket(addr, "app", None).await.unwrap();
    send(&mut ws, watch_frame(1, "count", json!({}))).await;
    assert_eq!(next(&mut ws, SOON).await, Some(json!({ "ok": 1 })));

    let started = Instant::now();
    stop.send(()).unwrap();
    served.await.unwrap();
    assert!(started.elapsed() < SHUTDOWN_GRACE, "took {:?}", started.elapsed());
    assert_eq!(next(&mut ws, SOON).await, Some(json!({ "closed": 1001 })));
}
