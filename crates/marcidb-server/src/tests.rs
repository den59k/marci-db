//! HTTP-level tests: the server runs on an ephemeral port in-process and is driven with hyper's client.

use std::{net::SocketAddr, sync::Arc, time::Duration};

use http_body_util::{BodyExt, Full};
use hyper::{HeaderMap, Method, Request, StatusCode, body::Bytes, server::conn::http1, service::service_fn};
use hyper_util::rt::TokioIo;
use marcidb::{BackupReader, MarciDB, OpenOptions, RestoreOptions};
use serde_json::{Value, json};
use tokio::net::{TcpListener, TcpStream};

use crate::{ServerContext, Slot, errors::ApiError, handle};

struct TestServer {
    addr: SocketAddr,
    dir: tempfile::TempDir,
}

async fn start(token: Option<&str>) -> TestServer {
    let dir = tempfile::tempdir().unwrap();
    let ctx = Arc::new(ServerContext::new(dir.path().to_path_buf(), token.map(str::to_string)));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else { return };
            let ctx = ctx.clone();
            tokio::spawn(async move {
                let _ = http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), service_fn(move |req| handle(req, ctx.clone())))
                    .await;
            });
        }
    });
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
    let stream = TcpStream::connect(server.addr).await.unwrap();
    let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(stream)).await.unwrap();
    tokio::spawn(async move {
        let _ = conn.await;
    });
    let mut req = Request::builder().method(method).uri(path).header("host", "localhost");
    if let Some(token) = token {
        req = req.header("authorization", format!("Bearer {}", token));
    }
    let res = sender.send_request(req.body(Full::new(body.into())).unwrap()).await.unwrap();
    let (parts, body) = res.into_parts();
    Reply { status: parts.status, headers: parts.headers, body: body.collect().await.expect("a complete body").to_bytes() }
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
    let ctx = Arc::new(ServerContext::new(dir.path().to_path_buf(), None));
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
