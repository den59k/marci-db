//! `GET /:db/$watch`: one client's watches over a WebSocket (the engine side: marcidb's watch.rs).
//!
//! ```text
//! → { "watch": 7, "op": { "model", "action", "query" } }   ← { "ok": 7 } | { "err": 7, "message", "kind" }
//! → { "unwatch": 7 }
//! ← { "changed": [7, 9] }   what committed since the last such frame, an id at most once
//! ```
//!
//! A socket is one engine `Watcher`; the ids are the client's. Between frames the socket holds no handle on
//! the database (a `$restore` waits for every handle), so a database closed under it — restored over,
//! dropped — only detaches the watcher: the socket then closes (1012), and the client re-subscribes and
//! re-reads. A closed socket takes its watches with it; a stopping server closes every socket (1001).

use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use http_body_util::Full;
use hyper::{Request, Response, StatusCode, body::{Bytes, Incoming}, header::{self, HeaderValue}};
use hyper_util::rt::TokioIo;
use marcidb::Watcher;
use serde_json::{Value, json};
use tokio::{io::{AsyncRead, AsyncWrite}, sync::{Notify, watch}};
use tokio_tungstenite::{WebSocketStream, tungstenite::{Message, handshake::derive_accept_key, protocol::{CloseFrame, Role, frame::coding::CloseCode}}};

use crate::{ServerContext, errors::ApiError, handlers::with_db, helpers::blocking};

/// Answers the upgrade (101) and serves the socket on a task of its own. An unknown database is a 404,
/// then a request that is not a WebSocket a 400.
pub async fn upgrade(req: Request<Incoming>, ctx: Arc<ServerContext>, db_name: String) -> Result<Response<Full<Bytes>>, ApiError> {
    let value = |name: header::HeaderName| req.headers().get(name).and_then(|v| v.to_str().ok()).unwrap_or("").to_string();
    let is_websocket = value(header::UPGRADE).eq_ignore_ascii_case("websocket")
        && value(header::CONNECTION).split(',').any(|t| t.trim().eq_ignore_ascii_case("upgrade"))
        && value(header::SEC_WEBSOCKET_VERSION) == "13";
    let key = value(header::SEC_WEBSOCKET_KEY);

    // The database first: a client that cannot open the socket learns why from a plain GET of the route.
    let probe = (ctx.clone(), db_name.clone());
    blocking(move || probe.0.get_db(&probe.1, false).map(drop)).await?;
    if !is_websocket || key.is_empty() {
        return Err(ApiError::BadRequest("GET /:db/$watch is a WebSocket — connect with `Upgrade: websocket` (version 13)".to_string()));
    }

    let log = ctx.log_requests;
    tokio::spawn(async move {
        match hyper::upgrade::on(req).await {
            Ok(upgraded) => session(WebSocketStream::from_raw_socket(TokioIo::new(upgraded), Role::Server, None).await, ctx, db_name).await,
            Err(e) => if log { eprintln!("  watch: the upgrade failed: {}", e) },
        }
    });

    let mut res = Response::new(Full::new(Bytes::new()));
    *res.status_mut() = StatusCode::SWITCHING_PROTOCOLS;
    let headers = res.headers_mut();
    headers.insert(header::UPGRADE, HeaderValue::from_static("websocket"));
    headers.insert(header::CONNECTION, HeaderValue::from_static("Upgrade"));
    headers.insert(header::SEC_WEBSOCKET_ACCEPT, HeaderValue::from_str(&derive_accept_key(key.as_bytes())).expect("base64 is a header value"));
    Ok(res)
}

async fn session<S: AsyncRead + AsyncWrite + Unpin>(ws: WebSocketStream<S>, ctx: Arc<ServerContext>, db_name: String) {
    let woken = Arc::new(Notify::new());
    let watcher = {
        let woken = woken.clone();
        Watcher::new(move || woken.notify_one())
    };
    let mut stop = ctx.stop.subscribe();
    let (mut out, mut frames) = ws.split();

    let close = loop {
        tokio::select! {
            frame = frames.next() => match frame {
                Some(Ok(Message::Text(text))) => {
                    let reply = answer(&ctx, &db_name, &watcher, text.as_str()).await;
                    if let Some(reply) = reply && out.send(Message::text(reply.to_string())).await.is_err() { break None }
                }
                Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break None,
                Some(Ok(_)) => {} // a ping (the codec answers it), a pong, binary
            },
            _ = woken.notified() => {
                let changed = watcher.take_changed();
                if !changed.is_empty() && out.send(Message::text(json!({ "changed": changed }).to_string())).await.is_err() { break None }
                if watcher.is_detached() { break Some((CloseCode::Restart, "the database was closed — reconnect")) }
            }
            _ = stopped(&mut stop) => break Some((CloseCode::Away, "the server is stopping")),
        }
    };
    if let Some((code, reason)) = close {
        let _ = out.send(Message::Close(Some(CloseFrame { code, reason: reason.into() }))).await;
    }
    if !watcher.is_detached() {
        let _ = blocking(move || with_db(&ctx, &db_name, |db| {
            db.unwatch_all(&watcher);
            Ok(())
        })).await;
    }
}

async fn stopped(stop: &mut watch::Receiver<bool>) {
    let _ = stop.wait_for(|stopping| *stopping).await;
}

/// The reply to one client frame (none to an `unwatch`).
async fn answer(ctx: &Arc<ServerContext>, db_name: &str, watcher: &Arc<Watcher>, text: &str) -> Option<Value> {
    let frame: Value = match serde_json::from_str(text) {
        Ok(frame) => frame,
        Err(e) => return Some(refusal(Value::Null, ApiError::BadRequest(format!("invalid frame: {}", e)))),
    };
    let (ctx, db_name, watcher) = (ctx.clone(), db_name.to_string(), watcher.clone());
    if let Some(id) = frame.get("unwatch") {
        let Some(id) = id.as_u64() else { return Some(refusal(id.clone(), bad_id())) };
        let _ = blocking(move || with_db(&ctx, &db_name, |db| Ok(db.unwatch(&watcher, id)))).await;
        return None;
    }
    if let Some(id) = frame.get("watch") {
        let Some(id) = id.as_u64() else { return Some(refusal(id.clone(), bad_id())) };
        let op = frame.get("op").cloned().unwrap_or(Value::Null);
        let registered = blocking(move || with_db(&ctx, &db_name, |db| db.watch(&watcher, id, &op).map_err(ApiError::from))).await;
        return Some(match registered {
            Ok(()) => json!({ "ok": id }),
            Err(e) => refusal(json!(id), e),
        });
    }
    Some(refusal(Value::Null, ApiError::BadRequest("a frame is {\"watch\": <id>, \"op\": {…}} or {\"unwatch\": <id>}".to_string())))
}

fn bad_id() -> ApiError {
    ApiError::BadRequest("a watch id is a non-negative integer".to_string())
}

fn refusal(id: Value, e: ApiError) -> Value {
    let (kind, _, message) = e.parts();
    json!({ "err": id, "message": message, "kind": kind })
}
