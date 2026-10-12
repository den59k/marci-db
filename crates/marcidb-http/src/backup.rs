//! `GET /:db/$backup` and `POST /:db/$restore` — the HTTP side of the engine's `.marcibin` backups.
//!
//! A backup streams: the engine writes the snapshot on the blocking pool into a channel, the response body
//! reads from it. A copy that fails mid-way ends the body with an error, so hyper aborts the connection —
//! the client never receives a truncated file that looks complete (and the trailer checksum would catch one).
//! A restore streams the upload to a file next to the databases, restores it into a staging directory while
//! the live database keeps serving, then swaps it in place ([`ServerContext::replace_db`]).

use std::io::{self, Write};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use http_body_util::BodyExt;
use hyper::body::{Body, Bytes, Frame};
use hyper::{Request, Response, header};
use marcidb::{MarciDB, OpenOptions, RestoreOptions};
use tokio::io::AsyncWriteExt;
use tokio::sync::{mpsc, oneshot};

use crate::errors::ApiError;
use crate::handlers::query_param;
use crate::helpers::{BoxError, ResponseBody, blocking, ok_response};
use crate::{ServerContext, is_valid_db_name};

/// Media type of a `.marcibin` body.
pub const BACKUP_MEDIA_TYPE: &str = "application/x-marcidb-backup";

/// Bytes gathered before a chunk goes to the response body.
const CHUNK_BYTES: usize = 64 * 1024;

/// A response body fed by the backup thread. An `Err` item aborts the response.
pub(crate) struct ChannelBody {
    pub(crate) rx: mpsc::Receiver<Result<Bytes, BoxError>>,
}

impl Body for ChannelBody {
    type Data = Bytes;
    type Error = BoxError;

    fn poll_frame(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Result<Frame<Bytes>, BoxError>>> {
        self.rx.poll_recv(cx).map(|item| item.map(|chunk| chunk.map(Frame::data)))
    }
}

/// The `Write` end of a [`ChannelBody`]. Blocks when the client reads slower than the copy runs; fails
/// with `BrokenPipe` once the client is gone, which ends the copy and releases the snapshot.
struct ChannelWriter {
    tx: mpsc::Sender<Result<Bytes, BoxError>>,
    buf: Vec<u8>,
}

impl ChannelWriter {
    fn send_buffered(&mut self) -> io::Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        let chunk = Bytes::from(std::mem::replace(&mut self.buf, Vec::with_capacity(CHUNK_BYTES)));
        self.tx
            .blocking_send(Ok(chunk))
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "the client went away"))
    }
}

impl Write for ChannelWriter {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        self.buf.extend_from_slice(data);
        if self.buf.len() >= CHUNK_BYTES {
            self.send_buffered()?;
        }
        Ok(data.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.send_buffered()
    }
}

/// `GET /:db/$backup`: streams a consistent backup of the database. The database's read lock is held only
/// while the snapshot is taken; the copy runs without it, so data operations and migrations alike proceed
/// (the snapshot is isolated from them). The handle itself is kept for the duration, so a `$restore` of
/// this database counts the backup as an in-flight request and waits for it.
pub async fn handle_backup(ctx: Arc<ServerContext>, db_name: String) -> Result<Response<ResponseBody>, ApiError> {
    let (tx, rx) = mpsc::channel::<Result<Bytes, BoxError>>(4);
    let (started_tx, started_rx) = oneshot::channel::<Result<String, ApiError>>();
    let name = db_name.clone();

    tokio::task::spawn_blocking(move || {
        let db = match ctx.get_db(&name, false) {
            Ok(db) => db,
            Err(e) => {
                let _ = started_tx.send(Err(e));
                return;
            }
        };
        let snapshot = {
            let guard = db.read().unwrap_or_else(|e| e.into_inner());
            match guard.backup_begin() {
                Ok(snapshot) => snapshot,
                Err(e) => {
                    let _ = started_tx.send(Err(ApiError::Internal(e.to_string())));
                    return;
                }
            }
        };
        let _ = started_tx.send(Ok(snapshot.header().created_at.clone()));

        let mut writer = ChannelWriter { tx: tx.clone(), buf: Vec::with_capacity(CHUNK_BYTES) };
        if let Err(e) = snapshot.write_to(&mut writer) {
            eprintln!("  backup of '{}' failed: {}", name, e);
            let _ = tx.blocking_send(Err(Box::new(e)));
        }
    });

    let created_at = started_rx.await.map_err(|_| ApiError::Internal("the backup task ended unexpectedly".to_string()))??;
    let stamp = chrono::DateTime::parse_from_rfc3339(&created_at)
        .map(|t| t.format("%Y%m%d-%H%M%S").to_string())
        .unwrap_or_else(|_| chrono::Utc::now().format("%Y%m%d-%H%M%S").to_string());

    Ok(Response::builder()
        .header(header::CONTENT_TYPE, BACKUP_MEDIA_TYPE)
        .header(header::CONTENT_DISPOSITION, format!("attachment; filename=\"{}-{}.marcibin\"", db_name, stamp))
        .body(ChannelBody { rx }.boxed())
        .expect("static headers are valid"))
}

/// The uploaded backup on disk; removed when dropped, whichever way the restore ends.
struct Upload(PathBuf);

impl Drop for Upload {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

/// `POST /:db/$restore[?into=<name>]`: the body is a `.marcibin` file. Replaces the database `:db` (or
/// `<name>`, created when it does not exist) with the backup's contents. Answers
/// `{ "restored": <name>, "bytes", "trees", "entries", "schemaVersion", "createdAt" }`.
pub async fn handle_restore(req: Request<hyper::body::Incoming>, ctx: Arc<ServerContext>, db_name: String) -> Result<Response<http_body_util::Full<Bytes>>, ApiError> {
    let query = req.uri().query().unwrap_or("").to_string();
    let target = query_param(&query, "into").unwrap_or(&db_name).to_string();
    for name in [&db_name, &target] {
        if !is_valid_db_name(name) {
            return Err(ApiError::BadRequest(format!("invalid database name '{}'", name)));
        }
    }
    let guard = ctx.begin_restore(&target)?;
    let root = ctx.restore_root()?.to_path_buf();

    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or(0);
    let upload = Upload(root.join(format!(".upload-{}-{}.marcibin", target, nanos)));
    receive(req, &upload.0).await?;

    // The upload and the guard move into the blocking task: they are released when the restore is over,
    // even if this request is dropped meanwhile.
    let restored = target.clone();
    let info = blocking(move || {
        let _guard = guard;
        let file = std::fs::File::open(&upload.0).map_err(|e| ApiError::Internal(format!("cannot read the upload: {}", e)))?;
        let options = RestoreOptions { overwrite: true, open: OpenOptions::default() };
        let staged = MarciDB::restore_prepare(&root.join(&restored).to_string_lossy(), file, &options)?;
        ctx.replace_db(&restored, staged)
    }).await?;

    let mut body = info.to_json();
    body["restored"] = serde_json::Value::String(target);
    Ok(ok_response(body.to_string()))
}

/// Streams the request body into `path`.
async fn receive(req: Request<hyper::body::Incoming>, path: &std::path::Path) -> Result<(), ApiError> {
    let internal = |e: io::Error| ApiError::Internal(format!("cannot store the upload: {}", e));
    let mut file = tokio::fs::File::create(path).await.map_err(internal)?;
    let mut body = req.into_body();
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|e| ApiError::BadRequest(format!("failed to read the upload: {}", e)))?;
        if let Ok(data) = frame.into_data() {
            file.write_all(&data).await.map_err(internal)?;
        }
    }
    file.flush().await.map_err(internal)
}
