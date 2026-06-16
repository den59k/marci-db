use http_body_util::{BodyExt, Full};
use hyper::{Request, Response, body::Bytes, header};
use serde_json::Value;

use crate::errors::ApiError;

/// Media type for a binary result buffer (the engine's `binary_encode` wire format). Opt-in via `Accept`;
/// JSON stays the default so curl and any non-negotiating client are unaffected.
pub const BINARY_MEDIA_TYPE: &str = "application/x-marcidb-rows";

/// Header carrying the client's schema fingerprint. The server returns binary only when it matches the
/// target database's current schema — the HTTP analogue of the FFI ABI handshake (see `schema_fingerprint`).
pub const SCHEMA_HEADER: &str = "x-marci-schema";

/// What a read handler decided to return — JSON (the default) or a binary result buffer.
pub enum ReadBody {
    Json(String),
    Binary(Vec<u8>),
}

/// Whether a request opted into the binary read path, and the schema fingerprint it claims. Parsed from the
/// request headers *before* the body is consumed (the body parse takes the request by value).
pub struct BinaryNeg {
    pub wants_binary: bool,
    pub schema_hash: Option<String>,
}

impl BinaryNeg {
    pub fn from_req(req: &Request<hyper::body::Incoming>) -> Self {
        let accept = req.headers().get(header::ACCEPT).and_then(|v| v.to_str().ok()).unwrap_or("");
        // `Accept: application/x-marcidb-rows, application/json` → binary preferred; strip any `;q=`/params.
        let wants_binary = accept
            .split(',')
            .any(|m| m.split(';').next().map(str::trim) == Some(BINARY_MEDIA_TYPE));
        let schema_hash = req.headers().get(SCHEMA_HEADER).and_then(|v| v.to_str().ok()).map(str::to_string);
        BinaryNeg { wants_binary, schema_hash }
    }
}

pub async fn parse_json_body(
    req: Request<hyper::body::Incoming>
) -> Result<Value, ApiError> {
    let body = req.collect().await
        .map_err(|e| ApiError::BadRequest(format!("Failed to read body: {}", e)))?
        .to_bytes();

    serde_json::from_slice(&body)
        .map_err(|e| ApiError::BadRequest(format!("Invalid JSON: {}", e)))
}

/// Reads the request body as text (for `$sync` — the body is the `.marci` schema text)
pub async fn parse_text_body(
    req: Request<hyper::body::Incoming>
) -> Result<String, ApiError> {
    let body = req.collect().await
        .map_err(|e| ApiError::BadRequest(format!("Failed to read body: {}", e)))?
        .to_bytes();

    String::from_utf8(body.to_vec())
        .map_err(|e| ApiError::BadRequest(format!("Invalid UTF-8 body: {}", e)))
}

/// Runs heavy work on the blocking pool
pub async fn blocking<F, T>(f: F) -> Result<T, ApiError>
where
    F: FnOnce() -> Result<T, ApiError> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| ApiError::Internal(e.to_string()))? // JoinError
}

/// Builds the final response
pub fn ok_response(body: impl Into<Bytes>) -> Response<Full<Bytes>> {
    Response::new(Full::new(body.into()))
}

/// A read response — JSON keeps the existing (untyped) body; binary carries the explicit media type so the
/// client can tell the two apart on the response side.
pub fn read_response(body: ReadBody) -> Response<Full<Bytes>> {
    match body {
        ReadBody::Json(s) => ok_response(s),
        ReadBody::Binary(b) => Response::builder()
            .header(header::CONTENT_TYPE, BINARY_MEDIA_TYPE)
            .body(Full::new(Bytes::from(b)))
            .unwrap(),
    }
}