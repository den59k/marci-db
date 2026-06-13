use http_body_util::{BodyExt, Full};
use hyper::{Request, Response, body::Bytes};
use serde_json::Value;

use crate::errors::ApiError;

pub async fn parse_json_body(
    req: Request<hyper::body::Incoming>
) -> Result<Value, ApiError> {
    let body = req.collect().await
        .map_err(|e| ApiError::BadRequest(format!("Failed to read body: {}", e)))?
        .to_bytes();

    serde_json::from_slice(&body)
        .map_err(|e| ApiError::BadRequest(format!("Invalid JSON: {}", e)))
}

/// Читает тело запроса как текст (для `$migrate` — тело является текстом схемы `.marci`)
pub async fn parse_text_body(
    req: Request<hyper::body::Incoming>
) -> Result<String, ApiError> {
    let body = req.collect().await
        .map_err(|e| ApiError::BadRequest(format!("Failed to read body: {}", e)))?
        .to_bytes();

    String::from_utf8(body.to_vec())
        .map_err(|e| ApiError::BadRequest(format!("Invalid UTF-8 body: {}", e)))
}

/// Запускает тяжёлую работу в blocking pool
pub async fn blocking<F, T>(f: F) -> Result<T, ApiError>
where
    F: FnOnce() -> Result<T, ApiError> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| ApiError::Internal(e.to_string()))? // JoinError
}

/// Строит финальный ответ
pub fn ok_response(body: impl Into<Bytes>) -> Response<Full<Bytes>> {
    Response::new(Full::new(body.into()))
}