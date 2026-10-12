use http_body_util::Full;
use hyper::{Response, StatusCode, body::Bytes, header};
use marcidb::{BackupError, BatchError, DeleteError, EncodeError, InsertError, OpError, UpdateError, UpsertError};

// errors.rs
/// What a request failed with. Every variant answers as JSON —
/// `{ "error": <message>, "kind": <kind>[, "field": "<Model.field>"] }` — with the status its kind means:
/// `kind` is what a client switches on, the message is for a person.
#[derive(Debug)]
pub enum ApiError {
    Unauthorized(String),
    NotFound(String),
    BadRequest(String),
    Internal(String),
    /// The database is being restored, or stayed busy past the restore's wait (503): retry shortly.
    Unavailable(String),
    /// A failure the engine classified itself (`OpError::kind`): a constraint — `unique`, `foreign_key`,
    /// `restrict`, `duplicate_id` (409) —, `not_found` (404), `bad_request` (400), `internal` (500).
    Engine { kind: &'static str, field: Option<String>, message: String },
}

// Error → Response conversion — once and for all
impl ApiError {
    pub fn into_response(self) -> Response<Full<Bytes>> {
        let (kind, field, message) = match self {
            ApiError::Unauthorized(m) => ("unauthorized", None, m),
            ApiError::NotFound(m) => ("not_found", None, m),
            ApiError::BadRequest(m) => ("bad_request", None, m),
            ApiError::Internal(m) => ("internal", None, m),
            ApiError::Unavailable(m) => ("unavailable", None, m),
            ApiError::Engine { kind, field, message } => (kind, field, message),
        };
        let status = match kind {
            "unique" | "foreign_key" | "restrict" | "duplicate_id" => StatusCode::CONFLICT,
            "not_found" => StatusCode::NOT_FOUND,
            "unauthorized" => StatusCode::UNAUTHORIZED,
            "bad_request" => StatusCode::BAD_REQUEST,
            "unavailable" => StatusCode::SERVICE_UNAVAILABLE,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let mut body = serde_json::json!({ "error": message, "kind": kind });
        if let Some(field) = field { body["field"] = serde_json::Value::String(field); }
        let mut res = Response::new(Full::new(Bytes::from(body.to_string())));
        *res.status_mut() = status;
        res.headers_mut().insert(header::CONTENT_TYPE, header::HeaderValue::from_static("application/json"));
        res
    }
}

fn engine(kind: &'static str, field: Option<&str>, message: String) -> ApiError {
    ApiError::Engine { kind, field: field.map(str::to_string), message }
}

// Engine errors → ApiError automatically via ?
impl From<InsertError> for ApiError {
    fn from(e: InsertError) -> Self { engine(e.kind(), e.field(), e.to_string()) }
}

impl From<UpdateError> for ApiError {
    fn from(e: UpdateError) -> Self { engine(e.kind(), e.field(), e.to_string()) }
}

impl From<DeleteError> for ApiError {
    fn from(e: DeleteError) -> Self { engine(e.kind(), e.field(), e.to_string()) }
}

impl From<UpsertError> for ApiError {
    fn from(e: UpsertError) -> Self { engine(e.kind(), e.field(), e.to_string()) }
}

impl From<OpError> for ApiError {
    fn from(e: OpError) -> Self { engine(e.kind(), e.field(), e.to_string()) }
}

impl From<BatchError> for ApiError {
    fn from(e: BatchError) -> Self { engine(e.kind(), e.field(), e.to_string()) }
}

impl From<EncodeError> for ApiError {
    fn from(e: EncodeError) -> Self { ApiError::BadRequest(e.to_string()) }
}

/// A malformed or damaged upload is the client's (400); everything else is the server's (500).
impl From<BackupError> for ApiError {
    fn from(e: BackupError) -> Self {
        match e {
            BackupError::Format(_) => ApiError::BadRequest(e.to_string()),
            _ => ApiError::Internal(e.to_string()),
        }
    }
}
