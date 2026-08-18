use http_body_util::Full;
use hyper::{Response, StatusCode, body::Bytes};
use marcidb::{DeleteError, EncodeError, InsertError, UpdateError};

// errors.rs
#[derive(Debug)]
pub enum ApiError {
    Unauthorized(String),
    NotFound(String),
    BadRequest(String),
    Internal(String),
}

// Error → Response conversion — once and for all
impl ApiError {
    pub fn into_response(self) -> Response<Full<Bytes>> {
        let (status, msg) = match self {
            ApiError::Unauthorized(m) => (StatusCode::UNAUTHORIZED, m),
            ApiError::NotFound(m)  => (StatusCode::NOT_FOUND, m),
            ApiError::BadRequest(m) => (StatusCode::BAD_REQUEST, m),
            ApiError::Internal(m)  => (StatusCode::INTERNAL_SERVER_ERROR, m),
        };
        let mut res = Response::new(Full::new(Bytes::from(msg)));
        *res.status_mut() = status;
        res
    }
}

// MarciDbError → ApiError automatically via ?
impl From<InsertError> for ApiError {
    fn from(e: InsertError) -> Self {
        ApiError::Internal(format!("{:?}", e))
    }
}

impl From<UpdateError> for ApiError {
    fn from(e: UpdateError) -> Self {
        ApiError::Internal(format!("{:?}", e))
    }
}

impl From<DeleteError> for ApiError {
    fn from(e: DeleteError) -> Self {
        ApiError::Internal(format!("{:?}", e))
    }
}

impl From<EncodeError> for ApiError {
    fn from(e: EncodeError) -> Self {
        ApiError::Internal(format!("{:?}", e))
    }
}