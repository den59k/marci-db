use std::{collections::HashMap, convert::Infallible, net::SocketAddr, path::PathBuf, sync::{Arc, Mutex, RwLock}};

use http_body_util::Full;
use hyper::{Method, Request, Response, body::Bytes, server::conn::http1, service::service_fn};
use hyper_util::rt::TokioIo;
use marcidb::MarciDB;
use tokio::{fs, net::TcpListener};

use crate::{errors::ApiError, handlers::{handle_aggregate, handle_count, handle_delete, handle_find_first, handle_find_many, handle_insert, handle_migrate, handle_transaction, handle_update}};

mod handlers;
mod errors;
mod helpers;

/// Хостит несколько БД из одной директории. БД открывается лениво при первом обращении;
/// `$migrate` создаёт её, если ещё нет. Каждая БД под своим `RwLock` (миграция эксклюзивна, данные — shared)
pub struct ServerContext {
    root: PathBuf,
    dbs: Mutex<HashMap<String, Arc<RwLock<MarciDB>>>>,
}

impl ServerContext {
    fn new(root: PathBuf) -> Self {
        ServerContext { root, dbs: Mutex::new(HashMap::new()) }
    }

    /// БД по имени. `allow_create` (для `$migrate`) создаёт пустую БД; иначе несуществующая → NotFound
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

        // canopydb требует существующую директорию БД
        std::fs::create_dir_all(&path).map_err(|e| ApiError::Internal(e.to_string()))?;

        let db = Arc::new(RwLock::new(MarciDB::open(path.to_str().unwrap())));
        dbs.insert(name.to_string(), db.clone());
        Ok(db)
    }
}

/// Имя БД — сегмент пути, поэтому без слешей/точек (защита от path traversal)
fn is_valid_db_name(name: &str) -> bool {
    !name.is_empty() && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

// Публичный хэндлер — перехватывает все ошибки, hyper видит только Ok
pub async fn handle(
    req: Request<hyper::body::Incoming>,
    ctx: Arc<ServerContext>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let response = handle_inner(req, ctx)
        .await
        .unwrap_or_else(|e| e.into_response());
    Ok(response)
}

// "/{db}/$migrate" | "/{db}/$transaction" | "/{db}/{model}/{action}[/{id}]"
async fn handle_inner(
    req: Request<hyper::body::Incoming>,
    ctx: Arc<ServerContext>,
) -> Result<Response<Full<Bytes>>, ApiError> {
    let method = req.method().clone();
    let path = req.uri().path().trim_matches('/').to_string();

    let (db_name, rest) = path.split_once('/')
        .ok_or_else(|| ApiError::BadRequest("expected path /<db>/...".to_string()))?;
    let db_name = db_name.to_string();

    if method == Method::POST {
        match rest {
            "$migrate" => return handle_migrate(req, ctx, db_name).await,
            "$transaction" => return handle_transaction(req, ctx, db_name).await,
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
        _ => Err(ApiError::NotFound(format!("Route {} /{} not found", method, path))),
    }
}

#[tokio::main]
async fn main() {
    fs::create_dir_all("./data").await.unwrap();
    let ctx: Arc<ServerContext> = Arc::new(ServerContext::new(PathBuf::from("./data")));

    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    let listener = TcpListener::bind(addr).await.unwrap();

    println!("MarciDB is running on http://{}", addr);

    loop {
        let (stream, _) = listener.accept().await.unwrap();
        let io = TokioIo::new(stream);
        let ctx = ctx.clone();

        tokio::task::spawn(async move {
            let resp = http1::Builder::new()
                .serve_connection(io, service_fn(move |req| { handle(req, ctx.clone()) }))
                .await;

            if let Err(err) = resp {
                eprintln!("Error serving connection: {:?}", err);
            }
        });
    }
}
