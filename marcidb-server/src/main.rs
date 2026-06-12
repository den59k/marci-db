use std::{convert::Infallible, net::SocketAddr, sync::Arc};

use http_body_util::Full;
use hyper::{Method, Request, Response, body::Bytes, server::conn::http1, service::service_fn};
use hyper_util::rt::TokioIo;
use marcidb::MarciDB;
use tokio::{fs, net::TcpListener};

use crate::{errors::ApiError, handlers::{handle_aggregate, handle_count, handle_delete, handle_find_first, handle_find_many, handle_insert, handle_update}};

mod handlers;
mod errors;
mod helpers;

pub struct ServerContext {
    pub db: MarciDB,
}

// Публичный хэндлер — перехватывает все ошибки, hyper видит только Ok
pub async fn handle(
    req: Request<hyper::body::Incoming>,
    ctx: Arc<ServerContext>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let response = handle_inner(req, ctx)
        .await
        .unwrap_or_else(|e| e.into_response()); // все ошибки сюда
    Ok(response)
}


// Внутренний хэндлер возвращает Result — можно использовать ?
async fn handle_inner(
    req: Request<hyper::body::Incoming>,
    ctx: Arc<ServerContext>,
) -> Result<Response<Full<Bytes>>, ApiError> {
    let (model_name, action, query) = parse_path(req.uri().path())?;

    let model = ctx.db.get_model_index(&model_name)
        .ok_or_else(|| ApiError::NotFound(format!("Model '{}' not found", model_name)))?;

    match (req.method(), action.as_str()) {
        (&Method::POST, "insert") => handle_insert(req, ctx.clone(), model).await,
        (&Method::POST, "findMany") => handle_find_many(req, ctx.clone(), model).await,
        (&Method::POST, "findFirst") => handle_find_first(req, ctx.clone(), model).await,
        (&Method::POST, "count") => handle_count(req, ctx.clone(), model).await,
        (&Method::POST, "aggregate") => handle_aggregate(req, ctx.clone(), model).await,
        (&Method::POST, "update") => {
            let Some(query) = query else { return Err(ApiError::BadRequest("Param :itemId required".to_string())) };
            handle_update(req, query, ctx.clone(), model).await
        },
        (&Method::POST, "delete") => {
            let Some(query) = query else { return Err(ApiError::BadRequest("Param :itemId required".to_string())) };
            handle_delete(query, ctx.clone(), model).await
        },
        // (&Method::GET,  "get")    => handle_get(req, ctx, model).await,
        _ => Err(ApiError::NotFound(format!(
            "Route {}:{} not found", req.method(), req.uri()
        ))),
    }
}

// Парсинг пути — отдельно, с нормальными ошибками
// "/{model}/{action}" → ("model", "action")
fn parse_path(path: &str) -> Result<(String, String, Option<String>), ApiError> {
    let parts: Vec<&str> = path.trim_matches('/').splitn(3, '/').collect();
    match parts.as_slice() {
        [model, action, id] if !model.is_empty() && !action.is_empty() => {
            Ok((model.to_string(), action.to_string(), Some(id.to_string())))
        }
        [model, action] if !model.is_empty() && !action.is_empty() => {
            Ok((model.to_string(), action.to_string(), None))
        }
        _ => Err(ApiError::BadRequest(format!("Invalid path: '{}'", path))),
    }
}

#[tokio::main]
async fn main() {

    let schema_str = &fs::read_to_string("schema.marci").await.expect("schema.marci file not found");

    fs::create_dir_all("./data").await.unwrap();
    let ctx: Arc<ServerContext> = Arc::new(ServerContext{ db: MarciDB::new(schema_str, "./data") });

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