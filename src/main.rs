use std::convert::Infallible;
use std::fs;
use std::net::SocketAddr;
use std::sync::Arc;

use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use serde_json::Value;
use tokio::net::TcpListener;

use crate::marci_db::{MarciDB, MarciSelect};
use crate::marci_decoder::decode_document;
use crate::marci_encoder::encode_document;
use crate::marci_select::{parse_select};
use crate::schema::parse_schema;

mod marci_db;
mod schema;
mod marci_encoder;
mod marci_decoder;
mod marci_select;

async fn handle(req: Request<hyper::body::Incoming>, db: Arc<MarciDB>) -> Result<Response<Full<Bytes>>, Infallible> {

    let path = req.uri().path();

    let slash_index = path[1..].find('/').map(|i| i + 1).unwrap_or(path.len());
    
    let model_name = &path[1..slash_index].to_string();

    let action = &path[slash_index+1..];
    let Some(model) = db.get_model(model_name) else {
        return Ok(error(StatusCode::NOT_FOUND, &format!("Model {} not found", &path[1..slash_index])));
    };

    match (req.method(), action) {
        (&Method::POST, "insert") => {

            let Ok(whole_body) = req.collect().await else {
                return Ok(error(StatusCode::BAD_REQUEST, "Failed to get body"));
            };
                
            // Преобразуем в &str или &[u8] и парсим JSON
            let Ok(json_val): Result<Value, _> = serde_json::from_slice(&whole_body.to_bytes()) else {
                return Ok(error(StatusCode::BAD_REQUEST, "Failed to parse JSON"));
            };

            // Теперь `json_val` — ваш JSON объект, с которым можно работать
            // Например: вставка в БД и т. д.
            // db.insert(json_val.clone()); // пример

            let mut structs = vec![];
            let (data, _) = match encode_document(model, &json_val, &mut structs) {
                Ok(result) => result,
                Err(err) => return Ok(error(StatusCode::BAD_REQUEST, &format!("Failed to encode document: {:?}", err)))
            };
            
            let new_id = match db.insert_data(model, &data, &structs) {
                Ok(result) => result,
                Err(err) => return Ok(error(StatusCode::BAD_REQUEST, &format!("Failed to insert document: {:?}", err))) 
            };

            // Возвращаем успешный ответ
            let body = Bytes::from(format!("{{ \"id\": {new_id} }}"));
            let resp = Response::new(Full::new(body));
            Ok(resp)
        }

        (&Method::GET, "findMany") => {

            let select = MarciSelect::all(&model.fields);

            let data = db.get_all(model, &select, | ctx | {
                return decode_document(ctx).unwrap();
            });

            let body = Bytes::from(Value::Array(data).to_string());
            let resp = Response::new(Full::new(body));
            Ok(resp)
        }

        (&Method::POST, "findMany") => {

            let Ok(whole_body) = req.collect().await else {
                return Ok(error(StatusCode::BAD_REQUEST, "Failed to get body"));
            };
                
            // Преобразуем в &str или &[u8] и парсим JSON
            let Ok(select): Result<Value, _> = serde_json::from_slice(&whole_body.to_bytes()) else {
                return Ok(error(StatusCode::BAD_REQUEST, "Failed to parse JSON"));
            };

            let select = match parse_select(&model.fields, &select, &db.schema) {
                Ok(result) => result,
                Err(err) => return Ok(error(StatusCode::BAD_REQUEST, &format!("Failed to insert document: {:?}", err))) 
            };

            let data = db.get_all(model, &select, |ctx | {
                return decode_document(ctx).unwrap();
            });

            let body = Bytes::from(Value::Array(data).to_string());
            let resp = Response::new(Full::new(body));
            Ok(resp)
        }

        (&Method::POST, "update") => {

            let Ok(whole_body) = req.collect().await else {
                return Ok(error(StatusCode::BAD_REQUEST, "Failed to get body"));
            };
                
            // Преобразуем в &str или &[u8] и парсим JSON
            let Ok(json_val): Result<Value, _> = serde_json::from_slice(&whole_body.to_bytes()) else {
                return Ok(error(StatusCode::BAD_REQUEST, "Failed to parse JSON"));
            };
            let Some(id) = json_val.get("id").and_then(|a| a.as_u64()) else {
                return Ok(error(StatusCode::BAD_REQUEST, "ID field required"));
            };

            let mut structs = vec![];
            let (new_data, changed_mask) = match encode_document(model, &json_val, &mut structs) {
                Ok(result) => result,
                Err(err) => return Ok(error(StatusCode::BAD_REQUEST, &format!("Failed to encode document: {:?}", err)))
            };

            let item_id = match db.update(model,  id, &new_data, changed_mask) {
                Ok(result) => result,
                Err(err) => return Ok(error(StatusCode::BAD_REQUEST, &format!("Failed to update document: {:?}", err))) 
            };

            let body = Bytes::from(format!("{{ \"id\": {} }}", item_id));
            let resp = Response::new(Full::new(body));
            Ok(resp)
        }

        (&Method::POST, "delete") => {
            let Ok(whole_body) = req.collect().await else {
                return Ok(error(StatusCode::BAD_REQUEST, "Failed to get body"));
            };
            let Ok(json_val): Result<Value, _> = serde_json::from_slice(&whole_body.to_bytes()) else {
                return Ok(error(StatusCode::BAD_REQUEST, "Failed to parse JSON"));
            };
            let Some(id) = json_val.get("id").and_then(|a| a.as_u64()) else {
                return Ok(error(StatusCode::BAD_REQUEST, "ID field required"));
            };

            let deleted = db.delete(model, id);
            if !deleted {
                return Ok(error(StatusCode::BAD_REQUEST, "Object not found"));
            }

            let body = Bytes::from(format!("{{ \"id\": {} }}", id));
            let resp = Response::new(Full::new(body));
            Ok(resp)
        }

        _ => {
            Ok(error(StatusCode::NOT_FOUND, &format!("Route {}:{} not found", req.method().as_str(), req.uri())))
        }
    }
}

fn error(code: StatusCode, msg: &str) -> Response<Full<Bytes>> {
    let mut res = Response::new(Full::new(Bytes::from(msg.to_string())));
    *res.status_mut() = code;
    res
}


#[tokio::main]
async fn main() {
    // Открываем хранилище

    let schema = parse_schema(&fs::read_to_string("schema.marci").unwrap());

    let db: Arc<MarciDB> = Arc::new(MarciDB::new(schema));

    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));

    // We create a TcpListener and bind it to 127.0.0.1:3000
    let listener = TcpListener::bind(addr).await.unwrap();

    // We start a loop to continuously accept incoming connections
    loop {
        let (stream, _) = listener.accept().await.unwrap();

        // Use an adapter to access something implementing `tokio::io` traits as if they implement
        // `hyper::rt` IO traits.
        let io = TokioIo::new(stream);

        let db = db.clone();

        // Spawn a tokio task to serve multiple connections concurrently
        tokio::task::spawn(async move {
            // Finally, we bind the incoming connection to our `hello` service
            if let Err(err) = http1::Builder::new()
                // `service_fn` converts our function in a `Service`
                .serve_connection(io, service_fn(move |req| {
                    handle(req, db.clone())
                }))
                .await
            {
                eprintln!("Error serving connection: {:?}", err);
            }
        });
    }

}
