use std::convert::Infallible;
use std::{fs, vec};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper::header::CONTENT_TYPE;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use marcidb::{Attribute, Entity, FieldType, MarciDB, MarciSelect, VectorIndexType, array_to_json, decode_document, decode_id, encode_document, encode_id, get_value_from_data, parse_schema, parse_select};
use serde_json::Value;
use tokio::net::TcpListener;

use marci_vector::{CustomDistance, ReadCluster, WriteCluster};

mod marci_vector_utils;

pub struct ServerContext {
    db: MarciDB
}

async fn handle(req: Request<hyper::body::Incoming>, ctx: Arc<ServerContext>) -> Result<Response<Full<Bytes>>, Infallible> {

    let path = req.uri().path();

    let slash_index = path[1..].find('/').map(|i| i + 1).unwrap_or(path.len());
    
    let model_name = &path[1..slash_index].to_string();

    let action = &path[slash_index+1..];
    let Some((model_index, model)) = ctx.db.get_model(model_name) else {
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
            let (data, _) = match encode_document(&ctx.db.schema, model, &json_val, &mut structs) {
                Ok(result) => result,
                Err(err) => return Ok(error(StatusCode::BAD_REQUEST, &format!("Failed to encode document: {:?}", err)))
            };
            let mut id = match encode_id(model, &json_val, true) {
                Ok(result) => result,
                Err(err) => return Ok(error(StatusCode::BAD_REQUEST, &format!("Failed to encode document: {:?}", err)))
            };
            
            if let Err(err) = ctx.db.insert_data(model, &mut id, &data, &mut structs) {
                return Ok(error(StatusCode::BAD_REQUEST, &format!("Failed to insert document: {:?}", err)));
            }

            let body = Bytes::from(decode_id(&id, model).unwrap().to_string());
            let resp = Response::new(Full::new(body));
            Ok(resp)
        }

        (&Method::GET, "findMany") => {

            let select = MarciSelect::all(&model.fields);

            let data = ctx.db.get_all(model, &select, | ctx | {
                return decode_document(ctx).unwrap();
            });

            let body = Bytes::from(array_to_json(&data));
            let resp = Response::new(Full::new(body));
            Ok(resp)
        }

        (&Method::POST, "findMany") => {
            let Ok(whole_body) = req.collect().await else {
                return Ok(error(StatusCode::BAD_REQUEST, "Failed to get body"));
            };
            
            // Преобразуем в &str или &[u8] и парсим JSON
            let Ok(body_json): Result<Value, _> = serde_json::from_slice(&whole_body.to_bytes()) else {
                return Ok(error(StatusCode::BAD_REQUEST, "Failed to parse JSON"));
            };

            let select = match parse_select(&model.fields, &body_json, &ctx.db.schema, None) {
                Ok(result) => result,
                Err(err) => return Ok(error(StatusCode::BAD_REQUEST, &format!("Failed to parse select: {:?}", err))) 
            };

            let now = Instant::now();
            
            let ids = match parse_where(&ctx, model, &body_json) {
                Ok(result) => result,
                Err(err) => return Ok(error(StatusCode::BAD_REQUEST, &format!("Failed to parse where: {:?}", err))) 
            };

            if let Some(ids) = &ids {
                println!("Get {} ids. Elapsed: {:?}", ids.len(), now.elapsed());
            }
            
            let data = match ids {
                Some(ids) => ctx.db.get_by_ids(&ids, model, &select, |ctx | {
                    return decode_document(ctx).unwrap();
                }),
                None => ctx.db.get_all(model, &select, |ctx | {
                    return decode_document(ctx).unwrap();
                })
            };

            let body = Bytes::from(array_to_json(&data));
            let mut resp = Response::new(Full::new(body));
            resp.headers_mut().insert(CONTENT_TYPE, "application/json".parse().unwrap());

            println!("Query time: {:?}", now.elapsed());

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

            let mut structs = vec![];
            let (new_data, changed_mask) = match encode_document(&ctx.db.schema, model, &json_val, &mut structs) {
                Ok(result) => result,
                Err(err) => return Ok(error(StatusCode::BAD_REQUEST, &format!("Failed to encode document: {:?}", err)))
            };

            let id = match encode_id(model, &json_val, false) {
                Ok(result) => result,
                Err(err) => return Ok(error(StatusCode::BAD_REQUEST, &format!("Failed to encode document: {:?}", err)))
            };

            if let Err(err) =  ctx.db.update(model, &id, &new_data, changed_mask, &structs) {
               return Ok(error(StatusCode::BAD_REQUEST, &format!("Failed to update document: {:?}", err)));
            }

            let body = Bytes::from(decode_id(&id, model).unwrap().to_string());
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

            let id = match encode_id(model, &json_val, false) {
                Ok(result) => result,
                Err(err) => return Ok(error(StatusCode::BAD_REQUEST, &format!("Failed to delete document: {:?}", err)))
            };

            if let Err(err) = ctx.db.delete(model_index, model, &id) {
                return Ok(error(StatusCode::BAD_REQUEST, &format!("Failed to delete document: {:?}", err)));
            };

            let body = Bytes::from(decode_id(&id, model).unwrap().to_string());
            let resp = Response::new(Full::new(body));
            Ok(resp)
        }

        // Only marci vector utils
        (&Method::POST, "index") => {
            for (field_index, field) in model.fields.iter().enumerate() {
                let Some(vector_index_type) = field.attributes.iter().find_map(|f| {
                    if let Attribute::VectorIndex(i) = f { Some(i) } else { None }
                }) else {
                    continue;
                };

                let (primitive_type, &arr_size) = match &field.ty {
                    FieldType::PrimitiveFixedList(primitive, size) => (primitive, size),
                    _ => { 
                        println!("You cannot use vector only with primitive fixed list fields");
                        continue;
                    }
                };
                let Some(el_size) = primitive_type.get_size() else {
                    println!("You cannot use vector index with no number values");
                    continue;
                };

                let mut select = MarciSelect::new(model);
                select.mask.set(field_index, true);
                
                let coordinates = ctx.db.get_all_filter(model, &select, |ctx| {
                    let Some(data) = get_value_from_data(field, ctx.id, ctx.data, el_size * arr_size) else {
                        return None;
                    };
                    let mut floats: Vec<f32> = data
                        .chunks(4)
                        .map(|f| f32::from_be_bytes(f.try_into().unwrap()))
                        .collect();

                    if floats.len() != arr_size {
                        println!("Warn: point size is wrong. Expected: {}, Received: {}", arr_size, floats.len());
                    }

                    if matches!(vector_index_type, VectorIndexType::Cosine) {
                        let norm: f32 = floats.iter().map(|x| x * x).sum::<f32>().sqrt();
                        for f in floats.iter_mut() {
                            *f /= norm;
                        }
                    }

                    Some((ctx.id.to_vec(), floats))
                });

                
                let tx = ctx.db.db.begin_write().unwrap();
                
                {
                    let index_name = [&field.full_name, ".vectorindex"].concat();
                    println!("Ready to write {} items to tree {}", coordinates.len(), index_name);
                    
                    let mut tree = tx.get_or_create_tree(index_name.as_bytes()).unwrap();
                    tree.clear().unwrap();
                    
                    let distance = match vector_index_type {
                        VectorIndexType::Cosine => CustomDistance::Cosine,
                        VectorIndexType::Euclidean => CustomDistance::Euclidean
                    };
                    ctx.create_cluster(&mut tree, &coordinates, distance);
                }

                tx.commit().unwrap();
            }

            let body = Bytes::from("{ \"ok\": true }");
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

#[derive(Debug)]
enum ParseWhereError {
    TypeMismatch { field: String, expected: String },
}

fn parse_where(ctx: &ServerContext, model: &Entity, body_json: &Value) -> Result<Option<Vec<Vec<u8>>>,ParseWhereError> {
    let Some(where_obj) = body_json.get("$where") else {
        return Ok(None)
    };
                
    for field in model.fields.iter() {
        let Some(where_field) = where_obj.get(&field.name) else { continue; };

        let vector_index_type = field.attributes.iter().find_map(|f| {
            if let Attribute::VectorIndex(i) = f { Some(i) } else { None }
        });

        if let Some(vector_index_type) = vector_index_type {
            let (_primitive_type, &size) = match &field.ty {
                FieldType::PrimitiveFixedList(primitive_type, size) => (primitive_type, size),
                _ => panic!("Wrong field type {}. Expected fixed list", field.full_name)
            };

            let mut point = where_field
                .as_object()
                .and_then(|obj| obj.get("$close"))
                .and_then(|close| close.as_array())
                .filter(|arr | arr.len() == size)
                .and_then(|arr| {
                    let mut points = Vec::with_capacity(arr.len());
                    for i in arr.iter() {
                        let Some(f) = i.as_f64() else { return None };
                        points.push(f as f32);
                    }
                    Some(points)
                })
                .ok_or_else(|| ParseWhereError::TypeMismatch {
                    field: field.full_name.clone(),
                    expected: format!("{{ $close: f32[{size}] }}"),
                })?;

            if matches!(vector_index_type, VectorIndexType::Cosine) {
                let norm: f32 = point.iter().map(|x| x * x).sum::<f32>().sqrt();
                for f in point.iter_mut() {
                    *f /= norm;
                }
            }
            
            let take = where_field
                .get("$take")
                .map(|f|f.as_u64())
                .unwrap_or(Some(10))
                .ok_or_else(|| ParseWhereError::TypeMismatch {
                    field: field.full_name.clone(),
                    expected: format!("{{ $take: number }}"),
                })?;

            let threshold = where_field
                .get("$threshold")
                .map(|f|f.as_f64())
                .unwrap_or(Some(0f64))
                .map(|f| f as f32)
                .ok_or_else(|| ParseWhereError::TypeMismatch {
                    field: field.full_name.clone(),
                    expected: format!("{{ $threshold: number }}"),
                })?;

            let rx = ctx.db.db.begin_read().unwrap();
            {   
                let index_name = [&field.full_name, ".vectorindex"].concat();
                let tree = rx.get_tree(index_name.as_bytes()).unwrap().unwrap();

                let distance = match vector_index_type {
                    VectorIndexType::Cosine => CustomDistance::Cosine,
                    VectorIndexType::Euclidean => CustomDistance::Euclidean
                };
                
                let ids = ctx.find_nearest_points(&tree, &point, take as usize, distance, threshold);
                println!("{:?} {:?}", vector_index_type, ids);

                return Ok(Some(ids.into_iter().map(|i|i.0).collect()))
            }
        }
    }

    return Ok(None);
}


#[tokio::main]
async fn main() {
    // Открываем хранилище

    let schema = parse_schema(&fs::read_to_string("schema.marci").unwrap());

    for model in schema.models.iter() {
        println!("{:#?}", model);
    }

    let ctx: Arc<ServerContext> = Arc::new(ServerContext{ db: MarciDB::new(schema) });

    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));

    // We create a TcpListener and bind it to 127.0.0.1:3000
    let listener = TcpListener::bind(addr).await.unwrap();

    // We start a loop to continuously accept incoming connections
    loop {
        let (stream, _) = listener.accept().await.unwrap();

        // Use an adapter to access something implementing `tokio::io` traits as if they implement
        // `hyper::rt` IO traits.
        let io = TokioIo::new(stream);

        let ctx = ctx.clone();

        // Spawn a tokio task to serve multiple connections concurrently
        tokio::task::spawn(async move {
            // Finally, we bind the incoming connection to our `hello` service
            if let Err(err) = http1::Builder::new()
                // `service_fn` converts our function in a `Service`
                .serve_connection(io, service_fn(move |req| {
                    handle(req, ctx.clone())
                }))
                .await
            {
                eprintln!("Error serving connection: {:?}", err);
            }
        });
    }

}
