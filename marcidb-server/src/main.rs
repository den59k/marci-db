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
use marcidb::{Attribute, Entity, FieldType, MarciDB, MarciSelect, VectorIndexType, array_to_json, decode_document, decode_id, encode_document, encode_id, encode_index_prefix, get_value_from_data, parse_schema, parse_select, PrimitiveFieldType};
use serde_json::Value;
use tokio::net::TcpListener;
use marci_vector::{CustomDistance, ReadCluster, WriteCluster};

mod marci_vector_utils;

use std::collections::HashSet;

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
                    let Some(data) = get_value_from_data(field, ctx.id, ctx.data, Some(el_size * arr_size)) else {
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
    TypeMismatch { _field: String, _expected: String },
}

// Метод возвращает ID элементов, если можно сделать выборку по индексу
fn parse_where(ctx: &ServerContext, model: &Entity, body_json: &Value) -> Result<Option<Vec<Vec<u8>>>,ParseWhereError> {
    let Some(where_obj) = body_json.get("$where") else {
        return Ok(None)
    };

    if let Some(and_array) = where_obj.get("$and").and_then(|v| v.as_array()) {
        return eval_and(ctx, model, and_array);
    }
    if let Some(or_array) = where_obj.get("$or").and_then(|v| v.as_array()) {
        return eval_or(ctx, model, or_array);
    }

    for field in model.fields.iter() {
        let Some(where_field) = where_obj.get(&field.name) else { continue; };

        if let Some(index) = field.get_field_index() {
            let rx = ctx.db.db.begin_read().unwrap();
            let tree = rx.get_tree(index.tree_name()).unwrap().unwrap();

            let val = encode_index_prefix(field, where_field).unwrap();
            let mut ids = tree.prefix_keys(&val).unwrap();
            if let Some(id) = ids.next() {
                return Ok(Some(vec![id.unwrap()[val.len()..].to_vec()]));
            } else {
                return Ok(Some(vec![]));
            }
        }

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
                    _field: field.full_name.clone(),
                    _expected: format!("{{ $close: f32[{size}] }}"),
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
                    _field: field.full_name.clone(),
                    _expected: format!("{{ $take: number }}"),
                })?;

            let threshold = where_field
                .get("$threshold")
                .map(|f|f.as_f64())
                .unwrap_or(Some(0f64))
                .map(|f| f as f32)
                .ok_or_else(|| ParseWhereError::TypeMismatch {
                    _field: field.full_name.clone(),
                    _expected: format!("{{ $threshold: number }}"),
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

        if let Some(id_idx) = field.id_idx {
            // Первичный ключ не может быть null
            if where_field.is_null() {
                continue;
            }

            // Если это первый компонент ключа, используем префиксный поиск
            if id_idx == 0 {
                let encoded = match &field.ty {
                    FieldType::Primitive(primitive) => {
                        let mut buf = Vec::new();
                        // Кодируем значение так же, как в encode_id
                        match primitive {
                            PrimitiveFieldType::String => {
                                let s = where_field.as_str().ok_or_else(|| ParseWhereError::TypeMismatch {
                                    _field: field.full_name.clone(),
                                    _expected: "string".to_string(),
                                })?;
                                buf.extend_from_slice(s.as_bytes());
                            }
                            PrimitiveFieldType::Int64 => {
                                let n = where_field.as_i64().ok_or_else(|| ParseWhereError::TypeMismatch {
                                    _field: field.full_name.clone(),
                                    _expected: "i64".to_string(),
                                })?;
                                buf.extend_from_slice(&n.to_be_bytes());
                            }
                            PrimitiveFieldType::UInt64 => {
                                let n = where_field.as_u64().ok_or_else(|| ParseWhereError::TypeMismatch {
                                    _field: field.full_name.clone(),
                                    _expected: "u64".to_string(),
                                })?;
                                buf.extend_from_slice(&n.to_be_bytes());
                            }
                            PrimitiveFieldType::Float => {
                                let n = where_field.as_f64().ok_or_else(|| ParseWhereError::TypeMismatch {
                                    _field: field.full_name.clone(),
                                    _expected: "float".to_string(),
                                })? as f32;
                                buf.extend_from_slice(&n.to_be_bytes());
                            }
                            PrimitiveFieldType::Double => {
                                let n = where_field.as_f64().ok_or_else(|| ParseWhereError::TypeMismatch {
                                    _field: field.full_name.clone(),
                                    _expected: "double".to_string(),
                                })?;
                                buf.extend_from_slice(&n.to_be_bytes());
                            }
                            PrimitiveFieldType::Bool => {
                                let b = where_field.as_bool().ok_or_else(|| ParseWhereError::TypeMismatch {
                                    _field: field.full_name.clone(),
                                    _expected: "bool".to_string(),
                                })?;
                                buf.push(if b { 1 } else { 0 });
                            }
                            PrimitiveFieldType::DateTime => {
                                // datetime в ключе хранится как i64 миллисекунд
                                let epoch = match where_field {
                                    Value::Number(_) => where_field.as_i64().ok_or_else(|| ParseWhereError::TypeMismatch {
                                        _field: field.full_name.clone(),
                                        _expected: "i64 (timestamp)".to_string(),
                                    })?,
                                    Value::String(s) => {
                                        let dt: chrono::DateTime<chrono::Utc> = s.parse().map_err(|_| {
                                            ParseWhereError::TypeMismatch {
                                                _field: field.full_name.clone(),
                                                _expected: "ISO-8601 datetime string".to_string(),
                                            }
                                        })?;
                                        dt.timestamp_millis()
                                    }
                                    _ => return Err(ParseWhereError::TypeMismatch {
                                        _field: field.full_name.clone(),
                                        _expected: "i64 or ISO-8601 string".to_string(),
                                    }),
                                };
                                buf.extend_from_slice(&epoch.to_be_bytes());
                            }
                        }
                        buf
                    }
                    FieldType::ModelRef(_) => {
                        // ModelRef в ключе хранится как u64 (id целевой модели)
                        let id_val = if let Some(num) = where_field.as_u64() {
                            num
                        } else if let Some(obj) = where_field.as_object() {
                            obj.get("id").and_then(|v| v.as_u64()).ok_or_else(|| ParseWhereError::TypeMismatch {
                                _field: field.full_name.clone(),
                                _expected: "{ id: u64 }".to_string(),
                            })?
                        } else {
                            return Err(ParseWhereError::TypeMismatch {
                                _field: field.full_name.clone(),
                                _expected: "u64 or object with id".to_string(),
                            });
                        };
                        id_val.to_be_bytes().to_vec()
                    }
                    _ => {
                        // Другие типы (например, списки) не могут быть частью ключа
                        return Err(ParseWhereError::TypeMismatch {
                            _field: field.full_name.clone(),
                            _expected: "primitive or reference".to_string(),
                        });
                    }
                };

                let rx = ctx.db.db.begin_read().unwrap();
                let tree = rx.get_tree(model.name.as_bytes()).unwrap().unwrap();
                let ids: Vec<Vec<u8>> = tree
                    .prefix_keys(&encoded)
                    .unwrap()
                    .map(|k| k.unwrap().to_vec())
                    .collect();
                return Ok(Some(ids));
            }
        }

        let rx = ctx.db.db.begin_read().unwrap();
        let tree = rx.get_tree(model.name.as_bytes()).unwrap().unwrap();

        let mut ids = Vec::new();

        let is_null_condition = where_field.is_null();

        let encoded_value = if !is_null_condition {
            match &field.ty {
                FieldType::Primitive(primitive) => {
                    Some(encode_index_prefix(field, where_field).map_err(|_| ParseWhereError::TypeMismatch {
                        _field: field.full_name.clone(),
                        _expected: format!("value compatible with {}", primitive),
                    })?)
                }
                FieldType::ModelRef(_model_idx) => {
                    // Ожидаем число или объект с полем id
                    let id_val = if let Some(num) = where_field.as_u64() {
                        num
                    } else if let Some(obj) = where_field.as_object() {
                        obj.get("id").and_then(|v| v.as_u64()).ok_or_else(|| ParseWhereError::TypeMismatch {
                            _field: field.full_name.clone(),
                            _expected: "{ id: u64 }".to_string(),
                        })?
                    } else {
                        return Err(ParseWhereError::TypeMismatch {
                            _field: field.full_name.clone(),
                            _expected: "u64 or object with id".to_string(),
                        });
                    };
                    Some(id_val.to_be_bytes().to_vec())
                }
                _ => {
                    return Err(ParseWhereError::TypeMismatch {
                        _field: field.full_name.clone(),
                        _expected: "primitive or reference".to_string(),
                    });
                }
            }
        } else {
            None
        };

        for entry in tree.iter().unwrap() {
            let (id, data) = entry.unwrap();
            let value_opt = get_value_from_data(field, &id, &data, field.get_size());
            println!("DEBUG: id = {:?}, value_opt = {:?}", id, value_opt);
            let matches = if is_null_condition {
                value_opt.is_none()
            } else if let Some(ref expected) = encoded_value {
                if let Some(actual) = value_opt {
                    // Для динамических полей (String) encode_index_prefix добавляет нулевой терминатор
                    if expected.ends_with(&[0]) {
                        actual == &expected[..expected.len()-1]
                    } else {
                        actual == expected.as_slice()
                    }
                } else {
                    false
                }
            } else {
                false
            };

            if matches {
                ids.push(id.to_vec());
            }
        }

        return Ok(Some(ids));
    }

    return Ok(None);
}

fn eval_and(ctx: &ServerContext, model: &Entity, conditions: &[Value]) -> Result<Option<Vec<Vec<u8>>>, ParseWhereError> {
    let mut result_set: Option<HashSet<Vec<u8>>> = None;

    for cond in conditions {
        let ids_opt = parse_where(ctx, model, &Value::Object(serde_json::Map::from_iter(vec![
            ("$where".to_string(), cond.clone())
        ])))?;

        match ids_opt {
            Some(ids) => {
                let current_set: HashSet<_> = ids.into_iter().collect();
                if let Some(prev_set) = result_set {
                    // пересечение
                    result_set = Some(prev_set.intersection(&current_set).cloned().collect());
                } else {
                    result_set = Some(current_set);
                }
            }
            None => {
                // условие не ограничивает, пропускаем
                continue;
            }
        }
    }

    Ok(result_set.map(|set| set.into_iter().collect()))
}

fn eval_or(ctx: &ServerContext, model: &Entity, conditions: &[Value]) -> Result<Option<Vec<Vec<u8>>>, ParseWhereError> {
    let mut result_set = HashSet::new();
    let mut any_some = false;

    for cond in conditions {
        let ids_opt = parse_where(ctx, model, &Value::Object(serde_json::Map::from_iter(vec![
            ("$where".to_string(), cond.clone())
        ])))?;

        if let Some(ids) = ids_opt {
            any_some = true;
            result_set.extend(ids);
        }
    }

    if any_some {
        Ok(Some(result_set.into_iter().collect()))
    } else {
        Ok(None)
    }
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