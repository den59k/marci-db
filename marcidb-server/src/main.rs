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
use marcidb::{Attribute, Field, Entity, FieldType, MarciDB, MarciSelect, VectorIndexType, array_to_json, decode_document, decode_id, encode_document, encode_id, get_value_from_data, parse_schema, parse_select, PrimitiveFieldType, find_by_direct, find_by_rev};
use serde_json::Value;
use tokio::net::TcpListener;
use marci_vector::{CustomDistance, ReadCluster, WriteCluster};

mod marci_vector_utils;

use std::collections::HashSet;
use std::collections::HashMap;
use std::ops::Bound;

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

            let ids = match parse_where(&ctx, model, &body_json, None) {
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
enum ConditionValue {
    Null,
    Bool(bool),
    Int64(i64),
    UInt64(u64),
    Float(f32),
    Double(f64),
    String(String),
    DateTime(i64),
}

#[derive(Debug)]
enum Operator {
    Eq,
    Ne,
    Gt,
    Lt,
    Ge,
    Le,
}

#[derive(Debug)]
enum ParseWhereError {
    TypeMismatch { _field: String, _expected: String },
    InvalidData,
}

fn type_mismatch(field: &Field, expected: impl Into<String>) -> ParseWhereError {
    ParseWhereError::TypeMismatch {
        _field: field.name.clone(),
        _expected: expected.into(),
    }
}

fn decode_field_value(field: &Field, bytes: &[u8]) -> Result<ConditionValue, ParseWhereError> {
    match &field.ty {
        FieldType::Primitive(prim) => decode_primitive(&field.name, prim, bytes),
        FieldType::ModelRef(_) => {
            if bytes.len() != 8 {
                return Err(ParseWhereError::TypeMismatch {
                    _field: field.full_name.clone(),
                    _expected: "8 bytes".into(),
                });
            }
            let arr: [u8; 8] = bytes.try_into().unwrap();
            Ok(ConditionValue::UInt64(u64::from_be_bytes(arr)))
        }
        _ => Err(ParseWhereError::TypeMismatch {
            _field: field.full_name.clone(),
            _expected: "primitive or reference".into(),
        }),
    }
}

fn decode_primitive(_field_name: &str, prim: &PrimitiveFieldType, bytes: &[u8]) -> Result<ConditionValue, ParseWhereError> {
    match prim {
        PrimitiveFieldType::Bool => Ok(ConditionValue::Bool(bytes.first().map_or(false, |&b| b != 0))),
        PrimitiveFieldType::Int64 => {
            let arr: [u8; 8] = bytes.try_into().map_err(|_| ParseWhereError::InvalidData)?;
            Ok(ConditionValue::Int64(i64::from_be_bytes(arr)))
        }
        PrimitiveFieldType::UInt64 => {
            let arr: [u8; 8] = bytes.try_into().map_err(|_| ParseWhereError::InvalidData)?;
            Ok(ConditionValue::UInt64(u64::from_be_bytes(arr)))
        }
        PrimitiveFieldType::Float => {
            let arr: [u8; 4] = bytes.try_into().map_err(|_| ParseWhereError::InvalidData)?;
            Ok(ConditionValue::Float(f32::from_be_bytes(arr)))
        }
        PrimitiveFieldType::Double => {
            let arr: [u8; 8] = bytes.try_into().map_err(|_| ParseWhereError::InvalidData)?;
            Ok(ConditionValue::Double(f64::from_be_bytes(arr)))
        }
        PrimitiveFieldType::String => {
            let s = String::from_utf8(bytes.to_vec()).map_err(|_| ParseWhereError::InvalidData)?;
            Ok(ConditionValue::String(s))
        }
        PrimitiveFieldType::DateTime => {
            let arr: [u8; 8] = bytes.try_into().map_err(|_| ParseWhereError::InvalidData)?;
            Ok(ConditionValue::DateTime(i64::from_be_bytes(arr)))
        }
    }
}

fn check_condition(value: &ConditionValue, op: &Operator, target: &ConditionValue) -> bool {
    match (value, target) {
        (ConditionValue::Null, ConditionValue::Null) => matches!(op, Operator::Eq),
        (ConditionValue::Null, _) => matches!(op, Operator::Ne),
        (_, ConditionValue::Null) => matches!(op, Operator::Ne),
        (ConditionValue::Bool(a), ConditionValue::Bool(b)) => cmp_op(a, b, op),
        (ConditionValue::Int64(a), ConditionValue::Int64(b)) => cmp_op(a, b, op),
        (ConditionValue::UInt64(a), ConditionValue::UInt64(b)) => cmp_op(a, b, op),
        (ConditionValue::Float(a), ConditionValue::Float(b)) => cmp_op(a, b, op),
        (ConditionValue::Double(a), ConditionValue::Double(b)) => cmp_op(a, b, op),
        (ConditionValue::String(a), ConditionValue::String(b)) => cmp_op(a, b, op),
        (ConditionValue::DateTime(a), ConditionValue::DateTime(b)) => cmp_op(a, b, op),
        _ => false,
    }
}

fn cmp_op<T: PartialOrd + PartialEq>(a: T, b: T, op: &Operator) -> bool {
    match op {
        Operator::Eq => a == b,
        Operator::Ne => a != b,
        Operator::Gt => a > b,
        Operator::Lt => a < b,
        Operator::Ge => a >= b,
        Operator::Le => a <= b,
    }
}

fn parse_condition_value(field: &Field, val: &Value) -> Result<ConditionValue, ParseWhereError> {
    match &field.ty {
        FieldType::Primitive(prim) => match prim {
            PrimitiveFieldType::String => Ok(ConditionValue::String(
                val.as_str().ok_or_else(|| type_mismatch(field, "string"))?.to_string(),
            )),
            PrimitiveFieldType::Int64 => Ok(ConditionValue::Int64(
                val.as_i64().ok_or_else(|| type_mismatch(field, "i64"))?,
            )),
            PrimitiveFieldType::UInt64 => Ok(ConditionValue::UInt64(
                val.as_u64().ok_or_else(|| type_mismatch(field, "u64"))?,
            )),
            PrimitiveFieldType::Float => Ok(ConditionValue::Float(
                val.as_f64().ok_or_else(|| type_mismatch(field, "float"))? as f32,
            )),
            PrimitiveFieldType::Double => Ok(ConditionValue::Double(
                val.as_f64().ok_or_else(|| type_mismatch(field, "double"))?,
            )),
            PrimitiveFieldType::Bool => Ok(ConditionValue::Bool(
                val.as_bool().ok_or_else(|| type_mismatch(field, "bool"))?,
            )),
            PrimitiveFieldType::DateTime => {
                let epoch = if let Some(n) = val.as_i64() {
                    n
                } else if let Some(s) = val.as_str() {
                    s.parse::<chrono::DateTime<chrono::Utc>>()
                        .map_err(|_| type_mismatch(field, "ISO-8601 string"))?
                        .timestamp_millis()
                } else {
                    return Err(type_mismatch(field, "i64 or ISO-8601 string"));
                };
                Ok(ConditionValue::DateTime(epoch))
            }
        },
        FieldType::ModelRef(_) => {
            let id = if let Some(num) = val.as_u64() {
                num
            } else if let Some(obj) = val.as_object() {
                obj.get("id")
                    .and_then(|v| v.as_u64())
                    .ok_or_else(|| type_mismatch(field, "{ id: u64 }"))?
            } else {
                return Err(type_mismatch(field, "u64 or object with id"));
            };
            Ok(ConditionValue::UInt64(id))
        }
        _ => Err(type_mismatch(field, "primitive or reference")),
    }
}

fn encode_condition_value(field: &Field, val: &ConditionValue) -> Result<Vec<u8>, ParseWhereError> {
    let mut out = Vec::new();
    match (&field.ty, val) {
        (FieldType::Primitive(PrimitiveFieldType::String), ConditionValue::String(s)) => {
            out.extend_from_slice(s.as_bytes())
        }
        (FieldType::Primitive(PrimitiveFieldType::Int64), ConditionValue::Int64(n)) => {
            out.extend_from_slice(&n.to_be_bytes())
        }
        (FieldType::Primitive(PrimitiveFieldType::UInt64), ConditionValue::UInt64(n)) => {
            out.extend_from_slice(&n.to_be_bytes())
        }
        (FieldType::Primitive(PrimitiveFieldType::Float), ConditionValue::Float(n)) => {
            out.extend_from_slice(&n.to_be_bytes())
        }
        (FieldType::Primitive(PrimitiveFieldType::Double), ConditionValue::Double(n)) => {
            out.extend_from_slice(&n.to_be_bytes())
        }
        (FieldType::Primitive(PrimitiveFieldType::Bool), ConditionValue::Bool(b)) => {
            out.push(if *b { 1 } else { 0 })
        }
        (FieldType::Primitive(PrimitiveFieldType::DateTime), ConditionValue::DateTime(ts)) => {
            out.extend_from_slice(&ts.to_be_bytes())
        }
        (FieldType::ModelRef(_), ConditionValue::UInt64(id)) => {
            out.extend_from_slice(&id.to_be_bytes())
        }
        _ => {
            return Err(ParseWhereError::TypeMismatch {
                _field: field.name.clone(),
                _expected: "matching type".into(),
            })
        }
    }
    if field.get_size().is_none() && !matches!(field.ty, FieldType::ModelRef(_) | FieldType::ModelRefList(_)) {
        out.push(0);
    }
    Ok(out)
}

// Добавить после определения ConditionValue, Operator и вспомогательных функций

/// Разбирает условие для одного поля: объект с операторами или простое значение ($eq)
fn parse_conditions(field: &Field, where_field: &Value) -> Result<Vec<(Operator, ConditionValue)>, ParseWhereError> {
    let mut ops = Vec::new();
    if let Some(obj) = where_field.as_object() {
        if let Some(val) = obj.get("$eq") {
            ops.push((Operator::Eq, parse_condition_value(field, val)?));
        }
        if let Some(val) = obj.get("$ne") {
            ops.push((Operator::Ne, parse_condition_value(field, val)?));
        }
        if let Some(val) = obj.get("$gt") {
            ops.push((Operator::Gt, parse_condition_value(field, val)?));
        }
        if let Some(val) = obj.get("$lt") {
            ops.push((Operator::Lt, parse_condition_value(field, val)?));
        }
        if let Some(val) = obj.get("$ge") {
            ops.push((Operator::Ge, parse_condition_value(field, val)?));
        }
        if let Some(val) = obj.get("$le") {
            ops.push((Operator::Le, parse_condition_value(field, val)?));
        }
        // Если объект содержит другие ключи (например, $close для векторов) – они обрабатываются отдельно
    } else {
        // Простое значение -> неявный $eq
        ops.push((Operator::Eq, parse_condition_value(field, where_field)?));
    }
    Ok(ops)
}

/// Возвращает ID записей, удовлетворяющих заданным операторам для поля.
/// Использует field index, прямой доступ по ID (если поле — единственный ключ) или полное сканирование.
fn get_ids_for_condition(
    ctx: &ServerContext,
    model: &Entity,
    field: &Field,
    ops: Vec<(Operator, ConditionValue)>,
    existing_ids: Option<&HashSet<Vec<u8>>>,
) -> Result<Vec<Vec<u8>>, ParseWhereError> {
    println!("[get_ids_for_condition] >>> Entering for model '{}', field '{}'", model.name, field.name);
    println!("[get_ids_for_condition] ops: {:?}", ops);

    if let Some(index) = field.get_field_index() {
        // Попытаемся использовать индекс для диапазона значений
        let mut lower_bound: Option<Vec<u8>> = None;
        let mut upper_bound: Option<Vec<u8>> = None;
        let mut has_range_op = false;

        for (op, val) in &ops {
            match op {
                Operator::Eq | Operator::Gt | Operator::Ge | Operator::Lt | Operator::Le => {
                    has_range_op = true;
                    let encoded = encode_condition_value(field, val)?;
                    match op {
                        Operator::Eq => {
                            lower_bound = Some(encoded.clone());
                            upper_bound = Some(encoded);
                        }
                        Operator::Gt | Operator::Ge => {
                            if let Some(lb) = &lower_bound {
                                if encoded > *lb {
                                    lower_bound = Some(encoded);
                                }
                            } else {
                                lower_bound = Some(encoded);
                            }
                        }
                        Operator::Lt | Operator::Le => {
                            if let Some(ub) = &upper_bound {
                                if encoded < *ub {
                                    upper_bound = Some(encoded);
                                }
                            } else {
                                upper_bound = Some(encoded);
                            }
                        }
                        _ => {}
                    }
                }
                Operator::Ne => {} // не помогает строить диапазон
            }
        }

        // Если нет ни одного оператора сравнения (только Ne) – индекс не поможет, идём на полное сканирование
        if !has_range_op {
            return get_ids_by_scanning(ctx, model, field, &ops, existing_ids);
        }

        // Проверяем, что границы не противоречат
        if let (Some(lb), Some(ub)) = (&lower_bound, &upper_bound) {
            if lb > ub {
                return Ok(vec![]); // пустой результат
            }
        }

        // Строим диапазон ключей для итерации
        let rx = ctx.db.db.begin_read().unwrap();
        let tree = rx.get_tree(index.tree_name()).unwrap().unwrap();

        let range = match (lower_bound, upper_bound) {
            (Some(lb), Some(ub)) => {
                let start = [lb.as_slice(), &[0u8; 8]].concat();
                let end = [ub.as_slice(), &[255u8; 8]].concat();
                (Bound::Included(start), Bound::Included(end))
            }
            (Some(lb), None) => {
                let start = [lb.as_slice(), &[0u8; 8]].concat();
                (Bound::Included(start), Bound::Unbounded)
            }
            (None, Some(ub)) => {
                let end = [ub.as_slice(), &[255u8; 8]].concat();
                (Bound::Unbounded, Bound::Included(end))
            }
            (None, None) => unreachable!(), // has_range_op гарантирует хотя бы одну границу
        };

        let mut ids = Vec::new();
        for item in tree.range(range).unwrap() {
            let (key, _) = item.unwrap();
            // Извлекаем байты значения (без последних 8 байт ID)
            let value_bytes = &key[..key.len() - 8];
            // Убираем завершающий нуль, если поле переменной длины
            let value_bytes_trimmed = if field.get_size().is_none() {
                &value_bytes[..value_bytes.len() - 1]
            } else {
                value_bytes
            };
            // Декодируем значение поля
            let cond_val = decode_field_value(field, value_bytes_trimmed)?;
            // Проверяем все операторы
            let passes = ops.iter().all(|(op, target)| check_condition(&cond_val, op, target));
            if passes {
                let id = key[key.len() - 8..].to_vec();
                ids.push(id);
            }
        }

        // Применяем фильтр existing_ids, если есть
        if let Some(existing) = existing_ids {
            ids.retain(|id| existing.contains(id));
        }

        return Ok(ids);
    }

    if let FieldType::ModelRef(_) = field.ty {
        if ops.len() == 1 && matches!(ops[0].0, Operator::Eq) {
            if let Some(rev_index) = field.get_rev_index() {
                let target = &ops[0].1;
                let encoded = encode_condition_value(field, target)?;
                println!("[get_ids_for_condition] Using rev index for ModelRef, field: {}, target value encoded: {:?}", field.name, encoded);
                let rx = ctx.db.db.begin_read().unwrap();
                let tree = rx.get_tree(rev_index.tree_name()).unwrap().unwrap();
                let keys_iter = tree.prefix_keys(&encoded).unwrap();
                let mut ids = Vec::new();
                let mut count = 0;
                for k in keys_iter {
                    let key = k.unwrap();
                    // rev индекс: [значение][id владельца] → извлекаем id владельца
                    let owner_id = key[encoded.len()..encoded.len()+8].to_vec();
                    println!("[get_ids_for_condition]   found rev key: {:?} -> owner_id: {:?}", key, owner_id);
                    ids.push(owner_id);
                    count += 1;
                }
                println!("[get_ids_for_condition] Rev index returned {} ids", count);
                if let Some(existing) = existing_ids {
                    let filtered: Vec<_> = ids.into_iter().filter(|id| existing.contains(id)).collect();
                    return Ok(filtered);
                }
                return Ok(ids);
            } else {
                println!("[get_ids_for_condition] No rev index for field {}", field.name);
            }
        }
    }

    // Если поле — единственное ключевое и оператор равенства — проверяем существование записи по прямому ID
    let key_fields_count = model.fields.iter().filter(|f| f.id_idx.is_some()).count();
    if key_fields_count == 1 && field.id_idx.is_some() && ops.len() == 1 && matches!(ops[0].0, Operator::Eq) {
        let target = &ops[0].1;
        println!("[get_ids_for_condition] Using primary key (field is sole key field)");
        let encoded = encode_condition_value(field, target)?;
        println!("[get_ids_for_condition]   encoded target: {:?}", encoded);
        let rx = ctx.db.db.begin_read().unwrap();
        let tree = rx.get_tree(model.name.as_bytes()).unwrap().unwrap();
        if tree.get(&encoded).unwrap().is_some() {
            println!("[get_ids_for_condition]   record found, returning id");
            if let Some(existing) = existing_ids {
                let filtered: Vec<_> = vec![encoded].into_iter().filter(|id| existing.contains(id)).collect();
                return Ok(filtered);
            }
            return Ok(vec![encoded]);
        } else {
            println!("[get_ids_for_condition]   record not found, returning empty vec");
            return Ok(vec![]);
        }
    } else if key_fields_count != 1 {
        println!("[get_ids_for_condition]   key field count = {}, not using primary key path", key_fields_count);
    } else if field.id_idx.is_none() {
        println!("[get_ids_for_condition]   field is not a key field, not using primary key path");
    }

    // Во всех остальных случаях — полное сканирование
    println!("[get_ids_for_condition] Falling back to full scan via get_ids_by_scanning");
    let result = get_ids_by_scanning(ctx, model, field, &ops, existing_ids);
    println!("[get_ids_for_condition] <<< Returning from scan, result length: {:?}", result.as_ref().map(|v| v.len()).unwrap_or(0));
    result
}

fn process_all_condition(ctx: &ServerContext, field: &Field, all_array: &[Value]) -> Result<Vec<Vec<u8>>, ParseWhereError> {
    fn extract_id(val: &Value) -> Result<Vec<u8>, ParseWhereError> {
        match val {
            Value::Number(n) => {
                let id = n.as_u64().ok_or_else(|| ParseWhereError::TypeMismatch {
                    _field: "".into(),
                    _expected: "u64".into(),
                })?;
                Ok(id.to_be_bytes().to_vec())
            }
            Value::Object(obj) => {
                let id_val = obj.get("id").ok_or_else(|| ParseWhereError::TypeMismatch {
                    _field: "".into(),
                    _expected: "{ id: u64 }".into(),
                })?;
                let id = id_val.as_u64().ok_or_else(|| ParseWhereError::TypeMismatch {
                    _field: "".into(),
                    _expected: "u64".into(),
                })?;
                Ok(id.to_be_bytes().to_vec())
            }
            _ => Err(ParseWhereError::TypeMismatch {
                _field: "".into(),
                _expected: "u64 or object with id".into(),
            }),
        }
    }

    if let Some(rev_index) = field.get_rev_index() {
        let mut result_sets: Option<HashSet<Vec<u8>>> = None;
        for val in all_array {
            let child_id = extract_id(val)?;
            let rx = ctx.db.db.begin_read().unwrap();
            let parent_ids = find_by_direct(&rx, rev_index.tree_name(), &child_id);
            let current_set: HashSet<Vec<u8>> = parent_ids.into_iter().collect();
            result_sets = match result_sets {
                Some(prev) => Some(prev.intersection(&current_set).cloned().collect()),
                None => Some(current_set),
            };
        }
        Ok(result_sets.map(|set| set.into_iter().collect()).unwrap_or_default())
    } else {
        let direct_index = field.get_direct_index().ok_or_else(|| ParseWhereError::TypeMismatch {
            _field: field.full_name.clone(),
            _expected: "either reverse or direct index required for $all query".into(),
        })?;

        let rx = ctx.db.db.begin_read().unwrap();
        let direct_tree = rx.get_tree(direct_index.tree_name()).unwrap().unwrap();

        let first_child = extract_id(&all_array[0])?;
        let parents = find_by_rev(&rx, direct_index.tree_name(), &first_child, &ctx.db.schema);
        let mut result: HashSet<Vec<u8>> = parents.into_iter().collect();

        for val in &all_array[1..] {
            let child_id = extract_id(val)?;
            let mut new_result = HashSet::new();
            for parent in &result {
                let mut key = parent.clone();
                key.extend_from_slice(&child_id);
                if direct_tree.get(&key).unwrap().is_some() {
                    new_result.insert(parent.clone());
                }
            }
            result = new_result;
            if result.is_empty() { break; }
        }

        Ok(result.into_iter().collect())
    }
}

fn get_parent_ids_for_struct_list_condition(
    ctx: &ServerContext,
    st: &Entity,
    cond: &Value,
    existing_ids: Option<&HashSet<Vec<u8>>>,
) -> Result<Vec<Vec<u8>>, ParseWhereError> {

    let condition_value = if cond.is_number() {
        let id_field = st.fields.iter()
            .find(|f| f.id_idx.is_some() && !f.name.starts_with('@'))
            .ok_or_else(|| ParseWhereError::TypeMismatch {
                _field: st.name.clone(),
                _expected: "структура должна иметь поле-идентификатор элемента (не родительское)".into(),
            })?;

        // Построить объект { field_name: cond }
        let mut map = serde_json::Map::new();
        map.insert(id_field.name.clone(), cond.clone());
        Value::Object(map)
    } else {
        cond.clone()
    };

    let sub_where = Value::Object(serde_json::Map::from_iter(vec![
        ("$where".to_string(), condition_value)
    ]));

    let ids_opt = parse_where(ctx, st, &sub_where, existing_ids)?;
    let keys = match ids_opt {
        Some(keys) => {
            keys
        }
        None => {
            return Ok(vec![]);
        }
    };

    let parent_ids: HashSet<Vec<u8>> = keys
        .into_iter()
        .map(|key| key[..8].to_vec())
        .collect();

    Ok(parent_ids.into_iter().collect())
}

fn parse_where(
    ctx: &ServerContext,
    model: &Entity,
    body_json: &Value,
    existing_ids: Option<&HashSet<Vec<u8>>>,
) -> Result<Option<Vec<Vec<u8>>>, ParseWhereError> {
    let Some(where_obj) = body_json.get("$where") else {
        return Ok(None);
    };

    // Обработка явных $and / $or
    if let Some(and_array) = where_obj.get("$and").and_then(|v| v.as_array()) {
        return eval_and(ctx, model, and_array, existing_ids);
    }
    if let Some(or_array) = where_obj.get("$or").and_then(|v| v.as_array()) {
        return eval_or(ctx, model, or_array, existing_ids);
    }

    let mut all_id_sets: Vec<Vec<Vec<u8>>> = Vec::new();

    for field in model.fields.iter() {
        let Some(where_field) = where_obj.get(&field.name) else {
            continue;
        };

        // Векторный индекс (особая обработка)
        if let Some(vector_index_type) = field.attributes.iter().find_map(|f| {
            if let Attribute::VectorIndex(i) = f { Some(i) } else { None }
        }) {
            let (_primitive_type, &size) = match &field.ty {
                FieldType::PrimitiveFixedList(primitive_type, size) => (primitive_type, size),
                _ => panic!("Wrong field type {}. Expected fixed list", field.full_name),
            };

            let point = where_field
                .as_object()
                .and_then(|obj| obj.get("$close"))
                .and_then(|close| close.as_array())
                .filter(|arr| arr.len() == size)
                .and_then(|arr| {
                    let mut points = Vec::with_capacity(arr.len());
                    for i in arr {
                        let Some(f) = i.as_f64() else { return None };
                        points.push(f as f32);
                    }
                    Some(points)
                })
                .ok_or_else(|| ParseWhereError::TypeMismatch {
                    _field: field.full_name.clone(),
                    _expected: format!("{{ $close: f32[{size}] }}"),
                })?;

            let take = where_field
                .get("$take")
                .and_then(|f| f.as_u64())
                .unwrap_or(10);
            let threshold = where_field
                .get("$threshold")
                .and_then(|f| f.as_f64())
                .unwrap_or(0.0) as f32;

            let ids = {
                let rx = ctx.db.db.begin_read().unwrap();
                let index_name = [&field.full_name, ".vectorindex"].concat();
                let tree = rx.get_tree(index_name.as_bytes()).unwrap().unwrap();
                let distance = match vector_index_type {
                    VectorIndexType::Cosine => CustomDistance::Cosine,
                    VectorIndexType::Euclidean => CustomDistance::Euclidean,
                };
                ctx.find_nearest_points(&tree, &point, take as usize, distance, threshold)
                    .into_iter()
                    .map(|(id, _)| id)
                    .collect()
            };
            all_id_sets.push(ids);
            continue;
        }

        if let FieldType::ModelRefList(_) = &field.ty {
            if let Some(all_array) = where_field.get("$all").and_then(|v| v.as_array()) {
                let ids = process_all_condition(ctx, field, all_array)?;
                all_id_sets.push(ids);
                continue;
            } else if where_field.is_array() {
                let all_array = where_field.as_array().unwrap();
                let ids = process_all_condition(ctx, field, all_array)?;
                all_id_sets.push(ids);
                continue;
            } else if where_field.is_number() || where_field.is_object() {
                let arr = vec![where_field.clone()];
                let ids = process_all_condition(ctx, field, &arr)?;
                all_id_sets.push(ids);
                continue;
            } else {
                return Err(ParseWhereError::TypeMismatch {
                    _field: field.full_name.clone(),
                    _expected: "array or object with $all".into(),
                });
            }
        }

        if let FieldType::ModelRef(_) = &field.ty {
            let ops = parse_conditions(field, where_field)?;
            if ops.is_empty() {
                continue;
            }
            let ids = get_ids_for_condition(ctx, model, field, ops, existing_ids)?;
            all_id_sets.push(ids);
            continue;
        }

        match &field.ty {
            FieldType::Struct(st) => {
                let cond_obj = where_field.as_object().ok_or_else(|| ParseWhereError::TypeMismatch {
                    _field: field.full_name.clone(),
                    _expected: "object with field conditions".into(),
                })?;
                // Рекурсивно ищем ID структур, удовлетворяющих условию
                let sub_where = Value::Object(serde_json::Map::from_iter(vec![
                    ("$where".to_string(), Value::Object(cond_obj.clone()))
                ]));
                if let Some(ids) = parse_where(ctx, st, &sub_where, existing_ids)? {
                    all_id_sets.push(ids);
                }
                // Если ids = None (условие пустое) — пропускаем, ничего не добавляем
            }
            FieldType::StructList(st) => {
                let cond_obj = where_field.as_object().ok_or_else(|| ParseWhereError::TypeMismatch {
                    _field: field.full_name.clone(),
                    _expected: "object with operators or field conditions".into(),
                })?;

                if let Some(all_array) = cond_obj.get("$all").and_then(|v| v.as_array()) {
                    println!("[parse_where] Processing $all for StructList field '{}', array length: {}", field.full_name, all_array.len());

                    if all_array.is_empty() {
                        println!("[parse_where]   $all array is empty, pushing empty result");
                        all_id_sets.push(vec![]);
                        continue;
                    }

                    // Шаг 2: получить родителей для первого элемента
                    println!("[parse_where]   Getting parents for first element of $all");
                    let first_parents = get_parent_ids_for_struct_list_condition(ctx, st, &all_array[0], existing_ids)?;
                    let mut candidate_parents: HashSet<Vec<u8>> = first_parents.into_iter().collect();
                    println!("[parse_where]   First element returned {} parents", candidate_parents.len());

                    // Если после первого элемента нет родителей – результат пуст
                    if candidate_parents.is_empty() {
                        println!("[parse_where]   No parents after first element, pushing empty result");
                        all_id_sets.push(vec![]);
                        continue;
                    }

                    // Определить поле идентификатора структуры (поле с id_idx, не родительское)
                    let id_field = st.fields.iter()
                        .find(|f| f.id_idx.is_some() && !f.name.starts_with('@'))
                        .ok_or_else(|| ParseWhereError::TypeMismatch {
                            _field: st.name.clone(),
                            _expected: "структура должна иметь поле-идентификатор элемента (не родительское)".into(),
                        })?;
                    println!("[parse_where]   Using id_field '{}' for encoding child keys", id_field.full_name);

                    // Открыть дерево структуры-списка
                    let rx = ctx.db.db.begin_read().unwrap();
                    let tree = rx.get_tree(st.name.as_bytes()).unwrap().unwrap();

                    // Шаг 3: обработать остальные элементы
                    for (idx, elem) in all_array[1..].iter().enumerate() {
                        println!("[parse_where]   Processing element #{} of remaining", idx+1);

                        // Получить байтовое представление идентификатора элемента
                        let cond_val = parse_condition_value(id_field, elem)?;
                        println!("[parse_where]");
                        let encoded_child = encode_condition_value(id_field, &cond_val)?;
                        println!("[parse_where]     Element value encoded as {:?}", encoded_child);

                        // Проверить каждого родителя
                        let mut surviving_parents = HashSet::new();
                        for parent_id in &candidate_parents {
                            let mut key = parent_id.clone();
                            key.extend_from_slice(&encoded_child);
                            if tree.get(&key).unwrap().is_some() {
                                surviving_parents.insert(parent_id.clone());
                            }
                        }

                        let before = candidate_parents.len();
                        candidate_parents = surviving_parents;
                        println!("[parse_where]     After element #{}: parents left {} (was {})", idx+1, candidate_parents.len(), before);

                        if candidate_parents.is_empty() {
                            println!("[parse_where]     No parents left, breaking early");
                            break;
                        }
                    }

                    println!("[parse_where]   Final number of parents for $all: {}", candidate_parents.len());
                    all_id_sets.push(candidate_parents.into_iter().collect());
                    continue;
                }
            }
            _ => {}
        }

        let ops = parse_conditions(field, where_field)?;
        if ops.is_empty() {
            continue;
        }

        let ids = get_ids_for_condition(ctx, model, field, ops, existing_ids)?;
        all_id_sets.push(ids);
    }

    if all_id_sets.is_empty() {
        return Ok(None);
    }

    // Пересекаем все множества
    let mut result: Option<HashSet<Vec<u8>>> = None;
    for ids in all_id_sets {
        let current_set: HashSet<_> = ids.into_iter().collect();
        result = match result {
            Some(prev_set) => {
                let intersected: HashSet<_> = prev_set.intersection(&current_set).cloned().collect();
                if intersected.is_empty() {
                    return Ok(Some(vec![])); // пересечение пусто
                }
                Some(intersected)
            }
            None => Some(current_set),
        };
    }

    Ok(result.map(|set| set.into_iter().collect()))
}

fn get_ids_by_scanning(
    ctx: &ServerContext,
    model: &Entity,
    field: &Field,
    ops: &[(Operator, ConditionValue)],
    existing_ids: Option<&HashSet<Vec<u8>>>,
) -> Result<Vec<Vec<u8>>, ParseWhereError> {
    println!("[get_ids_by_scanning] >>> Starting full scan for field '{}' on model '{}'", field.name, model.name);
    println!("[get_ids_by_scanning] Conditions ({} ops): {:?}", ops.len(), ops);

    let rx = ctx.db.db.begin_read().unwrap();
    let tree = rx.get_tree(model.name.as_bytes()).unwrap().unwrap();

    // Определяем, какие ID проверять
    let ids_to_check: Vec<Vec<u8>> = if let Some(existing) = existing_ids {
        println!("[get_ids_by_scanning] Using existing_ids set of size {}", existing.len());
        existing.iter().cloned().collect()
    } else {
        println!("[get_ids_by_scanning] No existing_ids – scanning all keys from tree");
        tree.keys().unwrap().map(|k| k.unwrap().to_vec()).collect()
    };
    println!("[get_ids_by_scanning] Total IDs to check: {}", ids_to_check.len());

    let mut ids = Vec::new();
    for id in ids_to_check {
        // Получаем данные записи по ID
        let data = match tree.get(&id).unwrap() {
            Some(d) => d,
            None => {
                println!("[get_ids_by_scanning]   ID {:02x?} – data not found (skipping)", id);
                continue;
            }
        };

        // Извлекаем значение поля
        let value_opt = get_value_from_data(field, &id, &data, field.get_size());
        let cond_value = if let Some(bytes) = value_opt {
            match decode_field_value(field, bytes) {
                Ok(val) => {
                    println!("[get_ids_by_scanning]   ID {:02x?} – extracted value bytes: {:?}, decoded: {:?}", id, bytes, val);
                    val
                }
                Err(e) => {
                    println!("[get_ids_by_scanning]   ID {:02x?} – ERROR decoding value: {:?}", id, e);
                    return Err(e);
                }
            }
        } else {
            println!("[get_ids_by_scanning]   ID {:02x?} – value is NULL", id);
            ConditionValue::Null
        };

        // Проверяем условия
        let passes = ops.iter().all(|(op, target)| {
            let result = check_condition(&cond_value, op, target);
            println!("[get_ids_by_scanning]     condition {:?} {:?} -> {}", op, target, result);
            result
        });

        if passes {
            println!("[get_ids_by_scanning]   --> ID {:02x?} PASSED, adding to result", id);
            ids.push(id);
        } else {
            println!("[get_ids_by_scanning]   --> ID {:02x?} FAILED", id);
        }
    }

    println!("[get_ids_by_scanning] <<< Finished. Found {} matching IDs", ids.len());
    Ok(ids)
}

fn eval_and(
    ctx: &ServerContext,
    model: &Entity,
    conditions: &[Value],
    existing_ids: Option<&HashSet<Vec<u8>>>,
) -> Result<Option<Vec<Vec<u8>>>, ParseWhereError> {
    println!("[eval_and] >>> Entering with model '{}'", model.name);
    if let Some(existing) = existing_ids {
        println!("[eval_and] Initial existing_ids size: {}", existing.len());
    } else {
        println!("[eval_and] No initial existing_ids, will start from first condition");
    }
    println!("[eval_and] Number of AND conditions: {}", conditions.len());
    println!("[eval_and] All AND conditions: {:?}", conditions); // <-- added line

    let mut current_ids: Option<HashSet<Vec<u8>>> = existing_ids.map(|s| s.clone());

    for (i, cond) in conditions.iter().enumerate() {
        println!("[eval_and] --- Processing condition #{} ---", i + 1);
        println!("[eval_and] Condition #{} value: {:?}", i + 1, cond);

        let ids_opt = parse_where(
            ctx,
            model,
            &Value::Object(serde_json::Map::from_iter(vec![
                ("$where".to_string(), cond.clone())
            ])),
            current_ids.as_ref(),
        )?;

        match ids_opt {
            Some(ids) => {
                println!("[eval_and]   Condition #{} returned {} IDs", i + 1, ids.len());
                let new_set: HashSet<_> = ids.into_iter().collect();
                current_ids = Some(match current_ids {
                    Some(prev) => {
                        let intersection_size_before = prev.len();
                        let result: HashSet<_> = prev.intersection(&new_set).cloned().collect();
                        println!("[eval_and]   Intersected previous set (size {}) with new set (size {}) -> result size {}",
                            intersection_size_before, new_set.len(), result.len());
                        result
                    }
                    None => {
                        println!("[eval_and]   First condition – set becomes size {}", new_set.len());
                        new_set
                    }
                });

                // если пересечение пусто – можно сразу вернуть пустой результат
                if current_ids.as_ref().map_or(false, |s| s.is_empty()) {
                    println!("[eval_and]   Intersection is empty, returning empty result early");
                    return Ok(Some(vec![]));
                }
            }
            None => {
                println!("[eval_and]   Condition #{} returned None (no restriction) – set unchanged", i + 1);
                // условие не накладывает ограничений – оставляем текущий набор без изменений
                continue;
            }
        }
    }

    let final_count = current_ids.as_ref().map(|s| s.len()).unwrap_or(0);
    println!("[eval_and] <<< Finished, final set size: {}", final_count);
    Ok(current_ids.map(|set| set.into_iter().collect()))
}

fn eval_or(
    ctx: &ServerContext,
    model: &Entity,
    conditions: &[Value],
    _existing_ids: Option<&HashSet<Vec<u8>>>,
) -> Result<Option<Vec<Vec<u8>>>, ParseWhereError> {
    let mut result_set = HashSet::new();
    let mut any_some = false;

    for cond in conditions {
        let ids_opt = parse_where(
            ctx,
            model,
            &Value::Object(serde_json::Map::from_iter(vec![
                ("$where".to_string(), cond.clone())
            ])),
            None, // для OR нельзя использовать предварительный фильтр
        )?;

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