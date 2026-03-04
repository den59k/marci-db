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
    UInt16(u16),
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
        FieldType::Enum(_) => {
            if bytes.len() < 2 {
                return Err(ParseWhereError::InvalidData);
            }
            let idx = u16::from_be_bytes(bytes[..2].try_into().unwrap());
            Ok(ConditionValue::UInt16(idx))
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
        (ConditionValue::UInt16(a), ConditionValue::UInt16(b)) => cmp_op(a, b, op),
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

        FieldType::Enum(en) => {
            if let Some(s) = val.as_str() {
                let idx = *en.variants_map.get(s).ok_or_else(|| type_mismatch(field, format!("one of: {}", en.variants_str())))?;
                Ok(ConditionValue::UInt16(idx))
            } else {
                Err(type_mismatch(field, "string (enum variant)"))
            }
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
        (FieldType::Enum(_), ConditionValue::UInt16(idx)) => {
            let mut out = Vec::with_capacity(2);
            out.extend_from_slice(&idx.to_be_bytes());
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

fn get_ids_for_condition(
    ctx: &ServerContext,
    model: &Entity,
    field: &Field,
    ops: Vec<(Operator, ConditionValue)>,
    existing_ids: Option<&HashSet<Vec<u8>>>,
) -> Result<Vec<Vec<u8>>, ParseWhereError> {

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

        let key_len = model.key_min_size();
        let min_id = vec![0u8; key_len];
        let max_id = vec![255u8; key_len];

        let start = match &lower_bound {
            Some(lb) => [lb.as_slice(), &min_id].concat(),
            None => vec![],
        };
        let end = match &upper_bound {
            Some(ub) => [ub.as_slice(), &max_id].concat(),
            None => vec![],
        };

        let range = match (lower_bound, upper_bound) {
            (Some(_), Some(_)) => (Bound::Included(start), Bound::Included(end)),
            (Some(_), None) => (Bound::Included(start), Bound::Unbounded),
            (None, Some(_)) => (Bound::Unbounded, Bound::Included(end)),
            (None, None) => unreachable!(),
        };

        let mut ids = Vec::new();
        for item in tree.range(range).unwrap() {
            let (key, _) = item.unwrap();
            // Извлекаем байты значения (без последних 8 байт ID)
            let value_bytes = &key[..key.len() - key_len];
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
                let id = key[key.len() - key_len..].to_vec();
                ids.push(id);
            }
        }

        // Применяем фильтр existing_ids, если есть
        if let Some(existing) = existing_ids {
            ids.retain(|id| existing.contains(id));
        }

        return Ok(ids);
    }

    if let Some(rev_index) = field.get_rev_index() {
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
                Operator::Ne => {}
            }
        }

        if !has_range_op {
            // только Ne — не можем эффективно использовать индекс
            return get_ids_by_scanning(ctx, model, field, &ops, existing_ids);
        }

        if let (Some(lb), Some(ub)) = (&lower_bound, &upper_bound) {
            if lb > ub {
                return Ok(vec![]);
            }
        }
        
        let rx = ctx.db.db.begin_read().unwrap();
        let tree = rx.get_tree(rev_index.tree_name()).unwrap().unwrap();

        // Ключи rev-индекса: [encoded_value][id] (последние 8 байт — id владельца)
        let key_len = model.key_min_size();
        let min_id = [0u8; 8];
        let max_id = [255u8; 8];

        let start_key = match &lower_bound {
            Some(lb) => [lb.as_slice(), &min_id].concat(),
            None => vec![],
        };
        let end_key = match &upper_bound {
            Some(ub) => [ub.as_slice(), &max_id].concat(),
            None => vec![],
        };

        let range = match (lower_bound, upper_bound) {
            (Some(_), Some(_)) => (Bound::Included(start_key), Bound::Included(end_key)),
            (Some(_), None) => (Bound::Included(start_key), Bound::Unbounded),
            (None, Some(_)) => (Bound::Unbounded, Bound::Included(end_key)),
            (None, None) => unreachable!(),
        };

        let mut ids = Vec::new();
        for item in tree.range(range).unwrap() {
            let (key, _) = item.unwrap();
            // Извлекаем значение поля (первые key.len()-8 байт)
            let value_bytes = &key[..key.len() - key_len];
            // Для переменной длины убираем завершающий ноль, если он есть
            let value_bytes_trimmed = if field.get_size().is_none() {
                if value_bytes.last() == Some(&0) {
                    &value_bytes[..value_bytes.len() - 1]
                } else {
                    value_bytes
                }
            } else {
                value_bytes
            };
            let cond_val = decode_field_value(field, value_bytes_trimmed)?;
            if ops.iter().all(|(op, target)| check_condition(&cond_val, op, target)) {
                let owner_key = key[key.len() - key_len..].to_vec(); // полный ключ структуры
                ids.push(owner_key);
            }
        }

        if let Some(existing) = existing_ids {
            ids.retain(|id| existing.contains(id));
        }
        return Ok(ids);
    }

    // --- Обработка поля, являющегося первичным ключом (id_idx) ---
    if field.id_idx.is_some() {
        // Аналогично строим диапазон по значению id
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
                Operator::Ne => {}
            }
        }

        if !has_range_op {
            return get_ids_by_scanning(ctx, model, field, &ops, existing_ids);
        }

        if let (Some(lb), Some(ub)) = (&lower_bound, &upper_bound) {
            if lb > ub {
                return Ok(vec![]);
            }
        }

        let rx = ctx.db.db.begin_read().unwrap();
        let tree = rx.get_tree(model.name.as_bytes()).unwrap().unwrap();

        let range = match (lower_bound, upper_bound) {
            (Some(lb), Some(ub)) => (Bound::Included(lb), Bound::Included(ub)),
            (Some(lb), None) => (Bound::Included(lb), Bound::Unbounded),
            (None, Some(ub)) => (Bound::Unbounded, Bound::Included(ub)),
            (None, None) => unreachable!(),
        };

        let mut ids = Vec::new();
        for item in tree.range(range).unwrap() {
            let (key, _) = item.unwrap();
            // Ключ — это само значение id, декодируем его
            let cond_val = decode_field_value(field, &key)?;
            if ops.iter().all(|(op, target)| check_condition(&cond_val, op, target)) {
                ids.push(key.to_vec());
            }
        }

        if let Some(existing) = existing_ids {
            ids.retain(|id| existing.contains(id));
        }
        return Ok(ids);
    }

    // --- Если ни один индекс не подошёл — полное сканирование ---
    get_ids_by_scanning(ctx, model, field, &ops, existing_ids)
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

    // Обработка $and / $or (они уже реализованы)
    if let Some(and_array) = where_obj.get("$and").and_then(|v| v.as_array()) {
        return eval_and(ctx, model, and_array, existing_ids);
    }
    if let Some(or_array) = where_obj.get("$or").and_then(|v| v.as_array()) {
        return eval_or(ctx, model, or_array, existing_ids);
    }

    // Собираем все поля с условиями
    let fields_with_cond: Vec<&Field> = model.fields.iter()
        .filter(|f| where_obj.get(&f.name).is_some())
        .collect();

    // Проверяем, есть ли среди них индексированные
    let has_indexed = fields_with_cond.iter().any(|f| {
        f.get_field_index().is_some() || f.get_rev_index().is_some() || f.id_idx.is_some()
    });

    if !has_indexed {
        // Ни одного индекса – выполняем единое сканирование
        return parse_where_scan_all(ctx, model, fields_with_cond, where_obj, existing_ids);
    } else {
        // Есть индексы – используем последовательную фильтрацию (текущая логика)
        return parse_where_sequential(ctx, model, fields_with_cond, where_obj, existing_ids);
    }
}

/// Единое сканирование для набора полей без индексов
fn parse_where_scan_all(
    ctx: &ServerContext,
    model: &Entity,
    fields_with_cond: Vec<&Field>,
    where_obj: &Value,
    existing_ids: Option<&HashSet<Vec<u8>>>,
) -> Result<Option<Vec<Vec<u8>>>, ParseWhereError> {
    let mut field_conditions = Vec::new();
    for field in fields_with_cond {
        let where_field = where_obj.get(field.name.as_str()).unwrap();

        match &field.ty {
            FieldType::Struct(_) | FieldType::StructList(_) | FieldType::ModelRefList(_) => {
                return parse_where_sequential(ctx, model, vec![field], where_obj, existing_ids);
            }
            _ => {}
        }

        let ops = parse_conditions(field, where_field)?;
        if ops.is_empty() {
            continue;
        }
        field_conditions.push((field, ops));
    }

    if field_conditions.is_empty() {
        return Ok(None);
    }

    let ids = scan_all_with_conditions(ctx, model, field_conditions, existing_ids)?;
    Ok(Some(ids))
}

/// Функция единого сканирования (только для простых типов)
fn scan_all_with_conditions(
    ctx: &ServerContext,
    model: &Entity,
    field_conditions: Vec<(&Field, Vec<(Operator, ConditionValue)>)>,
    existing_ids: Option<&HashSet<Vec<u8>>>,
) -> Result<Vec<Vec<u8>>, ParseWhereError> {
    let rx = ctx.db.db.begin_read().unwrap();
    let tree = rx.get_tree(model.name.as_bytes()).unwrap().unwrap();

    let ids_to_check: Vec<Vec<u8>> = if let Some(existing) = existing_ids {
        existing.iter().cloned().collect()
    } else {
        tree.keys().unwrap().map(|k| k.unwrap().to_vec()).collect()
    };

    let mut result_ids = Vec::new();
    'next_id: for id in ids_to_check {
        let data = match tree.get(&id).unwrap() {
            Some(d) => d,
            None => continue,
        };

        for (field, ops) in &field_conditions {
            let value_opt = get_value_from_data(field, &id, &data, field.get_size());
            let cond_value = if let Some(bytes) = value_opt {
                decode_field_value(field, bytes)?
            } else {
                ConditionValue::Null
            };

            if !ops.iter().all(|(op, target)| check_condition(&cond_value, op, target)) {
                continue 'next_id;
            }
        }
        result_ids.push(id);
    }
    Ok(result_ids)
}

fn parse_where_sequential(
    ctx: &ServerContext,
    model: &Entity,
    fields_with_cond: Vec<&Field>,
    where_obj: &Value,
    existing_ids: Option<&HashSet<Vec<u8>>>,
) -> Result<Option<Vec<Vec<u8>>>, ParseWhereError> {
    // Сортируем поля по приоритету индексов
    let mut fields_with_cond = fields_with_cond;
    fields_with_cond.sort_by_key(|f| {
        if f.get_field_index().is_some() { 0 }
        else if f.get_rev_index().is_some() { 1 }
        else if f.id_idx.is_some() { 2 }
        else { 3 }
    });

    let mut current_ids: Option<HashSet<Vec<u8>>> = existing_ids.map(|s| s.clone());

    for field in fields_with_cond {
        let where_field = where_obj.get(&field.name).unwrap();

        let ids_for_field: Vec<Vec<u8>> = if let Some((st_ref, aliases)) = &field.injected_fields {
            if !where_field.is_object() {
                return Err(ParseWhereError::TypeMismatch {
                    _field: field.full_name.clone(),
                    _expected: "object with field conditions".into(),
                });
            }

            let field_def = ctx.db.schema.get_field(st_ref);

            let st_entity = match &field_def.ty {
                FieldType::Struct(st) | FieldType::StructList(st) => st,
                _ => {
                    return Err(ParseWhereError::TypeMismatch {
                        _field: field.full_name.clone(),
                        _expected: "struct or struct list".into(),
                    });
                }
            };

            let current_model_index = match &field.ty {
                FieldType::ModelRefList(idx) => *idx,
                FieldType::StructList(_) => {
                    return Err(ParseWhereError::TypeMismatch {
                        _field: field.full_name.clone(),
                        _expected: "ModelRefList with inject".into(),
                    });
                }
                _ => unreachable!(),
            };

            let parent_field = st_entity.fields.iter()
                .find(|f| matches!(&f.ty, FieldType::ModelRef(idx) if *idx == current_model_index))
                .ok_or_else(|| {
                    ParseWhereError::TypeMismatch {
                        _field: field.full_name.clone(),
                        _expected: format!("struct {} must have a reference to model {}", st_entity.name, model.name),
                    }
                })?;

            let parent_id_idx = parent_field.id_idx.expect("reference field must be @id");

            let alias_to_field: HashMap<&String, String> = aliases
                .iter()
                .map(|(full_name, alias)| {
                    let field_name = full_name.split('.').last().unwrap().to_string();
                    (alias, field_name)
                })
                .collect();

            let mut transformed_map = serde_json::Map::new();
            if let Some(obj) = where_field.as_object() {
                for (key, value) in obj {
                    if key.starts_with('$') {
                        transformed_map.insert(key.clone(), value.clone());
                    } else if let Some(real_field) = alias_to_field.get(key) {
                        transformed_map.insert(real_field.clone(), value.clone());
                    } else {
                        return Err(ParseWhereError::TypeMismatch {
                            _field: field.full_name.clone(),
                            _expected: format!("unknown field alias '{}'", key),
                        });
                    }
                }
            }

            let sub_where = Value::Object(serde_json::Map::from_iter(vec![
                ("$where".to_string(), Value::Object(transformed_map))
            ]));

            let keys_opt = parse_where(ctx, st_entity, &sub_where, None)?;
            let keys = keys_opt.unwrap_or_default();

            let parent_ids: Vec<Vec<u8>> = keys
                .into_iter()
                .map(|key| {
                    let start = parent_id_idx * 8;
                    key[start + 8..start + 16].to_vec()
                })
                .collect();

            parent_ids
        
        } else {
            match &field.ty {
                _ if field.attributes.iter().any(|a| matches!(a, Attribute::VectorIndex(_))) => {
                    let vector_index_type = field.attributes.iter().find_map(|f| {
                        if let Attribute::VectorIndex(i) = f { Some(i) } else { None }
                    }).unwrap();
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
                    ids
                }

                FieldType::ModelRefList(_) => {
                    if let Some(all_array) = where_field.get("$all").and_then(|v| v.as_array()) {
                        process_all_condition(ctx, field, all_array)?
                    } else if where_field.is_array() {
                        let all_array = where_field.as_array().unwrap();
                        process_all_condition(ctx, field, all_array)?
                    } else if where_field.is_number() || where_field.is_object() {
                        let arr = vec![where_field.clone()];
                        process_all_condition(ctx, field, &arr)?
                    } else {
                        return Err(ParseWhereError::TypeMismatch {
                            _field: field.full_name.clone(),
                            _expected: "array or object with $all".into(),
                        });
                    }
                }

                FieldType::ModelRef(_) => {
                    let ops = parse_conditions(field, where_field)?;
                    if ops.is_empty() {
                        continue;
                    }
                    get_ids_for_condition(ctx, model, field, ops, current_ids.as_ref())?
                }

                FieldType::Struct(st) => {
                    let cond_obj = where_field.as_object().ok_or_else(|| ParseWhereError::TypeMismatch {
                        _field: field.full_name.clone(),
                        _expected: "object with field conditions".into(),
                    })?;
                    let sub_where = Value::Object(serde_json::Map::from_iter(vec![
                        ("$where".to_string(), Value::Object(cond_obj.clone()))
                    ]));
                    match parse_where(ctx, st, &sub_where, current_ids.as_ref())? {
                        Some(ids) => ids,
                        None => continue,
                    }
                }

                FieldType::StructList(st) => {
                    let cond_obj = where_field.as_object().ok_or_else(|| ParseWhereError::TypeMismatch {
                        _field: field.full_name.clone(),
                        _expected: "object with operators or field conditions".into(),
                    })?;

                    if let Some(all_array) = cond_obj.get("$all").and_then(|v| v.as_array()) {
                        if all_array.is_empty() {
                            vec![] // пустой $all не даёт результатов
                        } else {
                            // получаем родителей для первого элемента
                            let first_parents = get_parent_ids_for_struct_list_condition(ctx, st, &all_array[0], current_ids.as_ref())?;
                            let mut candidate_parents: HashSet<Vec<u8>> = first_parents.into_iter().collect();

                            if candidate_parents.is_empty() {
                                vec![]
                            } else {
                                let id_field = st.fields.iter()
                                    .find(|f| f.id_idx.is_some() && !f.name.starts_with('@'))
                                    .ok_or_else(|| ParseWhereError::TypeMismatch {
                                        _field: st.name.clone(),
                                        _expected: "структура должна иметь поле-идентификатор элемента (не родительское)".into(),
                                    })?;

                                let rx = ctx.db.db.begin_read().unwrap();
                                let tree = rx.get_tree(st.name.as_bytes()).unwrap().unwrap();

                                for elem in &all_array[1..] {
                                    let cond_val = parse_condition_value(id_field, elem)?;
                                    let encoded_child = encode_condition_value(id_field, &cond_val)?;

                                    let mut surviving_parents = HashSet::new();
                                    for parent_id in &candidate_parents {
                                        let mut key = parent_id.clone();
                                        key.extend_from_slice(&encoded_child);
                                        if tree.get(&key).unwrap().is_some() {
                                            surviving_parents.insert(parent_id.clone());
                                        }
                                    }
                                    candidate_parents = surviving_parents;

                                    if candidate_parents.is_empty() {
                                        break;
                                    }
                                }
                                candidate_parents.into_iter().collect()
                            }
                        }
                    } else {
                        continue;
                    }
                }

                _ => {
                    let ops = parse_conditions(field, where_field)?;
                    if ops.is_empty() {
                        continue;
                    }
                    get_ids_for_condition(ctx, model, field, ops, current_ids.as_ref())?
                }
            }
        };

        if ids_for_field.is_empty() {
            if current_ids.is_some() {
                return Ok(Some(vec![]));
            } else {
                return Ok(Some(vec![]));
            }
        } else {
            let new_set: HashSet<_> = ids_for_field.into_iter().collect();
            current_ids = match current_ids {
                Some(prev) => {
                    let intersected: HashSet<_> = prev.intersection(&new_set).cloned().collect();
                    if intersected.is_empty() {
                        return Ok(Some(vec![]));
                    }
                    Some(intersected)
                }
                None => Some(new_set),
            };
        }
    }

    Ok(current_ids.map(|set| set.into_iter().collect()))
}

fn get_ids_by_scanning(
    ctx: &ServerContext,
    model: &Entity,
    field: &Field,
    ops: &[(Operator, ConditionValue)],
    existing_ids: Option<&HashSet<Vec<u8>>>,
) -> Result<Vec<Vec<u8>>, ParseWhereError> {
    let rx = ctx.db.db.begin_read().unwrap();
    let tree = rx.get_tree(model.name.as_bytes()).unwrap().unwrap();

    // Определяем, какие ID проверять
    let ids_to_check: Vec<Vec<u8>> = if let Some(existing) = existing_ids {
        existing.iter().cloned().collect()
    } else {
        tree.keys().unwrap().map(|k| k.unwrap().to_vec()).collect()
    };

    let mut ids = Vec::new();
    for id in ids_to_check {
        // Получаем данные записи по ID
        let data = match tree.get(&id).unwrap() {
            Some(d) => d,
            None => continue,
        };

        // Извлекаем значение поля
        let value_opt = get_value_from_data(field, &id, &data, field.get_size());
        let cond_value = if let Some(bytes) = value_opt {
            match decode_field_value(field, bytes) {
                Ok(val) => val,
                Err(e) => return Err(e),
            }
        } else {
            ConditionValue::Null
        };

        // Проверяем условия
        let passes = ops.iter().all(|(op, target)| check_condition(&cond_value, op, target));

        if passes {
            ids.push(id);
        }
    }

    Ok(ids)
}

fn condition_priority(model: &Entity, cond: &Value) -> u8 {
    let obj = match cond.as_object() {
        Some(o) => o,
        None => return 3,
    };
    let mut best = 3;
    for (key, _) in obj {
        if key.starts_with('$') {
            continue; // операторы не дают индекса напрямую
        }
        if let Some(field) = model.fields.iter().find(|f| f.name == *key) {
            let prio = if field.get_field_index().is_some() {
                0
            } else if field.get_rev_index().is_some() {
                1
            } else if field.id_idx.is_some() {
                2
            } else {
                3
            };
            if prio < best {
                best = prio;
                if best == 0 {
                    return 0;
                }
            }
        }
    }
    best
}

fn eval_and(
    ctx: &ServerContext,
    model: &Entity,
    conditions: &[Value],
    existing_ids: Option<&HashSet<Vec<u8>>>,
) -> Result<Option<Vec<Vec<u8>>>, ParseWhereError> {
    // Сортируем условия по приоритету индексов
    let mut conditions_with_prio: Vec<(u8, &Value)> = conditions.iter()
        .map(|c| (condition_priority(model, c), c))
        .collect();
    conditions_with_prio.sort_by_key(|(p, _)| *p);

    let mut current_ids: Option<HashSet<Vec<u8>>> = existing_ids.map(|s| s.clone());

    for (_, cond) in conditions_with_prio {
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
                let new_set: HashSet<_> = ids.into_iter().collect();
                current_ids = Some(match current_ids {
                    Some(prev) => {
                        let intersected: HashSet<_> = prev.intersection(&new_set).cloned().collect();
                        intersected
                    }
                    None => new_set,
                });

                if current_ids.as_ref().map_or(false, |s| s.is_empty()) {
                    return Ok(Some(vec![]));
                }
            }
            None => continue, // условие не накладывает ограничений
        }
    }

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