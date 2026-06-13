use std::sync::Arc;

use http_body_util::Full;
use hyper::{Request, Response, body::Bytes};
use marcidb::{BatchErrorKind, MarciDB, MigrationApplyError, aggregate_to_json, array_to_json, decode_document, decode_id, execute_batch, parse_aggregate, parse_id_from_url, parse_insert, parse_query, parse_update};
use serde_json::Value;

use crate::{ServerContext, errors::ApiError, helpers::{blocking, ok_response, parse_json_body, parse_text_body}};

type HandlerResult = Result<Response<Full<Bytes>>, ApiError>;

/// Открывает БД по имени (read-lock) и выполняет операцию над ней
fn with_db<T>(ctx: &ServerContext, db_name: &str, f: impl FnOnce(&MarciDB) -> Result<T, ApiError>) -> Result<T, ApiError> {
    let db = ctx.get_db(db_name, false)?;
    let db = db.read().unwrap_or_else(|e| e.into_inner());
    f(&db)
}

fn model<'a>(db: &'a MarciDB, name: &str) -> Result<&'a marcidb::Entity, ApiError> {
    db.get_model(name).ok_or_else(|| ApiError::NotFound(format!("Model '{}' not found", name)))
}

/// Применяет схему к БД (push-миграция). Если БД нет — создаётся. Тело запроса — текст схемы `.marci`
pub async fn handle_migrate(req: Request<hyper::body::Incoming>, ctx: Arc<ServerContext>, db_name: String) -> HandlerResult {
    let schema_text = parse_text_body(req).await?;

    blocking(move || {
        let db = ctx.get_db(&db_name, true)?; // create-if-absent
        let mut db = db.write().unwrap_or_else(|e| e.into_inner());
        db.migrate_to(&schema_text).map_err(|e| match e {
            MigrationApplyError::Storage(_) => ApiError::Internal(format!("{:?}", e)),
            _ => ApiError::BadRequest(format!("{:?}", e)),
        })?;
        Ok::<_, ApiError>(String::new())
    }).await?;

    Ok(ok_response(Vec::new()))
}

/// Атомарная batch-транзакция: тело — массив операций `{ model, action, ... }`
pub async fn handle_transaction(req: Request<hyper::body::Incoming>, ctx: Arc<ServerContext>, db_name: String) -> HandlerResult {
    let json_val = parse_json_body(req).await?;

    let result = blocking(move || with_db(&ctx, &db_name, |db| {
        let ops = json_val.as_array()
            .ok_or_else(|| ApiError::BadRequest("Transaction body must be an array of operations".to_string()))?;

        match execute_batch(db, ops) {
            Ok(results) => Ok(serde_json::to_string(&Value::Array(results)).unwrap()),
            Err(e) if matches!(e.kind, BatchErrorKind::Storage(_)) => Err(ApiError::Internal(e.to_string())),
            Err(e) => Err(ApiError::BadRequest(e.to_string())),
        }
    })).await?;

    Ok(ok_response(result))
}

pub async fn handle_insert(req: Request<hyper::body::Incoming>, ctx: Arc<ServerContext>, db_name: String, model_name: String) -> HandlerResult {
    let json_val = parse_json_body(req).await?;

    let id = blocking(move || with_db(&ctx, &db_name, |db| {
        let entity = model(db, &model_name)?;
        let write_op = parse_insert(&db.schema, entity, &json_val)
            .map_err(|e| ApiError::BadRequest(format!("Failed to encode: {:?}", e)))?;
        let writed_id = db.insert_item(entity, &write_op)
            .map_err(|e| ApiError::BadRequest(format!("Failed to insert: {:?}", e)))?;
        Ok(decode_id(&writed_id, entity, &db.schema).to_string())
    })).await?;

    Ok(ok_response(id))
}

pub async fn handle_find_many(req: Request<hyper::body::Incoming>, ctx: Arc<ServerContext>, db_name: String, model_name: String) -> HandlerResult {
    let json_val = parse_json_body(req).await?;

    let result = blocking(move || with_db(&ctx, &db_name, |db| {
        let entity = model(db, &model_name)?;
        let query_op = parse_query(&db.schema, entity, &json_val)
            .map_err(|e| ApiError::BadRequest(format!("Failed to encode: {:?}", e)))?;
        let items = db.find_many(&query_op, |ctx| decode_document(ctx).unwrap())
            .map_err(|e| ApiError::Internal(format!("{}", e)))?;
        Ok(array_to_json(&items))
    })).await?;

    Ok(ok_response(result))
}

pub async fn handle_find_first(req: Request<hyper::body::Incoming>, ctx: Arc<ServerContext>, db_name: String, model_name: String) -> HandlerResult {
    let json_val = parse_json_body(req).await?;

    let result = blocking(move || with_db(&ctx, &db_name, |db| {
        let entity = model(db, &model_name)?;
        let query_op = parse_query(&db.schema, entity, &json_val)
            .map_err(|e| ApiError::BadRequest(format!("Failed to encode: {:?}", e)))?;
        let item = db.find_first(&query_op, |ctx| decode_document(ctx).unwrap())
            .map_err(|e| ApiError::Internal(format!("{}", e)))?;
        Ok(item.unwrap_or_else(|| "null".to_string()))
    })).await?;

    Ok(ok_response(result))
}

pub async fn handle_count(req: Request<hyper::body::Incoming>, ctx: Arc<ServerContext>, db_name: String, model_name: String) -> HandlerResult {
    let json_val = parse_json_body(req).await?;

    let result = blocking(move || with_db(&ctx, &db_name, |db| {
        let entity = model(db, &model_name)?;
        let mut aggregate_op = parse_aggregate(&db.schema, entity, &json_val)
            .map_err(|e| ApiError::BadRequest(format!("Failed to encode: {:?}", e)))?;
        aggregate_op.count = true;

        let result = db.aggregate(&aggregate_op)
            .map_err(|e| ApiError::Internal(format!("{}", e)))?;
        Ok(result.count.to_string())
    })).await?;

    Ok(ok_response(result))
}

pub async fn handle_aggregate(req: Request<hyper::body::Incoming>, ctx: Arc<ServerContext>, db_name: String, model_name: String) -> HandlerResult {
    let json_val = parse_json_body(req).await?;

    let result = blocking(move || with_db(&ctx, &db_name, |db| {
        let entity = model(db, &model_name)?;
        let aggregate_op = parse_aggregate(&db.schema, entity, &json_val)
            .map_err(|e| ApiError::BadRequest(format!("Failed to encode: {:?}", e)))?;
        if !aggregate_op.has_aggregates() {
            return Err(ApiError::BadRequest("At least one of $count, $sum, $avg, $min, $max is required".to_string()));
        }

        let result = db.aggregate(&aggregate_op)
            .map_err(|e| ApiError::Internal(format!("{}", e)))?;
        Ok(aggregate_to_json(&aggregate_op, &result))
    })).await?;

    Ok(ok_response(result))
}

pub async fn handle_update(req: Request<hyper::body::Incoming>, item_id: String, ctx: Arc<ServerContext>, db_name: String, model_name: String) -> HandlerResult {
    let json_val = parse_json_body(req).await?;

    blocking(move || with_db(&ctx, &db_name, |db| {
        let entity = model(db, &model_name)?;
        let id = parse_id_from_url(&db.schema, entity, &item_id)
            .map_err(|e| ApiError::BadRequest(format!("Failed to parse :item_id: {:?}", e)))?;
        let update_op = parse_update(&db.schema, entity, &json_val)
            .map_err(|e| ApiError::BadRequest(format!("Failed to encode: {:?}", e)))?;
        db.update_item(entity, &id, &update_op)
            .map_err(|e| ApiError::BadRequest(format!("Failed to update: {:?}", e)))?;
        Ok(())
    })).await?;

    Ok(ok_response(Vec::new()))
}

pub async fn handle_delete(item_id: String, ctx: Arc<ServerContext>, db_name: String, model_name: String) -> HandlerResult {
    blocking(move || with_db(&ctx, &db_name, |db| {
        let entity = model(db, &model_name)?;
        let id = parse_id_from_url(&db.schema, entity, &item_id)
            .map_err(|e| ApiError::BadRequest(format!("Failed to parse :item_id: {:?}", e)))?;
        db.delete_item(entity, &id)
            .map_err(|e| ApiError::BadRequest(format!("Failed to delete: {:?}", e)))?;
        Ok(())
    })).await?;

    Ok(ok_response(Vec::new()))
}
