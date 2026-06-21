use std::sync::Arc;

use marcidb::{
    array_to_json, decode_document, decode_id,
    parse_id_from_url, parse_insert, parse_query, parse_update,
};

use crate::ServerContext;

#[derive(Debug)]
pub enum HandlerError {
    BadRequest(String),
    NotFound(String),
    Internal(String),
}

impl HandlerError {
    pub fn message(&self) -> &str {
        match self {
            HandlerError::BadRequest(m) => m,
            HandlerError::NotFound(m) => m,
            HandlerError::Internal(m) => m,
        }
    }
}

type HandlerResult = Result<Vec<u8>, HandlerError>;

async fn blocking<F, T>(f: F) -> Result<T, HandlerError>
where
    F: FnOnce() -> Result<T, HandlerError> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| HandlerError::Internal(e.to_string()))?
}

fn parse_json(bytes: &[u8]) -> Result<serde_json::Value, HandlerError> {
    serde_json::from_slice(bytes)
        .map_err(|e| HandlerError::BadRequest(format!("Invalid JSON: {}", e)))
}

fn model_index(ctx: &ServerContext, model_name: &str) -> Result<usize, HandlerError> {
    ctx.db
        .get_model_index(model_name)
        .ok_or_else(|| HandlerError::NotFound(format!("Model '{}' not found", model_name)))
}

pub async fn handle_insert(
    ctx: Arc<ServerContext>,
    model_name: String,
    json_bytes: Vec<u8>,
) -> HandlerResult {
    let json_val = parse_json(&json_bytes)?;
    let model_idx = model_index(&ctx, &model_name)?;

    blocking(move || {
        let entity   = ctx.db.get_model_by_index(model_idx);
        let write_op = parse_insert(&ctx.db.schema, entity, &json_val)
            .map_err(|e| HandlerError::BadRequest(format!("Failed to encode: {:?}", e)))?;
        let written_id = ctx.db.insert_item(entity, &write_op)
            .map_err(|e| HandlerError::BadRequest(format!("Failed to insert: {:?}", e)))?;
        Ok(decode_id(&written_id, entity, &ctx.db.schema)
            .to_string()
            .into_bytes())
    })
    .await
}

pub async fn handle_find_many(
    ctx: Arc<ServerContext>,
    model_name: String,
    json_bytes: Vec<u8>,
) -> HandlerResult {
    let json_val = parse_json(&json_bytes)?;
    let model_idx = model_index(&ctx, &model_name)?;

    blocking(move || {
        let entity   = ctx.db.get_model_by_index(model_idx);
        let query_op = parse_query(&ctx.db.schema, entity, &json_val)
            .map_err(|e| HandlerError::BadRequest(format!("Failed to encode: {:?}", e)))?;
        let result = array_to_json(
            &ctx.db.find_many(&query_op, |ctx| decode_document(ctx).unwrap()),
        );
        Ok(result.into_bytes())
    })
    .await
}

pub async fn handle_find_first(
    ctx: Arc<ServerContext>,
    model_name: String,
    json_bytes: Vec<u8>,
) -> HandlerResult {
    let json_val = parse_json(&json_bytes)?;
    let model_idx = model_index(&ctx, &model_name)?;

    blocking(move || {
        let entity   = ctx.db.get_model_by_index(model_idx);
        let query_op = parse_query(&ctx.db.schema, entity, &json_val)
            .map_err(|e| HandlerError::BadRequest(format!("Failed to encode: {:?}", e)))?;
        let result = match ctx.db.find_first(&query_op, |ctx| decode_document(ctx).unwrap()) {
            Some(item) => item.into_bytes(),
            None       => b"null".to_vec(),
        };
        Ok(result)
    })
    .await
}

pub async fn handle_update(
    ctx: Arc<ServerContext>,
    model_name: String,
    item_id: String,
    json_bytes: Vec<u8>,
) -> HandlerResult {
    let json_val  = parse_json(&json_bytes)?;
    let model_idx = model_index(&ctx, &model_name)?;
    let item_id   = parse_id_from_url(
        &ctx.db.schema,
        ctx.db.get_model_by_index(model_idx),
        &item_id,
    )
    .map_err(|e| HandlerError::BadRequest(format!("Failed to parse item_id: {:?}", e)))?;

    blocking(move || {
        let entity    = ctx.db.get_model_by_index(model_idx);
        let update_op = parse_update(&ctx.db.schema, entity, &json_val)
            .map_err(|e| HandlerError::BadRequest(format!("Failed to encode: {:?}", e)))?;
        ctx.db.update_item(entity, &item_id, &update_op)
            .map_err(|e| HandlerError::BadRequest(format!("Failed to update: {:?}", e)))?;
        Ok(vec![])
    })
    .await
}

pub async fn handle_delete(
    ctx: Arc<ServerContext>,
    model_name: String,
    item_id: String,
) -> HandlerResult {
    let model_idx = model_index(&ctx, &model_name)?;
    let item_id   = parse_id_from_url(
        &ctx.db.schema,
        ctx.db.get_model_by_index(model_idx),
        &item_id,
    )
    .map_err(|e| HandlerError::BadRequest(format!("Failed to parse item_id: {:?}", e)))?;

    blocking(move || {
        let entity = ctx.db.get_model_by_index(model_idx);
        ctx.db.delete_item(entity, &item_id)
            .map_err(|e| HandlerError::BadRequest(format!("Failed to delete: {:?}", e)))?;
        Ok(vec![])
    })
    .await
}
