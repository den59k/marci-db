use std::sync::Arc;

use http_body_util::Full;
use hyper::{Request, Response, body::Bytes};
use marcidb::{array_to_json, decode_document, decode_id, parse_id_from_url, parse_insert, parse_query, parse_update};

use crate::{ServerContext, errors::ApiError, helpers::{blocking, ok_response, parse_json_body}};

type HandlerResult = Result<Response<Full<Bytes>>, ApiError>;

pub async fn handle_insert(req: Request<hyper::body::Incoming>, ctx: Arc<ServerContext>, model_index: usize) -> HandlerResult {
    let json_val = parse_json_body(req).await?;

    let id = blocking(move || {
        let entity = ctx.db.get_model_by_index(model_index);
        let write_op = parse_insert(&ctx.db.schema, entity, &json_val)
            .map_err(|e| ApiError::BadRequest(format!("Failed to encode: {:?}", e)))?;
        ctx.db.insert_item(entity, &write_op)
            .map_err(|e| ApiError::BadRequest(format!("Failed to insert: {:?}", e)))?;
        Ok(decode_id(&write_op.id, entity, &ctx.db.schema).to_string())
    }).await?;

    Ok(ok_response(id))
}

pub async fn handle_find_many(req: Request<hyper::body::Incoming>, ctx: Arc<ServerContext>, model_index: usize) -> HandlerResult {
    let json_val = parse_json_body(req).await?;

    let result = blocking(move || {
        let entity = ctx.db.get_model_by_index(model_index);
        let query_op = parse_query(&ctx.db.schema, entity, &json_val)
            .map_err(|e| ApiError::BadRequest(format!("Failed to encode: {:?}", e)))?;
        Ok(array_to_json(&ctx.db.find_many(&query_op, |ctx| decode_document(ctx).unwrap())))
    }).await?;

    Ok(ok_response(result))
}

pub async fn handle_update(req: Request<hyper::body::Incoming>, item_id: String, ctx: Arc<ServerContext>, model_index: usize) -> HandlerResult {
    let json_val = parse_json_body(req).await?;
    let item_id = parse_id_from_url(&ctx.db.schema, ctx.db.get_model_by_index(model_index), &item_id)
      .map_err(|e| ApiError::BadRequest(format!("Failed to parse :item_id: {:?}", e)))?;

    let result = blocking(move || {
        let entity = ctx.db.get_model_by_index(model_index);
        let update_op = parse_update(&ctx.db.schema, entity, &json_val)
            .map_err(|e| ApiError::BadRequest(format!("Failed to encode: {:?}", e)))?;
          
        ctx.db.update_item(entity, &item_id, &update_op)
            .map_err(|e| ApiError::BadRequest(format!("Failed to insert: {:?}", e)))?;
        Ok(vec![])
    }).await?;

    Ok(ok_response(result))
}

pub async fn handle_delete(item_id: String, ctx: Arc<ServerContext>, model_index: usize) -> HandlerResult  {

    let item_id = parse_id_from_url(&ctx.db.schema, ctx.db.get_model_by_index(model_index), &item_id)
    .map_err(|e| ApiError::BadRequest(format!("Failed to parse :item_id: {:?}", e)))?;

    let result = blocking(move || {
        let entity = ctx.db.get_model_by_index(model_index);

        ctx.db.delete_item(entity, &item_id)
            .map_err(|e| ApiError::BadRequest(format!("Failed to insert: {:?}", e)))?;
        Ok(vec![])
    }).await?;

    Ok(ok_response(result))
}