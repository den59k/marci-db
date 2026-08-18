use std::sync::Arc;

use http_body_util::Full;
use hyper::{Request, Response, body::Bytes};
use marcidb::{BatchErrorKind, MarciDB, MigrateApplyError, ProviderError, QueryError, ReindexError, aggregate_to_json, array_to_json, decode_document, decode_id, execute_batch, parse_aggregate, parse_id_from_url, parse_insert, parse_query, parse_update, query_binary_many, query_binary_one, schema_fingerprint, serialize_snapshot, shape_supported, filter_query};
use serde_json::Value;

use crate::{ServerContext, errors::ApiError, helpers::{blocking, ok_response, parse_json_body, parse_text_body, read_response, BinaryNeg, ReadBody}};

type HandlerResult = Result<Response<Full<Bytes>>, ApiError>;

/// A `@custom` index error → HTTP status. Storage faults are 5xx; a bad payload, missing provider, or
/// non-custom field are client errors (4xx).
fn reindex_error(e: ReindexError) -> ApiError {
    match &e {
        ReindexError::Storage(_) | ReindexError::Provider(ProviderError::Storage(_)) => ApiError::Internal(e.to_string()),
        _ => ApiError::BadRequest(e.to_string()),
    }
}

/// A read-query error → HTTP status. Plain storage faults are 5xx; a `$near`/`$search` problem maps like a
/// reindex error (bad payload / missing provider → 4xx).
fn query_error(e: QueryError) -> ApiError {
    match e {
        QueryError::Storage(e) => ApiError::Internal(format!("{:?}", e)),
        QueryError::Search(e) => reindex_error(e),
    }
}

/// Opens the DB by name (read-lock) and runs an operation against it
fn with_db<T>(ctx: &ServerContext, db_name: &str, f: impl FnOnce(&MarciDB) -> Result<T, ApiError>) -> Result<T, ApiError> {
    let db = ctx.get_db(db_name, false)?;
    let db = db.read().unwrap_or_else(|e| e.into_inner());
    f(&db)
}

fn model<'a>(db: &'a MarciDB, name: &str) -> Result<&'a marcidb::Entity, ApiError> {
    db.get_model(name).ok_or_else(|| ApiError::NotFound(format!("Model '{}' not found", name)))
}

/// Declarative schema synchronization (`$sync`). The body is the `.marci` schema text; the server diffs its
/// stored schema against the one sent and applies the diff. If the DB doesn't exist — it's created. An HTTP-only
/// escape-hatch for environments without a CLI/migration files — not exposed in the CLI to avoid confusion with
/// `migrate push`. Doesn't touch the ledger, so don't mix it with `$migrate` on the same DB. Caution:
/// a model missing from the schema = `drop model` (loss of the model's data); incompatible changes → 400
pub async fn handle_sync(req: Request<hyper::body::Incoming>, ctx: Arc<ServerContext>, db_name: String) -> HandlerResult {
    let schema_text = parse_text_body(req).await?;

    blocking(move || {
        let db = ctx.get_db(&db_name, true)?; // create-if-absent
        let mut db = db.write().unwrap_or_else(|e| e.into_inner());

        // The "smart" $sync path lives here, in the server: parse the .marci schema, carry slots/ids from
        // the stored snapshot, diff, then apply through the engine's commit primitive. The engine itself
        // stays dumb (no DSL parsing / diffing).
        let mut new_schema = marcidb::try_parse_schema(&schema_text)
            .map_err(|e| ApiError::BadRequest(format!("{}", e)))?;
        marcidb_schema::reconcile(&mut new_schema, &db.schema);
        let ops = marcidb_schema::diff(&db.schema, &new_schema)
            .map_err(|e| ApiError::BadRequest(format!("{}", e)))?;
        db.commit_schema(new_schema, &ops).map_err(|e| match e {
            MigrateApplyError::Storage(_) => ApiError::Internal(format!("{:?}", e)),
            _ => ApiError::BadRequest(format!("{}", e)),
        })?;
        Ok::<_, ApiError>(String::new())
    }).await?;

    Ok(ok_response(Vec::new()))
}

/// Dry run of `$sync` (`POST /:db/$sync?plan=1`): the same parse → reconcile → diff, but nothing is
/// committed and a missing database is NOT created (it is planned against an empty schema). Answers the
/// op list as JSON so a host can show — and require confirmation for — destructive changes before
/// applying. Incompatible changes fail with the same 400 the real `$sync` would give.
pub async fn handle_sync_plan(req: Request<hyper::body::Incoming>, ctx: Arc<ServerContext>, db_name: String) -> HandlerResult {
    let schema_text = parse_text_body(req).await?;

    let json = blocking(move || {
        let mut new_schema = marcidb::try_parse_schema(&schema_text)
            .map_err(|e| ApiError::BadRequest(format!("{}", e)))?;
        let empty = marcidb::Schema { models: Vec::new() };
        let ops = match ctx.get_db(&db_name, false) {
            Ok(db) => {
                let db = db.read().unwrap_or_else(|e| e.into_inner());
                marcidb_schema::reconcile(&mut new_schema, &db.schema);
                marcidb_schema::diff(&db.schema, &new_schema)
            }
            Err(ApiError::NotFound(_)) => {
                marcidb_schema::reconcile(&mut new_schema, &empty);
                marcidb_schema::diff(&empty, &new_schema)
            }
            Err(e) => return Err(e),
        }.map_err(|e| ApiError::BadRequest(format!("{}", e)))?;
        Ok::<_, ApiError>(plan_to_json(&ops).to_string())
    }).await?;

    Ok(ok_response(json))
}

/// `{ ops: [{ op, entity, field?, unique?, destructive, text }], destructive }` — `text` is the action as
/// `.march` spells it (`drop field User.bio`), for logs and confirmation dialogs.
fn plan_to_json(ops: &[marcidb::MigrateOp]) -> Value {
    use marcidb::MigrateOp::*;
    let items: Vec<Value> = ops.iter().map(|op| match op {
        CreateEntity { name } => serde_json::json!({ "op": "createEntity", "entity": name, "destructive": false, "text": format!("create entity {}", name) }),
        DropEntity { name } => serde_json::json!({ "op": "dropEntity", "entity": name, "destructive": true, "text": format!("drop entity {}", name) }),
        AddField { entity, field } => serde_json::json!({ "op": "addField", "entity": entity, "field": field, "destructive": false, "text": format!("add field {}.{}", entity, field) }),
        DropField { entity, field } => serde_json::json!({ "op": "dropField", "entity": entity, "field": field, "destructive": true, "text": format!("drop field {}.{}", entity, field) }),
        AlterField { entity, field } => serde_json::json!({ "op": "alterField", "entity": entity, "field": field, "destructive": false, "text": format!("alter field {}.{}", entity, field) }),
        AddIndex { entity, field, unique } => serde_json::json!({ "op": "addIndex", "entity": entity, "field": field, "unique": unique, "destructive": false, "text": format!("add {} {}.{}", if *unique { "unique" } else { "index" }, entity, field) }),
        DropIndex { entity, field, unique } => serde_json::json!({ "op": "dropIndex", "entity": entity, "field": field, "unique": unique, "destructive": false, "text": format!("drop {} {}.{}", if *unique { "unique" } else { "index" }, entity, field) }),
    }).collect();
    let destructive = items.iter().any(|i| i["destructive"] == Value::Bool(true));
    serde_json::json!({ "ops": items, "destructive": destructive })
}

/// Imperative migration (`$migrate`). The body is the TEXT of the migration actions (self-contained actions,
/// possibly several migrations in a row). The server dumbly lays them onto its state and applies them —
/// no ledger. Which actions to send is decided by the `marci-migrate` client based on `GET /:db/$snapshot`.
/// If the DB doesn't exist — it's created.
pub async fn handle_migrate(req: Request<hyper::body::Incoming>, ctx: Arc<ServerContext>, db_name: String) -> HandlerResult {
    let migration_text = parse_text_body(req).await?;

    blocking(move || {
        let db = ctx.get_db(&db_name, true)?; // create-if-absent
        let mut db = db.write().unwrap_or_else(|e| e.into_inner());

        // Dumb `$migrate`: lay the self-contained actions onto the current snapshot (`evolve`), parse the
        // result, extract the physical ops, and commit. The engine just applies — the `.march` text format
        // lives in `marcidb-schema`.
        let cur = marcidb::serialize_snapshot(&db.schema);
        let new_text = marcidb_schema::evolve(&cur, &migration_text)
            .map_err(|e| ApiError::BadRequest(format!("{}", e)))?;
        let new_schema = marcidb::parse_snapshot(&new_text)
            .map_err(|e| ApiError::BadRequest(format!("{}", e)))?;
        let ops = marcidb_schema::migration_ops(&migration_text)
            .map_err(|e| ApiError::BadRequest(format!("{}", e)))?;
        db.commit_schema(new_schema, &ops).map_err(|e| match e {
            MigrateApplyError::Storage(_) => ApiError::Internal(format!("{:?}", e)),
            _ => ApiError::BadRequest(format!("{}", e)),
        })?;
        Ok::<_, ApiError>(())
    }).await?;

    Ok(ok_response(Vec::new()))
}

/// Returns the current materialized snapshot of the DB — the client reconciles it against its local migration
/// history (`marci-migrate plan`) to figure out which actions haven't been applied yet and to catch drift
pub async fn handle_snapshot(ctx: Arc<ServerContext>, db_name: String) -> HandlerResult {
    let snapshot = blocking(move || with_db(&ctx, &db_name, |db| Ok(serialize_snapshot(&db.schema)))).await?;
    Ok(ok_response(snapshot))
}

/// Atomic batch transaction: the body is an array of operations `{ model, action, ... }`
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

/// Whether this binary-eligible request should actually get bytes: the client must accept binary, claim the
/// *same* schema fingerprint the target DB currently has, and the query shape must be binary-encodable. Any
/// miss → JSON (transparent, never wrong bytes). The fingerprint is computed only on the binary path (gated
/// by `wants_binary` first), so the common JSON request pays nothing.
fn binary_allowed(neg: &BinaryNeg, db: &MarciDB, query_op: &marcidb::QueryOp) -> bool {
    neg.wants_binary
        && shape_supported(query_op)
        && neg.schema_hash.as_deref() == Some(schema_fingerprint(&db.schema).as_str())
}

pub async fn handle_find_many(req: Request<hyper::body::Incoming>, ctx: Arc<ServerContext>, db_name: String, model_name: String) -> HandlerResult {
    let neg = BinaryNeg::from_req(&req);
    let json_val = parse_json_body(req).await?;

    let result = blocking(move || with_db(&ctx, &db_name, |db| {
        let entity = model(db, &model_name)?;
        let query_op = parse_query(&db.schema, entity, &json_val)
            .map_err(|e| ApiError::BadRequest(format!("Failed to encode: {:?}", e)))?;
        if binary_allowed(&neg, db, &query_op) {
            let bytes = query_binary_many(db, &query_op).map_err(query_error)?;
            return Ok(ReadBody::Binary(bytes));
        }
        let items = db.find_many(&query_op, |ctx| decode_document(ctx).unwrap())
            .map_err(query_error)?;
        Ok(ReadBody::Json(array_to_json(&items)))
    })).await?;

    Ok(read_response(result))
}

pub async fn handle_find_first(req: Request<hyper::body::Incoming>, ctx: Arc<ServerContext>, db_name: String, model_name: String) -> HandlerResult {
    let neg = BinaryNeg::from_req(&req);
    let json_val = parse_json_body(req).await?;

    let result = blocking(move || with_db(&ctx, &db_name, |db| {
        let entity = model(db, &model_name)?;
        let query_op = parse_query(&db.schema, entity, &json_val)
            .map_err(|e| ApiError::BadRequest(format!("Failed to encode: {:?}", e)))?;
        if binary_allowed(&neg, db, &query_op) {
            let bytes = query_binary_one(db, &query_op).map_err(query_error)?;
            return Ok(ReadBody::Binary(bytes));
        }
        let item = db.find_first(&query_op, |ctx| decode_document(ctx).unwrap())
            .map_err(query_error)?;
        Ok(ReadBody::Json(item.unwrap_or_else(|| "null".to_string())))
    })).await?;

    Ok(read_response(result))
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

/// `POST /{db}/{model}/updateMany` with a body of `{ $where, data }` — applies `data` to every matching
/// row in one transaction and responds with the number of rows matched.
pub async fn handle_update_many(req: Request<hyper::body::Incoming>, ctx: Arc<ServerContext>, db_name: String, model_name: String) -> HandlerResult {
    let json_val = parse_json_body(req).await?;

    let result = blocking(move || with_db(&ctx, &db_name, |db| {
        let entity = model(db, &model_name)?;
        let data = json_val.get("data")
            .ok_or_else(|| ApiError::BadRequest("Field 'data' required".to_string()))?;
        let update_op = parse_update(&db.schema, entity, data)
            .map_err(|e| ApiError::BadRequest(format!("Failed to encode: {:?}", e)))?;
        // The body carries `data` alongside the `$`-operators; keep only the latter for the scan
        let query_op = parse_query(&db.schema, entity, &filter_query(&json_val))
            .map_err(|e| ApiError::BadRequest(format!("Failed to encode: {:?}", e)))?;

        let updated = db.update_many(entity, &query_op, &update_op)
            .map_err(|e| ApiError::BadRequest(format!("Failed to update: {:?}", e)))?;
        Ok(updated.to_string())
    })).await?;

    Ok(ok_response(result))
}

/// `POST /{db}/{model}/deleteMany` with a body of `{ $where }` — deletes every matching row in one
/// transaction (cascades and restrict checks as for a single delete) and responds with the number deleted.
pub async fn handle_delete_many(req: Request<hyper::body::Incoming>, ctx: Arc<ServerContext>, db_name: String, model_name: String) -> HandlerResult {
    let json_val = parse_json_body(req).await?;

    let result = blocking(move || with_db(&ctx, &db_name, |db| {
        let entity = model(db, &model_name)?;
        let query_op = parse_query(&db.schema, entity, &filter_query(&json_val))
            .map_err(|e| ApiError::BadRequest(format!("Failed to encode: {:?}", e)))?;
        let deleted = db.delete_many(entity, &query_op)
            .map_err(|e| ApiError::BadRequest(format!("Failed to delete: {:?}", e)))?;
        Ok(deleted.to_string())
    })).await?;

    Ok(ok_response(result))
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

/// Rebuilds all `@custom` (module) indexes of one model from current data. Registry-driven, so any provider
/// (vector today, FTS later) is covered without a route change. Returns `{ ok, indexed: <tree count> }`.
pub async fn handle_reindex(ctx: Arc<ServerContext>, db_name: String, model_name: String) -> HandlerResult {
    let count = blocking(move || with_db(&ctx, &db_name, |db| {
        let entity = model(db, &model_name)?;
        db.reindex_entity(entity).map_err(reindex_error)
    })).await?;

    Ok(ok_response(format!("{{\"ok\":true,\"indexed\":{}}}", count)))
}

/// Rebuilds the `@custom` indexes of every model in the DB.
pub async fn handle_reindex_all(ctx: Arc<ServerContext>, db_name: String) -> HandlerResult {
    let count = blocking(move || with_db(&ctx, &db_name, |db| {
        let mut total = 0;
        for entity in db.schema.models.iter() {
            total += db.reindex_entity(entity).map_err(reindex_error)?;
        }
        Ok(total)
    })).await?;

    Ok(ok_response(format!("{{\"ok\":true,\"indexed\":{}}}", count)))
}
