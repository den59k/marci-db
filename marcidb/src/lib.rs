mod marci_db;
mod transaction;
mod batch;
mod migrate;
mod error;
mod write_op;
mod query_op;
mod aggregate_op;
mod delete_op;
mod utils;
mod json_parsers;
mod index_utils;
mod url_parser;
mod update_op;
mod num_utils;

// The schema model + parser + snapshot codec live in the foundation crate. Alias it as `crate::schema` so
// the engine's many `crate::schema::…` references keep resolving, and re-export the public types at the
// engine root (below) so downstream crates can still say `marcidb::Schema`, `marcidb::parse_schema`, etc.
pub(crate) use marcidb_schema as schema;

pub use marcidb_schema::{parse_schema, try_parse_schema, SchemaError, FieldRef, Schema, Field,Entity,FieldLocation,FieldType,PrimitiveFieldType,FieldExistsCondition,EnumInfo,RefInfo,RefBinding,Attribute,DeleteConstraint,FieldCustomFormat,FieldDefault};
pub use crate::json_parsers::{parse_insert,parse_update,parse_query,parse_aggregate,aggregate_to_json,decode_document,decode_id,array_to_json,parse_id,EncodeError};
pub use crate::aggregate_op::{AggregateOp,AggregateResult,SumValue};
pub use crate::url_parser::{parse_id_from_url,UrlParseError};
pub use crate::marci_db::MarciDB;
pub use crate::transaction::MarciTransaction;
pub use crate::error::StorageError;
pub use crate::batch::{execute_batch, BatchError, BatchErrorKind};
pub use crate::migrate::{apply, MigrateApplyError};
pub use marcidb_schema::{MigrateOp, MigrateError, serialize_snapshot, parse_snapshot, serialize_type, serialize_field};

pub use crate::query_op::DecodeCtx;
pub use crate::update_op::UpdateError;
pub use crate::write_op::InsertError;
pub use crate::delete_op::DeleteError;

#[cfg(test)]
pub(crate) use crate::write_op::WriteRelation;
