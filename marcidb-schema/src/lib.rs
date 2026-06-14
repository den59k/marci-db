//! The foundational schema layer for MarciDB — **no storage, no dependency on the engine**. It owns:
//! the schema model (types), the `.marci` DSL parser, the materialized-snapshot codec, and the migration
//! authoring tools (diff/reconcile + the `.march` file format). The storage engine (`marcidb`) depends on
//! this crate; the server and CLI use it for parsing, diffing, and snapshots.

mod schema;
mod snapshot;
mod op;
mod diff;
mod march;

// Schema model + `.marci` DSL parser
pub use crate::schema::{
    parse_schema, try_parse_schema, SchemaError, FieldRef, Schema, Field, Entity, FieldLocation, FieldType,
    PrimitiveFieldType, FieldExistsCondition, EnumInfo, RefInfo, RefBinding, Attribute, DeleteConstraint,
    FieldCustomFormat, FieldDefault, FieldIndex, FieldIndexNum, EntityDependency,
};
// Materialized-snapshot codec (the engine's schema persistence format)
pub use crate::snapshot::{serialize_snapshot, parse_snapshot, serialize_type, serialize_field};
// Migration operation set + authoring
pub use crate::op::{MigrateOp, MigrateError};
pub use crate::diff::{diff, reconcile};
pub use crate::march::{serialize_migration, evolve, migration_ops};
