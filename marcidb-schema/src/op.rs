//! The migration operation set and the (authoring-side) errors that `diff` can produce. These are pure
//! values shared by the authoring layer (which computes them) and the engine (which applies them).

/// A migration operation over a flat schema. Entities/fields are addressed by name;
/// resolved fields are taken from the `new`/`old` schema at apply time.
#[derive(Debug, Clone, PartialEq)]
pub enum MigrateOp {
    CreateEntity { name: String },
    DropEntity { name: String },
    AddField { entity: String, field: String },
    DropField { entity: String, field: String },
    /// Field metadata: nullable / default / format / added enum variants
    AlterField { entity: String, field: String },
    AddIndex { entity: String, field: String, unique: bool },
    DropIndex { entity: String, field: String, unique: bool },
}

#[derive(Debug, PartialEq)]
pub enum MigrateError {
    /// Changing a field's type requires data transformation — not supported
    UnsupportedTypeChange { entity: String, field: String, from: String, to: String },
    /// Changing the key (@id) requires a re-key — not supported
    UnsupportedKeyChange { entity: String, field: String },
    /// An existing field's slot moved — old rows would break (slots must be reconciled before diff)
    UnsupportedLayoutChange { entity: String, field: String },
    /// Destructive enum change (variant removed or id reassigned) — requires data migration
    UnsupportedEnumChange { entity: String, field: String, detail: String },
    /// A relation's storage binding changed (current_id / field_value / index_tree) — e.g. adding @bind to a
    /// composite-key relation flips index_tree → current_id. That moves where the relation lives, so it can't
    /// be applied as an in-place migration.
    UnsupportedBindingChange { entity: String, field: String, from: String, to: String },
}

impl std::fmt::Display for MigrateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MigrateError::UnsupportedTypeChange { entity, field, from, to } =>
                write!(f, "unsupported type change on {}.{}: {} -> {} (migrate the data manually)", entity, field, from, to),
            MigrateError::UnsupportedKeyChange { entity, field } =>
                write!(f, "unsupported key change on {}.{}", entity, field),
            MigrateError::UnsupportedLayoutChange { entity, field } =>
                write!(f, "field slot moved on {}.{} — existing rows would break (slots must be carried from the snapshot)", entity, field),
            MigrateError::UnsupportedEnumChange { entity, field, detail } =>
                write!(f, "unsupported enum change on {}.{}: {}", entity, field, detail),
            MigrateError::UnsupportedBindingChange { entity, field, from, to } =>
                write!(f, "relation binding changed on {}.{}: {} -> {} (a relation's storage layout can't be migrated in place — recreate the relation, or apply the schema to a fresh database via $sync)", entity, field, from, to),
        }
    }
}
