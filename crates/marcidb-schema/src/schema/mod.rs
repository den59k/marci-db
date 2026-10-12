use std::collections::HashMap;

mod schema_field;
mod schema_parse;
mod schema_attributes;
mod schema_enum;
mod schema_default_value;
mod schema_resolve_bindings;

pub use crate::schema::schema_attributes::{Attribute,DeleteConstraint,FieldCustomFormat};
pub use crate::schema::schema_field::{Field,FieldType,FieldLocation,PrimitiveFieldType,RefInfo,RefBinding,EnumInfo,FieldExistsCondition,FieldIndex,FieldIndexNum};
pub use crate::schema::{schema_parse::{parse_schema,try_parse_schema}};
pub(crate) use crate::schema::schema_parse::rebuild_caches;
pub use crate::schema::schema_default_value::FieldDefault;

/// Schema text parsing/validation error. The message is suitable for showing to the user (→ HTTP 400).
/// Returned by the fallible path ([`try_parse_schema`]); the infrastructural [`parse_schema`] panics on it
#[derive(Debug, PartialEq)]
pub struct SchemaError(pub String);

impl std::fmt::Display for SchemaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for SchemaError {}

#[derive(Debug,Clone)]
pub struct Schema {
    pub models: Vec<Entity>
}

#[derive(Debug,Clone)]
pub struct Entity {
    pub name: String,
    pub fields: Vec<Field>,
    pub payload_offset: usize,
    pub autoinsert: bool,
    pub rev_dependencies: Vec<EntityDependency>,
    /// Body `offset_pos` slots of dropped fields — permanently retired so they are never reused (old rows
    /// still hold dead bytes there). Kept in the snapshot as `@retired(N)` lines; `payload_offset` (the
    /// high-water mark) accounts for them so new fields always get a fresh slot above any retired one.
    pub retired_slots: Vec<usize>,
    /// The composite unique indexes: the fields carrying `@unique(<name>)`, grouped by name (a cache
    /// rebuilt from the attributes — `resolve_indexes`).
    pub unique_groups: Vec<UniqueGroup>,
}

impl Entity {
    pub fn new(name: String, fields: Vec<Field>) -> Self {
        Entity { name, fields, payload_offset: 0, autoinsert: false, rev_dependencies: vec![], retired_slots: vec![], unique_groups: vec![] }
    }
}

/// A composite unique index over several fields of one model (`user User @unique(pair)`, `post Post
/// @unique(pair)`): one tree, keyed by each part encoded as that field's value index would encode it, then
/// the row id. A row with a null part has no entry — nulls never collide, as in SQL.
#[derive(Debug, Clone)]
pub struct UniqueGroup {
    pub name: String,
    pub tree_name: String,
    /// (field index, how that field's value is encoded in the key), in field order
    pub parts: Vec<(usize, FieldIndex)>,
}

impl UniqueGroup {
    /// `Like.user, Like.post` — what an error names.
    pub fn fields_text(&self, entity: &Entity) -> String {
        self.parts.iter().map(|(fi, _)| entity.fields[*fi].full_name.as_str()).collect::<Vec<_>>().join(", ")
    }
    /// The parts' field names, in order (a migration's signature).
    pub fn field_names(&self, entity: &Entity) -> Vec<String> {
        self.parts.iter().map(|(fi, _)| entity.fields[*fi].name.clone()).collect()
    }
}

#[derive(Debug,Clone,PartialEq,Eq,Hash,PartialOrd)]
pub struct FieldRef {
    pub model_index: usize,
    pub field_index: usize
}

impl FieldRef {
    pub fn new(model_index: usize, field_index: usize) -> FieldRef {
        return FieldRef { model_index, field_index };
    }
}

impl Schema {
    pub fn build_model_name_map(&self) -> HashMap<String, usize> {
        self
            .models
            .iter()
            .enumerate()
            .map(|(i, model)| (model.name.clone(), i)).collect()
    }
}

#[derive(Debug,Clone)]
pub struct EntityDependency {
    pub model_index: usize,
    pub field_index: usize,
    pub constraint: DeleteConstraint
}

/// Total byte size of an entity's storage key, if every key field has a fixed size
/// (recursing through Ref-typed key fields). `None` when any key part is variable-length.
/// `@list` relations require a fixed-size target id — the inline array is `[u32 count][id]*`
/// and is split back into ids by this size.
pub fn fixed_id_size(models: &[Entity], entity: &Entity) -> Option<usize> {
    let mut size = 0;
    for field in entity.fields.iter() {
        if !matches!(field.location, FieldLocation::Key { .. }) { continue; }
        size += match &field.ty {
            FieldType::Ref(ref_info) => fixed_id_size(models, &models[ref_info.model_index])?,
            _ => field.get_size()?,
        };
    }
    Some(size)
}