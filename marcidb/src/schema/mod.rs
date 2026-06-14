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
    pub rev_dependencies: Vec<EntityDependency>
}

impl Entity {
    pub fn new(name: String, fields: Vec<Field>) -> Self {
        Entity { name, fields, payload_offset: 0, autoinsert: false, rev_dependencies: vec![] }
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