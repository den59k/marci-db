use std::collections::HashMap;

mod schema_field;
mod schema_parse;
mod schema_attributes;
mod schema_enum;
mod schema_default_value;
mod schema_resolve_bindings;

pub use crate::schema::schema_attributes::DeleteConstraint;
pub use crate::schema::schema_field::{Field,FieldType,FieldLocation,PrimitiveFieldType,RefInfo,RefBinding,EnumInfo,FieldExistsCondition,FieldIndex,FieldIndexNum};
pub use crate::schema::{schema_parse::parse_schema};
pub use crate::schema::schema_default_value::FieldDefault;

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

    pub(crate) fn is_parent_key(&self, field: &Field, entity: &Entity) -> bool {
        let FieldType::Ref (ref_info) = &field.ty else {
            return false;
        };
        if !matches!(field.location, FieldLocation::Key { .. }) {
            return false;
        }
        return self.models[ref_info.model_index].name == entity.name;
    }
}

#[derive(Debug,Clone)]
pub struct EntityDependency {
    pub model_index: usize,
    pub field_index: usize,
    pub constraint: DeleteConstraint
}