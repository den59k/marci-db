use std::collections::HashMap;

mod schema_field;
mod schema_parse;
mod schema_attributes;
mod schema_enum;

pub use crate::schema::schema_field::{Field,FieldType,FieldLocation,PrimitiveFieldType,FieldDefault,RefInfo,RefBinding,EnumInfo,FieldCondition};
pub use crate::schema::{schema_parse::parse_schema};

#[derive(Debug,Clone)]
pub struct Schema {
    pub models: Vec<Entity>
}

#[derive(Debug,Clone)]
pub struct Entity {
    pub name: String,
    pub fields: Vec<Field>,
    pub payload_offset: usize,
    pub autoinsert: bool
}

#[derive(Debug,Clone,PartialEq,Eq,Hash,PartialOrd)]
pub struct FieldRef {
    pub model_index: usize,
    pub field_index: usize,
    pub enum_variant_index: Option<(usize, usize)>
}

impl FieldRef {
    pub fn new(model_index: usize, field_index: usize) -> FieldRef {
        return FieldRef { model_index, field_index, enum_variant_index: None };
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
