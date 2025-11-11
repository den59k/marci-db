use serde_json::Value;
use bitvec::prelude::*;

use crate::{marci_db::{MarciSelect, MarciSelectInclude, MarciSelectVirtual}, schema::{FieldType, Model, Schema}};

#[derive(Debug)]
pub enum MarciSelectError {
  MissingField(String)
}

impl MarciSelect<'_> {
  pub fn all(model: &'_ Model) -> MarciSelect<'_> {
    return MarciSelect { select: bitvec![1; model.fields.len()+1], includes: vec![], virtual_fields: vec![] };
  }
}

pub fn parse_select<'a>(model: &'a Model, json: &Value, schema: &'a Schema) -> Result<MarciSelect<'a>, MarciSelectError> {

  if json.is_boolean() {
    return Ok(MarciSelect::all(model));
  }

  let mut fields = bitvec![0; model.fields.len()+1];
  let mut virtual_fields= vec![];
  let mut includes = vec![];

  if json.get("id").and_then(|i|i.as_bool()).is_some_and(|f| f) {
    fields.set(0, true);
  }

  for (field_index, field) in model.fields.iter().enumerate() {
    let Some(val) = json.get(&field.name) else {
      continue;
    };
    if matches!(val, Value::Bool(false)) {
      continue;
    }

    if let Some(model_ref) = field.derived_from.as_ref() {
      let select = parse_select(model, &val, schema)?;
      let ref_model = &schema.models[model_ref.model_index];
      let ref_field = &ref_model.fields[model_ref.field_index];
      let index_name = ref_field.index_name.as_ref()
          .unwrap_or_else(|| panic!("Index for field {}.{} not found", model.name, field.name))
          .as_bytes();

      match field.ty {
          FieldType::ModelRefList(_) => {
            virtual_fields.push(MarciSelectVirtual { 
              field_index, 
              index_name,
              model: ref_model,
              select: Box::new(select)
            });
          },
          _ => {}
      }
    } else if let FieldType::ModelRef(model_index) = field.ty {
      let model = &schema.models[model_index];
      let select = parse_select(model, &val, schema)?;

      let include = MarciSelectInclude { 
        offset: field.offset_pos, 
        field_index, 
        model, 
        select: Box::new(select), 
        is_array: false 
      };
      includes.push(include);
    } else if let FieldType::ModelRefList(model_index) = field.ty {
      let model = &schema.models[model_index];
      let select = parse_select(model, &val, schema)?;

      let include = MarciSelectInclude { 
        offset: field.offset_pos, 
        field_index, model, 
        select: Box::new(select), 
        is_array: true 
      };
      includes.push(include);
    } else {
      fields.set(field_index+1, true);
    }
  }

  return Ok(MarciSelect { select: fields, includes: includes, virtual_fields })
}