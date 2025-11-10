use serde_json::Value;
use bitvec::prelude::*;

use crate::{marci_db::{MarciSelect, MarciSelectInclude, MarciSelectVirtual}, schema::{FieldType, Model, Schema}};

#[derive(Debug)]
pub enum MarciSelectError {
  MissingField(String)
}

impl MarciSelect<'_> {
  pub fn all(model: &Model) -> MarciSelect {
    return MarciSelect { select: bitvec![1; model.fields.len()+1], includes: vec![], virtual_fields: vec![] };
  }
}

pub fn parse_select<'a>(model: &'a Model, json: &Value, schema: &'a Schema) -> Result<MarciSelect<'a>, MarciSelectError> {

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

    if let Some((model_idx, field_idx)) = field.derived_from {
      let select;
      if val.is_boolean() {
        select = MarciSelect::all(model);
      } else {
        select = parse_select(model, &val, schema)?;
      }
      let model = &schema.models[model_idx];
      
      match field.ty {
          FieldType::ModelRefList(_) => {
            let index_name = model.fields[field_idx].index_name.as_ref().unwrap();
            virtual_fields.push(MarciSelectVirtual { 
              field_index, 
              index_name: index_name.as_bytes(),
              model: model,
              select: Box::new(select)
            });
          },
          _ => {}
      }
    } else if let FieldType::ModelRef(model_index) = field.ty {
      let model = &schema.models[model_index];
      let select;
      if val.is_boolean() {
        select = MarciSelect::all(model);
      } else {
        select = parse_select(model, &val, schema)?;
      }
      let include = MarciSelectInclude { offset: field.offset_pos, field_index, model, select: Box::new(select) };
      includes.push(include);
    } else {
      fields.set(field_index+1, true);
    }
  }

  return Ok(MarciSelect { select: fields, includes: includes, virtual_fields })
}