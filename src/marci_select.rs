use serde_json::Value;
use bitvec::prelude::*;

use crate::{marci_db::{MarciSelect, MarciSelectInclude}, schema::{FieldType, Model, Schema}};

#[derive(Debug)]
pub enum MarciSelectError {
  MissingField(String)
}

impl MarciSelect {
  pub fn all(model: &Model) -> MarciSelect {
    return MarciSelect { select: bitvec![1; model.fields.len()], includes: vec![] };
  }
}

pub fn parse_select(model: &Model, json: &Value, schema: &Schema) -> Result<MarciSelect, MarciSelectError> {

  let mut fields = bitvec![0; model.fields.len()+1];
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

    if field.is_virtual {
      todo!("Add select virtual fields support");
    } else if let FieldType::ModelRef(model_index) = field.ty {
      let model = &schema.models[model_index];
      let select;
      if val.is_boolean() {
        select = MarciSelect::all(model);
      } else {
        let Some(select_json) = val.get("select") else {
          return Err(MarciSelectError::MissingField(format!("select")));
        };
        select = parse_select(model, select_json, schema)?;
      }
      let include = MarciSelectInclude { offset: field.offset_pos, field_index, model_index, select: Box::new(select) };
      includes.push(include);
    } else {
      fields.set(field_index+1, true);
    }
  }

  return Ok(MarciSelect { select: fields, includes: includes })
}