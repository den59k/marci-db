use serde_json::Value;
use bitvec::prelude::*;

use crate::{marci_db::{MarciSelect, MarciSelectBinding, MarciSelectInclude, MarciSelectVirtual}, schema::{Field, FieldType, Model, Schema}};

#[derive(Debug)]
pub enum MarciSelectError {
  MissingField(String)
}

impl MarciSelect<'_> {
  pub fn all(fields: &'_[Field]) -> MarciSelect<'_> {
    return MarciSelect { select: bitvec![1; fields.len()+1], includes: vec![] };
  }
}

pub fn parse_select<'a>(fields: &'a [Field], json: &Value, schema: &'a Schema) -> Result<MarciSelect<'a>, MarciSelectError> {

  if json.is_boolean() {
    return Ok(MarciSelect::all(fields));
  }

  let mut changed_mask = bitvec![0; fields.len()+1];
  let mut includes = vec![];

  if json.get("id").and_then(|i|i.as_bool()).is_some_and(|f| f) {
    changed_mask.set(0, true);
  }

  for (field_index, field) in fields.iter().enumerate() {
    let Some(val) = json.get(&field.name) else {
      continue;
    };
    if matches!(val, Value::Bool(false)) {
      continue;
    }

    match &field.ty {
      FieldType::ModelRef(model_index) => {
        let model = &schema.models[*model_index];
        let select = parse_select(&model.fields, &val, schema)?;

        includes.push(MarciSelectInclude {
          field_index,
          model,
          select,
          binding: MarciSelectBinding::One(field.offset_pos)
        });
      },
      FieldType::ModelRefList(model_index) => {
        let model = &schema.models[*model_index];
        let select = parse_select(&model.fields, &val, schema)?;
        let tree_name = field.select_index.as_ref().expect("Index not found").as_bytes();
        includes.push(MarciSelectInclude {
          field_index,
          model,
          select,
          binding: MarciSelectBinding::Many(tree_name)
        });
      },
      FieldType::Struct(st) => {
        let mut select = parse_select(&st.fields, &val, schema)?;
        if matches!(val, Value::Bool(true)) {
          select.select.set(0, false);
        }
        includes.push(MarciSelectInclude {
          field_index,
          model: st,
          select,
          binding: MarciSelectBinding::OneStruct()
        });
      },
      FieldType::StructList(st, _) => {
        let select = parse_select(&st.fields, &val, schema)?;
        includes.push(MarciSelectInclude {
          field_index,
          model: st,
          select,
          binding: MarciSelectBinding::ManyStruct()
        });
      },
      _ => {
        changed_mask.set(field_index+1, true);
      }
    }    

    // if let Some(model_ref) = field.derived_from.as_ref() {
    //   let select = parse_select(model, &val, schema)?;
    //   let ref_model = &schema.models[model_ref.model_index];
    //   let ref_field = &ref_model.fields[model_ref.field_index];
    //   let index_name = ref_field.index_name.as_ref()
    //       .unwrap_or_else(|| panic!("Index for field {}.{} not found", model.name, field.name))
    //       .as_bytes();

    //   match field.ty {
    //       FieldType::ModelRefList(_) => {
    //         virtual_fields.push(MarciSelectVirtual { 
    //           field_index, 
    //           index_name,
    //           model: ref_model,
    //           select: Box::new(select)
    //         });
    //       },
    //       _ => {}
    //   }
    // } else {
    //   match field.ty {
    //     FieldType::ModelRef(model_index)  => {
    //       let model = &schema.models[model_index];
    //       let select = parse_select(model, &val, schema)?;

    //       let include = MarciSelectInclude { 
    //         offset: field.offset_pos, 
    //         field_index, 
    //         model, 
    //         select: Box::new(select), 
    //         is_array: false 
    //       };
    //       includes.push(include);
    //     }

    //     FieldType::ModelRefList(model_index) => {
    //       let model = &schema.models[model_index];
    //       let select = parse_select(model, &val, schema)?;

    //       let include = MarciSelectInclude { 
    //         offset: field.offset_pos, 
    //         field_index, model, 
    //         select: Box::new(select), 
    //         is_array: true 
    //       };
    //       includes.push(include);
    //     }

    //     FieldType::Struct(ref st) => {
          
    //     }
    //     FieldType::StructList(ref st, _) => {
          
    //     }

    //     _ => {
    //       changed_mask.set(field_index+1, true);
    //     }
    //   }
    // }
  }

  return Ok(MarciSelect { select: changed_mask, includes: includes })
}