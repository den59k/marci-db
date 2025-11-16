use serde_json::Value;
use bitvec::prelude::*;

use crate::{marci_db::{MarciSelect, MarciSelectBinding, MarciSelectInclude, MarciSelectVirtual}, schema::{Field, FieldType, Model, Schema}};

#[derive(Debug)]
pub enum MarciSelectError {
  MissingField(String)
}

impl MarciSelect<'_> {
  pub fn all(fields: &'_[Field]) -> MarciSelect<'_> {
    return MarciSelect { select: bitvec![1; fields.len()], includes: vec![] };
  }
}

pub fn parse_select<'a>(fields: &'a [Field], json: &Value, schema: &'a Schema) -> Result<MarciSelect<'a>, MarciSelectError> {

  if json.is_boolean() {
    return Ok(MarciSelect::all(fields));
  }

  let mut changed_mask = bitvec![0; fields.len()];
  let mut includes = vec![];

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
        includes.push(MarciSelectInclude {
          field_index,
          model: st,
          select,
          binding: MarciSelectBinding::OneStruct()
        });
      },
      FieldType::StructList(st) => {
        let select = parse_select(&st.fields, &val, schema)?;
        includes.push(MarciSelectInclude {
          field_index,
          model: st,
          select,
          binding: MarciSelectBinding::ManyStruct()
        });
      },
      _ => {
        changed_mask.set(field_index, true);
      }
    } 
  }

  return Ok(MarciSelect { select: changed_mask, includes: includes })
}


#[cfg(test)]
mod tests {
use bitvec::prelude::*;
use serde_json::json;

use crate::{marci_select::parse_select, schema::parse_schema};

  #[test]
  fn test_parse_select() {
    let schema_str = "
model User {
  name        String
  surname     String
  info        UserInfo
}

struct UserInfo {
  bio         String
}
";
    let schema = parse_schema(schema_str);
    let model = &schema.models[0];

    let input = json!({
        "name": true
    });
    let parsed = parse_select(&model.fields, &input, &schema).unwrap();
    let mut expect = bitvec::bitvec![0; model.fields.len()];
    expect.set(1, true);
    assert_eq!(parsed.select, expect);
    assert_eq!(parsed.includes.len(), 0);
    
    let input = json!({
        "name": true,
        "info": true
    });
    let parsed = parse_select(&model.fields, &input, &schema).unwrap();
    let mut expect = bitvec::bitvec![0; model.fields.len()];
    expect.set(1, true);
    assert_eq!(parsed.select, expect);

    assert_eq!(parsed.includes.len(), 1);

    let mut expect = bitvec::bitvec![1; parsed.includes[0].model.fields().len()];
    expect.set(0, true);
    assert_eq!(parsed.includes[0].select.select, expect);
    
  }
}