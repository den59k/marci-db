
use serde_json::Value;
use bitvec::prelude::*;

use crate::{marci_db::{Injected, MarciSelect, MarciSelectBinding, MarciSelectInclude, MarciSelectVirtual}, schema::{Entity, Field, FieldType, Schema}};

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
          field,
          model,
          select,
          select_only_id: false,
          binding: MarciSelectBinding::One(),
          injected: parse_injected(schema, field, val)
        });
      },
      FieldType::ModelRefList(model_index) => {
        let model = &schema.models[*model_index];
        let select = parse_select(&model.fields, &val, schema)?;
        let tree_name = field.select_index.as_ref().expect("Index not found").as_bytes();
        includes.push(MarciSelectInclude {
          field,
          model,
          select,
          select_only_id: false,
          binding: MarciSelectBinding::Many(tree_name),
          injected: parse_injected(schema, field, val)
        });
      },
      FieldType::Struct(st) => {
        let select = parse_select(&st.fields, &val, schema)?;
        includes.push(MarciSelectInclude {
          field,
          model: st,
          select,
          select_only_id: false,
          binding: MarciSelectBinding::OneStruct(),
          injected: None
        });
      },
      FieldType::StructList(st) => {
        let select = parse_select(&st.fields, &val, schema)?;
        includes.push(MarciSelectInclude {
          field,
          model: st,
          select,
          select_only_id: false,
          binding: MarciSelectBinding::ManyStruct(),
          injected: None
        });
      },
      _ => {
        changed_mask.set(field_index, true);
      }
    } 
  }

  for include in includes.iter_mut() { 
    include.select_only_id = !include.model
      .fields
      .iter()
      .enumerate()
      .any(|(idx, field)| include.select.select[idx] && field.id_idx.is_none());

    if include.select_only_id {
      // println!("Optimize field (set only id): {:?}", include.field.name);
    }
  }

  return Ok(MarciSelect { select: changed_mask, includes: includes })
}

fn parse_injected<'a>(schema: &'a Schema, field: &'a Field, val: &Value) -> Option<Injected<'a>> {
  if !val.is_object() || val.is_null() { return None }

  let mut injected: Option<Injected<'_>> = None;
  for (field_ref, alias) in field.injected_fields.iter() {
    let field = &schema.models[field_ref.model_index].fields[field_ref.field_index];

    if val.get(alias) != Some(&Value::Bool(true)) { continue };

    match &field.ty {
      FieldType::Struct(st) | FieldType::StructList(st) => {
        if injected.as_ref().is_some_and(|f: &Injected<'_>| f.st.name != st.name) {
          panic!("Invalid inject fields from different structs");
        }
        let injected = injected
          .get_or_insert(Injected { st, select: bitvec!(0; st.fields.len()), aliases: None } );

        let struct_field_index = field_ref.struct_field_index.unwrap();
        injected.select.set(struct_field_index, true);
        if *alias != field.name {
          injected.aliases
            .get_or_insert_default()
            .insert(struct_field_index, &alias);
        }
      },
      _ => { panic!("Trying to inject field from non-struct") }
    } 
  }
  return injected;
}

#[cfg(test)]
mod tests {
use bitvec::prelude::*;
use serde_json::json;

use crate::{marci_db::Injected, marci_select::parse_select, schema::parse_schema};

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

    let mut expect = bitvec::bitvec![1; parsed.includes[0].model.fields.len()];
    expect.set(0, true);
    assert_eq!(parsed.includes[0].select.select, expect);
    
  }

  #[test]
  fn test_parse_select_with_injects() {

    let schema_str = "
model User {
  name        String
  surname     String
  projects    Project[]     @derived(Project.users.user) @inject(Project.users.role)
}

model Project {
  name        String
  users       UserRole[]
}

struct UserRole {
  user        User          @id
  role        String
}

";
    let schema = parse_schema(schema_str);

    let input = json!({
        "name": true,
        "projects": {
          "role": true,
          "name": true
        }
    });
    let model = &schema.models[0];
    let parsed = parse_select(&model.fields, &input, &schema).unwrap();
    assert_eq!(parsed.includes.len(), 1);

    assert!(parsed.includes[0].injected.is_some());

    let injected = parsed.includes[0].injected.as_ref().unwrap();
    assert_eq!(injected.st.name, "Project.users");

    let mut bitvec = bitvec!(0; injected.st.fields.len());
    bitvec.set(2, true);
    assert_eq!(injected.select, bitvec);

  }
}