
use std::collections::HashMap;

use serde_json::Value;
use bitvec::prelude::*;

use crate::{marci_db::{EnumSelect, Injected, MarciSelect, MarciSelectBinding, MarciSelectInclude}, schema::{Aliases, Entity, Field, FieldType, Schema}};

#[derive(Debug)]
pub enum MarciSelectError {
  MissingField(String)
}

impl MarciSelect<'_> {
  pub fn all(fields: &'_[Field]) -> MarciSelect<'_> {
    return MarciSelect { 
      mask: bitvec![1; fields.len()], 
      includes: vec![], 
      aliases: None,
      enum_selects: fields.iter().enumerate().filter_map(|(i, field)| {
        let FieldType::Enum(en) = &field.ty else { return None; };
        
        let variants_map: HashMap<u16, MarciSelect<'_>> = en
          .variants
          .iter()
          .enumerate()
          .filter_map(|(i, v)| {
            if v.fields.is_empty() {
                None
            } else {
                Some((i as u16, MarciSelect::all(&v.fields)))
            }
          })
          .collect();

        if variants_map.is_empty() { return None; };

        return Some((i, variants_map))
      }).collect()
    };
  }
}

pub fn parse_select<'a>(fields: &'a [Field], json: &Value, schema: &'a Schema, aliases: Option<&'a Aliases>) -> Result<MarciSelect<'a>, MarciSelectError> {

  if json.is_boolean() {
    return Ok(MarciSelect::all(fields));
  }

  let mut changed_mask = bitvec![0; fields.len()];
  let mut includes = vec![];
  let mut enum_selects: EnumSelect<'_> = HashMap::new();

  for (field_index, field) in fields.iter().enumerate() {

    let field_name: &str = {
      if let Some(aliases) = aliases {
        // Skip field if it not presented in aliases
        let Some(str) = aliases.get(&field.name) else { continue; };
        str
      } else {
        &field.name
      }
    };
    
    let Some(val) = json.get(field_name) else {
      continue;
    };
    if matches!(val, Value::Bool(false)) {
      continue;
    }

    match &field.ty {
      FieldType::ModelRef(model_index) => {
        let model = &schema.models[*model_index];
        let select = parse_select(&model.fields, &val, schema, None)?;
      
        includes.push(MarciSelectInclude {
          field,
          model,
          select,
          select_only_id: false,
          binding: MarciSelectBinding::One(),
          injected: parse_injected(schema, field, val)?
        });
      },
      FieldType::ModelRefList(model_index) => {
        let model = &schema.models[*model_index];
        let select = parse_select(&model.fields, &val, schema, None)?;
        let tree_name = field.select_index.as_ref().expect("Index not found").as_bytes();
        includes.push(MarciSelectInclude {
          field,
          model,
          select,
          select_only_id: false,
          binding: MarciSelectBinding::Many(tree_name),
          injected: parse_injected(schema, field, val)?
        });
      },
      FieldType::Struct(st) => {
        let select = parse_select(&st.fields, &val, schema, None)?;
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
        let select = parse_select(&st.fields, &val, schema, None)?;
        includes.push(MarciSelectInclude {
          field,
          model: st,
          select,
          select_only_id: false,
          binding: MarciSelectBinding::ManyStruct(),
          injected: None
        });
      },
      FieldType::Enum(en) => {
        changed_mask.set(field_index, true);
        for (variant_index, variant) in en.variants.iter().enumerate() {
          if variant.fields.is_empty() { continue; }

          let select = parse_select(&variant.fields, json, schema, aliases)?;
          if !select.includes.is_empty() || select.mask.any() {
            enum_selects
              .entry(field_index)
              .or_default()
              .insert(variant_index as u16, select);
          }
        }
      }
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
      .any(|(idx, field)| include.select.mask[idx] && field.id_idx.is_none());

    if include.select_only_id {
      // println!("Optimize field (set only id): {:?}", include.field.name);
    }
  }

  return Ok(MarciSelect { mask: changed_mask, includes: includes, enum_selects, aliases })
}

fn parse_injected<'a>(schema: &'a Schema, field: &'a Field, val: &Value) -> Result<Option<Injected<'a>>, MarciSelectError> {
  if !val.is_object() || val.is_null() { return Ok(None) }

  if let Some((st_ref, aliases)) = &field.injected_fields {
    let field = schema.get_field(&st_ref);
    let st = match &field.ty {
      FieldType::Struct(st) | FieldType::StructList(st) => st,
      _ => panic!("Trying to inject field from non-struct")
    };

    let select = parse_select(&st.fields, val, schema, Some(aliases))?;

    return Ok(Some(Injected { st, select }));
  }
  return Ok(None);
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
    let parsed = parse_select(&model.fields, &input, &schema, None).unwrap();
    let mut expect = bitvec::bitvec![0; model.fields.len()];
    expect.set(1, true);
    assert_eq!(parsed.mask, expect);
    assert_eq!(parsed.includes.len(), 0);
    
    let input = json!({
        "name": true,
        "info": true
    });
    let parsed = parse_select(&model.fields, &input, &schema, None).unwrap();
    let mut expect = bitvec::bitvec![0; model.fields.len()];
    expect.set(1, true);
    assert_eq!(parsed.mask, expect);

    assert_eq!(parsed.includes.len(), 1);

    let mut expect = bitvec::bitvec![1; parsed.includes[0].model.fields.len()];
    expect.set(0, true);
    assert_eq!(parsed.includes[0].select.mask, expect);
    
  }

  #[test]
  fn test_parse_select_with_injects() {

    let schema_str = "
model User {
  name        String
  surname     String
  projects    Project[]     @derived(Project.users.user) @inject(Project.users.role as role2)
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
          "role2": true,
          "name": true
        }
    });
    let model = &schema.models[0];
    let parsed = parse_select(&model.fields, &input, &schema, None).unwrap();
    assert_eq!(parsed.includes.len(), 1);

    assert!(parsed.includes[0].injected.is_some());

    let injected = parsed.includes[0].injected.as_ref().unwrap();
    assert_eq!(injected.st.name, "Project.users");

    let mut bitvec = bitvec!(0; injected.st.fields.len());
    bitvec.set(2, true);
    assert_eq!(injected.select.mask, bitvec);

  }
}