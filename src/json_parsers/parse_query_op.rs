use serde_json::{Map, Value};
use bitvec::prelude::*;

use crate::{Field, query_op::{PrefixKey, QueryInclude, QueryOp, QueryType}, schema::{Entity, FieldType, RefBinding, Schema}};

pub fn parse_query<'a>(schema: &'a Schema, entity: &'a Entity, json_val: &Value) -> Result<QueryOp<'a>, ParseError> {
  let Some(obj) = json_val.as_object() else {
    return Err(ParseError::NotAnObject)
  };
  return parse_query_internal(schema, entity, obj);
}

pub fn parse_query_internal<'a>(schema: &'a Schema, entity: &'a Entity, json_val: &Map<String,Value>) -> Result<QueryOp<'a>, ParseError> {
  
  let mut mask = bitvec![0; entity.fields.len()];
  let mut includes = vec![];
  
  for (field_index, field) in entity.fields.iter().enumerate() {
    let Some(val) = json_val.get(&field.name) else {
      continue;
    };
    if matches!(val, Value::Bool(false)) {
      continue;
    }
    match &field.ty {
      FieldType::Ref (ref_info) => {
        if matches!(val, Value::Bool(true)) {
          todo!("Add select all fields from model");
        }
        let Some(obj) = val.as_object() else {
          return Err(ParseError::type_mismatch(field, "object"))
        };
        
        let mut op = parse_query_internal(schema, &schema.models[ref_info.model_index], obj)?;
        match &ref_info.binding {
          RefBinding::CurrentId => {
            op.prefix_key = Some(PrefixKey::ParentId);
          }
          RefBinding::FieldValue => {
            op.prefix_key = Some(PrefixKey::ParentField(field));
          },
          RefBinding::IndexTree(tree_name) => {
            op.prefix_key = Some(PrefixKey::ParentIndexTree(tree_name.clone()))
          },
        }
        includes.push(QueryInclude { query_type: QueryType::One, field, query: op });
      }
      FieldType::RefList (ref_info) => {
        if matches!(val, Value::Bool(true)) {
          todo!("Add select all fields from model");
        }
        let Some(obj) = val.as_object() else {
          return Err(ParseError::type_mismatch(field, "object"))
        };
        let mut op = parse_query_internal(schema, &schema.models[ref_info.model_index], obj)?;
        match &ref_info.binding {
          RefBinding::CurrentId => {
            op.prefix_key = Some(PrefixKey::ParentId);
          }
          RefBinding::FieldValue => {
            op.prefix_key = Some(PrefixKey::ParentField(field));
          },
          RefBinding::IndexTree(tree_name) => {
            op.prefix_key = Some(PrefixKey::ParentIndexTree(tree_name.clone()))
          },
        }
        includes.push(QueryInclude { query_type: QueryType::Many, field, query: op });
      }
      _ => {
        mask.set(field_index, true);
      }
    }
  }

  Ok(QueryOp { mask, entity, sort: None, filter: None, prefix_key: None, includes, take: None, skip: None })
}

#[derive(Debug)]
pub enum ParseError {
  NotAnObject,
  TypeMismatch { field: String, expected: String },
}

impl ParseError {
    pub fn type_mismatch(field: &Field, expected: impl Into<String>) -> Self {
        ParseError::TypeMismatch {
            field: field.name.clone(),
            expected: expected.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::{parse_query, parse_schema, query_op::{PrefixKey}};

  
  #[test]
  fn test_encode_query_op() {
    let schema = parse_schema("
      model User {
          name        String
          info        UserInfo?
      }
      struct UserInfo {
          bio         String
      }
      model Project {
          name        String
          users       UserRole[]
      }
      struct UserRole {
        user        User          @id
        role        String
      }
    ");

    {
      let user_model = &schema.models[0];

      let input = json!({
          "name": true,
          "info": { "bio": true }
      });

      let encoded = parse_query(&schema, user_model, &input).unwrap();
      assert_eq!(encoded.includes.len(), 1);
      
      assert!(matches!(encoded.includes[0].query.prefix_key, Some(PrefixKey::ParentId)));
    }

    {
      let project_model = &schema.models[1];
      let input = json!({
          "name": true,
          "users": { "role": true }
      });

      let encoded = parse_query(&schema, project_model, &input).unwrap();
      assert_eq!(encoded.includes.len(), 1);
    }
  }

}