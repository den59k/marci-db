use serde_json::{Map, Value};
use bitvec::prelude::*;

use crate::{Field, query_op::{PrefixKey, QueryInclude, QueryOp, QueryType}, schema::{Entity, FieldType, Schema}};

pub fn parse_query<'a>(schema: &'a Schema, entity: &'a Entity, json_val: &Value) -> Result<QueryOp<'a>, ParseError> {
  let Some(obj) = json_val.as_object() else {
    return Err(ParseError::NotAnObject)
  };
  return parse_query_internal(schema, entity, obj, None);
}

pub fn parse_query_internal<'a>(schema: &'a Schema, entity: &'a Entity, json_val: &Map<String,Value>, parent: Option<&Entity>) -> Result<QueryOp<'a>, ParseError> {
  
  let mut mask = bitvec![0; entity.fields.len()];
  let mut includes = vec![];

  let mut prefix_key: Option<PrefixKey> = None;
  
  for (field_index, field) in entity.fields.iter().enumerate() {
    
    if let Some(parent) = parent && field_index == 0 && schema.is_parent_key(field, parent) {
      prefix_key = Some(PrefixKey::ParentId);
      continue;
    }

    let Some(val) = json_val.get(&field.name) else {
      continue;
    };
    if matches!(val, Value::Bool(false)) {
      continue;
    }
    match &field.ty {
      FieldType::Ref { model_index, .. } => {
        if matches!(val, Value::Bool(true)) {
          // TODO: add full struct here
          continue;
        }
        let Some(obj) = val.as_object() else {
          return Err(ParseError::type_mismatch(field, "object"))
        };
        
        let op = parse_query_internal(schema, &schema.models[*model_index], obj, Some(entity))?;
        includes.push(QueryInclude { query_type: QueryType::One, field, query: op });
      }
      FieldType::RefList { model_index, .. } => {
        if matches!(val, Value::Bool(true)) {
          // TODO: add full struct here
          continue;
        }
        let Some(obj) = val.as_object() else {
          return Err(ParseError::type_mismatch(field, "object"))
        };
        let op = parse_query_internal(schema, &schema.models[*model_index], obj, Some(entity))?;
        includes.push(QueryInclude { query_type: QueryType::Many, field, query: op });
      }
      _ => {
        mask.set(field_index, true);
      }
    }
  }

  Ok(QueryOp { mask, entity, sort: None, filter: None, prefix_key, includes, take: None, skip: None })
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
      
      println!("{:?}", encoded.includes[0].query);
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