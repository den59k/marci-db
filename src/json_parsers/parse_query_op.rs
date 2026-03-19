use serde_json::{Map, Value};
use bitvec::prelude::*;

use crate::{Field, json_parsers::parse_where::{ParseWhereError, parse_where}, query_op::{PrefixKey, QueryInclude, QueryOp, QueryType}, schema::{Entity, FieldExistsCondition, FieldType, RefBinding, Schema}};

pub fn parse_query<'a>(schema: &'a Schema, entity: &'a Entity, json_val: &Value) -> Result<QueryOp<'a>, ParseError> {
  let Some(obj) = json_val.as_object() else {
    return Err(ParseError::NotAnObject)
  };
  return parse_query_internal(schema, entity, obj);
}

pub fn parse_query_internal<'a>(schema: &'a Schema, entity: &'a Entity, json_val: &Map<String,Value>) -> Result<QueryOp<'a>, ParseError> {
  
  let mut mask = bitvec![0; entity.fields.len()];
  let mut includes = vec![];
  
  let mut filter = None;
  if let Some(where_value) = json_val.get("$where") {
    let filter_resp = parse_where(schema, entity, where_value)
      .map_err(|err| ParseError::WhereError(err))?;
    filter = Some(filter_resp);
  }
  
  for (field_index, field) in entity.fields.iter().enumerate() {
    let Some(val) = json_val.get(&field.name) else {
      continue;
    };
    if matches!(val, Value::Bool(false)) {
      continue;
    }
    if let FieldExistsCondition::EnumValue { field_index, .. }  = &field.condition && !mask[*field_index] {
      continue;
    }
    match &field.ty {
      FieldType::Ref (ref_info) => {
        if matches!(val, Value::Bool(true)) {
          return Ok(QueryOp::all(&entity))
        }
        let Some(obj) = val.as_object() else {
          return Err(ParseError::type_mismatch(field, "object"))
        };
        
        let mut op = parse_query_internal(schema, &schema.models[ref_info.model_index], obj)?;
        op.prefix_key = Some(get_prefix_key(&ref_info.binding, field));
        includes.push(QueryInclude { query_type: QueryType::One, field, query: op });
      }
      FieldType::RefList (ref_info) => {
        if matches!(val, Value::Bool(true)) {
          return Ok(QueryOp::all(&entity))
        }
        let Some(obj) = val.as_object() else {
          return Err(ParseError::type_mismatch(field, "object"))
        };
        let mut op = parse_query_internal(schema, &schema.models[ref_info.model_index], obj)?;
        op.prefix_key = Some(get_prefix_key(&ref_info.binding, field));
        includes.push(QueryInclude { query_type: QueryType::Many, field, query: op });
      }
      _ => {
        mask.set(field_index, true);
      }
    }
  }

  Ok(QueryOp { mask, entity, sort: None, filter, prefix_key: None, includes, take: None, skip: None })
}

pub fn get_prefix_key<'a>(binding: &'a RefBinding, field: &'a Field) -> PrefixKey<'a> {
  match &binding {
    RefBinding::CurrentId => {
      return PrefixKey::ParentId;
    }
    RefBinding::FieldValue => {
      return PrefixKey::ParentField(field);
    },
    RefBinding::IndexTree(tree_name) => {
      return PrefixKey::ParentIndexTree(tree_name.clone())
    },
  }
}

#[derive(Debug)]
pub enum ParseError {
  NotAnObject,
  WhereError(ParseWhereError),
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