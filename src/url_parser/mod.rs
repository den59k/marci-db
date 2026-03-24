mod url_parsers;
use std::collections::HashMap;

use crate::{Field, schema::{Entity, FieldLocation, FieldType, Schema}, url_parser::url_parsers::encode_primitive_value};

/// Парсит ID из url строки
pub fn parse_id_from_url(schema: &Schema, entity: &Entity, url: &str) -> Result<Vec<u8>,UrlParseError> {
  if !url.contains('=') {
    let field = get_one_id_field(entity)?;
    return match &field.ty {
      FieldType::Primitive(primitive_type) => {
        let mut dst = vec![];
        encode_primitive_value(&mut dst, field, &primitive_type, url)?;
        Ok(dst)
      },
      FieldType::Ref (ref_info) => {
        parse_id_from_url(schema, &schema.models[ref_info.model_index], url)
      },
      _ => Err(UrlParseError::UnavailableKeyFieldId(field.full_name.clone()))
    }
  }

  let mut map = HashMap::new();
  for part in url.split('&') {
    let Some((key,value)) = part.split_once('=') else {
      return Err(UrlParseError::WrongSyntax(part.to_string()))
    };
    map.insert(key, value);
  }

  let mut resp = vec![];
  encode_entity_id(&mut resp, &map, entity, schema, None)?;

  return Ok(resp);
}

fn encode_entity_id(dst: &mut Vec<u8>, map: &HashMap<&str,&str>, entity: &Entity, schema: &Schema, prefix_key: Option<&str>) -> Result<(), UrlParseError> {
  for field in entity.fields.iter() {
    if !matches!(field.location, FieldLocation::Key { .. }) {
      continue;
    }

    let key = if let Some(prefix_key) = prefix_key {
      [ prefix_key, ".", field.name.as_str() ].concat()
    } else {
      field.name.clone()
    };

    match &field.ty {
      FieldType::Primitive(primitive_type) => {
        let Some(value) = map.get(key.as_str()) else {
          return Err(UrlParseError::MissingIdField(key.to_string()))
        };
        encode_primitive_value(dst, field, primitive_type, value)?;
      },
      FieldType::Ref (ref_info) => {
        encode_entity_id(dst, map, &schema.models[ref_info.model_index], schema, Some(&key))?;
      },
      _ => {
        return Err(UrlParseError::UnavailableKeyFieldId(field.full_name.clone()))
      }
    }
  }

  Ok(())
}

#[derive(Debug)]
pub enum UrlParseError {
  FullIdExpected,
  WrongSyntax(String),
  WrongFirstFieldLocation,
  MissingIdField(String),
  UnavailableKeyFieldId(String),
  TypeMismatch { field: String, expected: String }
}


impl UrlParseError {
  pub fn type_mismatch(field: &Field, expected: impl Into<String>) -> Self {
    UrlParseError::TypeMismatch {
      field: field.name.clone(),
      expected: expected.into(),
    }
  }
}


fn get_one_id_field(entity: &Entity) -> Result<&Field,UrlParseError> {
  let count = entity.fields
    .iter()
    .filter(|f| matches!(f.location, FieldLocation::Key { .. }))
    .count();

  if count != 1 {
    return Err(UrlParseError::FullIdExpected)
  }

  let field = entity.fields.first().unwrap();
  if !matches!(field.location, FieldLocation::Key { .. }) {
    return Err(UrlParseError::WrongFirstFieldLocation);
  }

  return Ok(field)
}

#[cfg(test)]
mod tests {
  use serde_json::json;

use crate::{parse_id, parse_id_from_url, parse_schema};

  #[test]
  fn test_parse_url() {
    let schema = parse_schema("
        model User {
            name        String
            age         UInt
        }

        model Chat {
            uuid        String      @id
        }

        model ChatUser {
            chat        Chat        @id
            user        User        @id
        }
    ");  
    let user_model = &schema.models[0];
    let chat_model = &schema.models[1];
    let chat_user_model = &schema.models[2];
    
    assert_eq!(
      parse_id_from_url(&schema, user_model, "5", ).unwrap(),
      parse_id(&schema, user_model, &json!({ "id": 5 })).unwrap()
    );

    assert_eq!(
      parse_id_from_url(&schema, user_model, "id=5", ).unwrap(),
      parse_id(&schema, user_model, &json!({ "id": 5 })).unwrap()
    );

    assert_eq!(
      parse_id_from_url(&schema, chat_model, "36116d39-8376-4430-bb2e-d725923ab645", ).unwrap(),
      parse_id(&schema, chat_model, &json!({ "uuid": "36116d39-8376-4430-bb2e-d725923ab645" })).unwrap()
    );

    assert_eq!(
      parse_id_from_url(&schema, chat_model, "uuid=36116d39-8376-4430-bb2e-d725923ab645", ).unwrap(),
      parse_id(&schema, chat_model, &json!({ "uuid": "36116d39-8376-4430-bb2e-d725923ab645" })).unwrap()
    );

    assert_eq!(
      parse_id_from_url(&schema, chat_model, "36116d39-8376-4430-bb2e-d725923ab645", ).unwrap(),
      parse_id(&schema, chat_model, &json!({ "uuid": "36116d39-8376-4430-bb2e-d725923ab645" })).unwrap()
    );

    assert_eq!(
      parse_id_from_url(&schema, chat_user_model, "chat.uuid=36116d39-8376-4430-bb2e-d725923ab645&user.id=5", ).unwrap(),
      parse_id(&schema, chat_user_model, &json!({ 
        "chat": { "uuid": "36116d39-8376-4430-bb2e-d725923ab645" },
        "user": { "id": 5 }
      })).unwrap()
    );
  }
}