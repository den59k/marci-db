mod url_parsers;
use std::collections::HashMap;

use crate::{Field, schema::{Entity, FieldLocation, FieldType, PrimitiveFieldType, Schema}, url_parser::url_parsers::encode_primitive_value};

/// Parses a row's id from the `{id}` segment of a url (`/{db}/{model}/update/{id}`).
///
/// Two forms, as the clients write them (`encodeId`): the bare value of a single-key model (`5`, `latte`,
/// `36116d39-8376-4430-bb2e-d725923ab645`) and `field=value&field.sub=value` for a composite key. Values are
/// percent-encoded by the client (a string key may hold `/`, `=`, a space, any non-ASCII letter), so the form
/// is told apart on the raw text — an encoded `=` is `%3D` — and each value is decoded after the split.
pub fn parse_id_from_url(schema: &Schema, entity: &Entity, url: &str) -> Result<Vec<u8>,UrlParseError> {
  if !url.contains('=') {
    return parse_bare_id(schema, entity, &percent_decode(url)?);
  }

  let mut map = HashMap::new();
  for part in url.split('&') {
    let Some((key,value)) = part.split_once('=') else {
      return Err(UrlParseError::WrongSyntax(part.to_string()))
    };
    map.insert(percent_decode(key)?, percent_decode(value)?);
  }

  let mut resp = vec![];
  encode_entity_id(&mut resp, &map, entity, schema, None)?;

  return Ok(resp);
}

/// The bare form: the (already decoded) value of the model's one key field
fn parse_bare_id(schema: &Schema, entity: &Entity, value: &str) -> Result<Vec<u8>,UrlParseError> {
  let field = get_one_id_field(entity)?;
  match &field.ty {
    FieldType::Ref (ref_info) => parse_bare_id(schema, &schema.models[ref_info.model_index], value),
    _ => {
      let mut dst = vec![];
      encode_key_value(&mut dst, field, value)?;
      Ok(dst)
    }
  }
}

/// Encodes one non-relation key field from its url text, in the key's binary form (the same bytes
/// `parse_id` writes from JSON)
fn encode_key_value(dst: &mut Vec<u8>, field: &Field, value: &str) -> Result<(), UrlParseError> {
  match &field.ty {
    FieldType::Primitive(primitive_type) => {
      encode_primitive_value(dst, field, primitive_type, value)?;
      if primitive_type.get_size().is_none() {
        dst.push(b'\0');
      }
      Ok(())
    },
    // `Byte[16] @format(uuid)`, `Byte[N] @format(hex)`: hex text, dashes allowed — what the decoder prints
    FieldType::PrimitiveList(PrimitiveFieldType::Byte, Some(fixed_size)) => {
      let bytes = parse_hex_strict(value)
        .filter(|bytes| bytes.len() == *fixed_size)
        .ok_or_else(|| UrlParseError::type_mismatch(field, format!("{} bytes as hex", fixed_size)))?;
      dst.extend(bytes);
      Ok(())
    },
    _ => Err(UrlParseError::UnavailableKeyFieldId(field.full_name.clone()))
  }
}

/// Hex text → bytes; `-` separators are skipped (a UUID), anything else that is not a hex digit is refused
fn parse_hex_strict(input: &str) -> Option<Vec<u8>> {
  let mut digits = Vec::with_capacity(input.len());
  for c in input.chars() {
    if c == '-' { continue; }
    digits.push(c.to_digit(16)? as u8);
  }
  if digits.len() % 2 != 0 {
    return None;
  }
  Some(digits.chunks(2).map(|pair| (pair[0] << 4) | pair[1]).collect())
}

/// Decodes `%XX` escapes. A `%` that does not start an escape, or bytes that are not UTF-8, are refused
fn percent_decode(input: &str) -> Result<String, UrlParseError> {
  if !input.contains('%') {
    return Ok(input.to_string());
  }
  let bytes = input.as_bytes();
  let mut out = Vec::with_capacity(bytes.len());
  let mut i = 0;
  while i < bytes.len() {
    if bytes[i] == b'%' {
      let byte = bytes.get(i + 1..i + 3)
        .filter(|h| h.iter().all(|c| c.is_ascii_hexdigit()))
        .and_then(|h| std::str::from_utf8(h).ok())
        .and_then(|h| u8::from_str_radix(h, 16).ok())
        .ok_or_else(|| UrlParseError::WrongSyntax(input.to_string()))?;
      out.push(byte);
      i += 3;
    } else {
      out.push(bytes[i]);
      i += 1;
    }
  }
  String::from_utf8(out).map_err(|_| UrlParseError::WrongSyntax(input.to_string()))
}

fn encode_entity_id(dst: &mut Vec<u8>, map: &HashMap<String,String>, entity: &Entity, schema: &Schema, prefix_key: Option<&str>) -> Result<(), UrlParseError> {
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
      FieldType::Ref (ref_info) => {
        encode_entity_id(dst, map, &schema.models[ref_info.model_index], schema, Some(&key))?;
      },
      _ => {
        let Some(value) = map.get(key.as_str()) else {
          return Err(UrlParseError::MissingIdField(key.to_string()))
        };
        encode_key_value(dst, field, value)?;
      }
    }
  }

  Ok(())
}

#[derive(Debug)]
pub enum UrlParseError {
  FullIdExpected,
  WrongSyntax(String),
  MissingIdField(String),
  UnavailableKeyFieldId(String),
  TypeMismatch { field: String, expected: String }
}


impl UrlParseError {
  pub fn type_mismatch(field: &Field, expected: impl Into<String>) -> Self {
    UrlParseError::TypeMismatch {
      field: field.full_name.clone(),
      expected: expected.into(),
    }
  }
}

impl std::fmt::Display for UrlParseError {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      UrlParseError::FullIdExpected => write!(f, "the key has several fields — name each one (field=value&field=value)"),
      UrlParseError::WrongSyntax(part) => write!(f, "'{}' is not a key (expected a value, or field=value&field=value)", part),
      UrlParseError::MissingIdField(field) => write!(f, "{} is required (a row is named by its whole id)", field),
      UrlParseError::UnavailableKeyFieldId(field) => write!(f, "{}: a key of this type cannot be written in a url", field),
      UrlParseError::TypeMismatch { field, expected } => write!(f, "{}: expected {}", field, expected),
    }
  }
}

impl std::error::Error for UrlParseError {}


/// The one key field of the model (wherever it is declared); a composite key has no bare form
fn get_one_id_field(entity: &Entity) -> Result<&Field,UrlParseError> {
  let mut keys = entity.fields.iter().filter(|f| matches!(f.location, FieldLocation::Key { .. }));
  match (keys.next(), keys.next()) {
    (Some(field), None) => Ok(field),
    _ => Err(UrlParseError::FullIdExpected),
  }
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
      parse_id_from_url(&schema, chat_user_model, "chat.uuid=36116d39-8376-4430-bb2e-d725923ab645&user.id=5", ).unwrap(),
      parse_id(&schema, chat_user_model, &json!({
        "chat": { "uuid": "36116d39-8376-4430-bb2e-d725923ab645" },
        "user": { "id": 5 }
      })).unwrap()
    );
  }

  #[test]
  fn a_uuid_key_is_read_from_the_url() {
    let schema = parse_schema("
        model Chat {
            id          Byte[16]    @id @format(uuid)
            title       String
        }

        model Message {
            chat        Chat        @id
            n           UInt        @id
        }
    ");
    let chat = &schema.models[0];
    let message = &schema.models[1];
    let uuid = "36116d39-8376-4430-bb2e-d725923ab645";
    let from_json = parse_id(&schema, chat, &json!({ "id": uuid })).unwrap();
    assert_eq!(from_json.len(), 16);

    assert_eq!(parse_id_from_url(&schema, chat, uuid).unwrap(), from_json);
    assert_eq!(parse_id_from_url(&schema, chat, &uuid.to_uppercase()).unwrap(), from_json);
    assert_eq!(parse_id_from_url(&schema, chat, &format!("id={}", uuid)).unwrap(), from_json);
    assert_eq!(
      parse_id_from_url(&schema, message, &format!("chat.id={}&n=7", uuid)).unwrap(),
      parse_id(&schema, message, &json!({ "chat": { "id": uuid }, "n": 7 })).unwrap()
    );

    // not hex, too short, too long — refused, never a silently different key
    for bad in ["not-a-uuid", "36116d39-8376-4430-bb2e-d725923ab6", "36116d39-8376-4430-bb2e-d725923ab64500", "z6116d39-8376-4430-bb2e-d725923ab645", ""] {
      assert!(parse_id_from_url(&schema, chat, bad).is_err(), "{:?}", bad);
    }
    // the JSON form is as strict: stray characters are not dropped (they named the same row before)
    for bad in ["36116d39-8376-4430-bb2e-d725923ab645zz", "z36116d39-8376-4430-bb2e-d725923ab645", "not-a-uuid"] {
      assert!(parse_id(&schema, chat, &json!({ "id": bad })).is_err(), "{:?}", bad);
    }
  }

  #[test]
  fn a_value_is_percent_decoded() {
    let schema = parse_schema("
        model Tag {
            name        String      @id
        }

        model Note {
            tag         Tag         @id
            title       String      @id
        }
    ");
    let tag = &schema.models[0];
    let note = &schema.models[1];
    let id = |name: &str| parse_id(&schema, tag, &json!({ "name": name })).unwrap();

    // as encodeURIComponent writes them: a space, `/`, `=` and `&`, non-ASCII, `%` itself
    assert_eq!(parse_id_from_url(&schema, tag, "a%20b").unwrap(), id("a b"));
    assert_eq!(parse_id_from_url(&schema, tag, "a%2Fb").unwrap(), id("a/b"));
    assert_eq!(parse_id_from_url(&schema, tag, "a%3Db%26c").unwrap(), id("a=b&c"));
    assert_eq!(parse_id_from_url(&schema, tag, "%D0%BA%D0%BE%D1%82").unwrap(), id("кот"));
    assert_eq!(parse_id_from_url(&schema, tag, "50%25").unwrap(), id("50%"));
    assert_eq!(
      parse_id_from_url(&schema, note, "tag.name=a%3Db&title=x%20%26%20y").unwrap(),
      parse_id(&schema, note, &json!({ "tag": { "name": "a=b" }, "title": "x & y" })).unwrap()
    );

    assert!(parse_id_from_url(&schema, tag, "50%").is_err());
    assert!(parse_id_from_url(&schema, tag, "%zz").is_err());
    assert!(parse_id_from_url(&schema, tag, "%ff").is_err());
  }

  #[test]
  fn the_key_field_need_not_be_declared_first() {
    let schema = parse_schema("
        model Chat {
            title       String
            slug        String      @id
        }
    ");
    let chat = &schema.models[0];
    assert_eq!(
      parse_id_from_url(&schema, chat, "general").unwrap(),
      parse_id(&schema, chat, &json!({ "slug": "general" })).unwrap()
    );
  }
}
