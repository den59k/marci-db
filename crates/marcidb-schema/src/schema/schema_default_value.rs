use crate::{Field, schema::{FieldType, PrimitiveFieldType, SchemaError, schema_attributes::Attribute}};


#[derive(Debug,Clone,PartialEq)]
pub enum FieldDefault {
    Counter(usize),
    Now,
    Value(Vec<u8>),
    /// `Json @default(<JSON literal>)`: the literal, validated and re-serialised compact. The schema crate
    /// has no JSONB codec — the engine encodes it where a default becomes bytes (`default_bytes`).
    Json(String),
}

pub fn resolve_default_value(field: &mut Field) -> Result<(), SchemaError> {
    for item in field.attributes.iter() {
        let Attribute::Default(default_str) = item else {
            continue;
        };

        match &field.ty {
            FieldType::Enum(enum_def) => {
                let Some(val) = enum_def.variants_map.get(default_str) else {
                    return Err(SchemaError(format!("Cannot find default value {} in enum. Field: {}", default_str, field.full_name)));
                };
                field.default_value = Some(FieldDefault::Value(val.to_be_bytes().to_vec()));
            }
            FieldType::Primitive(ty) => {
                field.default_value = Some(parse_default_value(ty, default_str, field)?);
            },
            FieldType::PrimitiveList(ty, fixed_size) => {
                field.default_value = Some(parse_default_value_list(&ty, default_str, *fixed_size, field)?)
            },
            _ => return Err(SchemaError(format!("Default value for field {} is not supported", field.full_name)))
        }
    }
    Ok(())
}

fn parse_default_value(ty: &PrimitiveFieldType, value: &str, field: &Field) -> Result<FieldDefault, SchemaError> {
    if value == "autoincrement()" {
        return Ok(FieldDefault::Counter(0))
    }

    let bad = || SchemaError(format!("Failed to parse default value {}. Field: {}", value, field.full_name));

    Ok(match ty {
        PrimitiveFieldType::String => FieldDefault::Value(value.trim().trim_matches('"').as_bytes().to_vec()),
        PrimitiveFieldType::Int64 => FieldDefault::Value(value.parse::<i64>().map_err(|_| bad())?.to_be_bytes().to_vec()),
        PrimitiveFieldType::UInt64 => FieldDefault::Value(value.parse::<u64>().map_err(|_| bad())?.to_be_bytes().to_vec()),
        PrimitiveFieldType::Float => FieldDefault::Value(value.parse::<f32>().map_err(|_| bad())?.to_be_bytes().to_vec()),
        PrimitiveFieldType::Double => FieldDefault::Value(value.parse::<f64>().map_err(|_| bad())?.to_be_bytes().to_vec()),
        PrimitiveFieldType::Byte => FieldDefault::Value(value.parse::<u8>().map_err(|_| bad())?.to_be_bytes().to_vec()),
        PrimitiveFieldType::Bool => {
            match value.to_lowercase().as_str() {
                "true" => FieldDefault::Value(vec![1]),
                "false" => FieldDefault::Value(vec![0]),
                _ => return Err(bad())
            }
        },
        // `now()`, or an integer: epoch milliseconds, stored as a written DateTime is (i64 big-endian)
        PrimitiveFieldType::DateTime => {
            match value.trim() {
                "now()" => FieldDefault::Now,
                millis => FieldDefault::Value(millis.parse::<i64>().map_err(|_| bad())?.to_be_bytes().to_vec()),
            }
        },
        // Any JSON literal but `null` — a null Json value is the field's absence, not a value.
        PrimitiveFieldType::Json => {
            let json: serde_json::Value = serde_json::from_str(value.trim()).map_err(|e| SchemaError(format!(
                "Failed to parse Json default value {} ({}). Field: {}", value, e, field.full_name
            )))?;
            if json.is_null() {
                return Err(SchemaError(format!(
                    "A Json default can't be null: a null Json is no value — drop the @default (on a Json? field absence reads as null). Field: {}",
                    field.full_name
                )));
            }
            FieldDefault::Json(json.to_string())
        },
    })
}


fn parse_default_value_list(ty: &PrimitiveFieldType, value: &str, fixed_size: Option<usize>, field: &Field) -> Result<FieldDefault, SchemaError> {
    let Some(value) = value.strip_prefix('[').and_then(|f| f.strip_suffix(']')) else {
        return Err(SchemaError(format!("Failed to parse list default value {}. Field: {}", value, field.full_name)));
    };

    let is_dynamic_items = matches!(ty, PrimitiveFieldType::String);

    let mut buf = vec![];
    // `[]` is the empty list — not one empty item, which is what splitting "" would give; items are trimmed
    // so `[1, 2]` parses like `[1,2]`
    let items: Vec<&str> = if value.trim().is_empty() { vec![] } else { value.split(',').map(str::trim).collect() };
    let count = items.len();

    if let Some(fixed_size) = fixed_size && count != fixed_size {
        return Err(SchemaError(format!("Failed to parse default value: Wrong size, expected: {}. Field: {}", fixed_size, field.full_name)));
    }

    if fixed_size.is_none() {
        buf.extend_from_slice(&(count as u32).to_be_bytes());
    }
    let mut offset_index = buf.len();
    if is_dynamic_items {
        buf.resize(buf.len() + count*4 + 4, 0);
    }

    for item in items {
        let FieldDefault::Value(binary) = parse_default_value(ty, item, field)? else {
            return Err(SchemaError(format!("Failed to parse default value {}. Field: {}", item, field.full_name)));
        };
        if is_dynamic_items {
            let el_offset= buf.len() as u32;
            buf[offset_index..offset_index + 4].copy_from_slice(&el_offset.to_be_bytes());
            offset_index += 4;
        }
        buf.extend(binary);
    }

    if is_dynamic_items {
        let el_offset= buf.len() as u32;
        buf[offset_index..offset_index + 4].copy_from_slice(&el_offset.to_be_bytes());
    }

    Ok(FieldDefault::Value(buf))
}


#[cfg(test)]
mod tests {

    use crate::{Schema, evolve, parse_schema, parse_snapshot, serialize_snapshot, try_parse_schema, schema::FieldDefault};

  #[test]
  fn test_parse_default_value() {

    let schema = parse_schema("
        model Project {
            createdAt   DateTime          @default(now())
            age         UInt              @default(20)
            features    String[]          @default([ \"ar\", \"paid\" ])
        }
    ");

    let created_at_field = &schema.models[0].fields[1];
    assert_eq!(created_at_field.default_value, Some(FieldDefault::Now));

    let age_field = &schema.models[0].fields[2];
    assert_eq!(age_field.default_value, Some(FieldDefault::Value(20u64.to_be_bytes().to_vec())));

    // String[] default `["ar","paid"]` encodes as: count:u32, then (count+1) u32 offsets, then the bytes.
    // [count=2 | off0=16 | off1=18 | end=22 | "ar" "paid"]
    let features_field = &schema.models[0].fields[3];
    let expected: Vec<u8> = vec![
        0, 0, 0, 2,   0, 0, 0, 16,   0, 0, 0, 18,   0, 0, 0, 22,
        b'a', b'r', b'p', b'a', b'i', b'd',
    ];
    assert_eq!(features_field.default_value, Some(FieldDefault::Value(expected)));
  }

  #[test]
  fn an_empty_list_default_is_empty_and_items_may_carry_spaces() {
    let schema = parse_schema("
        model Post {
            tags    String[]  @default([])
            marks   Int[]     @default([1, 2])
            flags   Bool[]    @default([ ])
        }
    ");
    // String[]: count 0, then the one (end) offset — no item
    assert_eq!(schema.models[0].fields[1].default_value, Some(FieldDefault::Value(vec![0, 0, 0, 0,  0, 0, 0, 8])));
    let mut marks = 2u32.to_be_bytes().to_vec();
    marks.extend(1i64.to_be_bytes());
    marks.extend(2i64.to_be_bytes());
    assert_eq!(schema.models[0].fields[2].default_value, Some(FieldDefault::Value(marks)));
    assert_eq!(schema.models[0].fields[3].default_value, Some(FieldDefault::Value(vec![0, 0, 0, 0])));
  }

  fn default_of(schema: &Schema, model: &str, field: &str) -> Option<FieldDefault> {
    let model = schema.models.iter().find(|m| m.name == model).unwrap();
    model.fields.iter().find(|f| f.name == field).unwrap().default_value.clone()
  }

  fn parse_error(text: &str) -> String {
    match try_parse_schema(text) {
      Err(e) => e.0,
      Ok(_) => panic!("expected a schema error for: {}", text),
    }
  }

  #[test]
  fn a_datetime_default_is_now_or_epoch_millis() {
    let schema = parse_schema("
        model Event {
            at       DateTime     @default(1735689600000)
            before   DateTime     @default(-1)
            seen     DateTime     @default(now())
            marks    DateTime[]   @default([0, 1735689600000])
        }
    ");
    // the bytes a written DateTime has: i64 big-endian epoch millis
    assert_eq!(default_of(&schema, "Event", "at"), Some(FieldDefault::Value(1735689600000i64.to_be_bytes().to_vec())));
    assert_eq!(default_of(&schema, "Event", "before"), Some(FieldDefault::Value((-1i64).to_be_bytes().to_vec())));
    assert_eq!(default_of(&schema, "Event", "seen"), Some(FieldDefault::Now));
    let mut marks = 2u32.to_be_bytes().to_vec();
    marks.extend(0i64.to_be_bytes());
    marks.extend(1735689600000i64.to_be_bytes());
    assert_eq!(default_of(&schema, "Event", "marks"), Some(FieldDefault::Value(marks)));
  }

  #[test]
  fn a_non_integer_datetime_default_is_refused() {
    for bad in ["1.5", "tomorrow", "\"2025-01-01\"", "now"] {
      let err = parse_error(&format!("model Event {{\n  at DateTime @default({})\n}}", bad));
      assert_eq!(err, format!("Failed to parse default value {}. Field: Event.at", bad));
    }
  }

  #[test]
  fn a_json_default_is_any_json_literal_stored_compact() {
    let schema = parse_schema(r#"
        model Doc {
            list     Json    @default([])
            object   Json    @default({})
            nested   Json    @default({"a": 1, "b": [1, 2]})
            text     Json    @default("text")
            zero     Json    @default(0)
            flag     Json    @default(true)
            loose    Json?   @default( { "x" : [ 1.5 , -2 ] } )
        }
    "#);
    let json = |text: &str| Some(FieldDefault::Json(text.to_string()));
    assert_eq!(default_of(&schema, "Doc", "list"), json("[]"));
    assert_eq!(default_of(&schema, "Doc", "object"), json("{}"));
    assert_eq!(default_of(&schema, "Doc", "nested"), json(r#"{"a":1,"b":[1,2]}"#));
    assert_eq!(default_of(&schema, "Doc", "text"), json(r#""text""#));
    assert_eq!(default_of(&schema, "Doc", "zero"), json("0"));
    assert_eq!(default_of(&schema, "Doc", "flag"), json("true"));
    assert_eq!(default_of(&schema, "Doc", "loose"), json(r#"{"x":[1.5,-2]}"#));
  }

  /// Attributes split on `@` outside double quotes: a quoted default may hold `@`, `)`, an escaped quote and
  /// runs of spaces, and another attribute may follow it.
  #[test]
  fn attributes_split_outside_a_quoted_default() {
    let schema = parse_schema(r#"
        model Doc {
            meta    Json     @default({"to": "a@b.c", "p": ")", "q": "say \"@hi\" (", "s": "two  spaces"}) @index
            short   Json     @default({"a": 1})@index
            email   String   @default("a@b.c")   @unique
        }
    "#);
    let doc = schema.models.iter().find(|m| m.name == "Doc").unwrap();
    let field = |name: &str| doc.fields.iter().find(|f| f.name == name).unwrap();

    assert_eq!(field("meta").default_value, Some(FieldDefault::Json(r#"{"p":")","q":"say \"@hi\" (","s":"two  spaces","to":"a@b.c"}"#.to_string())));
    assert_eq!(field("meta").indexes.len(), 1);
    assert_eq!(field("short").default_value, Some(FieldDefault::Json(r#"{"a":1}"#.to_string())));
    assert_eq!(field("short").indexes.len(), 1);
    assert_eq!(field("email").default_value, Some(FieldDefault::Value(b"a@b.c".to_vec())));
    assert!(field("email").indexes[0].is_unique());
  }

  #[test]
  fn a_bad_json_default_is_refused() {
    let err = parse_error("model Doc {\n  meta Json @default({a: 1})\n}");
    assert!(err.starts_with("Failed to parse Json default value {a: 1} (") && err.ends_with("Field: Doc.meta"), "{}", err);
    let err = parse_error("model Doc {\n  meta Json @default({\"a\": 1)\n}");
    assert!(err.starts_with("Failed to parse Json default value {\"a\": 1 ("), "{}", err);
    let err = parse_error("model Doc {\n  meta Json? @default(null)\n}");
    assert!(err.starts_with("A Json default can't be null") && err.ends_with("Field: Doc.meta"), "{}", err);
    // an unclosed quote swallows the rest of the line instead of splitting it into bogus attributes
    let err = parse_error("model Doc {\n  name String @default(\"abc) @index\n}");
    assert!(err.starts_with("Unknown attribute: default(\"abc) @index"), "{}", err);
  }

  /// The snapshot keeps the attribute text and re-parses it on load; a `.march` rewrite of the line (an
  /// index or unique group added to the entity) must not split or collapse the quoted parts of it.
  #[test]
  fn json_and_datetime_defaults_survive_the_snapshot_and_march_rewrites() {
    let schema = parse_schema(r#"
        model Doc {
            name    String
            meta    Json      @default({"p": ")", "to": "a@b.c", "s": "a  @index b"})
            at      DateTime  @default(1735689600000)
        }
    "#);
    let snap = serialize_snapshot(&schema);
    let reparsed = parse_snapshot(&snap).unwrap();
    for f in ["meta", "at"] {
      assert_eq!(default_of(&reparsed, "Doc", f), default_of(&schema, "Doc", f), "{}", f);
    }
    assert_eq!(serialize_snapshot(&reparsed), snap);

    for action in ["add unique_group Doc.pair(name, at)", "drop index Doc.meta", "add index Doc.at"] {
      let evolved = parse_snapshot(&evolve(&snap, action).unwrap()).unwrap();
      assert_eq!(default_of(&evolved, "Doc", "meta"), default_of(&schema, "Doc", "meta"), "{}", action);
      assert_eq!(default_of(&evolved, "Doc", "at"), default_of(&schema, "Doc", "at"), "{}", action);
    }
  }

}