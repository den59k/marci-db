use crate::{Field, schema::{FieldType, PrimitiveFieldType, SchemaError, schema_attributes::Attribute}};


#[derive(Debug,Clone,PartialEq)]
pub enum FieldDefault {
    Counter(usize),
    Now,
    Value(Vec<u8>)
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
        PrimitiveFieldType::DateTime => {
            match value {
                "now()" => FieldDefault::Now,
                _ => return Err(bad())
            }
        },
    })
}


fn parse_default_value_list(ty: &PrimitiveFieldType, value: &str, fixed_size: Option<usize>, field: &Field) -> Result<FieldDefault, SchemaError> {
    let Some(value) = value.strip_prefix('[').and_then(|f| f.strip_suffix(']')) else {
        return Err(SchemaError(format!("Failed to parse list default value {}. Field: {}", value, field.full_name)));
    };

    let is_dynamic_items = matches!(ty, PrimitiveFieldType::String);

    let mut buf = vec![];
    let splited = value.split(',');

    let count = splited.clone().count();

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

    for item in splited {
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

    use serde_json::json;

    use crate::{json_parsers::encode_list, parse_schema, schema::{FieldDefault, PrimitiveFieldType}};

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

    let features_field = &schema.models[0].fields[3];

    let mut json_buf = vec![];
    encode_list(&mut json_buf, &json!([ "ar", "paid" ]), features_field, &PrimitiveFieldType::String, None).unwrap();
    
    let Some(FieldDefault::Value(buf)) = &features_field.default_value else {
        panic!("Wrong default_value: {:?}", features_field.default_value)
    };

    assert_eq!(buf, &json_buf);
  }

}