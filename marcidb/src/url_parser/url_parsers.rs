use chrono::{DateTime, Utc};

use crate::{Field, schema::{PrimitiveFieldType}, url_parser::UrlParseError};

pub fn encode_primitive_value(dst: &mut Vec<u8>, field: &Field, ty: &PrimitiveFieldType, value: &str) -> Result<(), UrlParseError> {
    match ty {
        PrimitiveFieldType::String => {
            dst.extend_from_slice(value.as_bytes());
        }

        PrimitiveFieldType::DateTime => {
            let epoch = parse_datetime(value, field)?;
            dst.extend_from_slice(&epoch.to_be_bytes());
        }

        PrimitiveFieldType::Int64 => {
            let n = value.parse::<i64>()
                .map_err(|_| UrlParseError::type_mismatch(field, "int64"))?;
            dst.extend_from_slice(&n.to_be_bytes());
        }

        PrimitiveFieldType::UInt64 => {
            let n = value.parse::<u64>()
                .map_err(|_| UrlParseError::type_mismatch(field, "uint64"))?;
            dst.extend_from_slice(&n.to_be_bytes());
        }

        PrimitiveFieldType::Float => {
            let n = value.parse::<f32>()
                .map_err(|_| UrlParseError::type_mismatch(field, "float"))?;
            dst.extend_from_slice(&n.to_be_bytes());
        }

        PrimitiveFieldType::Double => {
            let n = value.parse::<f64>()
                .map_err(|_| UrlParseError::type_mismatch(field, "double"))?;
            dst.extend_from_slice(&n.to_be_bytes());
        }

        PrimitiveFieldType::Bool => {
            let b = match value {
                "true" | "1" => true,
                "false" | "0" => false,
                _ => return Err(UrlParseError::type_mismatch(field, "bool (expected true/false/1/0)")),
            };
            dst.push(if b { 1 } else { 0 });
        }
    }

    Ok(())
}

pub fn parse_datetime(value: &str, field: &Field) -> Result<i64, UrlParseError> {
    // Сначала пробуем как i64 epoch
    if let Ok(n) = value.parse::<i64>() {
        return Ok(n);
    }
    // Иначе пробуем как ISO-8601
    let dt: DateTime<Utc> = value.parse().map_err(|_| {
        UrlParseError::type_mismatch(field, "ISO-8601 datetime string or int64 epoch")
    })?;
    Ok(dt.timestamp_millis())
}