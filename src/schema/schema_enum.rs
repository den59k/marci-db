use std::{collections::HashMap};

use crate::{Field, schema::{schema_parse::parse_fields}};

#[derive(Debug,Clone)]
pub struct EnumDef {
    pub variants: HashMap<String, Vec<Field>>,
    pub variants_map: HashMap<String, u16>
}

pub fn parse_enum_block(
    lines: &mut std::iter::Peekable<std::str::Lines<'_>>,
) -> EnumDef {
    let mut variants = HashMap::new();
    let mut variants_map = HashMap::new();

    let mut index = 0;
    while let Some(raw) = lines.peek() {
        let line = raw.trim();

        // пропускаем пустые строки
        if line.is_empty() {
            lines.next();
            continue;
        }

        if line.starts_with('}') {
            lines.next();
            break;
        }

        // теперь точно вариант enum'а
        let raw_line = lines.next().unwrap();
        let line = raw_line.trim();

        if let Some(brace_pos) = line.find('{') {
            let name_part = &line[..brace_pos];
            let variant_name = name_part.trim().to_string();
            // Offsets in enum has pre_header_size 4 bytes: 2 bytes - enum variant, 2 bytes - payload_offset
            let fields = parse_fields(lines); 
            variants.insert(variant_name.clone(), fields);
            variants_map.insert(variant_name.clone(), index);
        } else {
            let variant_name = line.to_string();
            variants.insert(variant_name.clone(), Vec::new());
            variants_map.insert(variant_name.clone(), index);
        }
        index += 1;
    }

    EnumDef {
        variants,
        variants_map,
    }
}


#[cfg(test)]
mod tests {
    use crate::schema::schema_enum::parse_enum_block;


    #[test]
    fn test_parse_enum() {
        let mut lines = "
        enum Role {
            creator
            admin {
                admin_features String[]
            }
        }
        ".lines().peekable();

        lines.next();
        lines.next();

        let en = parse_enum_block(&mut lines);

        assert_eq!(en.variants.len(), 2);
        assert_eq!(en.variants_map.len(), 2);
        assert_eq!(en.variants.get("creator").unwrap().len(), 0);
        assert_eq!(en.variants.get("admin").unwrap().len(), 1);
    }

}