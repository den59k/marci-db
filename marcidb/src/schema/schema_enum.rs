use std::collections::HashMap;

use crate::schema::{Entity, schema_parse::parse_fields};


#[derive(Debug,Clone)]
pub struct EnumDef {
    pub name: String,
    pub variants: Vec<Entity>,
    pub variants_map: HashMap<String, u16>
}

impl EnumDef {
    pub fn variants_str(&self) -> String {
        let vec: Vec<&str> = self.variants.iter().map(|f| f.name.as_str()).collect();
        return vec.join(", ");
    }
}


pub fn parse_enum_block(
    name: String,
    lines: &mut std::iter::Peekable<std::str::Lines<'_>>,
) -> EnumDef {
    let mut variants = Vec::new();

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
            let (fields, payload_offset) = parse_fields(lines, 4); 
            variants.push(Entity {
                name: variant_name,
                fields,
                payload_offset
            });
        } else {
            let variant_name = line.to_string();
            variants.push(Entity {
                name: variant_name,
                fields: Vec::new(),
                payload_offset: 0
            });
        }
    }

    let variants_map: HashMap<String, u16> = variants
      .iter()
      .enumerate()
      .map(|(idx, val)| (val.name.clone(), idx as u16))
      .collect();

    EnumDef {
        name,
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

        let en = parse_enum_block("Role".to_string(), &mut lines);

        assert_eq!(en.variants.len(), 2);
        assert_eq!(en.variants_map.len(), 2);
        assert_eq!(en.variants[0].fields.len(), 0);
        assert_eq!(en.variants[1].fields.len(), 1);
    }

}