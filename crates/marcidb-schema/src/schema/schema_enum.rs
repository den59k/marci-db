use std::{collections::HashMap};

use crate::{Field, schema::{SchemaError, schema_parse::parse_fields}};

/// An enum field together with the list of variants it belongs to.
/// A shared field (`admin | creator { ... }`) is one physical field with several variants
#[derive(Debug,Clone)]
pub struct EnumFieldDef {
    pub variants: Vec<u16>,
    pub field: Field
}

#[derive(Debug,Clone)]
pub struct EnumDef {
    pub fields: Vec<EnumFieldDef>,
    pub variants_map: HashMap<String, u16>
}

// Each enum line is `name1 | name2 [{ fields }]`.
// A variant is declared on first mention, the index is the order of first mention.
// The block attaches its fields to all the listed variants
pub fn parse_enum_block(
    lines: &mut std::iter::Peekable<std::str::Lines<'_>>,
) -> Result<EnumDef, SchemaError> {
    let mut fields: Vec<EnumFieldDef> = Vec::new();
    let mut variants_map = HashMap::new();

    while let Some(raw) = lines.peek() {
        let line = raw.trim();

        // skip empty lines
        if line.is_empty() {
            lines.next();
            continue;
        }

        if line.starts_with('}') {
            lines.next();
            break;
        }

        // now this is definitely a line with enum variants
        let raw_line = lines.next().unwrap();
        let line = raw_line.trim();

        let (names_part, has_block) = match line.find('{') {
            Some(brace_pos) => (&line[..brace_pos], true),
            None => (line, false)
        };

        let mut line_variants: Vec<u16> = Vec::new();
        for name in names_part.split('|') {
            let name = name.trim();
            if name.is_empty() {
                return Err(SchemaError(format!("Empty variant name in enum line: \"{}\"", line)));
            }
            let next_index = variants_map.len() as u16;
            let variant = *variants_map.entry(name.to_string()).or_insert(next_index);
            if line_variants.contains(&variant) {
                return Err(SchemaError(format!("Duplicate variant {} in enum line: \"{}\"", name, line)));
            }
            line_variants.push(variant);
        }

        if has_block {
            // Offsets in enum has pre_header_size 4 bytes: 2 bytes - enum variant, 2 bytes - payload_offset
            for field in parse_fields(lines)? {
                if fields.iter().any(|f| f.field.name == field.name) {
                    return Err(SchemaError(format!("Duplicate enum field {}. To share a field between variants use `a | b {{ ... }}`", field.name)));
                }
                fields.push(EnumFieldDef { variants: line_variants.clone(), field });
            }
        }
    }

    Ok(EnumDef {
        fields,
        variants_map,
    })
}


#[cfg(test)]
mod tests {
    use crate::schema::schema_enum::parse_enum_block;

    fn parse(input: &str) -> super::EnumDef {
        let mut lines = input.lines().peekable();
        lines.next();
        lines.next();
        parse_enum_block(&mut lines).unwrap()
    }

    #[test]
    fn test_parse_enum() {
        let en = parse("
        enum Role {
            creator
            admin {
                admin_features String[]
            }
        }
        ");

        assert_eq!(en.variants_map.len(), 2);
        assert_eq!(en.variants_map.get("creator"), Some(&0));
        assert_eq!(en.variants_map.get("admin"), Some(&1));

        assert_eq!(en.fields.len(), 1);
        assert_eq!(en.fields[0].field.name, "admin_features");
        assert_eq!(en.fields[0].variants, vec![1]);
    }

    #[test]
    fn test_parse_enum_union() {
        let en = parse("
        enum Role {
            creator
            admin
            admin | creator {
                sign      String
            }
            admin {
                level     Int
            }
        }
        ");

        // A union block does not declare new variants and does not change their order
        assert_eq!(en.variants_map.len(), 2);
        assert_eq!(en.variants_map.get("creator"), Some(&0));
        assert_eq!(en.variants_map.get("admin"), Some(&1));

        // sign is one field belonging to both variants; level is defined further by the second admin block
        assert_eq!(en.fields.len(), 2);
        assert_eq!(en.fields[0].field.name, "sign");
        assert_eq!(en.fields[0].variants, vec![1, 0]);
        assert_eq!(en.fields[1].field.name, "level");
        assert_eq!(en.fields[1].variants, vec![1]);
    }

    #[test]
    fn test_parse_enum_union_declares_variants() {
        let en = parse("
        enum Role {
            viewer
            creator | admin {
                sign      String
            }
        }
        ");

        // Compact form: the union line itself declares the variants in order of mention
        assert_eq!(en.variants_map.len(), 3);
        assert_eq!(en.variants_map.get("viewer"), Some(&0));
        assert_eq!(en.variants_map.get("creator"), Some(&1));
        assert_eq!(en.variants_map.get("admin"), Some(&2));

        assert_eq!(en.fields.len(), 1);
        assert_eq!(en.fields[0].variants, vec![1, 2]);
    }

    #[test]
    #[should_panic(expected = "Duplicate enum field sign")]
    fn test_parse_enum_duplicate_field() {
        parse("
        enum Role {
            creator {
                sign      String
            }
            admin {
                sign      String
            }
        }
        ");
    }
}
