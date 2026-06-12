use std::{collections::HashMap};

use crate::{Field, schema::{schema_parse::parse_fields}};

/// Поле enum вместе со списком вариантов, которым оно принадлежит.
/// Общее поле (`admin | creator { ... }`) — одно физическое поле с несколькими вариантами
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

// Каждая строка enum — это `имя1 | имя2 [{ поля }]`.
// Вариант объявляется при первом упоминании, индекс — порядок первого упоминания.
// Блок присоединяет свои поля ко всем перечисленным вариантам
pub fn parse_enum_block(
    lines: &mut std::iter::Peekable<std::str::Lines<'_>>,
) -> EnumDef {
    let mut fields: Vec<EnumFieldDef> = Vec::new();
    let mut variants_map = HashMap::new();

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

        // теперь точно строка с вариантами enum'а
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
                panic!("Empty variant name in enum line: \"{}\"", line);
            }
            let next_index = variants_map.len() as u16;
            let variant = *variants_map.entry(name.to_string()).or_insert(next_index);
            if line_variants.contains(&variant) {
                panic!("Duplicate variant {} in enum line: \"{}\"", name, line);
            }
            line_variants.push(variant);
        }

        if has_block {
            // Offsets in enum has pre_header_size 4 bytes: 2 bytes - enum variant, 2 bytes - payload_offset
            for field in parse_fields(lines) {
                if fields.iter().any(|f| f.field.name == field.name) {
                    panic!("Duplicate enum field {}. To share a field between variants use `a | b {{ ... }}`", field.name);
                }
                fields.push(EnumFieldDef { variants: line_variants.clone(), field });
            }
        }
    }

    EnumDef {
        fields,
        variants_map,
    }
}


#[cfg(test)]
mod tests {
    use crate::schema::schema_enum::parse_enum_block;

    fn parse(input: &str) -> super::EnumDef {
        let mut lines = input.lines().peekable();
        lines.next();
        lines.next();
        parse_enum_block(&mut lines)
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

        // Union-блок не объявляет новые варианты и не меняет их порядок
        assert_eq!(en.variants_map.len(), 2);
        assert_eq!(en.variants_map.get("creator"), Some(&0));
        assert_eq!(en.variants_map.get("admin"), Some(&1));

        // sign — одно поле, принадлежащее обоим вариантам; level доопределен вторым блоком admin
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

        // Компактная форма: union-строка сама объявляет варианты в порядке упоминания
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
