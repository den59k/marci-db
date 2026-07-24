
use crate::schema::SchemaError;

#[derive(Debug,Clone)]
pub enum Attribute {
    Index,
    BindUnresolved (String),
    Id,
    Default(String),
    Unique,
    /// A module-provided index, e.g. `@custom(vector, cosine)` or `@custom(fulltext, english)`.
    /// `name` selects the index provider; `args` is the raw remainder, parsed by the provider itself
    /// (the schema layer stays provider-agnostic). Materialized into [`super::FieldIndex::Custom`].
    Custom { name: String, args: String },
    /// `@list` on a relation list: the related ids are stored inline in the row body as an ordered
    /// array (insertion order is preserved) instead of a virtual index-tree relation.
    /// Materialized into [`super::RefBinding::IdList`].
    List,
    InjectUnresolved(Vec<(String,String)>),
    OnDelete(DeleteConstraint),
    Format(FieldCustomFormat)
}

#[derive(Debug,Clone)]
pub enum FieldCustomFormat {
    Uuid,
    Hex
}

#[derive(Debug,Clone,PartialEq)]
pub enum DeleteConstraint {
    SetNull,
    Restrict,
    Cascade,
    RemoveItem
}

pub fn parse_attribute(s: &str) -> Result<Attribute, SchemaError> {
    if s.starts_with("index") {
        return Ok(Attribute::Index)
    }

    if s.starts_with("unique") {
        return Ok(Attribute::Unique)
    }

    if s.starts_with("id") {
        return Ok(Attribute::Id)
    }

    if s == "list" || s.starts_with("list ") {
        return Ok(Attribute::List)
    }

    if let Some(inside) = s.strip_prefix("default(").and_then(|x| x.strip_suffix(')')) {
        return Ok(Attribute::Default(inside.to_string()))
    }

    if let Some(inside) = s.strip_prefix("custom(").and_then(|x| x.strip_suffix(')')) {
        let (name, args) = split_custom_args(inside);
        if name.is_empty() {
            return Err(SchemaError(format!("@custom requires a provider name: {}", s)));
        }
        return Ok(Attribute::Custom { name, args })
    }

    if let Some(inside) = s.strip_prefix("bind(").and_then(|x| x.strip_suffix(')')) {
        return Ok(Attribute::BindUnresolved(inside.to_string()))
    }

    if let Some(inside) = s.strip_prefix("inject(").and_then(|x| x.strip_suffix(')')) {
        return Ok(Attribute::InjectUnresolved(parse_inject_attrs(inside)));
    }

    if let Some(inside) = s.strip_prefix("format(").and_then(|x| x.strip_suffix(')')) {
        return Ok(Attribute::Format(match inside.to_lowercase().as_str() {
            "uuid" => FieldCustomFormat::Uuid,
            "hex" => FieldCustomFormat::Hex,
            _ => return Err(SchemaError(format!("Unknown format: {}", inside)))
        }));
    }

    if let Some(inside) = s.strip_prefix("onDelete(").and_then(|x| x.strip_suffix(')')) {
        return Ok(Attribute::OnDelete(match inside.to_uppercase().as_str() {
            "CASCADE" => DeleteConstraint::Cascade,
            "RESTRICT" => DeleteConstraint::Restrict,
            "SETNULL" => DeleteConstraint::SetNull,
            "SET_NULL" => DeleteConstraint::SetNull,
            _ => return Err(SchemaError(format!("Unknown onDelete constraint: {}", inside)))
        }));
    }

    // Catch-all: an attribute that matches no built-in is treated as a module-provided index, where the
    // attribute keyword IS the provider name — `@vector(cosine)`, `@fulltext(english)`, `@<provider>(args)`.
    // (Equivalent to the explicit `@custom(<provider>, args)` form.) The name must be a plain identifier, so
    // structurally malformed attributes (`@default(abc`) still fail rather than becoming bogus indexes.
    let (name, args) = match s.strip_suffix(')').and_then(|x| x.split_once('(')) {
        Some((name, args)) => (name.trim(), args.trim()),
        None => (s.trim(), ""),
    };
    if name.is_empty() || !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Err(SchemaError(format!("Unknown attribute: {}", s)));
    }
    Ok(Attribute::Custom { name: name.to_string(), args: args.to_string() })
}

/// Splits the inside of `@custom(...)` into `(name, args)`: the first comma-separated token is the
/// provider name, the (trimmed) remainder is handed verbatim to the provider. `@custom(vector)` → args "".
pub fn split_custom_args(inside: &str) -> (String, String) {
    match inside.split_once(',') {
        Some((name, args)) => (name.trim().to_string(), args.trim().to_string()),
        None => (inside.trim().to_string(), String::new()),
    }
}

pub fn parse_inject_attrs(s: &str) -> Vec<(String,String)> {

    let mut items = Vec::new();
    let mut raw_iter = s.split(',');

    let mut base_glob = None;

    loop {
        let Some(raw_item) = raw_iter.next() else { break };
        let mut item = raw_item.trim();
        if item.is_empty() {
            continue;
        }

        if let Some((first, second)) = item.split_once('{') {
            base_glob = Some(first.trim_end().trim_end_matches('.'));
            item = second;
        }
        if base_glob.is_some() && let Some((first, _)) = item.split_once('}') {
            item = first
        }

        let mut parts = item.split_whitespace();
        let path = match parts.next() {
            Some(p) => p,
            None => continue,
        };

        let alias = match parts.next() {
            Some("as") => {
                match parts.next() {
                    Some(a) => a.to_string(),
                    None => split_once_end(path, '.').unwrap_or(("", path)).1.to_string(),
                }
            }
            _ => split_once_end(path, '.').unwrap_or(("", path)).1.to_string(),
        };

        let path = base_glob.map(|i| [ i, path ].join(".")).unwrap_or(path.to_string());

        items.push((path, alias));
    }
    return items;
}

pub fn split_once_end(s: &str, c: char) -> Option<(&str, &str)> {
    s.rfind(c).map(|idx| {
        let (left, right_with_char) = s.split_at(idx);
        let right = &right_with_char[c.len_utf8()..]; // remove the separator itself
        (left, right)
    })
}

#[cfg(test)]
mod tests {
    use crate::schema::schema_attributes::parse_inject_attrs;

    #[test]
    fn test_parse_inject_attrs() {
        assert_eq!(parse_inject_attrs("item"), vec![ ("item".to_string(), "item".to_string() )]);
        assert_eq!(parse_inject_attrs("User.name"), vec![ ("User.name".to_string(), "name".to_string() )]);
        assert_eq!(parse_inject_attrs("User.name as naming"), vec![ ("User.name".to_string(), "naming".to_string() )]);
        assert_eq!(
            parse_inject_attrs("User.name as naming, User.test.tests"), 
            vec![ ("User.name".to_string(), "naming".to_string() ), ("User.test.tests".to_string(), "tests".to_string() ) ]
        );

        assert_eq!(
            parse_inject_attrs("User.name { name as user_name, test, surname as user_surname }"), 
            vec![ 
                ("User.name.name".to_string(), "user_name".to_string() ), 
                ("User.name.test".to_string(), "test".to_string() ), 
                ("User.name.surname".to_string(), "user_surname".to_string() 
            ) ]
        );
        assert_eq!(
            parse_inject_attrs("User.name.{ name as user_name, test, surname as user_surname }, User.tests { a }"), 
            vec![ 
                ("User.name.name".to_string(), "user_name".to_string() ), 
                ("User.name.test".to_string(), "test".to_string() ), 
                ("User.name.surname".to_string(), "user_surname".to_string() ),
                ("User.tests.a".to_string(), "a".to_string() )
            ]
        );
    }
}