
#[derive(Debug,Clone)]
pub enum Attribute {
    Index,
    DerivedUnresolved (String),
    Id,
    InjectUnresolved(Vec<(String,String)>),
    OnDelete(DeleteConstraint)
}

#[derive(Debug,Clone,PartialEq)]
pub enum DeleteConstraint {
    SetNull,
    Restrict,
    Cascade,
    RemoveItem
}

pub fn parse_attribute(s: &str) -> Attribute {
    if s.starts_with("index") {
        return Attribute::Index
    }

    if s.starts_with("id") {
        return Attribute::Id
    }

    if let Some(inside) = s.strip_prefix("derived(").and_then(|x| x.strip_suffix(')')) {
        return Attribute::DerivedUnresolved(inside.to_string())
    }

    if let Some(inside) = s.strip_prefix("inject(").and_then(|x| x.strip_suffix(')')) {
        return Attribute::InjectUnresolved(parse_inject_attrs(inside));
    }

    if let Some(inside) = s.strip_prefix("onDelete(").and_then(|x| x.strip_suffix(')')) {
        return Attribute::OnDelete(match inside.to_uppercase().as_str() {
            "CASCADE" => DeleteConstraint::Cascade,
            "RESTRICT" => DeleteConstraint::Restrict,
            "SETNULL" => DeleteConstraint::SetNull,
            "SET_NULL" => DeleteConstraint::SetNull,
            _ => panic!("Unknown onDelete constraint: {}", inside)
        });
    }

    panic!("Unknown attribute {}", s)
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
        let right = &right_with_char[c.len_utf8()..]; // убрать сам разделитель
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