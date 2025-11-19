
#[derive(Debug,Clone)]
pub enum Attribute {
    Index,
    DerivedUnresolved (String),
    Id,
    InjectUnresolved(Vec<(String,String)>)
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

    panic!("Unknown attribute {}", s)
}

pub fn parse_inject_attrs(s: &str) -> Vec<(String,String)> {

    let mut items = Vec::new();
    for raw_item in s.split(',') {
        let item = raw_item.trim();
        if item.is_empty() {
            continue;
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
                    None => path.rsplit('.').next().unwrap_or(path).to_string(),
                }
            }
            _ => path.rsplit('.').next().unwrap_or(path).to_string(),
        };

        items.push((path.to_string(), alias));
    }
    return items;
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
    }
}