//! The `.march` migration-file format: rendering ops → actions text (`serialize_migration`), and the dumb
//! application of actions onto a snapshot (`evolve`, `migration_ops`). This is the file/snapshot TEXT layer
//! — the runtime engine (`marcidb`) only ever consumes the resulting snapshot via `parse_snapshot` and the
//! ops via `apply`. A migration file is a list of SELF-CONTAINED actions, one per line.

use crate::{Entity, Field, MigrateOp, Schema, SchemaError, serialize_field};

// ─────────────────────────── rendering (ops → .march text) ───────────────────────────

/// Serializes the operations into migration-file text — one action per line. `create entity` is a bare
/// line (its fields follow as `add field` actions); `add/alter field` carry the field line; index/drop
/// ops reference by name. Definitions are taken from `schema` (the target).
pub fn serialize_migration(ops: &[MigrateOp], schema: &Schema) -> String {
    ops.iter().map(|op| serialize_op(op, schema)).collect::<Vec<_>>().join("\n")
}

fn serialize_op(op: &MigrateOp, schema: &Schema) -> String {
    match op {
        MigrateOp::CreateEntity { name } => format!("create entity {}", name),
        MigrateOp::DropEntity { name } => format!("drop entity {}", name),
        MigrateOp::AddField { entity, field } => format!("add field {}.{}", entity, field_spec(schema, entity, field)),
        MigrateOp::AlterField { entity, field } => format!("alter field {}.{}", entity, field_spec(schema, entity, field)),
        MigrateOp::DropField { entity, field } => format!("drop field {}.{}", entity, field),
        MigrateOp::AddIndex { entity, field, unique } => format!("add {} {}.{}", index_kw(*unique), entity, field),
        MigrateOp::DropIndex { entity, field, unique } => format!("drop {} {}.{}", index_kw(*unique), entity, field),
    }
}

fn index_kw(unique: bool) -> &'static str { if unique { "unique" } else { "index" } }

/// Field spec = the snapshot line without the leading name — the name is already in `E.field`
fn field_spec(schema: &Schema, entity: &str, field: &str) -> String {
    let e = find_entity(schema, entity);
    let f = find_field(e, field);
    let line = serialize_field(schema, e, f);
    let prefix = format!("{} ", field);
    format!("{} {}", field, line.strip_prefix(&prefix).unwrap_or(&line))
}

fn find_entity<'a>(schema: &'a Schema, name: &str) -> &'a Entity {
    schema.models.iter().find(|m| m.name == name).unwrap_or_else(|| panic!("entity {} not found", name))
}

fn find_field<'a>(entity: &'a Entity, name: &str) -> &'a Field {
    entity.fields.iter().find(|f| f.name == name).unwrap_or_else(|| panic!("field {}.{} not found", entity.name, name))
}

// ─────────────────────────── application (.march text → ops / snapshot) ───────────────────────────

/// An action from a migration file. Field actions carry the definition text (snapshot line) for `evolve`
#[derive(Debug, Clone)]
enum FileOp {
    CreateEntity { name: String },
    DropEntity { name: String },
    AddField { entity: String, field: String, line: String },
    AlterField { entity: String, field: String, line: String },
    DropField { entity: String, field: String },
    AddIndex { entity: String, field: String, unique: bool },
    DropIndex { entity: String, field: String, unique: bool },
}

impl FileOp {
    /// The physical operation (without definitions) for `apply`
    fn to_migrate_op(&self) -> MigrateOp {
        match self {
            FileOp::CreateEntity { name } => MigrateOp::CreateEntity { name: name.clone() },
            FileOp::DropEntity { name } => MigrateOp::DropEntity { name: name.clone() },
            FileOp::AddField { entity, field, .. } => MigrateOp::AddField { entity: entity.clone(), field: field.clone() },
            FileOp::AlterField { entity, field, .. } => MigrateOp::AlterField { entity: entity.clone(), field: field.clone() },
            FileOp::DropField { entity, field } => MigrateOp::DropField { entity: entity.clone(), field: field.clone() },
            FileOp::AddIndex { entity, field, unique } => MigrateOp::AddIndex { entity: entity.clone(), field: field.clone(), unique: *unique },
            FileOp::DropIndex { entity, field, unique } => MigrateOp::DropIndex { entity: entity.clone(), field: field.clone(), unique: *unique },
        }
    }
}

/// The physical operations from migration-file text (for the engine's `apply`)
pub fn migration_ops(text: &str) -> Result<Vec<MigrateOp>, SchemaError> {
    Ok(parse_migration(text)?.iter().map(FileOp::to_migrate_op).collect())
}

/// Applies migration actions to a snapshot → new snapshot text. Used by the server (`$migrate`) and the
/// client `marci-migrate` (computing prev / planning). Actions are self-contained, so slots/definitions are
/// taken from them; the result is a snapshot that the engine's `parse_snapshot` parses next.
pub fn evolve(old_snapshot_text: &str, migration_text: &str) -> Result<String, SchemaError> {
    let mut blocks = snapshot_blocks(old_snapshot_text);
    for op in parse_migration(migration_text)? {
        apply_file_op(&mut blocks, op)?;
    }
    Ok(emit_blocks(&blocks))
}

fn apply_file_op(blocks: &mut Vec<(String, Vec<String>)>, op: FileOp) -> Result<(), SchemaError> {
    let find = |blocks: &mut Vec<(String, Vec<String>)>, name: &str| -> Option<usize> {
        blocks.iter().position(|(n, _)| n == name)
    };
    match op {
        FileOp::CreateEntity { name } => {
            if find(blocks, &name).is_some() {
                return Err(SchemaError(format!("evolve: entity {} already exists", name)));
            }
            blocks.push((name, vec![]));   // empty entity; fields arrive as subsequent AddField actions
        }
        FileOp::DropEntity { name } => { blocks.retain(|(n, _)| n != &name); }
        FileOp::AddField { entity, field, line } => {
            let i = find(blocks, &entity).ok_or_else(|| SchemaError(format!("evolve: unknown entity {}", entity)))?;
            if blocks[i].1.iter().any(|l| field_name_of(l) == field) {
                return Err(SchemaError(format!("evolve: field {}.{} already exists", entity, field)));
            }
            blocks[i].1.push(line);
        }
        FileOp::AlterField { entity, field, line } => {
            let i = find(blocks, &entity).ok_or_else(|| SchemaError(format!("evolve: unknown entity {}", entity)))?;
            let f = blocks[i].1.iter_mut().find(|l| field_name_of(l) == field)
                .ok_or_else(|| SchemaError(format!("evolve: unknown field {}.{}", entity, field)))?;
            *f = line;
        }
        FileOp::DropField { entity, field } => {
            let i = find(blocks, &entity).ok_or_else(|| SchemaError(format!("evolve: unknown entity {}", entity)))?;
            // Replace the field line with a retired-slot tombstone so the slot is never reused (old rows keep
            // dead bytes there). A field with no slot (a virtual relation) is removed — `apply` rejects
            // relation drops anyway.
            if let Some(pos) = blocks[i].1.iter().position(|l| field_name_of(l) == field) {
                match slot_of(&blocks[i].1[pos]) {
                    Some(slot) => blocks[i].1[pos] = format!("@retired({})", slot),
                    None => { blocks[i].1.remove(pos); }
                }
            }
        }
        FileOp::AddIndex { entity, field, unique } => set_field_index(blocks, &entity, &field, Some(unique))?,
        FileOp::DropIndex { entity, field, .. } => set_field_index(blocks, &entity, &field, None)?,
    }
    Ok(())
}

/// Changes the index attribute in a field line (None — remove; Some(true) — `@unique`; Some(false) — `@index`)
fn set_field_index(blocks: &mut [(String, Vec<String>)], entity: &str, field: &str, idx: Option<bool>) -> Result<(), SchemaError> {
    let block = blocks.iter_mut().find(|(n, _)| n == entity)
        .ok_or_else(|| SchemaError(format!("evolve: unknown entity {}", entity)))?;
    let line = block.1.iter_mut().find(|l| field_name_of(l) == field)
        .ok_or_else(|| SchemaError(format!("evolve: unknown field {}.{}", entity, field)))?;
    let mut toks: Vec<&str> = line.split_whitespace().filter(|t| *t != "@index" && *t != "@unique").collect();
    match idx {
        Some(true) => toks.push("@unique"),
        Some(false) => toks.push("@index"),
        None => {}
    }
    *line = toks.join(" ");
    Ok(())
}

fn field_name_of(line: &str) -> &str {
    line.split_whitespace().next().unwrap_or("")
}

/// Extracts the Body `@slot(N)` from a field line, if any (virtual fields have none).
fn slot_of(line: &str) -> Option<usize> {
    line.split_whitespace()
        .find_map(|tok| tok.strip_prefix("@slot(").and_then(|x| x.strip_suffix(")")).and_then(|n| n.parse().ok()))
}

/// Splits snapshot text into blocks `(entity name, field lines)`, preserving order.
fn snapshot_blocks(text: &str) -> Vec<(String, Vec<String>)> {
    let mut blocks = vec![];
    let mut lines = text.lines();
    while let Some(line) = lines.next() {
        let t = line.trim();
        let Some(rest) = t.strip_prefix("Entity ") else { continue };
        let name = rest.trim_end_matches('{').trim().to_string();
        let mut fields = vec![];
        for l in lines.by_ref() {
            let lt = l.trim();
            if lt == "}" { break; }
            if lt.is_empty() { continue; }
            fields.push(lt.to_string());
        }
        blocks.push((name, fields));
    }
    blocks
}

fn emit_blocks(blocks: &[(String, Vec<String>)]) -> String {
    let mut out = String::new();
    for (i, (name, fields)) in blocks.iter().enumerate() {
        if i > 0 { out.push('\n'); }
        out.push_str(&format!("Entity {} {{\n", name));
        for f in fields { out.push_str(&format!("  {}\n", f)); }
        out.push_str("}\n");
    }
    out
}

/// Parses migration-file text into actions — strictly one action per line.
fn parse_migration(text: &str) -> Result<Vec<FileOp>, SchemaError> {
    let mut ops = vec![];
    for raw in text.lines() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') || line.starts_with("//") { continue; }
        ops.push(parse_line_op(line)?);
    }
    Ok(ops)
}

fn parse_line_op(line: &str) -> Result<FileOp, SchemaError> {
    if let Some(rest) = line.strip_prefix("create entity ") {
        return Ok(FileOp::CreateEntity { name: rest.trim().to_string() });
    }
    if let Some(rest) = line.strip_prefix("drop entity ") {
        return Ok(FileOp::DropEntity { name: rest.trim().to_string() });
    }
    if let Some(rest) = line.strip_prefix("add field ") {
        let (entity, field, fline) = parse_field_action(rest)?;
        return Ok(FileOp::AddField { entity, field, line: fline });
    }
    if let Some(rest) = line.strip_prefix("alter field ") {
        let (entity, field, fline) = parse_field_action(rest)?;
        return Ok(FileOp::AlterField { entity, field, line: fline });
    }
    if let Some(rest) = line.strip_prefix("drop field ") {
        let (entity, field) = split_path(rest.trim())?;
        return Ok(FileOp::DropField { entity, field });
    }
    for (kw, unique) in [("add unique ", true), ("add index ", false)] {
        if let Some(rest) = line.strip_prefix(kw) {
            let (entity, field) = split_path(rest.trim())?;
            return Ok(FileOp::AddIndex { entity, field, unique });
        }
    }
    for (kw, unique) in [("drop unique ", true), ("drop index ", false)] {
        if let Some(rest) = line.strip_prefix(kw) {
            let (entity, field) = split_path(rest.trim())?;
            return Ok(FileOp::DropIndex { entity, field, unique });
        }
    }
    Err(SchemaError(format!("unknown migration action: {}", line)))
}

/// `Entity.field String @slot(8) @unique` → (entity, field, "field String @slot(8) @unique")
fn parse_field_action(rest: &str) -> Result<(String, String, String), SchemaError> {
    let rest = rest.trim();
    let (path, spec) = rest.split_once(char::is_whitespace)
        .ok_or_else(|| SchemaError(format!("expected Entity.field <spec> in: \"{}\"", rest)))?;
    let (entity, field) = split_path(path)?;
    let line = format!("{} {}", field, spec.trim());
    Ok((entity, field, line))
}

/// `Entity.field` → ("Entity", "field"). The entity name may contain dots (`User.info`) — split on the last one
fn split_path(path: &str) -> Result<(String, String), SchemaError> {
    let (entity, field) = path.rsplit_once('.')
        .ok_or_else(|| SchemaError(format!("expected Entity.field in: \"{}\"", path)))?;
    Ok((entity.to_string(), field.to_string()))
}

#[cfg(test)]
mod tests {
    use super::{evolve, migration_ops, serialize_migration};
    use crate::diff::{diff, reconcile};
    use crate::{MigrateOp, Schema, parse_schema, parse_snapshot, serialize_snapshot};

    /// Normalizes snapshot text to canonical form (for comparison)
    fn canon(snapshot_text: &str) -> String {
        serialize_snapshot(&parse_snapshot(snapshot_text).unwrap())
    }

    /// A new entity = a bare `create entity` line + one `add field` per field — no block syntax.
    #[test]
    fn migration_file_is_self_contained() {
        let new = parse_schema("model User {\n  name String\n  email String @unique\n}");
        let file = serialize_migration(&diff(&parse_schema(""), &new).unwrap(), &new);

        assert!(file.contains("create entity User"), "file:\n{}", file);
        assert!(file.contains("add field User.email String @slot(8) @unique"), "file:\n{}", file);
        assert!(!file.contains('{'), "block syntax is gone:\n{}", file);
        let ops = migration_ops(&file).unwrap();
        assert!(ops.contains(&MigrateOp::CreateEntity { name: "User".into() }), "ops: {:?}", ops);
        assert!(ops.contains(&MigrateOp::AddField { entity: "User".into(), field: "email".into() }), "ops: {:?}", ops);
    }

    #[test]
    fn evolve_from_empty_equals_target() {
        let new = parse_schema("model User {\n  name String\n  email String @unique\n}\nmodel Post {\n  title String\n  author User?\n}");
        let file = serialize_migration(&diff(&parse_schema(""), &new).unwrap(), &new);
        let evolved = evolve("", &file).unwrap();
        assert_eq!(canon(&evolved), serialize_snapshot(&new));
    }

    #[test]
    fn evolve_incremental() {
        let v0 = parse_schema("model User {\n  name String\n}");
        let m0 = serialize_migration(&diff(&parse_schema(""), &v0).unwrap(), &v0);
        let snap0 = evolve("", &m0).unwrap();

        let mut v1 = parse_schema("model User {\n  name String\n  email String @unique\n  age UInt\n}");
        reconcile(&mut v1, &parse_snapshot(&snap0).unwrap());
        let prev: Schema = parse_snapshot(&snap0).unwrap();
        let m1 = serialize_migration(&diff(&prev, &v1).unwrap(), &v1);

        assert!(m1.contains("add field User.email"), "m1:\n{}", m1);
        assert!(m1.contains("add index User.email") || m1.contains("add unique User.email"), "m1:\n{}", m1);

        let snap1 = evolve(&snap0, &m1).unwrap();
        assert_eq!(canon(&snap1), serialize_snapshot(&v1));
    }
}
