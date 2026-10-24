//! The binary read transport's view of a schema — the one definition shared by the engine's row encoder
//! (`marcidb::binary_encode`), the codegen (the `MODELS` descriptors it bakes into a client) and the
//! schema handshake between them.
//!
//! ## Why the order is canonical, not positional
//!
//! A client is generated from `schema.marci`, where fields sit in *declaration* order. The engine holds the
//! materialized schema, where body fields sit in *slot* order — and a field added by a migration takes the
//! next free slot however early it is declared. The two orders diverge on any migrated database, so a wire
//! format that walks "the entity's fields in order" on both sides silently swaps same-typed fields and
//! mis-reads the rest. The wire order is therefore **by field name** (byte order of the UTF-8 name — the
//! same order `str::cmp` and a code-point comparison in JS give), which both sides compute from the set of
//! selected names alone, without agreeing on slots or declaration order.
//!
//! ## The handshake
//!
//! [`wire_fingerprint`] hashes exactly what the decoder depends on — model names, selectable field names,
//! their kind (`key`/`body`/`one`/`many`), scalar type code and relation target — and nothing else (no
//! slots, no field order, no indexes, no defaults). A client sends the fingerprint of the schema it was
//! generated from; the engine answers binary only when it equals the fingerprint of its live schema, else
//! the read falls back to JSON. That keeps a stale client correct, while a client merely generated from a
//! differently-ordered (but equivalent) schema still gets the fast path.

use crate::schema::{Entity, Field, FieldExistsCondition, FieldLocation, FieldType, PrimitiveFieldType, Schema};

/// What a selectable field is on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WireKind {
    /// An `@id` part: the value, no presence tag.
    Key,
    /// A body scalar: presence tag, then the value.
    Body,
    /// A to-one relation: null/present byte, then the related row.
    One,
    /// A to-many relation: `u32 count`, then the related rows.
    Many,
}

impl WireKind {
    /// The `k` code the codegen emits (`{ n, k, t, m }`).
    pub fn code(self) -> &'static str {
        match self {
            WireKind::Key => "key",
            WireKind::Body => "body",
            WireKind::One => "one",
            WireKind::Many => "many",
        }
    }
}

/// One field as the binary decoder sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireField<'a> {
    pub name: &'a str,
    pub kind: WireKind,
    /// The scalar type code for a binary-decodable scalar; `None` for a scalar the binary path does not
    /// carry (a `@format`ted value, an enum, a list, a variant-payload field) — selecting it sends the whole
    /// query over JSON — and for relations.
    pub ty: Option<&'static str>,
    /// The related model, for `One`/`Many`.
    pub target: Option<&'a str>,
}

/// The type code of a primitive on the wire — what the client's `readScalar` switches on.
pub fn wire_type_code(ty: &PrimitiveFieldType) -> &'static str {
    match ty {
        PrimitiveFieldType::String => "str",
        PrimitiveFieldType::Int64 => "i64",
        PrimitiveFieldType::UInt64 => "u64",
        PrimitiveFieldType::Float => "f64",
        PrimitiveFieldType::Double => "f64",
        PrimitiveFieldType::Bool => "bool",
        PrimitiveFieldType::Byte => "u8",
        PrimitiveFieldType::DateTime => "i64",
        PrimitiveFieldType::Json => "json",
    }
}

/// The wire descriptor of a field, or `None` for an internal (`@`-prefixed) field — those are never
/// selectable, so they never reach the wire.
pub fn wire_field<'a>(schema: &'a Schema, field: &'a Field) -> Option<WireField<'a>> {
    if field.name.starts_with('@') {
        return None;
    }
    let name = field.name.as_str();
    Some(match &field.ty {
        FieldType::Ref(ref_info) => WireField { name, kind: WireKind::One, ty: None, target: Some(&schema.models[ref_info.model_index].name) },
        FieldType::RefList(ref_info) => WireField { name, kind: WireKind::Many, ty: None, target: Some(&schema.models[ref_info.model_index].name) },
        ty => {
            let kind = if matches!(field.location, FieldLocation::Key { .. }) { WireKind::Key } else { WireKind::Body };
            // Binary-decodable only as an unformatted primitive that is not a variant payload (a variant
            // field is *absent*, not null, when its enum is on another value — the presence tag can't say that).
            let code = match ty {
                FieldType::Primitive(primitive) if field.format.is_none() && matches!(field.condition, FieldExistsCondition::None) => Some(wire_type_code(primitive)),
                _ => None,
            };
            WireField { name, kind, ty: code, target: None }
        }
    })
}

/// The selectable fields of an entity as the wire sees them, in `entity.fields` order. (Positional order
/// carries no meaning on the wire — see the module docs — but a stable order keeps codegen output stable.)
pub fn wire_fields<'a>(schema: &'a Schema, entity: &'a Entity) -> Vec<WireField<'a>> {
    entity.fields.iter().filter_map(|field| wire_field(schema, field)).collect()
}

/// The canonical wire order of an entity's fields: indices into `entity.fields`, sorted by field name. The
/// encoder emits the selected fields in this order; the client decoder sorts the selected names the same way.
pub fn wire_order(entity: &Entity) -> Vec<usize> {
    let mut order: Vec<usize> = (0..entity.fields.len()).collect();
    order.sort_by(|&a, &b| entity.fields[a].name.as_str().cmp(entity.fields[b].name.as_str()));
    order
}

/// The canonical text the fingerprint hashes: every model (by name) with its wire fields (by name), each as
/// `name:kind:type:target`. Deterministic, and blind to everything the decoder does not depend on.
pub fn wire_descriptor(schema: &Schema) -> String {
    let mut models: Vec<&Entity> = schema.models.iter().collect();
    models.sort_by(|a, b| a.name.cmp(&b.name));

    let mut out = String::new();
    for model in models {
        let mut fields = wire_fields(schema, model);
        fields.sort_by(|a, b| a.name.cmp(b.name));
        out.push_str(&model.name);
        out.push('{');
        for field in fields {
            out.push_str(field.name);
            out.push(':');
            out.push_str(field.kind.code());
            out.push(':');
            out.push_str(field.ty.unwrap_or("-"));
            out.push(':');
            out.push_str(field.target.unwrap_or("-"));
            out.push(';');
        }
        out.push_str("}\n");
    }
    out
}

/// The schema fingerprint of the binary handshake: FNV-1a (64-bit, hex) over [`wire_descriptor`]. A client
/// bakes in the fingerprint of the schema it was generated from; the engine compares it with the
/// fingerprint of its live schema before answering a read in binary.
pub fn wire_fingerprint(schema: &Schema) -> String {
    let mut hash: u64 = 0xcbf29ce484222325; // FNV offset basis
    for byte in wire_descriptor(schema).as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x100000001b3); // FNV prime
    }
    format!("{:016x}", hash)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{evolve, parse_schema, parse_snapshot, serialize_snapshot};

    // `active` is declared second but was added by a later migration, so in the live schema it owns the last
    // slot — the scenario the wire order and the fingerprint must both be indifferent to.
    const DECLARED: &str = "model Item {
  title  String
  active Bool    @default(true)
  flag   Bool    @default(false)
  note   String?
}
";
    const M0: &str = "create entity Item
add field Item.id UInt @id @default(autoincrement())
add field Item.title String @slot(4)
add field Item.flag Bool @slot(8) @default(false)
add field Item.note String @nullable @slot(12)";
    const M1: &str = "add field Item.active Bool @slot(16) @default(true)";

    /// The live schema of a database that applied the migrations (what the engine encodes with).
    fn migrated() -> Schema {
        let snapshot = evolve(&evolve("", M0).unwrap(), M1).unwrap();
        parse_snapshot(&snapshot).unwrap()
    }

    #[test]
    fn wire_order_is_by_name_whatever_the_slots() {
        let declared = parse_schema(DECLARED);
        let live = migrated();
        let names = |schema: &Schema| -> Vec<String> {
            let entity = &schema.models[0];
            wire_order(entity).into_iter().map(|i| entity.fields[i].name.clone()).collect()
        };
        // The two schemas hold the fields in different positions …
        let positional = |schema: &Schema| schema.models[0].fields.iter().map(|f| f.name.clone()).collect::<Vec<_>>();
        assert_ne!(positional(&declared), positional(&live), "the scenario needs diverging field orders");
        // … and the same wire order.
        assert_eq!(names(&declared), names(&live));
        assert_eq!(names(&live), ["active", "flag", "id", "note", "title"]);
    }

    #[test]
    fn fingerprint_ignores_slots_and_field_order_but_sees_types() {
        let declared = parse_schema(DECLARED);
        let live = migrated();
        assert_eq!(wire_fingerprint(&declared), wire_fingerprint(&live), "declared vs migrated:\n{}", wire_descriptor(&live));
        // The snapshot round-trip the engine takes on open changes nothing either.
        assert_eq!(wire_fingerprint(&declared), wire_fingerprint(&parse_snapshot(&serialize_snapshot(&declared)).unwrap()));
        assert_eq!(wire_fingerprint(&declared).len(), 16);

        // Things the decoder depends on must move the fingerprint: a type, a format, a new field, a kind.
        assert_ne!(wire_fingerprint(&declared), wire_fingerprint(&parse_schema(&DECLARED.replace("flag   Bool    @default(false)", "flag   Int     @default(0)"))));
        assert_ne!(wire_fingerprint(&declared), wire_fingerprint(&parse_schema(&DECLARED.replace("title  String", "title  Byte[16] @format(uuid)"))));
        assert_ne!(wire_fingerprint(&declared), wire_fingerprint(&parse_schema(&DECLARED.replace("  note   String?\n", ""))));
        // Things it does not depend on must not: nullability, defaults, an index.
        assert_eq!(wire_fingerprint(&declared), wire_fingerprint(&parse_schema(&DECLARED.replace("note   String?", "note   String @index"))));
        assert_eq!(wire_fingerprint(&declared), wire_fingerprint(&parse_schema(&DECLARED.replace("@default(true)", ""))));
    }

    #[test]
    fn descriptor_covers_relations_and_gated_scalars() {
        let schema = parse_schema(
"model User {
  name   String
  uid    Byte[16]  @format(uuid)
  tags   String[]
  role   Role
  posts  Post[]    @bind(Post.author)
}
model Post {
  title  String
  author User?
}
enum Role {
  basic
  pro { seats Int }
}
",
        );
        let text = wire_descriptor(&schema);
        assert!(text.contains("id:key:u64:-;"), "{text}");
        assert!(text.contains("name:body:str:-;"), "{text}");
        assert!(text.contains("uid:body:-:-;"), "formatted scalar is not binary-decodable\n{text}");
        assert!(text.contains("tags:body:-:-;"), "list is not binary-decodable\n{text}");
        assert!(text.contains("role:body:-:-;"), "enum is not binary-decodable\n{text}");
        assert!(text.contains("posts:many:-:Post;"), "{text}");
        assert!(text.contains("author:one:-:User;"), "{text}");
        assert!(!text.contains('@'), "internal fields never reach the wire\n{text}");
        // A payload enum's variant field is gated (no type code) — the decoder falls back to JSON for it.
        for line in text.lines().filter(|l| l.contains("seats")) {
            assert!(line.contains("seats:body:-:-;"), "{line}");
        }
    }
}
