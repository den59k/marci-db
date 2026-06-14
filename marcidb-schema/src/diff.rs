//! Migration diff between two materialized schemas: `diff` (compute ops) and `reconcile` (carry slots +
//! enum ids from history, canonicalize field order). Operates only on the flat `Schema`; the physical
//! apply lives in the engine, and rendering ops to `.march` text lives in `march.rs`.

use std::collections::HashMap;

use marcidb::{
    Attribute, Entity, EnumInfo, Field, FieldDefault, FieldExistsCondition, FieldLocation, FieldType,
    MigrateError, MigrateOp, RefBinding, Schema, serialize_type,
};

/// Size of the row "pre-header" (the first Body slot starts at this byte offset)
const PRE_HEADER: usize = 4;

/// Diff of two flat schemas: `old` (old snapshot) → `new` (materialized new schema).
/// Both must be in snapshot form (slots/variants reconciled); slot reconciliation is a separate step before diff.
pub fn diff(old: &Schema, new: &Schema) -> Result<Vec<MigrateOp>, MigrateError> {
    let old_by: HashMap<&str, &_> = old.models.iter().map(|e| (e.name.as_str(), e)).collect();
    let new_by: HashMap<&str, &_> = new.models.iter().map(|e| (e.name.as_str(), e)).collect();

    let mut ops = vec![];

    // Created and existing entities. A new entity is just "diff against nothing": CreateEntity, then
    // every field flows through diff_fields as AddField/AddIndex — one field-definition surface, no
    // special-casing of new entities.
    for e in new.models.iter() {
        let old_fields: &[Field] = match old_by.get(e.name.as_str()) {
            Some(old_e) => &old_e.fields,
            None => {
                ops.push(MigrateOp::CreateEntity { name: e.name.clone() });
                &[]
            }
        };
        diff_fields(old, new, &e.name, old_fields, &e.fields, &mut ops)?;
    }

    // Removed entities
    for e in old.models.iter() {
        if !new_by.contains_key(e.name.as_str()) {
            ops.push(MigrateOp::DropEntity { name: e.name.clone() });
        }
    }

    Ok(ops)
}

fn diff_fields(
    old_schema: &Schema, new_schema: &Schema,
    entity: &str, old_fields: &[Field], new_fields: &[Field],
    ops: &mut Vec<MigrateOp>,
) -> Result<(), MigrateError> {
    let old_by: HashMap<&str, &Field> = old_fields.iter().map(|f| (f.name.as_str(), f)).collect();
    let new_by: HashMap<&str, &Field> = new_fields.iter().map(|f| (f.name.as_str(), f)).collect();

    // Added fields (+ index as a separate operation)
    for f in new_fields.iter() {
        if !old_by.contains_key(f.name.as_str()) {
            ops.push(MigrateOp::AddField { entity: entity.to_string(), field: f.name.clone() });
            if let Some(unique) = index_kind(f) {
                ops.push(MigrateOp::AddIndex { entity: entity.to_string(), field: f.name.clone(), unique });
            }
        }
    }

    // Changed fields
    for f in new_fields.iter() {
        let Some(old_f) = old_by.get(f.name.as_str()) else { continue };

        if is_key(old_f) != is_key(f) {
            return Err(MigrateError::UnsupportedKeyChange { entity: entity.to_string(), field: f.name.clone() });
        }
        if let (FieldLocation::Body { offset_pos: a }, FieldLocation::Body { offset_pos: b }) = (&old_f.location, &f.location) {
            if a != b {
                return Err(MigrateError::UnsupportedLayoutChange { entity: entity.to_string(), field: f.name.clone() });
            }
        }

        // Type comparison: enum — separately (adding variants is allowed), everything else — by name
        let mut needs_alter = false;
        match type_cmp(old_schema, old_f, new_schema, f) {
            TypeCmp::Same => {}
            TypeCmp::AdditiveEnum => needs_alter = true,
            TypeCmp::EnumIncompatible(detail) =>
                return Err(MigrateError::UnsupportedEnumChange { entity: entity.to_string(), field: f.name.clone(), detail }),
            TypeCmp::Incompatible =>
                return Err(MigrateError::UnsupportedTypeChange {
                    entity: entity.to_string(), field: f.name.clone(),
                    from: serialize_type(old_schema, old_f), to: serialize_type(new_schema, f),
                }),
        }

        // A relation's storage binding changing (e.g. adding @bind flips index_tree → current_id) moves the
        // relation's physical home — not an in-place migration. Catch it so it never becomes a silent no-op.
        if let (Some(a), Some(b)) = (ref_binding(old_f), ref_binding(f)) && a != b {
            return Err(MigrateError::UnsupportedBindingChange {
                entity: entity.to_string(), field: f.name.clone(),
                from: binding_kind(a).to_string(), to: binding_kind(b).to_string(),
            });
        }

        // Metadata: nullable / default / format
        if old_f.nullable != f.nullable || default_attr(old_f) != default_attr(f) || format_attr(old_f) != format_attr(f) {
            needs_alter = true;
        }
        if needs_alter {
            ops.push(MigrateOp::AlterField { entity: entity.to_string(), field: f.name.clone() });
        }

        // Index: drop the old one / add the new one
        let (old_idx, new_idx) = (index_kind(old_f), index_kind(f));
        if old_idx != new_idx {
            if let Some(unique) = old_idx {
                ops.push(MigrateOp::DropIndex { entity: entity.to_string(), field: f.name.clone(), unique });
            }
            if let Some(unique) = new_idx {
                ops.push(MigrateOp::AddIndex { entity: entity.to_string(), field: f.name.clone(), unique });
            }
        }
    }

    // Removed fields
    for f in old_fields.iter() {
        if !new_by.contains_key(f.name.as_str()) {
            if let Some(unique) = index_kind(f) {
                ops.push(MigrateOp::DropIndex { entity: entity.to_string(), field: f.name.clone(), unique });
            }
            ops.push(MigrateOp::DropField { entity: entity.to_string(), field: f.name.clone() });
        }
    }

    Ok(())
}

// ─────────────────────────────── slot reconciliation ───────────────────────────────

/// Carries everything order-dependent from the old snapshot into the freshly parsed schema: Body-field
/// slots AND enum variant ids (stored data depends on them). Then canonicalizes field order. Run BEFORE [`diff`].
pub fn reconcile(new: &mut Schema, old: &Schema) {
    carry_slots(new, old);
    carry_enum_ids(new, old);
    canonicalize_field_order(new);
}

fn carry_slots(new: &mut Schema, old: &Schema) {
    let old_by: HashMap<&str, &Entity> = old.models.iter().map(|e| (e.name.as_str(), e)).collect();

    for entity in new.models.iter_mut() {
        let Some(old_entity) = old_by.get(entity.name.as_str()) else { continue };

        let mut old_slots: HashMap<&str, usize> = HashMap::new();
        let mut max_offset = 0usize;
        for f in old_entity.fields.iter() {
            if let FieldLocation::Body { offset_pos } = f.location {
                old_slots.insert(f.name.as_str(), offset_pos);
                max_offset = max_offset.max(offset_pos);
            }
        }
        let mut next = if max_offset == 0 { PRE_HEADER } else { max_offset + 4 };

        for f in entity.fields.iter_mut() {
            if let FieldLocation::Body { offset_pos } = &mut f.location {
                match old_slots.get(f.name.as_str()) {
                    Some(&old_off) => *offset_pos = old_off,
                    None => { *offset_pos = next; next += 4; }
                }
            }
        }

        entity.payload_offset = next;
    }
}

fn carry_enum_ids(new: &mut Schema, old: &Schema) {
    let old_by: HashMap<&str, &Entity> = old.models.iter().map(|e| (e.name.as_str(), e)).collect();

    for entity in new.models.iter_mut() {
        let Some(old_entity) = old_by.get(entity.name.as_str()) else { continue };
        let old_fields: HashMap<&str, &Field> = old_entity.fields.iter().map(|f| (f.name.as_str(), f)).collect();

        let mut remaps: Vec<(usize, HashMap<u16, u16>)> = vec![];
        for (fi, field) in entity.fields.iter().enumerate() {
            let FieldType::Enum(new_ei) = &field.ty else { continue };
            let Some(old_field) = old_fields.get(field.name.as_str()) else { continue };
            let FieldType::Enum(old_ei) = &old_field.ty else { continue };

            let mut next = old_ei.variants_map.values().copied().max().map(|m| m + 1).unwrap_or(0);
            let mut remap = HashMap::new();
            let mut changed = false;
            let mut variants: Vec<(&String, u16)> = new_ei.variants_map.iter().map(|(n, i)| (n, *i)).collect();
            variants.sort_by_key(|(_, i)| *i);
            for (name, parser_id) in variants {
                let reconciled = match old_ei.variants_map.get(name) {
                    Some(&old_id) => old_id,
                    None => { let id = next; next += 1; id }
                };
                if reconciled != parser_id { changed = true; }
                remap.insert(parser_id, reconciled);
            }
            if changed { remaps.push((fi, remap)); }
        }

        for (fi, remap) in remaps {
            if let FieldType::Enum(ei) = &mut entity.fields[fi].ty {
                ei.variants_map = std::mem::take(&mut ei.variants_map).into_iter().map(|(n, id)| (n, remap[&id])).collect();
                ei.variants = std::mem::take(&mut ei.variants).into_iter().map(|(id, v)| (remap[&id], v)).collect();
                ei.variants_names_map = ei.variants_map.iter().map(|(n, id)| (*id, n.clone())).collect();
            }
            if let Some(FieldDefault::Value(bytes)) = &mut entity.fields[fi].default_value {
                if bytes.len() == 2 {
                    let parser_id = u16::from_be_bytes([bytes[0], bytes[1]]);
                    if let Some(&reconciled) = remap.get(&parser_id) {
                        *bytes = reconciled.to_be_bytes().to_vec();
                    }
                }
            }
            for field in entity.fields.iter_mut() {
                if let FieldExistsCondition::EnumValue { field_index, variants } = &mut field.condition {
                    if *field_index == fi {
                        for v in variants.iter_mut() { *v = remap[v]; }
                    }
                }
            }
        }
    }
}

/// Brings each entity's field order to canonical: keys, then Body (by offset_pos), then Virtual.
/// This is the row-format invariant. Index references (condition.field_index, enum variants, rev_field_idx) are remapped.
fn canonicalize_field_order(schema: &mut Schema) {
    let mut perms: Vec<Vec<usize>> = Vec::with_capacity(schema.models.len());
    for entity in schema.models.iter_mut() {
        let n = entity.fields.len();
        let mut order: Vec<usize> = (0..n).collect();
        order.sort_by_key(|&i| field_sort_key(&entity.fields[i], i));

        let mut perm = vec![0usize; n];
        for (new_idx, &old_idx) in order.iter().enumerate() { perm[old_idx] = new_idx; }

        let mut reordered: Vec<Field> = Vec::with_capacity(n);
        for &old_idx in order.iter() { reordered.push(entity.fields[old_idx].clone()); }
        entity.fields = reordered;
        perms.push(perm);
    }

    for (ei, entity) in schema.models.iter_mut().enumerate() {
        let self_perm = &perms[ei];
        for field in entity.fields.iter_mut() {
            if let FieldExistsCondition::EnumValue { field_index, .. } = &mut field.condition {
                *field_index = self_perm[*field_index];
            }
            match &mut field.ty {
                FieldType::Enum(enum_info) => {
                    for indices in enum_info.variants.values_mut() {
                        for idx in indices.iter_mut() { *idx = self_perm[*idx]; }
                    }
                }
                FieldType::Ref(ref_info) | FieldType::RefList(ref_info) => {
                    if let Some(rev) = ref_info.rev_field_idx {
                        ref_info.rev_field_idx = Some(perms[ref_info.model_index][rev]);
                    }
                }
                _ => {}
            }
        }
    }
}

fn field_sort_key(field: &Field, original_index: usize) -> (u8, usize) {
    match field.location {
        FieldLocation::Key { index } => (0, index),
        FieldLocation::Body { offset_pos } => (1, offset_pos),
        FieldLocation::Virtual => (2, original_index),
    }
}

// ─────────────────────────────── type comparison ───────────────────────────────

enum TypeCmp {
    Same,
    AdditiveEnum,
    EnumIncompatible(String),
    Incompatible,
}

fn type_cmp(old_schema: &Schema, old_f: &Field, new_schema: &Schema, new_f: &Field) -> TypeCmp {
    match (&old_f.ty, &new_f.ty) {
        (FieldType::Enum(a), FieldType::Enum(b)) => enum_cmp(a, b),
        _ => {
            if serialize_type(old_schema, old_f) == serialize_type(new_schema, new_f) {
                TypeCmp::Same
            } else {
                TypeCmp::Incompatible
            }
        }
    }
}

fn enum_cmp(old: &EnumInfo, new: &EnumInfo) -> TypeCmp {
    for (name, id) in old.variants_map.iter() {
        match new.variants_map.get(name) {
            Some(new_id) if new_id == id => {}
            Some(new_id) => return TypeCmp::EnumIncompatible(format!("variant '{}' changed id {} -> {}", name, id, new_id)),
            None => return TypeCmp::EnumIncompatible(format!("variant '{}' removed", name)),
        }
    }
    if new.variants_map.len() == old.variants_map.len() { TypeCmp::Same } else { TypeCmp::AdditiveEnum }
}

// ─────────────────────────────── field attributes ───────────────────────────────

fn is_key(field: &Field) -> bool {
    matches!(field.location, FieldLocation::Key { .. })
}

fn ref_binding(field: &Field) -> Option<&RefBinding> {
    match &field.ty {
        FieldType::Ref(ri) | FieldType::RefList(ri) => Some(&ri.binding),
        _ => None,
    }
}

fn binding_kind(binding: &RefBinding) -> &'static str {
    match binding {
        RefBinding::CurrentId => "current_id",
        RefBinding::FieldValue => "field_value",
        RefBinding::IndexTree(_) => "index_tree",
    }
}

/// None — no index, Some(false) — `@index`, Some(true) — `@unique`
fn index_kind(field: &Field) -> Option<bool> {
    let mut kind = None;
    for attr in field.attributes.iter() {
        match attr {
            Attribute::Unique => kind = Some(true),
            Attribute::Index if kind.is_none() => kind = Some(false),
            _ => {}
        }
    }
    kind
}

fn default_attr(field: &Field) -> Option<&str> {
    field.attributes.iter().find_map(|a| if let Attribute::Default(s) = a { Some(s.as_str()) } else { None })
}

fn format_attr(field: &Field) -> Option<String> {
    field.attributes.iter().find_map(|a| if let Attribute::Format(fmt) = a { Some(format!("{:?}", fmt)) } else { None })
}

#[cfg(test)]
mod tests {
    use super::{diff, reconcile};
    use marcidb::{FieldLocation, MigrateError, MigrateOp, Schema, parse_schema};

    fn diff_text(old: &str, new: &str) -> Result<Vec<MigrateOp>, MigrateError> {
        diff(&parse_schema(old), &parse_schema(new))
    }

    #[test]
    fn diff_create_and_drop_entity() {
        let old = "model User {\n  name String\n}";
        let new = "model User {\n  name String\n}\nmodel Post {\n  title String\n}";
        assert_eq!(diff_text(old, new).unwrap(), vec![
            MigrateOp::CreateEntity { name: "Post".into() },
            MigrateOp::AddField { entity: "Post".into(), field: "id".into() },
            MigrateOp::AddField { entity: "Post".into(), field: "title".into() },
        ]);
        assert_eq!(diff_text(new, old).unwrap(), vec![MigrateOp::DropEntity { name: "Post".into() }]);
    }

    #[test]
    fn diff_add_field_with_index() {
        let old = "model User {\n  name String\n}";
        let new = "model User {\n  name String\n  email String @unique\n}";
        assert_eq!(diff_text(old, new).unwrap(), vec![
            MigrateOp::AddField { entity: "User".into(), field: "email".into() },
            MigrateOp::AddIndex { entity: "User".into(), field: "email".into(), unique: true },
        ]);
    }

    #[test]
    fn diff_alter_nullable_and_index_swap() {
        let old = "model User {\n  email String @index\n}";
        let new = "model User {\n  email String?\n}";
        assert_eq!(diff_text(old, new).unwrap(), vec![
            MigrateOp::AlterField { entity: "User".into(), field: "email".into() },
            MigrateOp::DropIndex { entity: "User".into(), field: "email".into(), unique: false },
        ]);
    }

    #[test]
    fn diff_drop_field() {
        let old = "model User {\n  name String\n  age UInt\n}";
        let new = "model User {\n  name String\n}";
        assert_eq!(diff_text(old, new).unwrap(), vec![MigrateOp::DropField { entity: "User".into(), field: "age".into() }]);
    }

    #[test]
    fn diff_type_change_rejected() {
        let old = "model User {\n  age UInt\n}";
        let new = "model User {\n  age String\n}";
        assert!(matches!(diff_text(old, new), Err(MigrateError::UnsupportedTypeChange { .. })));
    }

    #[test]
    fn diff_struct_is_just_entities() {
        let old = "model User {\n  name String\n}";
        let new = "model User {\n  name String\n  info Info?\n}\nstruct Info {\n  bio String\n}";
        let ops = diff_text(old, new).unwrap();
        assert!(ops.contains(&MigrateOp::CreateEntity { name: "User.info".into() }));
        assert!(ops.contains(&MigrateOp::AddField { entity: "User".into(), field: "info".into() }));
    }

    #[test]
    fn diff_enum_add_variant_is_additive() {
        let old = "model A {\n  t E\n}\nenum E {\n  a\n  b {\n    x Int\n  }\n}";
        let new = "model A {\n  t E\n}\nenum E {\n  a\n  b {\n    x Int\n  }\n  c {\n    y Int\n  }\n}";
        let ops = diff_text(old, new).unwrap();
        assert!(ops.contains(&MigrateOp::AlterField { entity: "A".into(), field: "t".into() }), "ops: {:?}", ops);
        assert!(ops.contains(&MigrateOp::AddField { entity: "A".into(), field: "y".into() }), "ops: {:?}", ops);
    }

    #[test]
    fn diff_enum_remove_variant_rejected() {
        let old = "model A {\n  t E\n}\nenum E {\n  a\n  b\n}";
        let new = "model A {\n  t E\n}\nenum E {\n  a\n}";
        assert!(matches!(diff_text(old, new), Err(MigrateError::UnsupportedEnumChange { .. })));
    }

    #[test]
    fn diff_unchanged_is_empty() {
        let s = "model User {\n  name String\n  email String @unique\n}";
        assert!(diff_text(s, s).unwrap().is_empty());
    }

    fn body_offset(schema: &Schema, entity: &str, field: &str) -> usize {
        let e = schema.models.iter().find(|m| m.name == entity).unwrap();
        let f = e.fields.iter().find(|f| f.name == field).unwrap();
        match f.location { FieldLocation::Body { offset_pos } => offset_pos, _ => panic!("{}.{} is not Body", entity, field) }
    }

    #[test]
    fn insert_middle_shifts_slots_without_reconcile() {
        let old = parse_schema("model M {\n  a String\n  b String\n}");
        let new = parse_schema("model M {\n  a String\n  c String\n  b String\n}");
        assert_eq!(body_offset(&old, "M", "b"), 8);
        assert_eq!(body_offset(&new, "M", "b"), 12);
        assert!(matches!(diff(&old, &new), Err(MigrateError::UnsupportedLayoutChange { .. })));
    }

    #[test]
    fn reconcile_carries_slots_then_diff_is_clean() {
        let old = parse_schema("model M {\n  a String\n  b String\n}");
        let mut new = parse_schema("model M {\n  a String\n  c String\n  b String\n}");

        reconcile(&mut new, &old);

        assert_eq!(body_offset(&new, "M", "a"), 4);
        assert_eq!(body_offset(&new, "M", "b"), 8);
        assert_eq!(body_offset(&new, "M", "c"), 12);

        assert_eq!(diff(&old, &new).unwrap(), vec![MigrateOp::AddField { entity: "M".into(), field: "c".into() }]);
    }

    #[test]
    fn reconcile_leaves_new_entity_untouched() {
        let old = parse_schema("model M {\n  a String\n}");
        let mut new = parse_schema("model M {\n  a String\n}\nmodel N {\n  x String\n  y String\n}");
        reconcile(&mut new, &old);
        assert_eq!(body_offset(&new, "N", "x"), 4);
        assert_eq!(body_offset(&new, "N", "y"), 8);
    }

    #[test]
    fn reconcile_carries_enum_variant_ids() {
        let old = parse_schema("model A {\n  t E\n}\nenum E {\n  a\n  b\n}");
        let mut new = parse_schema("model A {\n  t E\n}\nenum E {\n  b\n  a\n}");

        assert!(matches!(diff(&old, &new), Err(MigrateError::UnsupportedEnumChange { .. })));

        reconcile(&mut new, &old);
        assert!(diff(&old, &new).unwrap().is_empty());
    }
}
