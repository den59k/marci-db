//! Движок миграций (Schema-уровень, без БД): модель операций, парсер/сериализатор `.mig` DSL
//! и `diff` двух схем. Работает на СЫРЫХ моделях (`collect_blocks`) — там тип уже `RefUnresolved(name)`,
//! без синтетических struct-моделей и инжекта enum, поэтому поле тривиально сериализуется в синтаксис схемы.
//!
//! `apply` (исполнение против БД, слоты, `__marci_meta__`) и CLI — следующие шаги.

use std::collections::HashMap;

use crate::schema::{Attribute, Entity, Field, FieldCustomFormat, FieldLocation, FieldType, PrimitiveFieldType, collect_blocks, parse_field_raw, parse_model_block};

/// Одна операция миграции. Сериализуется в строку `.mig` DSL и обратно.
#[derive(Debug, Clone)]
pub enum MigrationOp {
  CreateModel { name: String, fields: Vec<Field> },
  DropModel { name: String },
  AddField { model: String, field: Field },
  AlterField { model: String, field: Field },
  DropField { model: String, field: String },
  AddIndex { model: String, field: String, unique: bool },
  DropIndex { model: String, field: String, unique: bool },
}

#[derive(Debug, PartialEq)]
pub enum MigrationError {
  /// Смена типа поля требует трансформации данных — в v1 не поддержано (правьте вручную)
  UnsupportedTypeChange { model: String, field: String, from: String, to: String },
  /// Смена `@id`/ключа требует re-key — в v1 не поддержано
  UnsupportedKeyChange { model: String, field: String },
}

// ─────────────────────────────── сериализация поля ───────────────────────────────

/// Имя примитива в синтаксисе схемы (канонические `String`/`Int`/`UInt`/…)
fn primitive_name(ty: &PrimitiveFieldType) -> &'static str {
  match ty {
    PrimitiveFieldType::String => "String",
    PrimitiveFieldType::Int64 => "Int",
    PrimitiveFieldType::UInt64 => "UInt",
    PrimitiveFieldType::Float => "Float",
    PrimitiveFieldType::Double => "Double",
    PrimitiveFieldType::Bool => "Bool",
    PrimitiveFieldType::Byte => "Byte",
    PrimitiveFieldType::DateTime => "DateTime",
  }
}

/// Тип поля без `?` (для сравнения идентичности типа в диффе)
fn base_type(field: &Field) -> String {
  match &field.ty {
    FieldType::Primitive(ty) => primitive_name(ty).to_string(),
    FieldType::PrimitiveList(ty, None) => format!("{}[]", primitive_name(ty)),
    FieldType::PrimitiveList(ty, Some(n)) => format!("{}[{}]", primitive_name(ty), n),
    FieldType::RefUnresolved(name) => name.clone(),
    FieldType::RefListUnresolved(name) => format!("{}[]", name),
    _ => unreachable!("resolved field type in a raw schema"),
  }
}

/// Тип с `?` как в синтаксисе схемы (списки-связи `T[]` уже nullable — без `?`)
fn type_spec(field: &Field) -> String {
  let nullable = field.nullable && !matches!(field.ty, FieldType::RefListUnresolved(_));
  if nullable { format!("{}?", base_type(field)) } else { base_type(field) }
}

fn render_attr(attr: &Attribute) -> Option<String> {
  Some(match attr {
    Attribute::Id => "@id".to_string(),
    Attribute::Index => "@index".to_string(),
    Attribute::Unique => "@unique".to_string(),
    Attribute::Default(s) => format!("@default({})", s),
    Attribute::BindUnresolved(s) => format!("@bind({})", s),
    Attribute::Format(FieldCustomFormat::Uuid) => "@format(uuid)".to_string(),
    Attribute::Format(FieldCustomFormat::Hex) => "@format(hex)".to_string(),
    Attribute::OnDelete(c) => format!("@onDelete({:?})", c),
    // Редкие/служебные атрибуты в v1-миграциях не рендерим
    Attribute::VectorIndex(_) | Attribute::InjectUnresolved(_) => return None,
  })
}

/// Сериализует поле в синтаксис схемы: `name Type? @attr …`
pub fn field_to_spec(field: &Field) -> String {
  let mut s = format!("{} {}", field.name, type_spec(field));
  for attr in field.attributes.iter() {
    if let Some(rendered) = render_attr(attr) {
      s.push(' ');
      s.push_str(&rendered);
    }
  }
  s
}

/// Копия поля без index/unique-атрибутов: для `add field`/`alter field` индексы выносятся отдельными операциями
fn without_index(field: &Field) -> Field {
  let mut field = field.clone();
  field.attributes.retain(|a| !matches!(a, Attribute::Index | Attribute::Unique));
  field
}

// ─────────────────────────────── сериализация миграции ───────────────────────────────

pub fn serialize_migration(ops: &[MigrationOp]) -> String {
  ops.iter().map(serialize_op).collect::<Vec<_>>().join("\n")
}

fn serialize_op(op: &MigrationOp) -> String {
  match op {
    MigrationOp::CreateModel { name, fields } => {
      let body: String = fields.iter().map(|f| format!("  {}", field_to_spec(f))).collect::<Vec<_>>().join("\n");
      format!("create model {} {{\n{}\n}}", name, body)
    }
    MigrationOp::DropModel { name } => format!("drop model {}", name),
    MigrationOp::AddField { model, field } => format!("add field {}.{}", model, field_to_spec(field)),
    MigrationOp::AlterField { model, field } => format!("alter field {}.{}", model, field_to_spec(field)),
    MigrationOp::DropField { model, field } => format!("drop field {}.{}", model, field),
    MigrationOp::AddIndex { model, field, unique } => format!("add {} {}.{}", index_kw(*unique), model, field),
    MigrationOp::DropIndex { model, field, unique } => format!("drop {} {}.{}", index_kw(*unique), model, field),
  }
}

fn index_kw(unique: bool) -> &'static str {
  if unique { "unique" } else { "index" }
}

// ─────────────────────────────── парсер миграции ───────────────────────────────

pub fn parse_migration(input: &str) -> Vec<MigrationOp> {
  let mut ops = vec![];
  let mut lines = input.lines().peekable();
  while let Some(raw) = lines.next() {
    let line = raw.trim();
    if line.is_empty() || line.starts_with('#') || line.starts_with("//") {
      continue;
    }
    ops.push(parse_op(line, &mut lines));
  }
  ops
}

fn parse_op(line: &str, lines: &mut std::iter::Peekable<std::str::Lines<'_>>) -> MigrationOp {
  if let Some(rest) = line.strip_prefix("create model ") {
    let name = rest.trim().trim_end_matches('{').trim().to_string();
    let entity = parse_model_block(name.clone(), lines);
    return MigrationOp::CreateModel { name, fields: entity.fields };
  }
  if let Some(rest) = line.strip_prefix("drop model ") {
    return MigrationOp::DropModel { name: rest.trim().to_string() };
  }
  if let Some(rest) = line.strip_prefix("add field ") {
    let (model, field) = parse_field_path(rest);
    return MigrationOp::AddField { model, field };
  }
  if let Some(rest) = line.strip_prefix("alter field ") {
    let (model, field) = parse_field_path(rest);
    return MigrationOp::AlterField { model, field };
  }
  if let Some(rest) = line.strip_prefix("drop field ") {
    let (model, field) = split_path(rest);
    return MigrationOp::DropField { model, field };
  }
  for (kw, unique) in [("add unique ", true), ("add index ", false)] {
    if let Some(rest) = line.strip_prefix(kw) {
      let (model, field) = split_path(rest);
      return MigrationOp::AddIndex { model, field, unique };
    }
  }
  for (kw, unique) in [("drop unique ", true), ("drop index ", false)] {
    if let Some(rest) = line.strip_prefix(kw) {
      let (model, field) = split_path(rest);
      return MigrationOp::DropIndex { model, field, unique };
    }
  }
  panic!("Unknown migration op: {}", line);
}

/// `User.bio String?` → ("User", parse_field_raw("bio String?")).
/// Первая `.` — разделитель model.field: имена моделей/полей её не содержат, а значения идут после типа
fn parse_field_path(rest: &str) -> (String, Field) {
  let (model, field_spec) = rest.trim().split_once('.').expect("expected Model.field");
  (model.to_string(), parse_field_raw(field_spec))
}

/// `User.email` → ("User", "email")
fn split_path(rest: &str) -> (String, String) {
  let (model, field) = rest.trim().split_once('.').expect("expected Model.field");
  (model.to_string(), field.trim().to_string())
}

// ─────────────────────────────── дифф ───────────────────────────────

fn is_key(field: &Field) -> bool {
  matches!(field.location, FieldLocation::Key { .. })
}

/// None — индекса нет, Some(false) — `@index`, Some(true) — `@unique`
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

/// Дифф двух схем (тексты `.marci`) → список операций. Покрывает create/drop model,
/// add/drop/alter field, add/drop index|unique. Смена типа/ключа отклоняется (v1).
/// Переименования, изменения структур и енумов — будущие итерации.
pub fn diff(old_text: &str, new_text: &str) -> Result<Vec<MigrationOp>, MigrationError> {
  let (old_models, _, _) = collect_blocks(old_text);
  let (new_models, _, _) = collect_blocks(new_text);

  let old_by: HashMap<&str, &Entity> = old_models.iter().map(|m| (m.name.as_str(), m)).collect();
  let new_by: HashMap<&str, &Entity> = new_models.iter().map(|m| (m.name.as_str(), m)).collect();

  let mut ops = vec![];

  // Новые модели
  for m in new_models.iter() {
    if !old_by.contains_key(m.name.as_str()) {
      ops.push(MigrationOp::CreateModel { name: m.name.clone(), fields: m.fields.clone() });
    }
  }

  // Изменения внутри существующих моделей
  for m in new_models.iter() {
    if let Some(old_m) = old_by.get(m.name.as_str()) {
      diff_fields(&m.name, old_m, m, &mut ops)?;
    }
  }

  // Удалённые модели
  for m in old_models.iter() {
    if !new_by.contains_key(m.name.as_str()) {
      ops.push(MigrationOp::DropModel { name: m.name.clone() });
    }
  }

  Ok(ops)
}

fn diff_fields(model: &str, old_m: &Entity, new_m: &Entity, ops: &mut Vec<MigrationOp>) -> Result<(), MigrationError> {
  let old_by: HashMap<&str, &Field> = old_m.fields.iter().map(|f| (f.name.as_str(), f)).collect();
  let new_by: HashMap<&str, &Field> = new_m.fields.iter().map(|f| (f.name.as_str(), f)).collect();

  // Добавленные поля (+ индекс отдельной операцией)
  for f in new_m.fields.iter() {
    if !old_by.contains_key(f.name.as_str()) {
      ops.push(MigrationOp::AddField { model: model.to_string(), field: without_index(f) });
      if let Some(unique) = index_kind(f) {
        ops.push(MigrationOp::AddIndex { model: model.to_string(), field: f.name.clone(), unique });
      }
    }
  }

  // Изменённые поля
  for f in new_m.fields.iter() {
    let Some(old_f) = old_by.get(f.name.as_str()) else { continue };

    if is_key(old_f) != is_key(f) {
      return Err(MigrationError::UnsupportedKeyChange { model: model.to_string(), field: f.name.clone() });
    }
    if base_type(old_f) != base_type(f) {
      return Err(MigrationError::UnsupportedTypeChange {
        model: model.to_string(), field: f.name.clone(),
        from: base_type(old_f), to: base_type(f),
      });
    }

    // default / format / nullable → alter
    if old_f.nullable != f.nullable || default_attr(old_f) != default_attr(f) || format_attr(old_f) != format_attr(f) {
      ops.push(MigrationOp::AlterField { model: model.to_string(), field: without_index(f) });
    }

    // Изменение индекса: убрать старый (если был) и добавить новый (если есть)
    let (old_idx, new_idx) = (index_kind(old_f), index_kind(f));
    if old_idx != new_idx {
      if let Some(unique) = old_idx {
        ops.push(MigrationOp::DropIndex { model: model.to_string(), field: f.name.clone(), unique });
      }
      if let Some(unique) = new_idx {
        ops.push(MigrationOp::AddIndex { model: model.to_string(), field: f.name.clone(), unique });
      }
    }
  }

  // Удалённые поля
  for f in old_m.fields.iter() {
    if !new_by.contains_key(f.name.as_str()) {
      ops.push(MigrationOp::DropField { model: model.to_string(), field: f.name.clone() });
    }
  }

  Ok(())
}

#[cfg(test)]
mod tests {
  use super::*;

  /// Сырое поле сериализуется в спек, который `parse_field_raw` читает обратно в тот же спек
  #[test]
  fn field_spec_roundtrip() {
    for spec in [
      "name String",
      "email String?",
      "views UInt @index @default(0)",
      "id Byte[16] @id @format(uuid)",
      "tags String[]",
      "coords Float[2]",
      "author User?",
      "posts Post[] @bind(Post.author)",
    ] {
      let field = parse_field_raw(spec);
      assert_eq!(field_to_spec(&field), spec, "spec did not round-trip");
    }
  }

  /// `.mig` текст: parse → serialize даёт тот же текст (без косметического `#`-заголовка)
  #[test]
  fn migration_roundtrip() {
    let mig = "\
create model User {
  name String
  email String? @unique
}
add field Post.views UInt
add index Post.views
alter field User.email String
drop field User.legacy
drop model Session";

    assert_eq!(serialize_migration(&parse_migration(mig)), mig);
  }

  #[test]
  fn diff_add_field_and_index() {
    let old = "
      model User {
        name String
      }";
    let new = "
      model User {
        name String
        age  UInt @index
      }";
    assert_eq!(serialize_migration(&diff(old, new).unwrap()), "add field User.age UInt\nadd index User.age");
  }

  #[test]
  fn diff_create_and_drop_model() {
    let old = "
      model User {
        name String
      }
      model Old {
        x Int
      }";
    let new = "
      model User {
        name String
      }
      model New {
        y String
      }";
    assert_eq!(serialize_migration(&diff(old, new).unwrap()), "create model New {\n  y String\n}\ndrop model Old");
  }

  #[test]
  fn diff_alter_nullable_and_index_changes() {
    let old = "
      model User {
        name  String
        email String @index
      }";
    let new = "
      model User {
        name  String
        email String? @unique
      }";
    // email стал nullable → alter; @index → @unique → drop index + add unique
    assert_eq!(
      serialize_migration(&diff(old, new).unwrap()),
      "alter field User.email String?\ndrop index User.email\nadd unique User.email"
    );
  }

  #[test]
  fn diff_drop_field() {
    let old = "
      model User {
        name String
        age  UInt
      }";
    let new = "
      model User {
        name String
      }";
    assert_eq!(serialize_migration(&diff(old, new).unwrap()), "drop field User.age");
  }

  #[test]
  fn diff_type_change_rejected() {
    let old = "model User {\n age UInt \n}";
    let new = "model User {\n age String \n}";
    assert!(matches!(diff(old, new), Err(MigrationError::UnsupportedTypeChange { .. })));
  }
}
