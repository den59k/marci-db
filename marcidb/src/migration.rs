//! Движок миграций (Schema-уровень, без БД): модель операций, парсер/сериализатор `.mig` DSL
//! и `diff` двух схем. Работает на СЫРЫХ моделях (`collect_blocks`) — там тип уже `RefUnresolved(name)`,
//! без синтетических struct-моделей и инжекта enum, поэтому поле тривиально сериализуется в синтаксис схемы.
//!
//! `apply` (исполнение против БД, слоты, `__marci_meta__`) и CLI — следующие шаги.

use std::collections::{HashMap, HashSet};

use canopydb::WriteTransaction;

use crate::{StorageError, index_utils::{encode_full_index, encode_index}, schema::{Attribute, Entity, Field, FieldCustomFormat, FieldLocation, FieldType, PrimitiveFieldType, RefBinding, Schema, collect_blocks, parse_field_raw, parse_model_block}, utils::get_data};

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

// ─────────────────────────────── evolve (replay ops → текст схемы) ───────────────────────────────

/// Верхнеуровневый блок схемы с исходным текстом (для verbatim-переноса struct/enum)
struct SchemaBlock {
  kind: String,
  name: String,
  text: String,
}

/// Баланс фигурных скобок в строке (`{` минус `}`)
fn brace_delta(line: &str) -> i32 {
  line.bytes().filter(|&b| b == b'{').count() as i32 - line.bytes().filter(|&b| b == b'}').count() as i32
}

/// Разбивает текст схемы на блоки `model|struct|enum Name { … }`, сохраняя исходный текст каждого.
/// Конец блока — по балансу скобок (enum c payload-вариантами имеет вложенные `{ }`)
fn split_blocks(text: &str) -> Vec<SchemaBlock> {
  let mut blocks = vec![];
  let mut lines = text.lines();
  while let Some(line) = lines.next() {
    let t = line.trim();
    if !(t.starts_with("model ") || t.starts_with("struct ") || t.starts_with("enum ")) {
      continue;
    }
    let (kind, rest) = t.split_once(' ').unwrap();
    let name = rest.trim_end_matches('{').trim().to_string();
    let mut block_text = format!("{}\n", line);
    let mut depth = brace_delta(line);
    while depth > 0 {
      let Some(l) = lines.next() else { break };
      block_text.push_str(l);
      block_text.push('\n');
      depth += brace_delta(l);
    }
    blocks.push(SchemaBlock { kind: kind.to_string(), name, text: block_text });
  }
  blocks
}

/// Сериализует модель в синтаксис схемы из сырых полей (collect_blocks-форма)
fn serialize_model_block(entity: &Entity) -> String {
  let mut s = format!("model {} {{\n", entity.name);
  for field in entity.fields.iter() {
    s.push_str("  ");
    s.push_str(&field_to_spec(field));
    s.push('\n');
  }
  s.push('}');
  s
}

fn find_model<'a>(models: &'a mut [Entity], name: &str) -> Option<&'a mut Entity> {
  models.iter_mut().find(|m| m.name == name)
}

/// Выставляет/снимает index|unique-атрибут поля (хранятся отдельными ops, не в самом поле)
fn set_index_attr(field: &mut Field, unique: bool, present: bool) {
  field.attributes.retain(|a| !matches!(a, Attribute::Index | Attribute::Unique));
  if present {
    field.attributes.push(if unique { Attribute::Unique } else { Attribute::Index });
  }
}

/// Применяет операции миграции к ТЕКСТУ схемы → новый текст. Структурный «replay», обратный к
/// [`diff`]: имея сохранённую схему и `.mig`, сервер получает схему-после-миграции, не завися от
/// клиента. Модели пересобираются из сырых полей; struct/enum переносятся verbatim (v1 их не трогает)
pub fn evolve_schema(old_text: &str, ops: &[MigrationOp]) -> String {
  let (mut models, _structs, _enums) = collect_blocks(old_text);
  let mut dropped: HashSet<String> = HashSet::new();

  for op in ops {
    match op {
      MigrationOp::CreateModel { name, fields } => {
        if !models.iter().any(|m| &m.name == name) {
          models.push(Entity::new(name.clone(), fields.clone()));
        }
        dropped.remove(name);
      }
      MigrationOp::DropModel { name } => { dropped.insert(name.clone()); }
      MigrationOp::AddField { model, field } => {
        if let Some(m) = find_model(&mut models, model) {
          if !m.fields.iter().any(|f| f.name == field.name) {
            m.fields.push(field.clone());
          }
        }
      }
      MigrationOp::AlterField { model, field } => {
        if let Some(m) = find_model(&mut models, model) {
          if let Some(existing) = m.fields.iter_mut().find(|f| f.name == field.name) {
            // index/unique — отдельные ops; переносим их со старого поля на новое
            let idx_attrs: Vec<Attribute> = existing.attributes.iter()
              .filter(|a| matches!(a, Attribute::Index | Attribute::Unique)).cloned().collect();
            *existing = field.clone();
            existing.attributes.extend(idx_attrs);
          }
        }
      }
      MigrationOp::DropField { model, field } => {
        if let Some(m) = find_model(&mut models, model) {
          m.fields.retain(|f| &f.name != field);
        }
      }
      MigrationOp::AddIndex { model, field, unique } => {
        if let Some(m) = find_model(&mut models, model) {
          if let Some(f) = m.fields.iter_mut().find(|f| &f.name == field) {
            set_index_attr(f, *unique, true);
          }
        }
      }
      MigrationOp::DropIndex { model, field, .. } => {
        if let Some(m) = find_model(&mut models, model) {
          if let Some(f) = m.fields.iter_mut().find(|f| &f.name == field) {
            set_index_attr(f, false, false);
          }
        }
      }
    }
  }

  // Сборка нового текста: struct/enum — verbatim, модели — пересериализованы, в исходном порядке
  let blocks = split_blocks(old_text);
  let mut emitted: HashSet<String> = HashSet::new();
  let mut out: Vec<String> = vec![];

  for block in blocks.iter() {
    if block.kind == "model" {
      if dropped.contains(&block.name) { continue; }
      if let Some(m) = models.iter().find(|m| m.name == block.name) {
        out.push(serialize_model_block(m));
        emitted.insert(block.name.clone());
      }
    } else {
      out.push(block.text.trim_end().to_string());
    }
  }

  // Новые модели (CreateModel), которых не было в исходном тексте — в конец
  for m in models.iter() {
    if !emitted.contains(&m.name) && !dropped.contains(&m.name) {
      out.push(serialize_model_block(m));
    }
  }

  out.join("\n\n")
}

// ─────────────────────────────── apply (исполнение против БД) ───────────────────────────────

/// Служебное дерево с состоянием миграций: ключи `schema` (текст .marci), `version` (u64 BE)
/// и `applied` (ledger применённых миграций — id через `\n`)
pub const META_TREE: &[u8] = b"__marci_meta__";

#[derive(Debug)]
pub enum MigrationApplyError {
  /// Операция не поддержана в v1 (drop field, реордер, смена типа …)
  Unsupported(&'static str),
  /// `add unique` нашёл дубликаты в существующих данных
  UniqueViolation { field: String },
  /// Ошибка вычисления диффа (в `migrate_to`)
  Diff(MigrationError),
  /// Присланная история миграций расходится с применённой на сервере (ledger).
  /// Применённые миграции должны быть префиксом присланных — иначе порядок/содержимое разошлись
  HistoryDiverged { position: usize, applied: String, incoming: String },
  Storage(StorageError),
}

impl From<canopydb::Error> for MigrationApplyError {
  fn from(e: canopydb::Error) -> Self { MigrationApplyError::Storage(StorageError(e)) }
}
impl From<StorageError> for MigrationApplyError {
  fn from(e: StorageError) -> Self { MigrationApplyError::Storage(e) }
}
impl From<MigrationError> for MigrationApplyError {
  fn from(e: MigrationError) -> Self { MigrationApplyError::Diff(e) }
}

/// Проверяет, что слоты существующих полей не изменились: новые поля должны дописываться
/// в КОНЕЦ модели. Формат требует «порядок payload == порядок слотов», поэтому перестановка
/// или вставка поля в середину сдвинула бы оффсеты существующих полей и сломала старые строки.
/// Безопасный гард вместо молчаливой порчи; полная поддержка реордера — следующий шаг.
fn check_layout_stable(old: &Schema, new: &Schema) -> Result<(), MigrationApplyError> {
  let old_by: HashMap<&str, &Entity> = old.models.iter().map(|m| (m.name.as_str(), m)).collect();

  for model in new.models.iter() {
    let Some(old_model) = old_by.get(model.name.as_str()) else { continue };

    let old_slot: HashMap<&str, usize> = old_model.fields.iter()
      .filter_map(|f| if let FieldLocation::Body { offset_pos } = f.location { Some((f.name.as_str(), offset_pos)) } else { None })
      .collect();

    for f in model.fields.iter() {
      if let FieldLocation::Body { offset_pos } = f.location {
        if let Some(&old_off) = old_slot.get(f.name.as_str()) {
          if old_off != offset_pos {
            return Err(MigrationApplyError::Unsupported(
              "поле переставлено/вставлено в середину модели — в v1 добавляйте новые поля в конец"));
          }
        }
      }
    }
  }
  Ok(())
}

/// Исполняет физические операции миграции в открытой write-транзакции.
/// add/alter field — только метаданные (формат v2, без переписывания строк);
/// add index — строит дерево из существующих строк; drop index — удаляет дерево.
pub fn apply_ops(tx: &WriteTransaction, ops: &[MigrationOp], old: &Schema, new: &Schema) -> Result<(), MigrationApplyError> {
  // Слоты существующих полей должны сохраниться — иначе старые строки сломаются
  check_layout_stable(old, new)?;

  for op in ops {
    match op {
      // Метаданные: новый слот уже в `new`, старые строки читаются forward-compatible reader'ом
      MigrationOp::AddField { .. } | MigrationOp::AlterField { .. } => {}
      MigrationOp::AddIndex { model, field, .. } => build_index(tx, new, model, field)?,
      MigrationOp::DropIndex { model, field, .. } => drop_index(tx, old, model, field)?,
      MigrationOp::CreateModel { name, .. } => create_entity_trees(tx, find_entity(new, name))?,
      MigrationOp::DropModel { name } => drop_entity_trees(tx, find_entity(old, name))?,
      MigrationOp::DropField { .. } => return Err(MigrationApplyError::Unsupported("drop field (slot tombstone) пока не поддержан")),
    }
  }
  Ok(())
}

/// Создаёт деревья сущности: основное + индексные + relation-index. Используется и при инициализации
/// `MarciDB`, и при apply `CreateModel` (модель пустая, бэкфилл не нужен)
pub fn create_entity_trees(tx: &WriteTransaction, entity: &Entity) -> Result<(), canopydb::Error> {
  tx.get_or_create_tree(entity.name.as_bytes())?;
  for field in entity.fields.iter() {
    if let FieldType::Ref(ref_info) | FieldType::RefList(ref_info) = &field.ty {
      if let RefBinding::IndexTree(tree_name) = &ref_info.binding {
        tx.get_or_create_tree(tree_name.as_bytes())?;
      }
    }
    for index in field.indexes.iter() {
      tx.get_or_create_tree(index.tree_name())?;
    }
  }
  Ok(())
}

/// Удаляет все деревья сущности (основное + индексные + relation-index)
fn drop_entity_trees(tx: &WriteTransaction, entity: &Entity) -> Result<(), canopydb::Error> {
  for field in entity.fields.iter() {
    if let FieldType::Ref(ref_info) | FieldType::RefList(ref_info) = &field.ty {
      if let RefBinding::IndexTree(tree_name) = &ref_info.binding {
        tx.delete_tree(tree_name.as_bytes())?;
      }
    }
    for index in field.indexes.iter() {
      tx.delete_tree(index.tree_name())?;
    }
  }
  tx.delete_tree(entity.name.as_bytes())?;
  Ok(())
}

fn find_entity<'a>(schema: &'a Schema, name: &str) -> &'a Entity {
  schema.models.iter().find(|m| m.name == name).unwrap_or_else(|| panic!("model {} not found", name))
}

fn find_field<'a>(entity: &'a Entity, name: &str) -> &'a Field {
  entity.fields.iter().find(|f| f.name == name).unwrap_or_else(|| panic!("field {}.{} not found", entity.name, name))
}

/// Строит индексное дерево поля из всех существующих строк модели (бэкфилл).
/// Для уже добавленного только что поля старые строки дают `None` (поле отсутствует) — корректно
fn build_index(tx: &WriteTransaction, schema: &Schema, model: &str, field_name: &str) -> Result<(), MigrationApplyError> {
  let entity = find_entity(schema, model);
  let field = find_field(entity, field_name);
  if field.indexes.is_empty() {
    return Err(MigrationApplyError::Unsupported("индекс по полю такого типа пока не поддержан"));
  }

  let model_tree = tx.get_tree(entity.name.as_bytes())?.unwrap();
  for index in field.indexes.iter() {
    let mut index_tree = tx.get_or_create_tree(index.tree_name())?;
    for row in model_tree.iter()? {
      let (id, body) = row?;
      let Some(value) = get_data(entity, field, &id, &body, schema) else { continue };
      if index.is_unique() {
        let encoded = encode_index(field, index, value);
        if index_tree.prefix_keys(&encoded)?.next().transpose()?.is_some() {
          return Err(MigrationApplyError::UniqueViolation { field: field.full_name.clone() });
        }
      }
      index_tree.insert(&encode_full_index(field, index, &id, value), &[])?;
    }
  }
  Ok(())
}

fn drop_index(tx: &WriteTransaction, old: &Schema, model: &str, field_name: &str) -> Result<(), MigrationApplyError> {
  let field = find_field(find_entity(old, model), field_name);
  for index in field.indexes.iter() {
    tx.delete_tree(index.tree_name())?;
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

  /// evolve — обратная к diff: применив дифф к старой схеме, получаем текст,
  /// структурно эквивалентный новой (повторный diff пуст)
  #[test]
  fn evolve_is_inverse_of_diff() {
    let cases = [
      ("model User {\n  name String\n}",
       "model User {\n  name String\n  age UInt @index\n}"),
      ("model A {\n  x String\n}\nmodel Old {\n  y Int\n}",
       "model A {\n  x String?\n}\nmodel New {\n  z String\n}"),
      ("model P {\n  slug String @index\n}",
       "model P {\n  slug String @unique\n}"),
    ];
    for (old, new) in cases {
      let ops = diff(old, new).unwrap();
      let evolved = evolve_schema(old, &ops);
      assert!(diff(&evolved, new).unwrap().is_empty(),
        "evolve(old, diff(old,new)) != new.\nevolved:\n{}", evolved);
    }
  }

  /// enum переносится evolve verbatim (вложенные `{ }` payload-варианта не рвут блок),
  /// миграция поля модели его не задевает
  #[test]
  fn evolve_preserves_enum_block() {
    let old = "model Account {\n  name String\n  type AccountType\n}\nenum AccountType {\n  basic\n  pro {\n    sign String\n  }\n}";
    let new = "model Account {\n  name String\n  type AccountType\n  active Bool\n}\nenum AccountType {\n  basic\n  pro {\n    sign String\n  }\n}";
    let ops = diff(old, new).unwrap();
    let evolved = evolve_schema(old, &ops);
    assert!(evolved.contains("enum AccountType"), "enum потерян:\n{}", evolved);
    assert!(evolved.contains("sign String"), "payload-вариант потерян:\n{}", evolved);
    assert!(diff(&evolved, new).unwrap().is_empty(), "evolved:\n{}", evolved);
  }
}
