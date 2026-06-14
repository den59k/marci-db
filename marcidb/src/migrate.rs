//! Новый движок миграций поверх materialized-снапшота (см. [`crate::snapshot`]).
//! Работает на ПЛОСКИХ резолвнутых `Schema` — struct уже модели, enum уже впечатан в поля,
//! ссылки/слоты/биндинги запинены. Поэтому diff тривиален: сравнение entities и полей по имени,
//! без раскрытия сахара. Это полностью заменило старый текстовый движок миграций.

use std::collections::HashMap;

use canopydb::WriteTransaction;

use crate::index_utils::{encode_full_index, encode_index};
use crate::schema::{Attribute, Entity, EnumInfo, Field, FieldExistsCondition, FieldLocation, FieldType, RefBinding, Schema, SchemaError};
use crate::snapshot::serialize_type;
use crate::utils::get_data;
use crate::StorageError;

/// Размер «pre-header» строки (первый Body-слот начинается с этого байтового оффсета)
const PRE_HEADER: usize = 4;

/// Операция миграции над плоской схемой. Сущности/поля адресуются по имени;
/// резолвнутые поля берутся из `new`/`old` схемы при apply.
#[derive(Debug, Clone, PartialEq)]
pub enum MigrateOp {
  CreateEntity { name: String },
  DropEntity { name: String },
  AddField { entity: String, field: String },
  DropField { entity: String, field: String },
  /// Метаданные поля: nullable / default / format / добавленные варианты enum
  AlterField { entity: String, field: String },
  AddIndex { entity: String, field: String, unique: bool },
  DropIndex { entity: String, field: String, unique: bool },
}

#[derive(Debug, PartialEq)]
pub enum MigrateError {
  /// Смена типа поля требует трансформации данных — не поддержано
  UnsupportedTypeChange { entity: String, field: String, from: String, to: String },
  /// Смена ключа (@id) требует re-key — не поддержано
  UnsupportedKeyChange { entity: String, field: String },
  /// Слот существующего поля сдвинулся — старые строки сломались бы (нужна сверка слотов до diff)
  UnsupportedLayoutChange { entity: String, field: String },
  /// Деструктивное изменение enum (вариант удалён или переназначен id) — требует миграции данных
  UnsupportedEnumChange { entity: String, field: String, detail: String },
}

impl std::fmt::Display for MigrateError {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      MigrateError::UnsupportedTypeChange { entity, field, from, to } =>
        write!(f, "unsupported type change on {}.{}: {} -> {} (migrate the data manually)", entity, field, from, to),
      MigrateError::UnsupportedKeyChange { entity, field } =>
        write!(f, "unsupported key change on {}.{}", entity, field),
      MigrateError::UnsupportedLayoutChange { entity, field } =>
        write!(f, "field slot moved on {}.{} — existing rows would break (slots must be carried from the snapshot)", entity, field),
      MigrateError::UnsupportedEnumChange { entity, field, detail } =>
        write!(f, "unsupported enum change on {}.{}: {}", entity, field, detail),
    }
  }
}

/// Дифф двух плоских схем: `old` (старый снапшот) → `new` (материализованная новая схема).
/// Обе должны быть в snapshot-форме (слоты/варианты согласованы); сверка слотов — отдельный шаг до diff.
pub fn diff(old: &Schema, new: &Schema) -> Result<Vec<MigrateOp>, MigrateError> {
  let old_by: HashMap<&str, &_> = old.models.iter().map(|e| (e.name.as_str(), e)).collect();
  let new_by: HashMap<&str, &_> = new.models.iter().map(|e| (e.name.as_str(), e)).collect();

  let mut ops = vec![];

  // Новые сущности (apply создаст дерево + индексные/relation-деревья из полей)
  for e in new.models.iter() {
    if !old_by.contains_key(e.name.as_str()) {
      ops.push(MigrateOp::CreateEntity { name: e.name.clone() });
    }
  }

  // Изменения внутри существующих сущностей
  for e in new.models.iter() {
    if let Some(old_e) = old_by.get(e.name.as_str()) {
      diff_fields(old, new, &e.name, old_e, e, &mut ops)?;
    }
  }

  // Удалённые сущности
  for e in old.models.iter() {
    if !new_by.contains_key(e.name.as_str()) {
      ops.push(MigrateOp::DropEntity { name: e.name.clone() });
    }
  }

  Ok(ops)
}

fn diff_fields(
  old_schema: &Schema, new_schema: &Schema,
  entity: &str, old_e: &crate::schema::Entity, new_e: &crate::schema::Entity,
  ops: &mut Vec<MigrateOp>,
) -> Result<(), MigrateError> {
  let old_by: HashMap<&str, &Field> = old_e.fields.iter().map(|f| (f.name.as_str(), f)).collect();
  let new_by: HashMap<&str, &Field> = new_e.fields.iter().map(|f| (f.name.as_str(), f)).collect();

  // Добавленные поля (+ индекс отдельной операцией)
  for f in new_e.fields.iter() {
    if !old_by.contains_key(f.name.as_str()) {
      ops.push(MigrateOp::AddField { entity: entity.to_string(), field: f.name.clone() });
      if let Some(unique) = index_kind(f) {
        ops.push(MigrateOp::AddIndex { entity: entity.to_string(), field: f.name.clone(), unique });
      }
    }
  }

  // Изменённые поля
  for f in new_e.fields.iter() {
    let Some(old_f) = old_by.get(f.name.as_str()) else { continue };

    if is_key(old_f) != is_key(f) {
      return Err(MigrateError::UnsupportedKeyChange { entity: entity.to_string(), field: f.name.clone() });
    }
    if let (FieldLocation::Body { offset_pos: a }, FieldLocation::Body { offset_pos: b }) = (&old_f.location, &f.location) {
      if a != b {
        return Err(MigrateError::UnsupportedLayoutChange { entity: entity.to_string(), field: f.name.clone() });
      }
    }

    // Сравнение типа: enum — отдельно (добавление вариантов допустимо), остальное — по имени
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

    // Метаданные: nullable / default / format
    if old_f.nullable != f.nullable || default_attr(old_f) != default_attr(f) || format_attr(old_f) != format_attr(f) {
      needs_alter = true;
    }
    if needs_alter {
      ops.push(MigrateOp::AlterField { entity: entity.to_string(), field: f.name.clone() });
    }

    // Индекс: снять старый / поставить новый
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

  // Удалённые поля
  for f in old_e.fields.iter() {
    if !new_by.contains_key(f.name.as_str()) {
      if let Some(unique) = index_kind(f) {
        ops.push(MigrateOp::DropIndex { entity: entity.to_string(), field: f.name.clone(), unique });
      }
      ops.push(MigrateOp::DropField { entity: entity.to_string(), field: f.name.clone() });
    }
  }

  Ok(())
}

// ─────────────────────────────── сверка слотов ───────────────────────────────

/// Переносит слоты Body-полей из старого снапшота в новую материализованную схему.
/// Совпавшим по имени полям — слот из `old`; новым — следующий свободный (append-only, выше
/// высшей точки старой entity). Так «владение слотами» переходит из парсера (порядок объявления,
/// нестабильно при вставке поля в середину) в миграционный слой (история). Запускать ДО [`diff`].
///
/// Запинены только слоты; id вариантов enum (тоже order-dependent) — отдельная сверка (TODO).
pub fn reconcile_slots(new: &mut Schema, old: &Schema) {
  let old_by: HashMap<&str, &Entity> = old.models.iter().map(|e| (e.name.as_str(), e)).collect();

  for entity in new.models.iter_mut() {
    // Новой entity (нет в old) переназначать нечего — слоты парсера годятся
    let Some(old_entity) = old_by.get(entity.name.as_str()) else { continue };

    // Старые слоты по имени + высшая точка (max байтовый оффсет)
    let mut old_slots: HashMap<&str, usize> = HashMap::new();
    let mut max_offset = 0usize;
    for f in old_entity.fields.iter() {
      if let FieldLocation::Body { offset_pos } = f.location {
        old_slots.insert(f.name.as_str(), offset_pos);
        max_offset = max_offset.max(offset_pos);
      }
    }
    // Следующий свободный слот: выше старой высшей точки (или с самого начала, если Body не было).
    // Так новые поля никогда не наезжают на перенесённые (а позже — на ретайр-слоты).
    let mut next = if max_offset == 0 { PRE_HEADER } else { max_offset + 4 };

    for f in entity.fields.iter_mut() {
      if let FieldLocation::Body { offset_pos } = &mut f.location {
        match old_slots.get(f.name.as_str()) {
          Some(&old_off) => *offset_pos = old_off,   // совпало по имени — берём старый слот
          None => { *offset_pos = next; next += 4; } // новое поле — следующий свободный
        }
      }
    }

    entity.payload_offset = next; // размер заголовка = за последним слотом
  }

  // После переноса слотов порядок полей в массиве мог разойтись с порядком слотов
  // (новое поле в середине объявления получило высокий слот) — канонизируем
  canonicalize_field_order(new);
}

/// Приводит порядок полей каждой entity к каноническому: ключи (по key-index), затем Body
/// (по offset_pos), затем Virtual. Это инвариант формата строки: писатель пишет Body-поля в
/// порядке МАССИВА, а `get_end` читает их в порядке СЛОТОВ — порядки обязаны совпадать.
/// Индексные ссылки (condition.field_index, enum variants, rev_field_idx) переотображаются.
fn canonicalize_field_order(schema: &mut Schema) {
  // 1) для каждой entity — порядок order[new] = old и обратная перестановка perm[old] = new
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

  // 2) переотображаем индексные ссылки (значения в полях ещё ссылаются на СТАРЫЕ индексы)
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

/// Ключ сортировки поля в каноническом порядке: (категория, под-порядок)
fn field_sort_key(field: &Field, original_index: usize) -> (u8, usize) {
  match field.location {
    FieldLocation::Key { index } => (0, index),
    FieldLocation::Body { offset_pos } => (1, offset_pos),
    FieldLocation::Virtual => (2, original_index),
  }
}

// ─────────────────────────────── сравнение типов ───────────────────────────────

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

/// Допустимо только ДОБАВЛЕНИЕ вариантов: каждый старый `name→id` обязан сохраниться с тем же id.
/// Удаление/переназначение id — деструктивно (хранимые u16-дискриминанты стали бы означать другое).
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

// ─────────────────────────────── атрибуты поля ───────────────────────────────

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

// ─────────────────────────────── apply (исполнение против БД) ───────────────────────────────

/// Служебное дерево с состоянием миграций: ключи `schema` (materialized-снапшот), `version` (u64 BE)
/// и `applied` (ledger применённых id миграций через `\n`)
pub const META_TREE: &[u8] = b"__marci_meta__";

#[derive(Debug)]
pub enum MigrateApplyError {
  /// Операция не поддержана (пока): drop field (нужен tombstone слота) и т.п.
  Unsupported(&'static str),
  /// `add unique` нашёл дубликаты в существующих данных
  UniqueViolation { field: String },
  /// Ошибка вычисления диффа
  Diff(MigrateError),
  /// Присланная история миграций расходится с применённой (ledger): применённое должно быть префиксом
  HistoryDiverged { position: usize, applied: String, incoming: String },
  /// Невалидный снапшот/схема
  Schema(SchemaError),
  Storage(StorageError),
}

impl std::fmt::Display for MigrateApplyError {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      MigrateApplyError::Unsupported(s) => write!(f, "{}", s),
      MigrateApplyError::UniqueViolation { field } => write!(f, "unique violation on {} in existing data", field),
      MigrateApplyError::Diff(e) => write!(f, "{}", e),
      MigrateApplyError::HistoryDiverged { position, applied, incoming } =>
        write!(f, "migration history diverged at #{}: server applied \"{}\", got \"{}\"", position, applied, incoming),
      MigrateApplyError::Schema(e) => write!(f, "{}", e),
      MigrateApplyError::Storage(e) => write!(f, "{:?}", e),
    }
  }
}

impl From<canopydb::Error> for MigrateApplyError { fn from(e: canopydb::Error) -> Self { MigrateApplyError::Storage(StorageError(e)) } }
impl From<StorageError> for MigrateApplyError { fn from(e: StorageError) -> Self { MigrateApplyError::Storage(e) } }
impl From<SchemaError> for MigrateApplyError { fn from(e: SchemaError) -> Self { MigrateApplyError::Schema(e) } }
impl From<MigrateError> for MigrateApplyError { fn from(e: MigrateError) -> Self { MigrateApplyError::Diff(e) } }

/// Исполняет физические операции миграции в открытой write-транзакции.
/// add/alter field — только метаданные (формат v2, без переписывания строк); add index — строит
/// дерево из существующих строк; drop index — удаляет дерево; create/drop entity — деревья сущности.
/// Сдвиг слотов уже отклонён в [`diff`] (`UnsupportedLayoutChange`), поэтому здесь его не проверяем.
pub fn apply(tx: &WriteTransaction, old: &Schema, new: &Schema, ops: &[MigrateOp]) -> Result<(), MigrateApplyError> {
  for op in ops {
    match op {
      // Метаданные: новый слот уже в `new`, старые строки читаются forward-compatible reader'ом
      MigrateOp::AddField { .. } | MigrateOp::AlterField { .. } => {}
      MigrateOp::AddIndex { entity, field, .. } => build_index(tx, new, entity, field)?,
      MigrateOp::DropIndex { entity, field, .. } => drop_index(tx, old, entity, field)?,
      MigrateOp::CreateEntity { name } => create_entity_trees(tx, find_entity(new, name))?,
      MigrateOp::DropEntity { name } => drop_entity_trees(tx, find_entity(old, name))?,
      MigrateOp::DropField { .. } => return Err(MigrateApplyError::Unsupported("drop field (slot tombstone) пока не поддержан")),
    }
  }
  Ok(())
}

/// Создаёт деревья сущности: основное + индексные + relation-index. Используется и при инициализации
/// `MarciDB`, и при apply `CreateEntity` (сущность пустая, бэкфилл не нужен)
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
  schema.models.iter().find(|m| m.name == name).unwrap_or_else(|| panic!("entity {} not found", name))
}

fn find_field<'a>(entity: &'a Entity, name: &str) -> &'a Field {
  entity.fields.iter().find(|f| f.name == name).unwrap_or_else(|| panic!("field {}.{} not found", entity.name, name))
}

/// Строит индексное дерево поля из всех существующих строк сущности (бэкфилл).
/// Для только что добавленного поля старые строки дают `None` (поле отсутствует) — корректно
fn build_index(tx: &WriteTransaction, schema: &Schema, entity: &str, field_name: &str) -> Result<(), MigrateApplyError> {
  let entity = find_entity(schema, entity);
  let field = find_field(entity, field_name);
  if field.indexes.is_empty() {
    return Err(MigrateApplyError::Unsupported("индекс по полю такого типа пока не поддержан"));
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
          return Err(MigrateApplyError::UniqueViolation { field: field.full_name.clone() });
        }
      }
      index_tree.insert(&encode_full_index(field, index, &id, value), &[])?;
    }
  }
  Ok(())
}

fn drop_index(tx: &WriteTransaction, old: &Schema, entity: &str, field_name: &str) -> Result<(), MigrateApplyError> {
  let field = find_field(find_entity(old, entity), field_name);
  for index in field.indexes.iter() {
    tx.delete_tree(index.tree_name())?;
  }
  Ok(())
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::parse_schema;

  fn diff_text(old: &str, new: &str) -> Result<Vec<MigrateOp>, MigrateError> {
    diff(&parse_schema(old), &parse_schema(new))
  }

  #[test]
  fn diff_create_and_drop_entity() {
    let old = "model User {\n  name String\n}";
    let new = "model User {\n  name String\n}\nmodel Post {\n  title String\n}";
    assert_eq!(diff_text(old, new).unwrap(), vec![MigrateOp::CreateEntity { name: "Post".into() }]);
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
    // email: @index → нет индекса (drop), и стал nullable (alter)
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
    // struct разворачивается в модель Parent.field — добавление struct-поля = новая entity + alter? нет:
    // info становится новой моделью User.info; в User появляется виртуальное поле info
    let old = "model User {\n  name String\n}";
    let new = "model User {\n  name String\n  info Info?\n}\nstruct Info {\n  bio String\n}";
    let ops = diff_text(old, new).unwrap();
    assert!(ops.contains(&MigrateOp::CreateEntity { name: "User.info".into() }));
    assert!(ops.contains(&MigrateOp::AddField { entity: "User".into(), field: "info".into() }));
  }

  #[test]
  fn diff_enum_add_variant_is_additive() {
    // Добавление варианта enum в конец: дискриминант → AlterField, новый payload → AddField
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

  // ─────────── reconcile_slots ───────────

  fn body_offset(schema: &Schema, entity: &str, field: &str) -> usize {
    let e = schema.models.iter().find(|m| m.name == entity).unwrap();
    let f = e.fields.iter().find(|f| f.name == field).unwrap();
    match f.location { FieldLocation::Body { offset_pos } => offset_pos, _ => panic!("{}.{} не Body", entity, field) }
  }

  /// Вставка поля в середину сдвигает слоты в parse_schema → без сверки diff отклонит как layout change
  #[test]
  fn insert_middle_shifts_slots_without_reconcile() {
    let old = parse_schema("model M {\n  a String\n  b String\n}");
    let new = parse_schema("model M {\n  a String\n  c String\n  b String\n}");
    // b сдвинулся 8 → 12
    assert_eq!(body_offset(&old, "M", "b"), 8);
    assert_eq!(body_offset(&new, "M", "b"), 12);
    assert!(matches!(diff(&old, &new), Err(MigrateError::UnsupportedLayoutChange { .. })));
  }

  /// Со сверкой: совпавшие поля держат старые слоты, новое получает следующий свободный
  #[test]
  fn reconcile_carries_slots_then_diff_is_clean() {
    let old = parse_schema("model M {\n  a String\n  b String\n}");
    let mut new = parse_schema("model M {\n  a String\n  c String\n  b String\n}");

    reconcile_slots(&mut new, &old);

    // a и b сохранили слоты из old; c — следующий свободный (после max=8 → 12)
    assert_eq!(body_offset(&new, "M", "a"), 4);
    assert_eq!(body_offset(&new, "M", "b"), 8);
    assert_eq!(body_offset(&new, "M", "c"), 12);

    // теперь diff чистый: только добавление c
    assert_eq!(diff(&old, &new).unwrap(), vec![MigrateOp::AddField { entity: "M".into(), field: "c".into() }]);
  }

  /// Новой entity (нет в old) сверка не трогает — слоты парсера годятся
  #[test]
  fn reconcile_leaves_new_entity_untouched() {
    let old = parse_schema("model M {\n  a String\n}");
    let mut new = parse_schema("model M {\n  a String\n}\nmodel N {\n  x String\n  y String\n}");
    reconcile_slots(&mut new, &old);
    assert_eq!(body_offset(&new, "N", "x"), 4);
    assert_eq!(body_offset(&new, "N", "y"), 8);
  }
}
