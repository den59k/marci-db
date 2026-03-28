mod process_update;

use crate::{Field, num_utils::NumberValue, schema::Entity, write_op::WriteOp};
pub use process_update::process_update;

#[derive(Debug)]
pub struct UpdateOp<'a> {
  pub fields: Vec<UpdateField<'a>>,
  pub refs: Vec<UpdateRelation<'a>>,
}

#[derive(Debug)]
pub struct UpdateField<'a> {
  pub field: &'a Field,
  pub offset_pos: usize,
  pub value: UpdateValue
}

#[derive(Debug)]
pub enum UpdateValue {
  Null,
  Value(Vec<u8>),
  Increment(NumberValue)
}

#[derive(Debug,PartialEq)]
pub enum UpdateError {
  ItemNotFound,
  UniqueViolation(String, Vec<u8>),
}

#[derive(Debug)]
pub struct UpdateRelation<'a> {
  field: &'a Field,
  st: &'a Entity,
  op: UpdateRelationOp<'a>
}

#[derive(Debug)]
pub enum UpdateRelationOp<'a> {
  /// Заменяет все объекты на новый объект
  Replace(WriteOp<'a>),
  /// Создает объект только там, где поле = null
  Create(WriteOp<'a>),
  /// Обновляет объект, если он не null
  Update(UpdateOp<'a>),
  /// Удаляет объект
  SetNull,

  /// Добавляет элементы в массив
  Push(Vec<WriteOp<'a>>),
  /// Заменяет все элементы на элементы из массива
  ReplaceAll(Vec<WriteOp<'a>>),
  /// Удаляет все элементы из массива
  Remove(Vec<Vec<u8>>),

  /// Связывает сущность с существующими объектами
  Connect(Vec<Vec<u8>>),
  /// Удаляет связи с существующими объектами
  Disconnect(Vec<Vec<u8>>),
}