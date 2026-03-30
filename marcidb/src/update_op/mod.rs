mod process_update;

use crate::{Field, InsertError, delete_op::{DeleteError, DeleteOp}, num_utils::NumberValue, schema::{Entity, RefInfo}, write_op::{WriteIndexesError, WriteOp}};
pub use process_update::process_update;

#[derive(Debug)]
pub struct UpdateOp<'a> {
  pub update_fields: Vec<UpdateField<'a>>,
  pub update_refs: Vec<UpdateRelation<'a>>,
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
  DeleteError(DeleteError),
  InsertError(InsertError),
  WriteIndexesError(WriteIndexesError)
}

#[derive(Debug)]
pub struct UpdateRelation<'a> {
  pub field: &'a Field,
  pub ref_info: &'a RefInfo,
  pub rev_ref_info: Option<&'a RefInfo>,
  pub st: &'a Entity,
  pub op: UpdateRelationOp<'a>
}

#[derive(Debug)]
pub enum UpdateRelationOp<'a> {
  /// Удаляет объекты
  Remove(DeleteOp<'a>),
  /// Удаляет связи без удаления объектов
  DisconnectAll,
  /// Заменяет все объекты на новый объект
  // Replace(WriteOp<'a>,DeleteOp<'a>),
  /// Создает объект только там, где поле = null
  Create(WriteOp<'a>),
  /// Обновляет объект, если он не null
  Update(UpdateOp<'a>),

  /// Добавляет элементы в массив
  Push(Vec<WriteOp<'a>>),
  // /// Заменяет все элементы на элементы из массива
  RemoveAll(DeleteOp<'a>),
  /// Удаляет элементы из массива
  RemoveItems(Vec<Vec<u8>>,DeleteOp<'a>),

  /// Связывает сущность с существующими объектами
  Connect(Vec<Vec<u8>>),
  /// Удаляет связи с существующими объектами
  Disconnect(Vec<Vec<u8>>),
}