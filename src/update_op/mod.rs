mod process_update;

use crate::{Field, num_utils::NumberValue};
pub use process_update::process_update;

#[derive(Debug)]
pub struct UpdateOp<'a> {
  pub fields: Vec<UpdateField<'a>>
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

#[derive(Debug)]
pub enum UpdateError {
  ItemNotFound
}