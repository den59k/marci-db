use std::collections::HashMap;
use bitvec::vec::BitVec;
use crate::{marci_db::get_offset_from_field, schema::{Aliases, Entity, Field}};

pub use crate::select::process_select::{process_data, ProcessDataContext};

mod process_select;


#[derive(Debug)]
pub struct MarciSelectInclude<'a> {
  pub field: &'a Field,
  pub model: &'a Entity,
  pub select: MarciSelect<'a>,
  pub select_only_id: bool,
  pub binding: MarciSelectBinding<'a>,
  pub injected: Option<Injected<'a>>
}

#[derive(Debug)]
pub struct Injected<'a> {
  pub st: &'a Entity,
  pub select: MarciSelect<'a>
}

#[derive(Debug)]
pub enum MarciSelectBinding<'a> {
  One (),
  Many(&'a[u8]),
  OneStruct(),
  ManyStruct(),
}

pub type EnumSelect<'a> = HashMap<usize, HashMap<u16, MarciSelect<'a>>>;

#[derive(Debug)]
pub struct MarciSelect<'a> {
  pub mask: BitVec,
  pub includes: Vec<MarciSelectInclude<'a>>,
  pub enum_selects: EnumSelect<'a>,
  pub aliases: Option<&'a Aliases>
}

pub struct DecodeCtx<'a, U> {
  pub id: &'a [u8],
  pub data: &'a [u8],
  pub entity: &'a Entity,
  pub select: &'a BitVec,
  pub includes: Vec<IncludeResult<'a, U>>,
  pub inject: Option<U>,
  pub aliases: Option<&'a Aliases>
}

pub enum IncludeResult<'a, U> {
  None(&'a Field),
  One(&'a Field,U),
  Many(&'a Field,Vec<U>)
}


// TODO: Make this method without size
#[inline(always)]
pub fn get_value_from_data<'a>(field: &'a Field, id: &'a[u8], data: &'a[u8], size: usize) -> Option<&'a[u8]> {
  if let Some(id_idx) = field.id_idx {
    return Some(get_value_from_id(id, id_idx, field))
  } else {
    let offset = get_offset_from_field(data, field);
    if offset == 0 {
      return None;
    }
    Some(&data[offset..offset + size])
  }
}

#[inline(always)]
pub fn get_value_from_id<'a>(id: &'a [u8], id_idx: usize, field: &'a Field) -> &'a [u8] {
  if id.len() < id_idx*8+8 {
    panic!("ID too small. Field: {}, ID: {:?}, idx: {}", field.name, id, id_idx);
  }
  return &id[id_idx*8..id_idx*8+8];
}
