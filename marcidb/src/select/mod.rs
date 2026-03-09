use bitvec::prelude::*;
use std::{collections::HashMap, sync::Arc};
use bitvec::vec::BitVec;
use crate::{marci_db::{get_end, get_offset_from_field}, schema::{Aliases, Entity, Field, FieldType}};

pub use crate::select::process_select::{process_data, TransationContext,ProcessDataContext};

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
  pub aliases: Option<&'a Aliases>,
  pub model: &'a Entity
}

impl MarciSelect<'_> {
  pub fn all(model: &'_ Entity) -> MarciSelect<'_> {
    return MarciSelect {
      model,
      mask: bitvec![1; model.fields.len()],
      includes: vec![],
      aliases: None,
      enum_selects: model.fields.iter().enumerate().filter_map(|(i, field)| {
        let FieldType::Enum(en) = &field.ty else { return None; };

        let variants_map: HashMap<u16, MarciSelect<'_>> = en
          .variants
          .iter()
          .enumerate()
          .filter_map(|(i, v)| {
            if v.fields.is_empty() {
                None
            } else {
                Some((i as u16, MarciSelect::all(&v)))
            }
          })
          .collect();

        if variants_map.is_empty() { return None; };

        return Some((i, variants_map))
      }).collect()
    };
  }

  pub fn new(model: &'_ Entity) -> MarciSelect<'_> {
    return MarciSelect {
      mask: bitvec![0; model.fields.len()],
      includes: vec![],
      enum_selects: HashMap::new(),
      aliases: None,
      model
    }
  }
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
  One(&'a Field,Arc<U>),
  Many(&'a Field,Vec<Arc<U>>)
}

// TODO: Make this method without size
#[inline(always)]
pub fn get_value_from_data<'a>(field: &'a Field, id: &'a[u8], data: &'a[u8], known_size: Option<usize>) -> Option<&'a[u8]> {
  if let Some(id_idx) = field.id_idx {
    return Some(get_value_from_id(id, id_idx, field))
  } else {
    let offset = get_offset_from_field(data, field);
    if offset == 0 {
      return None;
    }

    let end = known_size.map(|size| offset + size).unwrap_or_else(|| {
      get_end(data, field.offset_pos, u16::from_be_bytes(data[1..3].try_into().unwrap()) as usize)
    });
    Some(&data[offset..end])
  }
}

#[inline(always)]
pub fn get_value_from_id<'a>(id: &'a [u8], id_idx: usize, field: &'a Field) -> &'a [u8] {
  if id.len() < id_idx*8+8 {
    panic!("ID too small. Field: {}, ID: {:?}, idx: {}", field.name, id, id_idx);
  }
  return &id[id_idx*8..id_idx*8+8];
}