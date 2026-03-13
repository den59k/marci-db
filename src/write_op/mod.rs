use bitvec::vec::BitVec;

mod process_write;
use crate::{Field, schema::{Entity, FieldDefault}};

pub use process_write::{write_data,InsertError};

#[derive(Debug)]
pub struct WriteOp<'a> {
  pub id: Vec<u8>,
  pub data: Vec<u8>,
  pub refs: Vec<WriteRelation<'a>>,
  pub mask: BitVec,
  pub entity: &'a Entity,
  pub defaults: Vec<WriteDefault<'a>>
}

#[derive(Debug)]
pub enum WriteDefault<'a> {
    // Записать значение в Key в заданный offset
    Key(usize, &'a FieldDefault),
    // Записать значение в Body в заданный offset
    Body(usize, &'a FieldDefault),
    // Записать значение parentId в Key в заданный offset
    ParentId(usize)
}

#[derive(Debug)]
pub enum WriteRelation<'a> {
    None {
        field: &'a Field,
        st: &'a Entity,
    },
    Empty {
        field: &'a Field,
        st: &'a Entity,
    },
    Create {
        field: &'a Field,
        op: WriteOp<'a>
    },
    CreateMany {
        field: &'a Field,
        ops: Vec<WriteOp<'a>>,
    },
    Connect {
        field: &'a Field,
        st: &'a Entity,
        ids: Vec<Vec<u8>>
    },
    Update {
        field: &'a Field,
        op: WriteOp<'a>
    },
    Push {
        field: &'a Field,
        op: WriteOp<'a>
    },
}