mod process_write;
use crate::{Field, schema::{Entity, FieldDefault}};

pub use process_write::{write_data,InsertError};

#[derive(Debug)]
pub struct WriteOp<'a> {
  pub id: Vec<u8>,
  pub data: Vec<u8>,
  pub refs: Vec<WriteRelation<'a>>,
  pub defaults: Vec<WriteDefault<'a>>,
  pub write_indexes: Vec<WriteIndex>
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
pub enum WriteIndex {
    Value(String,Vec<u8>)
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
        op: WriteOp<'a>,
        st: &'a Entity,
    },
    CreateMany {
        field: &'a Field,
        ops: Vec<WriteOp<'a>>,
        st: &'a Entity,
    },
    Connect {
        field: &'a Field,
        ids: Vec<Vec<u8>>,
        st: &'a Entity,
    },
    Update {
        field: &'a Field,
        op: WriteOp<'a>,
        st: &'a Entity,
    },
    Push {
        field: &'a Field,
        op: WriteOp<'a>,
        st: &'a Entity,
    },
}