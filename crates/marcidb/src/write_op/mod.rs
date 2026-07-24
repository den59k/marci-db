mod process_write;
use crate::{Field, schema::{Entity, FieldDefault, FieldIndex, RefInfo}};

pub use process_write::{process_write,InsertError,write_ref_indexes,delete_ref_indexes,WriteIndexesError};

#[derive(Debug)]
pub struct WriteOp<'a> {
  pub id: Vec<u8>,
  pub data: Vec<u8>,
  pub refs: Vec<WriteRelation<'a>>,
  pub defaults: Vec<WriteDefault<'a>>,
  pub write_indexes: Vec<WriteIndex<'a>>
}

#[derive(Debug)]
pub enum WriteDefault<'a> {
    // Write the value into Key at the given offset
    Key(usize, &'a FieldDefault),
    // Write the value into Body at the given offset
    Body(usize, &'a FieldDefault),

    KeyInsert(usize, WriteDefaultInsert),
    BodyInsert(usize,usize, WriteDefaultInsert),
}

#[derive(Debug)]
pub enum WriteDefaultInsert {
    ParentId
}

#[derive(Debug)]
pub enum WriteIndex<'a> {
    Value(&'a Field, &'a FieldIndex,Vec<u8>)
}

#[derive(Debug)]
pub enum WriteRelation<'a> {
    Create {
        field: &'a Field,
        ref_info: &'a RefInfo,
        op: WriteOp<'a>,
        st: &'a Entity,
    },
    CreateMany {
        field: &'a Field,
        ref_info: &'a RefInfo,
        ops: Vec<WriteOp<'a>>,
        st: &'a Entity,
    },
    Connect {
        field: &'a Field,
        ref_info: &'a RefInfo,
        ids: Vec<Vec<u8>>,
        st: &'a Entity,
    }
}