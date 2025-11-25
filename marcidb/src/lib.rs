mod marci_db;
mod schema;
mod marci_encoder;
mod marci_decoder;
mod marci_select;
mod update_data;
mod select;

pub use crate::marci_db::{MarciDB};
pub use crate::select::{MarciSelect,get_value_from_data};
pub use crate::marci_decoder::{decode_document, decode_id, array_to_json};
pub use crate::marci_encoder::{encode_document, encode_id};
pub use crate::marci_select::{parse_select};
pub use crate::schema::{parse_schema,Field,Attribute,FieldType,FieldRef,PrimitiveFieldType};

use canopydb::RangeIter;
pub use canopydb::{Tree,WriteTransaction,ReadTransaction};

pub struct MarciIter<'b> {
    inner: RangeIter<'b>,
}

impl<'b> Iterator for MarciIter<'b> {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|entry| {
            let (k, v) = entry.unwrap();
            (k.to_vec(), v.to_vec())
        })
    }
}

pub fn iter_tree_by_prefix<'a>(tree: &'a Tree, prefix: &[u8]) -> MarciIter<'a> {
  let iter = tree.prefix(&prefix).unwrap();
  return MarciIter { inner: iter };
}