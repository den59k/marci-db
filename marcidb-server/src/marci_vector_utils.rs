use marci_vector::{ReadCluster, WriteCluster, bytes_to_point, point_to_bytes};
use marcidb::{RangeIter, Tree};

use crate::ServerContext;

pub struct MarciIter<'b> {
    inner: RangeIter<'b>,
}

impl<'b> Iterator for MarciIter<'b> {
    type Item = (Vec<u8>, Vec<f32>);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|entry| {
            let (k, v) = entry.unwrap();
            let p =  bytes_to_point(&v);
            (k.to_vec(), p.to_vec())
        })
    }
}

pub fn iter_tree_by_prefix<'a>(tree: &'a Tree, prefix: &[u8]) -> MarciIter<'a> {
  let iter = tree.prefix(&prefix).unwrap();
  return MarciIter { inner: iter };
}


impl<'a> WriteCluster<'a> for ServerContext {
    type WriteContext = Tree<'a>;

    fn write_data(&self, ctx: &mut Tree<'a>, id: Vec<u8>, point: &[f32]) {
        ctx.insert(&id, point_to_bytes(point)).unwrap()
    }
}


impl<'a> ReadCluster<'a> for ServerContext {
    type ReadContext = Tree<'a>;
    type Iter<'b> = MarciIter<'b>;

    fn read_data<'b>(
        &'b self,
        ctx: &'b Tree<'a>,
        prefix: Vec<u8>,
    ) -> MarciIter<'b> {
        iter_tree_by_prefix(ctx, &prefix)
    }
}