mod write_cluster;
mod read_cluster;

pub use write_cluster::WriteCluster;
pub use read_cluster::ReadCluster;


#[inline(always)]
pub fn bytes_to_point(buf: &[u8]) -> &[f32] {
    bytemuck::cast_slice(buf)
}


#[inline(always)]
pub fn point_to_bytes(point: &[f32]) -> &[u8] {
    bytemuck::cast_slice(point)
}

#[cfg(test)]
mod tests {
  use std::{collections::{BTreeMap, HashSet}, sync::Mutex};

use super::*;

  struct TestDB {
    storage: Mutex<BTreeMap<Vec<u8>, Vec<f32>>>
  }

  impl<'a> WriteCluster<'a> for TestDB {
    type WriteContext = BTreeMap<Vec<u8>, Vec<f32>>;

    fn write_data(&self, ctx: &mut Self::WriteContext, id: Vec<u8>, data: &[f32]) {
      // let point: &[f32] = bytemuck::cast_slice(data);
      println!("Write data {:?} {:?}", id, data);
      ctx.insert(id, data.to_vec());
    }
  }

  pub struct TestDBIter<'b> {
    inner: std::collections::btree_map::Range<'b, Vec<u8>, Vec<f32>>,
  }

  impl<'b> Iterator for TestDBIter<'b> {
      type Item = (Vec<u8>, Vec<f32>);

      fn next(&mut self) -> Option<Self::Item> {
          self.inner.next().map(|entry| (entry.0.clone(), entry.1.clone()))
      }
  }

  fn increment_bytes(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut out = prefix.to_vec();

    // Идём с конца к началу
    for i in (0..out.len()).rev() {
        if out[i] != 0xFF {
            out[i] += 1;
            return Some(out[..=i].to_vec()); // отбрасываем хвост
        }
    }

    // Если все байты были 0xFF → следующего префикса не существует
    None
  }

  impl<'a> ReadCluster<'a> for TestDB {
    type ReadContext = BTreeMap<Vec<u8>, Vec<f32>>;
    
    type Iter<'b> = TestDBIter<'b>;
    
    fn read_data<'b>(
        &'b self,
        ctx: &'b Self::ReadContext,
        prefix: Vec<u8>,
    ) -> Self::Iter<'b> {
        let end = increment_bytes(&prefix).unwrap();
        println!("Try to read bytes... {:?} {:?}", prefix, end);
        let range = ctx.range(prefix..end);
        TestDBIter { inner: range }
    }
  }
    
  #[test]
  fn test_write_and_read_cluster() {

    let db = TestDB { storage: Mutex::new(BTreeMap::new()) };
    
    let points: &[(f32, f32)] = &[
      (0.0, 0.0),     // 0
      (0.0, 2.0),     // 1
      (0.0, 1.0),     // 2
      (1.0, 0.0),     // 3
      (-5.0, 20.0),   // 4
      (-4.0, 20.0),   // 5
      (-3.0, 20.0),   // 6
      (20.0, 20.0),   // 7
      (20.0, 22.0),   // 8
      (20.0, 23.0),   // 9
      (-5.0, 21.0),   // 10
    ];

    let coordinates: Vec<(Vec<u8>, Vec<f32>)> = points
      .iter()
      .enumerate()
      .map(|(i, p)| ((i as u32).to_be_bytes().to_vec(), vec![p.0, p.1] ))
      .collect();

    {
      let mut storage = db.storage.lock().unwrap();
      db.create_cluster(&mut storage, &coordinates);
      
      let clusters_count: HashSet<u16> = storage
        .iter()
        .map(|i| u16::from_be_bytes(i.0[2..4].try_into().unwrap()))
        .collect();

      assert_eq!(clusters_count.len(), 3); // Three clusters created
    }

    {
      let storage = db.storage.lock().unwrap();
      let point: Vec<f32> = vec![0.0, 0.5];
      let points = db.find_nearest_points(&storage, &point, 3);

      println!("{:?}", points);

      let mut point_ids: Vec<u32> = points.iter().map(|i| u32::from_be_bytes(i.0.as_slice().try_into().unwrap())).collect();
      point_ids.sort();

      assert_eq!(point_ids, vec![ 0, 2, 3 ]);
    }

  }

  

}