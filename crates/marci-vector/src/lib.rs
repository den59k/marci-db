#![feature(portable_simd)]

mod write_cluster;
mod read_cluster;
mod distances;

pub use write_cluster::WriteCluster;
pub use read_cluster::ReadCluster;
pub use distances::CustomDistance;


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

  impl<'a> WriteCluster<'a, 4> for TestDB {
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
    
  // Поиск ближайших географических точек
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
      db.create_cluster(&mut storage, &coordinates, CustomDistance::Euclidean);
      
      let clusters_count: HashSet<u16> = storage
        .iter()
        .map(|i| u16::from_be_bytes(i.0[2..4].try_into().unwrap()))
        .collect();

      assert_eq!(clusters_count.len(), 3); // Three clusters created
    }

    {
      let storage = db.storage.lock().unwrap();
      let point: Vec<f32> = vec![0.0, 0.5];
      let points = db.find_nearest_points(&storage, &point, 3, CustomDistance::Euclidean, 0f32);

      let mut point_ids: Vec<u32> = points.iter().map(|i| u32::from_be_bytes(i.0.as_slice().try_into().unwrap())).collect();
      point_ids.sort();

      assert_eq!(point_ids, vec![ 0, 2, 3 ]);
    }

  }

   // Поиск ближайших географических точек и ближайших по смыслу
  #[test]
  fn test_write_and_read_cluster_with_embedding() {
    let db = TestDB { storage: Mutex::new(BTreeMap::new()) };

    let points: &[Vec<f32>] = &[
      vec! [ 0.0, 0.0 ],     // 0
      vec! [ 0.0, 2.0 ],     // 1
      vec! [ 0.0, 1.0 ],     // 2
      vec! [ 1.0, 0.0 ],     // 3
      vec! [ -5.0, 20.0 ],   // 4
      vec! [ -4.0, 20.0 ],   // 5
      vec! [ -3.0, 20.0 ],   // 6
      vec! [ 20.0, 20.0 ],   // 7
      vec! [ 20.0, 22.0 ],   // 8
      vec! [ 20.0, 23.0 ],   // 9
      vec! [ -5.0, 21.0 ],   // 10
    ];

    let embeddings: &[Vec<f32>] = &[
        // ===== Кластер A =====
        vec![  0.50,  0.50,  0.50,  0.50,  0.00,  0.00,  0.00,  0.00 ], // 0 - центр кластера A
        vec![  0.55,  0.45,  0.50,  0.40,  0.00,  0.00,  0.00,  0.00 ], // 1 - похож на 0
        vec![  0.45,  0.55,  0.40,  0.50,  0.00,  0.00,  0.00,  0.00 ], // 2 - похож на 0
        vec![  0.40,  0.50,  0.55,  0.45,  0.00,  0.00,  0.00,  0.00 ], // 3 - похож на 0

        // ===== Кластер B =====
        vec![ -0.50,  0.00, -0.50,  0.00,  0.50,  0.50,  0.00,  0.00 ], // 4 - центр кластера B
        vec![ -0.45, -0.05, -0.55,  0.05,  0.55,  0.40,  0.00,  0.00 ], // 5 - похож на 4
        vec![ -0.55,  0.05, -0.45, -0.05,  0.40,  0.55,  0.00,  0.00 ], // 6 - похож на 4

        // ===== Разрозненные =====
        vec![  0.00,  0.00,  0.00,  0.00,  1.00,  0.00,  0.00,  0.00 ], // 7 - почти ортогонален всем
        vec![  0.00,  0.00,  0.00,  0.00,  0.00,  1.00,  0.00,  0.00 ], // 8
        vec![  0.00,  0.00,  0.00,  0.00,  0.00,  0.00,  1.00,  0.00 ], // 9
        vec![  0.00,  0.00,  0.00,  0.00,  0.00,  0.00,  0.00,  1.00 ], // 10
    ];

    let data: Vec<(Vec<u8>, Vec<f32>)> = points
      .iter()
      .zip(embeddings)
      .enumerate()
      .map(|(i, (p, e))| ((i as u32).to_be_bytes().to_vec(), {
        let mut v = Vec::with_capacity(p.len() + e.len());
        v.extend_from_slice(p);
        v.extend_from_slice(e);
        v
      } ))
      .collect();

    let distance: CustomDistance<f32> = CustomDistance::Complex(vec![
      (Box::new(CustomDistance::Euclidean), 0, 2, 0.2),
      (Box::new(CustomDistance::Cosine), 2, 10, 0.3),
    ]);
    
    {
      let mut storage = db.storage.lock().unwrap();
      db.create_cluster(&mut storage, &data, distance.clone());
      
      let clusters_count: HashSet<u16> = storage
        .iter()
        .map(|i| u16::from_be_bytes(i.0[2..4].try_into().unwrap()))
        .collect();

      assert_eq!(clusters_count.len(), 3); // Three clusters created
    }

    {
      let storage = db.storage.lock().unwrap();
      let point = &data[0].1;
      let points = db.find_nearest_points(&storage, point, 3, distance.clone(), 0f32);

      let mut point_ids: Vec<u32> = points.iter().map(|i| u32::from_be_bytes(i.0.as_slice().try_into().unwrap())).collect();
      point_ids.sort();

      assert_eq!(point_ids, vec![ 0, 2, 3 ]);
    }


  }

}