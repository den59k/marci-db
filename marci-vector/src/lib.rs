mod write_cluster;
mod read_cluster;

pub use write_cluster::WriteCluster;
pub use read_cluster::ReadCluster;


#[cfg(test)]
mod tests {
  use std::{collections::BTreeMap, sync::Mutex};

use super::*;

  struct TestDB {
    storage: Mutex<BTreeMap<Vec<u8>, Vec<u8>>>
  }

  impl<'a> WriteCluster<'a> for TestDB {
    type WriteContext = BTreeMap<Vec<u8>, Vec<u8>>;

    fn write_data(&self, ctx: &mut Self::WriteContext, id: Vec<u8>, data: &[u8]) {
      println!("Write data {:?}", id);
      ctx.insert(id, data.to_vec());
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
      
      

      assert_eq!(storage.len(), 3); // Three clusters created
    }

  }

  

}