use std::{cmp::Ordering, collections::BinaryHeap};

use kmeans::DistanceFunction;

use crate::CustomDistance;




#[derive(Debug)]
struct HeapElem {
    dist2: f32, 
    id: Vec<u8>
}

// Мы хотим max-heap, т.е. большее расстояние = больший приоритет
impl Ord for HeapElem {
    fn cmp(&self, other: &Self) -> Ordering {
        self.dist2.partial_cmp(&other.dist2).unwrap()
    }
}

impl PartialOrd for HeapElem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for HeapElem {
    fn eq(&self, other: &Self) -> bool {
        self.dist2 == other.dist2
    }
}

impl Eq for HeapElem {}

const LANES: usize = 8;

pub trait ReadCluster<'a> {
    type ReadContext: 'a;
    type Iter<'b>: Iterator<Item = (Vec<u8>, Vec<f32>)> where Self: 'b;

    fn read_data<'b>(
        &'b self,
        ctx: &'b Self::ReadContext,
        prefix: Vec<u8>,
    ) -> Self::Iter<'b>;

    fn find_nearest_points(&self, ctx: &Self::ReadContext, target_point: &[f32], k: usize, distance: CustomDistance<f32>, threshold: f32) -> Vec<(Vec<u8>,f32)> {
        let mut heap: BinaryHeap<HeapElem> = BinaryHeap::new();
        let path = Vec::new();

        let rem = target_point.len() % LANES;
        if target_point.len() != 2 && rem != 0 {
            let mut padded_point = Vec::with_capacity(target_point.len() + (LANES - rem));
            padded_point.extend_from_slice(target_point);
            padded_point.resize(target_point.len() + (LANES - rem), 0.0);
            find_nearest_points_internal(self, ctx, &padded_point, k, &path, &mut heap, distance);
        } else {
            find_nearest_points_internal(self, ctx, target_point, k, &path, &mut heap, distance);
        }

        let mut resp = heap.into_sorted_vec().into_iter().map(|i| (i.id, i.dist2)).collect();
        if threshold > 0f32 {
            filter_by_threshold(&mut resp, threshold);
        }

        return resp;
    }
}


pub fn filter_by_threshold(data: &mut Vec<(Vec<u8>, f32)>, threshold: f32) {
    let mut truncate = None;
    
    for i in 0..data.len()-1 {
        let prev_value = data[i].1;
        let current_value = data[i+1].1;
        
        if current_value > prev_value + threshold {
            truncate = Some(i+1);
            break
        }
    }
    
    if let Some(truncate) = truncate {
        data.truncate(truncate);
    }
}


fn find_nearest_points_internal<'a, R>(
    reader: &R,
    ctx: &R::ReadContext,
    target_point: &[f32],
    k: usize,
    path: &[u16],
    heap: &mut BinaryHeap<HeapElem>,
    distance: CustomDistance<f32>
)
where
    R: ReadCluster<'a> + ?Sized,
{
    let prefix = build_prefix(&path);
    let item_path_len = path.len() + 1;
    let mut heap_clusters: BinaryHeap<HeapElem> = BinaryHeap::new();

    for (id, point_data) in reader.read_data(ctx, prefix) {
        let is_cluster = id[2 + item_path_len * 2] == 0x00;

        let point: &[f32] = bytemuck::cast_slice(&point_data);

        let dist2 = match target_point.len() {
            2 =>  <CustomDistance<f32> as DistanceFunction<f32, 2>>::distance(
            &distance, target_point, point,
            ),
            _ =>  {
                if target_point.len() > point.len() {
                    let mut padded_point = Vec::with_capacity(target_point.len());
                    padded_point.extend_from_slice(point);
                    padded_point.resize(target_point.len(), 0.0);
                    <CustomDistance<f32> as DistanceFunction<f32, LANES>>::distance(
                    &distance, target_point, &padded_point,
                    )
                } else {
                    <CustomDistance<f32> as DistanceFunction<f32, LANES>>::distance(
                    &distance, target_point, point,
                    )
                }
            }
        };

        if is_cluster {
            if heap.len() == k && dist2 >= heap.peek().unwrap().dist2 * 3f32 {
                continue; // эта точка хуже, чем худшая в куче — пропускаем
            }
            heap_clusters.push(HeapElem { dist2, id });
            continue;
        }

        let item_id = id[2 + item_path_len*2 + 1 ..].to_vec();
        // println!("{} {}", u32::from_be_bytes(item_id.as_slice().try_into().unwrap()), dist2);

        if heap.len() == k && dist2 >= heap.peek().unwrap().dist2 {
            continue; // эта точка хуже, чем худшая в куче — пропускаем
        }

        heap.push(HeapElem { dist2, id: item_id });
        if heap.len() > k {
            heap.pop();  // удаляем самую далёкую
        }
    }

    while let Some(item) = heap_clusters.pop() {
        if heap.len() == k && item.dist2 >= heap.peek().unwrap().dist2 * 3f32 {
            break;
        }
        let path_f = u16::from_be_bytes(item.id[2+path.len()*2..2+item_path_len*2].try_into().unwrap());
        let mut item_path = Vec::with_capacity(path.len() + 1);
        item_path.extend_from_slice(path);
        item_path.push(path_f);

        find_nearest_points_internal(reader, ctx, target_point, k, &item_path, heap, distance.clone());
    }
}

/// Строит префикс, чтобы найти все дочерние элементы у текущего path
fn build_prefix(path: &[u16]) -> Vec<u8> {
    let mut out = Vec::with_capacity(path.len()*2 + 2);
    out.extend_from_slice(&(path.len() as u16 + 1).to_be_bytes());
    for &p in path {
        out.extend_from_slice(&p.to_be_bytes());
    }
    return out;
}