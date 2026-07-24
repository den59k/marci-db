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
        // total_cmp is a *total* order even for NaN/inf distances. `partial_cmp(...).unwrap()` would panic
        // if a distance were ever NaN (e.g. a zero-norm vector under some metric).
        self.dist2.total_cmp(&other.dist2)
    }
}

impl PartialOrd for HeapElem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for HeapElem {
    fn eq(&self, other: &Self) -> bool {
        // Consistent with `Ord` (keeps `Eq` reflexive for NaN).
        self.dist2.total_cmp(&other.dist2) == Ordering::Equal
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

    // saturating_sub avoids the `0usize - 1` underflow when `data` is empty (a search that matched nothing).
    for i in 0..data.len().saturating_sub(1) {
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

#[cfg(test)]
mod tests {
    use super::{HeapElem, filter_by_threshold};
    use std::collections::BinaryHeap;

    #[test]
    fn heap_does_not_panic_on_nan_distance() {
        // Regression: `Ord` used `partial_cmp(...).unwrap()`, which panics when a distance is NaN.
        let mut heap = BinaryHeap::new();
        heap.push(HeapElem { dist2: 1.0, id: vec![1] });
        heap.push(HeapElem { dist2: f32::NAN, id: vec![2] });
        heap.push(HeapElem { dist2: 0.5, id: vec![3] });
        let mut popped = 0;
        while heap.pop().is_some() { popped += 1; }
        assert_eq!(popped, 3);
    }

    #[test]
    fn empty_result_does_not_underflow() {
        // Regression: `0..data.len()-1` used to panic on an empty result with threshold > 0.
        let mut data: Vec<(Vec<u8>, f32)> = vec![];
        filter_by_threshold(&mut data, 0.5);
        assert!(data.is_empty());
    }

    #[test]
    fn single_element_is_kept() {
        let mut data = vec![(vec![1], 0.1)];
        filter_by_threshold(&mut data, 0.5);
        assert_eq!(data.len(), 1);
    }

    #[test]
    fn truncates_at_the_first_gap_over_threshold() {
        // distances 0.1, 0.2, 1.0 — the 0.2→1.0 jump (0.8) exceeds the 0.5 threshold.
        let mut data = vec![(vec![1], 0.1), (vec![2], 0.2), (vec![3], 1.0)];
        filter_by_threshold(&mut data, 0.5);
        assert_eq!(data.iter().map(|(_, d)| *d).collect::<Vec<_>>(), vec![0.1, 0.2]);
    }
}