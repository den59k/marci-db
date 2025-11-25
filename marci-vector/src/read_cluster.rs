use std::{cmp::Ordering, collections::BinaryHeap};




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


pub trait ReadCluster<'a> {
    type ReadContext: 'a;
    type Iter<'b>: Iterator<Item = (Vec<u8>, Vec<f32>)> where Self: 'b;

    fn read_data<'b>(
        &'b self,
        ctx: &'b Self::ReadContext,
        prefix: Vec<u8>,
    ) -> Self::Iter<'b>;

    fn find_nearest_points(&self, ctx: &Self::ReadContext, point: &[f32], k: usize) -> Vec<(Vec<u8>,f32)> {
        let mut heap: BinaryHeap<HeapElem> = BinaryHeap::new();
        let path = Vec::new();

        find_nearest_points_internal(self, ctx, point, k, &path, &mut heap);
        return heap.into_sorted_vec().into_iter().map(|i| (i.id, i.dist2)).collect();
    }
}

fn find_nearest_points_internal<'a, R>(
    reader: &R,
    ctx: &R::ReadContext,
    point: &[f32],
    k: usize,
    path: &[u16],
    heap: &mut BinaryHeap<HeapElem>,
)
where
    R: ReadCluster<'a> + ?Sized,
{
    let prefix = build_prefix(&path);
    let item_path_len = path.len() + 1;
    let mut heap_clusters: BinaryHeap<HeapElem> = BinaryHeap::new();

    println!("Finding in cluster {:?}", path);

    for (id, point_data) in reader.read_data(ctx, prefix) {
        let is_cluster = id[2 + item_path_len * 2] == 0x00;

        let floats: &[f32] = bytemuck::cast_slice(&point_data);

        let dist2 = floats
            .iter()
            .zip(point.iter())
            .map(|(a, b)| { 
                let diff = a - b;
                diff * diff
            }).sum();

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

        find_nearest_points_internal(reader, ctx, point, k, &item_path, heap);
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