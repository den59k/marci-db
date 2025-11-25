use std::collections::{HashMap};

// use kentro::KMeans;
use kmeans::{EuclideanDistance, KMeans, KMeansConfig};

const ZERO_PATH: &[u16] = &[0];

pub trait WriteCluster<'a> {
    type WriteContext: 'a;

    fn write_data(&self, ctx: &mut Self::WriteContext, cluster_id: Vec<u8>, centroid: &[f32]);

    /// Разделяет точки на кластеры. Если создается только один кластер - возвращает false
    fn create_cluster_internal(&self, ctx: &mut Self::WriteContext, path: &[u16], ids: &[&[u8]], data: &[f32], dim: usize) -> usize {
        const DEFAULT_K: usize = 8;
        
        let n = data.len() / dim;
        if n == 0 {
            return 0;
        }

        // K не может быть больше числа точек
        let k = DEFAULT_K.min(n / 3);
        if k <= 1 {
            let path = if path.is_empty() {
                let zero_cluster_id = build_cluster_key(ZERO_PATH);
                // let point_data: &[u8] = bytemuck::cast_slice(&data[0..dim]);
                self.write_data(ctx, zero_cluster_id, &data[0..dim]);
                ZERO_PATH
            } else {
                path 
            };
                
            for (i, point) in data.chunks(dim).enumerate() { 
                let leaf_key = build_leaf_key(path, &ids[i]);
                // let point_data: &[u8] = bytemuck::cast_slice(point);
                self.write_data(ctx, leaf_key, point);
            }
            return 1;
        }

        // Настраиваем и запускаем KMeans
        let kmeans: KMeans<f32, 8, EuclideanDistance> = KMeans::new(data, n, dim, EuclideanDistance);

        // train возвращает разбиение на кластеры:
        // Vec<Vec<usize>>, где каждый внутренний вектор — индексы точек
        let result = kmeans.kmeans_lloyd(k, 100, KMeans::init_kmeanplusplus, &KMeansConfig::default());

        let mut points_map = HashMap::new();
        for (point_id, &cluster_id) in result.assignments.iter().enumerate() {
            points_map
                .entry(cluster_id)
                .or_insert(vec![])
                .push(point_id);
        }

        let clusters_count = points_map.len();

        if clusters_count <= 1 {
            for (i, point) in data.chunks(dim).enumerate() { 
                let leaf_key = build_leaf_key(path, &ids[i]);
                // let point_data: &[u8] = bytemuck::cast_slice(point);
                self.write_data(ctx, leaf_key, point);
            }
            return clusters_count;
        }
        
        println!("Layer: {}. Write {} clusters ({} points)", path.len(), clusters_count, n);

        // // 1) Записываем центры кластеров
        for (cluster_id, point_ids) in points_map {

            if point_ids.is_empty() { continue; }

            let item_cluster_id = [ path, &[cluster_id as u16] ].concat();

            let centroid_center = &result.centroids[cluster_id];

            let cluster_key = build_cluster_key( &item_cluster_id);
            // let centroid_data: &[u8] = bytemuck::cast_slice(centroid_center);

            let mut sub_ids = Vec::with_capacity(point_ids.len());
            let mut sub_data= Vec::with_capacity(point_ids.len() * dim);
            // let mut sub_data = Array2::<f32>::zeros((points.len(), dim));
            for &point_index in point_ids.iter() {
                sub_ids.push(ids[point_index]);

                sub_data.extend_from_slice(&data[point_index*dim..point_index*dim+dim]);
            }

            let created_clusters = self.create_cluster_internal(ctx, &item_cluster_id, &sub_ids, &sub_data, dim);

            if created_clusters > 1 {
                self.write_data(ctx, cluster_key, centroid_center);
            }
      
        }

        return clusters_count;
    }

    fn create_cluster(&self, ctx: &mut Self::WriteContext, coordinates: &[(Vec<u8>, Vec<f32>)]) {
        
        let n = coordinates.len();

        let dim = 2;

        let mut point_ids = vec![];

        // Собираем данные в матрицу NxD для kentro
        let mut data = Vec::with_capacity(dim * n);
        for vec in coordinates.iter() {
            point_ids.push(vec.0.as_slice());
            data.extend_from_slice(&vec.1);
        }

        self.create_cluster_internal(ctx, &[], &point_ids, &data, dim);
    }

    fn create_cluster_from_bytes(&self, ctx: &mut Self::WriteContext, coordinates_buf: &[(Vec<u8>, Vec<u8>)]) {
        
        let n = coordinates_buf.len();
        let dim = coordinates_buf[0].1.len();

        let mut point_ids = vec![];

        // Собираем данные в матрицу NxD для kentro
        let mut data = Vec::with_capacity(dim * n);

        for vec in coordinates_buf.iter() {
            point_ids.push(vec.0.as_slice());
            let float_slice: &[f32] = bytemuck::cast_slice(&vec.1);
            data.extend_from_slice(float_slice);
        }

        self.create_cluster_internal(ctx,  &[], &point_ids, &data, dim);
    }
}


fn build_cluster_key(path: &[u16]) -> Vec<u8> {
    let mut out = Vec::with_capacity(path.len()*2 + 1);
    out.extend_from_slice(&(path.len() as u16).to_be_bytes());
    for &p in path {
        out.extend_from_slice(&p.to_be_bytes());
    }
    out.push(0x00);
    out
}
fn build_leaf_key(path: &[u16], point_id: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(path.len()*2 + point_id.len() + 1);
    out.extend_from_slice(&(path.len() as u16).to_be_bytes());
    for &p in path {
        out.extend_from_slice(&p.to_be_bytes());
    }
    out.push(0x01);
    out.extend_from_slice(point_id);
    out
}

#[cfg(test)]
mod tests {

    use super::*;

    /// Простая мок-структура, чтобы собирать вызовы write_data.
    struct MockStorage {
        
    }

    type MockData = Vec<(Vec<u8>,Vec<f32>)>;

    impl WriteCluster<'_> for MockStorage {
        type WriteContext = MockData;
        fn write_data(&self, ctx: &mut MockData, key: Vec<u8>, centroid_data: &[f32]) {
            ctx.push((key.to_vec(), centroid_data.to_vec()));
        }
    }

    /// Создаём два отчётливо разделённых кластера, чтобы алгоритм разбил точки.
    fn generate_test_points() -> Vec<(Vec<u8>, Vec<f32>)> {
        let mut pts = vec![];

        // cluster 1 around (0,0)
        for i in 0..20 {
            pts.push((vec![i as u8], vec![0.0 + i as f32 * 0.01, 0.0]));
        }

        // cluster 2 around (10,10)
        for i in 0..20 {
            pts.push((vec![100 + i as u8], vec![10.0 + i as f32 * 0.01, 10.0]));
        }

        pts
    }

     #[test]
    fn test_cluster_creation() {
        let storage = MockStorage {};

        let points = generate_test_points();
        
        let mut written: MockData = vec![]; 
        storage.create_cluster(&mut written, &points);

        // Должно быть больше 2 записей (2 центроида + листья)
        assert!(!written.is_empty());
        assert!(written.len() > 2);

        // Проверяем структуру ключей
        for (key, vec) in written.iter() {
            assert!(!key.is_empty(), "key should not be empty");

            let layer= u16::from_be_bytes(key[0..2].try_into().unwrap()) as usize;

            let tag = key[layer * 2];

            match tag {
                0x00 => {
                    // cluster key
                    // формат: [len:u16_be][p0][p1]...[p(n-1)][0x00]
                    assert!(key.len() >= 3, "cluster key too short");
                }
                0x01 => {
                    // leaf key:  [len][path][0x01][point_id]
                    assert!(key.len() >= 4, "leaf key too short");
                }
                _ => panic!("unknown key tag: {}", tag),
            }

            // centroid/point must be nonempty vector
            assert!(!vec.is_empty());
        }

        // Проверяем, что есть хотя бы один cluster key и один leaf key
        let has_cluster = written.iter().any(|(k, _)| k.last() == Some(&0x00));
        let has_leaf = written.iter().any(|(k, _)| k.last() == Some(&0x01));

        assert!(has_cluster, "no cluster keys were written");
        assert!(has_leaf, "no leaf keys were written");
    }

}