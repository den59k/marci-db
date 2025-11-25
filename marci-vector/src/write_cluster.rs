use kentro::KMeans;
use ndarray::Array2;

const MIN_POINTS_IN_CLUSTER: usize = 20;

pub trait WriteCluster<'a> {
    type WriteContext: 'a;

    fn write_data(&self, ctx: &mut Self::WriteContext, cluster_id: Vec<u8>, centroid: &[u8]);

    fn create_cluster_internal(&self, ctx: &mut Self::WriteContext, path: &[u16], ids: &[&[u8]], data: &Array2::<f32>) {
        const DEFAULT_K: usize = 8;

        let n = data.nrows();
        let dim = data.ncols();

        // K не может быть больше числа точек
        let k = DEFAULT_K.min(n / 3);
        if k <= 1 {
            for (i, point) in data.rows().into_iter().enumerate() { 
                let leaf_key = build_leaf_key(path, &ids[i]);
                let point_data: &[u8] = bytemuck::cast_slice(point.as_slice().unwrap());
                self.write_data(ctx, leaf_key, point_data);
            }
            return;
        }

        // Настраиваем и запускаем KMeans
        let mut kmeans = KMeans::new(k)
            .with_euclidean(true)  // стандартная евклидова метрика
            .with_iterations(100)
            .with_verbose(false);

        // train возвращает разбиение на кластеры:
        // Vec<Vec<usize>>, где каждый внутренний вектор — индексы точек
        let clusters = kmeans
            .train(data.view(), None)
            .expect("k-means training failed");

        let count_clusters = clusters.iter()
            .filter(|c| !c.is_empty())
            .count();

        if count_clusters <= 1 {
            for (i, point) in data.rows().into_iter().enumerate() { 
                let leaf_key = build_leaf_key(path, &ids[i]);
                let point_data: &[u8] = bytemuck::cast_slice(point.as_slice().unwrap());
                self.write_data(ctx, leaf_key, point_data);
            }
            return;
        }

        // Получаем центроиды после обучения
        let centroids = kmeans
            .centroids()
            .expect("centroids not available after training");
        
        println!("Layer: {}. Write {} clusters ({} points)", path.len(), count_clusters, n);

        // // 1) Записываем центры кластеров
        for (cluster_id, centroid_row) in centroids.outer_iter().enumerate() {

            let points = &clusters[cluster_id];
            if points.is_empty() { continue; }

            let item_cluster_id = [ path, &[cluster_id as u16] ].concat();

            if points.len() < MIN_POINTS_IN_CLUSTER {
                for &point_index in points { 
                    let point = &data.row(point_index);
                    let point_id = &ids[point_index];
                    let leaf_key = build_leaf_key( &item_cluster_id, point_id);

                    let point_data: &[u8] = bytemuck::cast_slice(point.as_slice().unwrap());

                    self.write_data(ctx, leaf_key, point_data);
                }
            } else {
                let cluster_key = build_cluster_key( &item_cluster_id);
                let centroid_data: &[u8] = bytemuck::cast_slice(centroid_row.as_slice().unwrap());
                self.write_data(ctx, cluster_key, centroid_data);

                let mut sub_ids = Vec::with_capacity(points.len());
                let mut sub_data = Array2::<f32>::zeros((points.len(), dim));
                for (i, &point_index) in points.iter().enumerate() {
                    sub_ids.push(ids[point_index]);
                    sub_data.row_mut(i).assign(&data.row(point_index));
                }
                self.create_cluster_internal(ctx, &item_cluster_id, &sub_ids, &sub_data);
            }
        }

    }

    fn create_cluster(&self, ctx: &mut Self::WriteContext, coordinates: &[(Vec<u8>, Vec<f32>)]) {
        
        let n = coordinates.len();

        let dim = 2;

        let mut point_ids = vec![];

        // Собираем данные в матрицу NxD для kentro
        let mut data = Array2::<f32>::zeros((n, dim));
        for (i, vec) in coordinates.iter().enumerate() {
            point_ids.push(vec.0.as_slice());
            for (j, &val) in vec.1.iter().enumerate() {
                data[(i, j)] = val;
            }
        }

        self.create_cluster_internal(ctx, &[], &point_ids, &data);
    }

    fn create_cluster_from_bytes(&self, ctx: &mut Self::WriteContext, coordinates_buf: &[(Vec<u8>, Vec<u8>)]) {
        
        let n = coordinates_buf.len();
        let dim = coordinates_buf[0].1.len();

        let mut point_ids = vec![];

        // Собираем данные в матрицу NxD для kentro
        let mut data = Array2::<f32>::zeros((n, dim));
        for (i, vec) in coordinates_buf.iter().enumerate() {
            point_ids.push(vec.0.as_slice());
            for (j, val) in vec.1.chunks(4).enumerate() {
                data[(i, j)] = f32::from_be_bytes(val.try_into().unwrap());
            }
        }

        self.create_cluster_internal(ctx,  &[], &point_ids, &data);
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

    type MockData = Vec<(Vec<u8>,Vec<u8>)>;

    impl WriteCluster<'_> for MockStorage {
        type WriteContext = MockData;
        fn write_data(&self, ctx: &mut MockData, key: Vec<u8>, centroid_data: &[u8]) {
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