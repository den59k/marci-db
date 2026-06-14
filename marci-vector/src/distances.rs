use std::{ops, simd::{Simd, SimdElement, num::SimdFloat}};

use kmeans::{DistanceFunction, Primitive};

#[derive(Clone)]
pub enum CustomDistance<T> where T: Primitive {
  Complex(Vec<(Box<CustomDistance<T>>,usize,usize,T)>),  // First element - type, second - offset, third - end, last - koef
  Euclidean,
  Cosine
}

// portable_simd dropped LaneCount/SupportedLaneCount: `Simd<T, N>` now only needs `T: SimdElement`, and the
// usable lane counts come from whichever Simd op impls exist. We bound directly on the ops the body uses.
impl<T, const LANES: usize> DistanceFunction<T, LANES> for CustomDistance<T>
where
    T: Primitive + SimdElement,
    Simd<T, LANES>: ops::Sub<Output = Simd<T, LANES>>
        + ops::Add<Output = Simd<T, LANES>>
        + ops::Mul<Output = Simd<T, LANES>>
        + SimdFloat<Scalar = T>,
{
    #[inline(always)]
    fn distance(&self, a: &[T], b: &[T]) -> T {
      debug_assert_eq!(a.len(), b.len(), "distance: slices must have same length");

      match self {
        CustomDistance::Cosine => {
          let acc = a.chunks_exact(LANES)
            .map(Simd::from_slice)
            .zip(b.chunks_exact(LANES).map(Simd::from_slice))
            .fold(Simd::<T, LANES>::splat(T::zero()), |acc, (sp, cp)| {
                acc + sp * cp
            });

          T::one() - acc.reduce_sum()
        },
        CustomDistance::Euclidean => {
          let acc = a.chunks_exact(LANES)
            .map(Simd::from_slice)
            .zip(b.chunks_exact(LANES).map(Simd::from_slice))
            .fold(Simd::<T, LANES>::splat(T::zero()), |acc, (sp, cp)| {
                let diff = sp - cp;
                acc + diff * diff
            });

          acc.reduce_sum()
        },
        CustomDistance::Complex(vec) => {
          vec.iter()
            .map(|&(ref ty, offset, end, koef)| {
              if end - offset == 2 {
                let dist = <CustomDistance<T> as DistanceFunction<T, LANES>>::distance(
                  &ty, &a[offset..end], &b[offset..end],
                );
                dist * koef
              } else {
                debug_assert_eq!((end-offset) % LANES, 0);

                let dist = ty.distance(&a[offset..end], &b[offset..end]);
                dist * koef
              }
            })
            .sum()
        }
      }
        
    }
}
