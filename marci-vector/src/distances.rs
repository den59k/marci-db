use std::{iter::{self}, ops::{self}, simd::{LaneCount, Simd, SupportedLaneCount, num::SimdFloat}};

// use kentro::KMeans;
use kmeans::{DistanceFunction, Primitive};

#[derive(Clone)]
pub enum CustomDistance<T> where T: Primitive {
  Complex(Vec<(Box<CustomDistance<T>>,usize,usize,T)>),  // First element - type, second - offset, third - end, last - koef
  Euclidean,
  Cosine
}


pub trait SupportedSimdArray<T: Primitive, const LANES: usize>:
    ops::Sub<Output = Self> + ops::Add<Output = Self> + ops::Mul<Output = Self> + ops::Div<Output = Self> + iter::Sum + SimdFloat<Scalar = T>
{
}

impl<T: Primitive, const LANES: usize> SupportedSimdArray<T, LANES> for Simd<T, LANES>
where
    LaneCount<LANES>: SupportedLaneCount,
    Simd<T, LANES>: ops::Sub<Output = Simd<T, LANES>>
        + ops::Add<Output = Simd<T, LANES>>
        + ops::Mul<Output = Simd<T, LANES>>
        + ops::Div<Output = Simd<T, LANES>>
        + iter::Sum
        + SimdFloat<Scalar = T>,
{
}


impl<T, const LANES: usize> DistanceFunction<T, LANES> for CustomDistance<T>
where
    T: Primitive,
    LaneCount<LANES>: SupportedLaneCount,
    Simd<T, LANES>: SupportedSimdArray<T, LANES>,
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
