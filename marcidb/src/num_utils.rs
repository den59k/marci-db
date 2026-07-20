use std::cmp::Ordering;

#[derive(Debug, Clone, PartialEq)]
pub enum NumberValue {
    Int64(i64),
    UInt64(u64),
    Float(f32),
    Double(f64),
    DateTime(i64),
}


impl NumberValue {
    pub fn compare_with_bytes(&self, data: &[u8]) -> Option<Ordering> {
        match self {
            NumberValue::Int64(f) => {
                Some(i64::from_be_bytes(data.try_into().ok()?).cmp(f))
            },
            NumberValue::UInt64(f) => {
                u64::from_be_bytes(data.try_into().ok()?).partial_cmp(f)
            },
            NumberValue::Float(f) => {
                f32::from_be_bytes(data.try_into().ok()?).partial_cmp(f)
            },
            NumberValue::Double(f) => {
                f64::from_be_bytes(data.try_into().ok()?).partial_cmp(f)
            },
            NumberValue::DateTime(f) => {
                Some(i64::from_be_bytes(data.try_into().ok()?).cmp(f))
            },
        }
    }

    // pub fn get_num_type(&self) -> Option<FieldIndexNum> {
    //     return match *self {
    //         WhereNumValue::DateTime(_) | WhereNumValue::Int64(_) => Some(FieldIndexNum::Int64),
    //         WhereNumValue::Float(_) => Some(FieldIndexNum::Float),
    //         WhereNumValue::Double(_) => Some(FieldIndexNum::Double),
    //         WhereNumValue::UInt64(_) => Some(FieldIndexNum::UInt64)
    //     }
    // }
}

/// The delta of an `$increment`. Deliberately separate from [`NumberValue`]: a delta on an *unsigned* field
/// is itself signed — `UInt64` is stored as `u64` but may be decremented — so a value and a delta cannot
/// share a representation. The variant names the field's storage type; the payload is the signed delta.
#[derive(Debug, Clone, PartialEq)]
pub enum NumberDelta {
    Int64(i64),
    UInt64(i64),
    Float(f32),
    Double(f64),
    DateTime(i64),
}

impl NumberDelta {
    /// Applies the delta to a stored big-endian value, returning `None` if the result would leave the
    /// field's range — including a negative result on an unsigned field. The caller rejects the update
    /// rather than storing a wrapped number: with overflow checks off (the release default) `+` would
    /// silently wrap, which for a counter or a balance is worse than a failed write.
    pub fn checked_apply(&self, data: &[u8]) -> Option<Vec<u8>> {
        Some(match self {
            NumberDelta::Int64(v) | NumberDelta::DateTime(v) => {
                i64::from_be_bytes(data.try_into().ok()?).checked_add(*v)?.to_be_bytes().to_vec()
            }
            NumberDelta::UInt64(v) => {
                u64::from_be_bytes(data.try_into().ok()?).checked_add_signed(*v)?.to_be_bytes().to_vec()
            }
            // Floats saturate to an infinity instead of wrapping, so range is checked on the result.
            NumberDelta::Float(v) => {
                let val = f32::from_be_bytes(data.try_into().ok()?) + v;
                if !val.is_finite() { return None }
                val.to_be_bytes().to_vec()
            }
            NumberDelta::Double(v) => {
                let val = f64::from_be_bytes(data.try_into().ok()?) + v;
                if !val.is_finite() { return None }
                val.to_be_bytes().to_vec()
            }
        })
    }
}