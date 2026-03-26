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

    pub fn increment_in_place(&self, data: &mut [u8]) {
        match self {
            NumberValue::Int64(v) => {
                let mut val = i64::from_be_bytes(data.try_into().unwrap());
                val += v;
                data.copy_from_slice(&val.to_be_bytes());
            }

            NumberValue::UInt64(v) => {
                let mut val = u64::from_be_bytes(data.try_into().unwrap());
                val += v;
                data.copy_from_slice(&val.to_be_bytes());
            }

            NumberValue::Float(v) => {
                let mut val = f32::from_be_bytes(data.try_into().unwrap());
                val += v;
                data.copy_from_slice(&val.to_be_bytes());
            }

            NumberValue::Double(v) => {
                let mut val = f64::from_be_bytes(data.try_into().unwrap());
                val += v;
                data.copy_from_slice(&val.to_be_bytes());
            }

            NumberValue::DateTime(v) => {
                let mut val = i64::from_be_bytes(data.try_into().unwrap());
                val += v;
                data.copy_from_slice(&val.to_be_bytes());
            }
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