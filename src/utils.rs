#[cfg(test)]
use crate::schema::{Entity, FieldLocation};

pub fn get_offset<'a>(data: &'a [u8], offset_pos: usize) -> usize {
  return u32::from_be_bytes(data[offset_pos..offset_pos + 4].try_into().unwrap()) as usize;
}

pub fn get_end(data: &[u8], offset_pos: usize, payload_offset: usize) -> usize {
  for j in ((offset_pos+4)..payload_offset).step_by(4) {
    let off_j = get_offset(data, j);
    if off_j != 0 {
      return off_j;
    }
  }

  return data.len();
}

#[cfg(test)]
pub fn get_offsets(data: &[u8], model: &Entity) -> Vec<usize> {
  let mut arr = vec![];
  for field in model.fields.iter() {
    let FieldLocation::Body { offset } = field.location else {
      continue;
    };
    let offset = get_offset(data, offset);
    arr.push(offset);
  }
  return arr;
}