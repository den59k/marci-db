use bitvec::vec::BitVec;

use crate::{marci_db::{get_end, get_offset, move_offsets, set_offset, set_offset_null}, schema::Field};

pub fn update_data(fields: &[Field], payload_offset: usize, data: &[u8], new_data: &[u8], changed_mask: &BitVec) -> Vec<u8> {
  let mut data = data.to_vec();

  for field in fields.iter() {

    if field.offset_pos == 0 {
      continue;
    }

    let update_offset = get_offset(new_data, field.offset_pos);
    // Skip if hasn't new data
    if !changed_mask[field.offset_index] {
      continue;
    }

    let offset = get_offset(&mut data, field.offset_pos);
    
    if offset == 0 && update_offset == 0 {
      continue;
    }

    let end = get_end(&data, field.offset_pos, payload_offset);
    let update_end = if update_offset == 0 { 0 } else { get_end(new_data, field.offset_pos, payload_offset) };

    let update_len = if update_offset == 0 { 0 } else { update_end-update_offset };
    let len = if offset == 0 { 0 } else { end - offset };

    let diff = update_len as isize - len as isize;
    
    let new_offset = if offset == 0 { end } else { offset };
    let new_end = (new_offset + update_len) as usize;

    // Сдвигаем offsets, если изменилась длина поля
    if diff != 0 {
      shift_and_resize(&mut data, end, new_end, diff);
      move_offsets(&mut data, field.offset_pos+4, payload_offset, diff);
    }

    if update_offset == 0 {
      set_offset_null(&mut data, field.offset_pos);
    } else {
      data[new_offset..new_end].copy_from_slice(&new_data[update_offset..update_end]);

      if new_offset != offset {
        set_offset(&mut data, field.offset_pos, new_offset);
      }
    }
  }

  return data;
}

#[inline(always)]
fn shift_and_resize(data: &mut Vec<u8>, from: usize, to: usize, diff: isize) {
  let len = data.len();
  let new_len = ((data.len() as isize) + diff) as usize;

  if from == len {
    data.resize(new_len, 0u8);
    return;
  }
  
  if diff > 0 {
    data.resize(new_len, 0u8);
    data.copy_within(from..len, to);
  } else {
    data.copy_within(from..len, to);
    data.truncate(new_len);
  }
}


#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::{marci_db::{InsertStruct, get_offsets}, marci_encoder::encode_document, schema::parse_schema, update_data::update_data};


  #[test]
  fn test_update_doc() {
    let schema_str = "
model User {
  name        String
  surname     String
  age         Int
}
";
    let schema = parse_schema(schema_str);

    let mut structs: Vec<InsertStruct> = vec![];
    let json = json!({
      "name": "Bob"
    });
    let model = &schema.models[0];
    let (mut data, _) = encode_document(model, &json, &mut structs).unwrap();

    let payload_offset = u16::from_be_bytes(data[1..3].try_into().unwrap()) as usize;
    assert_eq!(payload_offset, 3 + 4 * 3);
    println!("{:?} {}", data, data.len());

    assert_eq!(data.len(), payload_offset + 3);
    assert_eq!(get_offsets(&data, model), vec![payload_offset, 0, 0]);

    // Update data
    let json_update = json!({
      "age": 30
    });
    let (new_data, changed_mask) = encode_document(model, &json_update, &mut structs).unwrap();

    data = update_data(&model.fields, model.payload_offset, &data, &new_data, &changed_mask);

    let payload_offset = u16::from_be_bytes(data[1..3].try_into().unwrap()) as usize;
    assert_eq!(payload_offset, 3 + 4 * 3);

    assert_eq!(get_offsets(&data, model), vec![payload_offset, 0, payload_offset+3]);

    // Update data v2
    let json_update = json!({
      "name": "Bobber",
      "surname": "Tester"
    });
    let (new_data, changed_mask) = encode_document(model, &json_update, &mut structs).unwrap();

    data = update_data(&model.fields, model.payload_offset, &data, &new_data, &changed_mask);

    let payload_offset = u16::from_be_bytes(data[1..3].try_into().unwrap()) as usize;
    assert_eq!(payload_offset, 3 + 4 * 3);
    assert_eq!(get_offsets(&data, model), vec![payload_offset, payload_offset + 6, payload_offset + 6 + 6]);

    // Update data v3
    let json_update = json!({
      "name": null,
      "surname": "",
      "age": 80
    });
    let (new_data, changed_mask) = encode_document(model, &json_update, &mut structs).unwrap();

    data = update_data(&model.fields, model.payload_offset, &data, &new_data, &changed_mask);

    let payload_offset = u16::from_be_bytes(data[1..3].try_into().unwrap()) as usize;
    assert_eq!(payload_offset, 3 + 4 * 3);
    assert_eq!(get_offsets(&data, model), vec![0, payload_offset, payload_offset]);

  }

}