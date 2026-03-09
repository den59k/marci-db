use bitvec::slice::BitSlice;

use crate::{marci_db::{get_end, get_offset, move_offsets, set_offset, set_offset_null}, schema::{Entity, FieldType}};

pub fn update_data(entity: &Entity, data: &[u8], new_data: &[u8], changed_mask: &BitSlice) -> Vec<u8> {
  let mut data = data.to_vec();
  let mut enum_data;

  let mut bitslice_idx = entity.fields.len();

  for (field_index, field) in entity.fields.iter().enumerate() {

    if field.offset_pos == 0 {
      continue;
    }

    let update_offset = get_offset(new_data, field.offset_pos);
    // Skip if hasn't new data
    if !changed_mask[field_index] {
      continue;
    }

    let offset = get_offset(&mut data, field.offset_pos);
    
    if offset == 0 && update_offset == 0 {
      continue;
    }

    let end = get_end(&data, field.offset_pos, entity.payload_offset);
    let update_end = if update_offset == 0 { 0 } else { get_end(new_data, field.offset_pos, entity.payload_offset) };

    if update_offset == 0 {
      let diff = -((end - offset) as isize);
      shift_and_resize(&mut data, end, offset, diff);
      move_offsets(&mut data, field.offset_pos+4, entity.payload_offset, diff);
      set_offset_null(&mut data, field.offset_pos);
      continue;
    }

    let mut new_data = &new_data[update_offset..update_end];
    if offset != 0 && let FieldType::Enum(en) = &field.ty {
      if &new_data[0..2] == &data[offset..offset+2] {
        let variant = &en.variants[u16::from_be_bytes(new_data[0..2].try_into().unwrap()) as usize];
        enum_data = update_data(&variant, &data[offset..], &new_data, &changed_mask[bitslice_idx..]);
        new_data = &enum_data;

        bitslice_idx += variant.fields.len();
      }
    }

    let len = if offset == 0 { 0 } else { end - offset };

    let diff = new_data.len() as isize - len as isize;
    
    let new_offset = if offset == 0 { end } else { offset };
    let new_end = (new_offset + new_data.len()) as usize;

    // Сдвигаем offsets, если изменилась длина поля
    if diff != 0 {
      shift_and_resize(&mut data, end, new_end, diff);
      move_offsets(&mut data, field.offset_pos+4, entity.payload_offset, diff);
    }

    data[new_offset..new_end].copy_from_slice(&new_data);

    if new_offset != offset {
      set_offset(&mut data, field.offset_pos, new_offset);
    }
  }

  return data;
}

pub fn set_field_null(entity: &Entity, data: &[u8], field_index: usize, enum_idx: Option<(usize, usize)>) -> Option<Vec<u8>> {

  let field = &entity.fields[field_index];
  let offset = get_offset(&data, field.offset_pos);
  if offset == 0 { return None }
  let offset_end: usize = get_end(&data, field.offset_pos, entity.payload_offset);

  if let Some(enum_idx) = enum_idx {
    let FieldType::Enum(en) = &field.ty else {
      panic!("Trying to get enum variant from non-enum field {}", field.full_name)
    };
    let variant = &en.variants[enum_idx.0];

    let Some(enum_data) = set_field_null(variant, &data[offset..offset_end], enum_idx.1, None) else {
      return None;
    };

    let new_offset_end = offset + enum_data.len();
    let diff = (new_offset_end as isize) - offset_end as isize;

    let mut data = data.to_vec();
    shift_and_resize(&mut data, offset_end, new_offset_end, diff);
    move_offsets(&mut data, field.offset_pos+4, entity.payload_offset, diff);

    return Some(data);
  } else {
    let diff = offset  as isize - offset_end as isize;

    let mut data = data.to_vec();
    shift_and_resize(&mut data, offset_end, offset, diff);
    move_offsets(&mut data, field.offset_pos+4, entity.payload_offset, diff);

    set_offset(&mut data, field.offset_pos, 0);
    return Some(data);
  }
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
    use serde_json::{Value, json};

    use crate::{marci_db::{InsertStruct, get_offset_from_field, get_offsets}, marci_decoder::decode_fields, marci_encoder::encode_document, schema::{FieldType, parse_schema}, update_data::{set_field_null, update_data}};


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
    let (data, _) = encode_document(&schema, model, &json, &mut structs).unwrap();

    let payload_offset = u16::from_be_bytes(data[1..3].try_into().unwrap()) as usize;
    assert_eq!(payload_offset, model.payload_offset);

    assert_eq!(data.len(), payload_offset + 3);
    assert_eq!(get_offsets(&data, model), vec![payload_offset, 0, 0]);

    // Update data
    let json_update = json!({
      "age": 30
    });
    let (new_data, changed_mask) = encode_document(&schema, model, &json_update, &mut structs).unwrap();

    let data = update_data(&model, &data, &new_data, &changed_mask);

    let payload_offset = u16::from_be_bytes(data[1..3].try_into().unwrap()) as usize;
    assert_eq!(payload_offset, model.payload_offset);

    assert_eq!(get_offsets(&data, model), vec![payload_offset, 0, payload_offset+3]);

    // Update data v2
    let json_update = json!({
      "name": "Bobber",
      "surname": "Tester"
    });
    let (new_data, changed_mask) = encode_document(&schema, model, &json_update, &mut structs).unwrap();

    let data = update_data(&model, &data, &new_data, &changed_mask);

    let payload_offset = u16::from_be_bytes(data[1..3].try_into().unwrap()) as usize;
    assert_eq!(payload_offset, model.payload_offset);
    assert_eq!(get_offsets(&data, model), vec![payload_offset, payload_offset + 6, payload_offset + 6 + 6]);

    // Update data v3
    let json_update = json!({
      "name": null,
      "surname": "",
      "age": 80
    });
    let (new_data, changed_mask) = encode_document(&schema, model, &json_update, &mut structs).unwrap();

    let data = update_data(&model, &data, &new_data, &changed_mask);

    let payload_offset = u16::from_be_bytes(data[1..3].try_into().unwrap()) as usize;
    assert_eq!(payload_offset, model.payload_offset);
    assert_eq!(get_offsets(&data, model), vec![0, payload_offset, payload_offset]);

  }

  #[test]
  pub fn test_update_enum() {
    let schema_str = "
      model Role {
        role        RoleKind
      }
      enum RoleKind {
        creator
        admin {
          admin_count    Int
          admin_features String[]
        }
      }
    ";

    let schema = parse_schema(schema_str);
    let model = &schema.models[0];
    let FieldType::Enum(en) = &model.fields[1].ty else {
      panic!("Field [0] is not a enum");
    };

    let json = json!({
      "role": "creator"
    });

    let mut structs = vec![];
    let (data, _) = encode_document(&schema, model, &json, &mut structs).unwrap();
    let mask = &bitvec::bitvec!(1;en.variants[1].fields.len());


    let json_update = json!({
      "role": "admin",
      "admin_count": 3,
      "admin_features": [ "tester", "tester2" ]
    });
    let (new_data, changed_mask) = encode_document(&schema, model, &json_update, &mut structs).unwrap();

    let data = update_data(&model, &data, &new_data, &changed_mask);
    assert_eq!(&data[model.payload_offset..model.payload_offset+2], &[0,1]);

    let mut obj = String::with_capacity(256);

    obj.push('{');
    decode_fields(&[], &data[model.payload_offset..], &en.variants[1].fields, &mut obj, mask, None, en.variants[1].payload_offset).unwrap();
    obj.push('}');

    assert_eq!(serde_json::from_str::<Value>(&obj).unwrap(), json!({
      "admin_count": 3,
      "admin_features": [ "tester", "tester2" ]
    }));



    let json_update = json!({
      "role": "admin",
      "admin_count": 10
    });
    let (new_data, changed_mask) = encode_document(&schema, model, &json_update, &mut structs).unwrap();

    let data = update_data(&model, &data, &new_data, &changed_mask);
    assert_eq!(&data[model.payload_offset..model.payload_offset+2], &[0,1]);

    let mut obj = String::with_capacity(256);
    obj.push('{');
    decode_fields(&[], &data[model.payload_offset..], &en.variants[1].fields, &mut obj, mask, None, en.variants[1].payload_offset).unwrap();

    obj.push('}');
    assert_eq!(serde_json::from_str::<Value>(&obj).unwrap(), json!({
      "admin_count": 10,
      "admin_features": [ "tester", "tester2" ]
    }));

  }

  #[test]
  pub fn test_set_null_field() {
    let schema_str = "
      model Role {
        role        RoleKind
      }

      enum RoleKind {
        creator
        admin {
          admin_count    Int
          admin_features String[]
        }
      }
    ";

    let schema = parse_schema(schema_str);
    let model = &schema.models[0];

    let FieldType::Enum(en) = &model.fields[1].ty else {
        panic!("Field is not enum");
    };

    //
    // 1) Кодируем документ с вариантом admin
    //
    let json = json!({
      "role": "admin",
      "admin_count": 3,
      "admin_features": [ "tester", "tester2" ]
    });

    let mut structs = vec![];
    let (data, _) = encode_document(&schema, model, &json, &mut structs).unwrap();

    // Проверяем, что вариант — admin (tag=1)
    assert_eq!(&data[model.payload_offset..model.payload_offset+2], &[0,1]);

    //
    // 2) Удаляем поле admin_features через set_field_null
    //
    let admin_variant = &en.variants[1];
    let field_idx_features = admin_variant.fields.iter()
        .position(|f| f.name == "admin_features")
        .unwrap();

    let updated_1 = set_field_null(
        admin_variant,
        &data[model.payload_offset..],        // данные enum
        field_idx_features,                   // индекс поля
        None                                  // нет вложенного enum
    ).expect("expected non-null after removing admin_features");

    // Проверяем что admin_features удалён
    {
        let mut obj = String::with_capacity(256);
        obj.push('{');

        let mask = &bitvec::bitvec!(1; admin_variant.fields.len());
        decode_fields(
            &[],
            &updated_1,
            &admin_variant.fields,
            &mut obj,
            mask,
            None,
            admin_variant.payload_offset
        ).unwrap();

        obj.push('}');

        assert_eq!(
            serde_json::from_str::<Value>(&obj).unwrap(),
            json!({
              "admin_count": 3,
              "admin_features": null
            })
        );
    }

    //
    // 3) Удаляем admin_count тоже
    //
    let field_idx_count = admin_variant.fields.iter()
        .position(|f| f.name == "admin_count")
        .unwrap();

    let updated_2 = set_field_null(
        admin_variant,
        &updated_1,
        field_idx_count,
        None
    ).expect("expected non-null after removing admin_count");

    // Теперь payload должен быть пустой объект
    {
        let mut obj = String::with_capacity(256);
        obj.push('{');
        let mask = &bitvec::bitvec!(1; admin_variant.fields.len());
        decode_fields(
            &[],
            &updated_2,
            &admin_variant.fields,
            &mut obj,
            mask,
            None,
            admin_variant.payload_offset
        ).unwrap();
        obj.push('}');

        assert_eq!(serde_json::from_str::<Value>(&obj).unwrap(), json!({ "admin_count": null, "admin_features": null }));

        assert_eq!(updated_2.len(), admin_variant.payload_offset);
    }


    let updated_full = set_field_null(model, &data, 1, None)
        .expect("enum should be nulled fully");

    // Проверяем что тег enum стал 0 (offset=0 → null)
    let offset = get_offset_from_field(&updated_full, &model.fields[1]);
    assert_eq!(offset, 0);

    assert_eq!(updated_full.len(), model.payload_offset);

  }

}