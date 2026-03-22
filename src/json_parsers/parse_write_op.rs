use bitvec::{bitvec};
use chrono::Local;
use serde_json::{Map, Value};

use crate::{Field, index_utils::{encode_data, encode_index_number}, json_parsers::parsers::{EncodeError, encode_enum, encode_list, encode_primitive_value}, schema::{Entity, FieldDefault, FieldIndex, FieldLocation, FieldType, Schema}, utils::check_exists_condition, write_op::{WriteDefault, WriteIndex, WriteOp, WriteRelation}};

const VERSION: u8 = 1;

pub fn parse_insert<'a>(schema: &'a Schema, entity: &'a Entity, json_val: &Value) -> Result<WriteOp<'a>, EncodeError> {
    let obj = json_val
        .as_object()
        .ok_or(EncodeError::NotAnObject)?;

    return from_json_internal(schema, entity, obj, true, None);
}    

pub fn parse_update<'a>(schema: &'a Schema, entity: &'a Entity, json_val: &Value) -> Result<WriteOp<'a>, EncodeError> {
    let obj = json_val
        .as_object()
        .ok_or(EncodeError::NotAnObject)?;

    return from_json_internal(schema, entity, obj, false, None);
}    

fn from_json_internal<'a>(
    schema: &'a Schema, 
    entity: &'a Entity, 
    obj: &Map<String, Value>,
    is_create: bool,
    parent: Option<&Entity>
) -> Result<WriteOp<'a>, EncodeError> {
    let mut mask = bitvec![0; entity.fields.len()];
    let mut refs: Vec<WriteRelation<'_>> = vec![];
    let mut defaults: Vec<WriteDefault> = vec![];
    let mut write_indexes = vec![];

    let mut data = Vec::with_capacity(entity.payload_offset + 128);
    let mut id = vec![];
    // version
    data.push(VERSION);
    // payload_offset
    data.extend_from_slice(&(entity.payload_offset as u16).to_be_bytes());
    // offsets (плейсхолдеры)
    data.resize(entity.payload_offset, 0);

    for (field_index, field) in entity.fields.iter().enumerate() {
        if is_create {
            if let Some(default_value) = &field.default_value {
                match field.location {
                    FieldLocation::Key { index: _ } => {
                        if let Some(offset) = parse_default_value(&mut id, field, default_value)? {
                            defaults.push(WriteDefault::Key(offset, default_value));
                        }
                    }
                    FieldLocation::Body { offset } => {
                        write_header(&mut data, offset);
                        if let Some(offset) = parse_default_value(&mut data, field, default_value)? {
                            defaults.push(WriteDefault::Body(offset, default_value));
                        }
                    }
                    _ => {}
                }
                continue;
            }
        }

        // Если ключ является родительским, то мы сразу его заполняем значением
        if let Some(parent) = parent && schema.is_parent_key(field, parent) {
            defaults.push(WriteDefault::ParentId(id.len()));
            continue;
        }

        // Пропускаем внутренние значения enum, если у нас стоит не тот enum
        if !check_exists_condition(entity, &field.condition, &id, &data, schema) {
            continue;
        }

        let Some(value) = obj.get(&field.name) else {
            if matches!(field.location, FieldLocation::Key { .. }) {
                return Err(EncodeError::MissingIdField(field.full_name.clone()));
            }
            // TODO: check for required fields here
            continue;
        };

        mask.set(field_index, true);

        if value.is_null() {
            // TODO: add check not-null here
            continue;
        }

        let value_data: &[u8];

        match field.location {
            FieldLocation::Key { index: _ } => {
                let start_offset = id.len();
                encode_id_value(&mut id, field, schema, value)?;

                // Добавляем нуль-терминатор в id, если размер неизвестен (для разделения)
                if field.get_size().is_none() {
                    id.push(b'\0');
                    value_data = &id[start_offset..id.len()-1];
                } else {
                    value_data = &id[start_offset..];
                }
            },
            FieldLocation::Body { offset } => {
                let start_offset = data.len();
                match &field.ty {
                    FieldType::Primitive(primitive_type) => {
                        write_header(&mut data, offset);
                        encode_primitive_value(&mut data, field, &primitive_type, value)?;
                    }
                    FieldType::PrimitiveList(primitive_type) => {
                        write_header(&mut data, offset);
                        encode_list(&mut data, value, field, &primitive_type, None)?;
                    },
                    FieldType::PrimitiveFixedList(primitive_type, fixed_size) => {
                        write_header(&mut data, offset);
                        encode_list(&mut data, value, field, &primitive_type, Some(*fixed_size))?;
                    },
                    FieldType::Ref (ref_info) => {
                        let ref_entity = &schema.models[ref_info.model_index];
                        let connect_id = encode_id(schema, ref_entity, value)?;
                        write_header(&mut data, offset);
                        data.extend(connect_id);
                        
                        let ref_id = encode_id(schema, ref_entity, value)?;
                        refs.push(WriteRelation::Connect { field, ids: vec![ ref_id ] });
                    },
                    FieldType::Enum(enum_def) => {
                        write_header(&mut data, offset);
                        encode_enum(&mut data, field, enum_def, value)?;
                    }
                    _ => { 
                        return Err(EncodeError::UnavailableKeyField(field.full_name.clone()));
                    }
                }
                value_data = &data[start_offset..];
            }
            FieldLocation::Virtual => {
                value_data = &[];
                match &field.ty {
                    FieldType::Ref (ref_info) => {
                        // Здесь может быть как структура, так и модель. Если структура, используем insert
                        let ref_entity = &schema.models[ref_info.model_index];
                        if ref_entity.autoinsert {
                            let Some(value) = value.as_object() else {
                                return Err(EncodeError::type_mismatch(field, "{ }"))
                            };
                            let op = from_json_internal(schema, ref_entity, value, true, Some(entity))?;
                            refs.push(WriteRelation::Create { field, op });
                        } else {
                            let ref_id = encode_id(schema, ref_entity, value)?;
                            refs.push(WriteRelation::Connect { field, ids: vec![ref_id] });
                        }
                    },
                    FieldType::RefList (ref_info) => {
                        let ref_entity = &schema.models[ref_info.model_index];
                        let Some(value) = value.as_array() else {
                            return Err(EncodeError::type_mismatch(field, "Array"))
                        };
                        if value.is_empty() {
                            refs.push(WriteRelation::Empty { field, st: ref_entity });
                            continue;
                        }

                        if ref_entity.autoinsert {
                            let mut ops = Vec::with_capacity(value.len());
                            for item in value {
                                let Some(item) = item.as_object() else {
                                return Err(EncodeError::type_mismatch(field, "{ }"))
                                };
                                let op = from_json_internal(schema, ref_entity, item, true, Some(entity))?;
                                ops.push(op);
                            }
                            refs.push(WriteRelation::CreateMany { field, ops });
                        } else {
                            let mut ids = Vec::with_capacity(value.len());
                            for obj in value.iter() {
                                let id = encode_id(schema, ref_entity, obj)?;
                                ids.push(id);
                            }
                            refs.push(WriteRelation::Connect { field, ids });
                        }
                    },
                    _ => { 
                        return Err(EncodeError::DerivedFieldNotWritable(field.full_name.clone()));
                    }
                }
            }
        }

        for index in field.indexes.iter() {
            match index {
                FieldIndex::Value { tree_name, .. } => {
                    write_indexes.push(WriteIndex::Value(tree_name.clone(), encode_data(field, value_data)));
                }
                FieldIndex::Number { tree_name, ty, .. } => {
                    write_indexes.push(WriteIndex::Value(tree_name.clone(), encode_index_number(ty, value_data) ));
                }
                _ => {}
            }
        }
   }

   Ok(WriteOp { id, data, refs, mask, entity, defaults, write_indexes })
}

// Метод, который кодирует только ID (и ругается, если пропущено какое-либо поле)
pub fn encode_id<'a>(schema: &'a Schema, entity: &'a Entity, json_val: &Value) -> Result<Vec<u8>, EncodeError> {
   let obj = json_val
      .as_object()
      .ok_or(EncodeError::NotAnObject)?;

   let mut id = vec![];
   for field in entity.fields.iter() {
      let FieldLocation::Key { index: _ } = field.location else {
         continue;
      };
      let Some(value) = obj.get(&field.name) else {
         return Err(EncodeError::MissingIdField(field.full_name.clone()));
      };
      if value.is_null() {
         return Err(EncodeError::IdFieldIsNull(field.full_name.clone()));
      }
      
      encode_id_value(&mut id, field, schema, value)?;
   }

   Ok(id)
}

// Записывает значение по умолчанию. Если значение вычисляется во время записи - возвращает offset
fn parse_default_value(dst: &mut Vec<u8>, field: &Field, default_value: &FieldDefault) -> Result<Option<usize>, EncodeError> {
    match default_value {
        FieldDefault::Value(val) => {
            dst.extend_from_slice(val);
            return Ok(None)
        },
        FieldDefault::Now => {
            let now = Local::now().timestamp_millis();
            dst.extend_from_slice(&now.to_be_bytes());
            return Ok(None)
        },
        FieldDefault::Counter(_) => {
            let offset = dst.len();
            encode_empty(dst, field)?;
            return Ok(Some(offset));
        }
    }
}

// Записывает пустые байты. Само значение заполняется во время выполнения insert
fn encode_empty(dst: &mut Vec<u8>, field: &Field) -> Result<(), EncodeError> {
    match field.ty {
        FieldType::Primitive(primitive_type) => {
            let Some(size) = primitive_type.get_size() else {
                return Err(EncodeError::FieldHasDynamicSize(field.full_name.clone()))
            };
            dst.resize(dst.len() + size, 0);
        }
        _ => {
            return Err(EncodeError::FieldHasDynamicSize(field.full_name.clone()))
        }
    }
    Ok(())
}

fn encode_id_value(dst: &mut Vec<u8>, field: &Field, schema: &Schema, value: &Value) -> Result<(), EncodeError> {
    match &field.ty {
        FieldType::Primitive(primitive_type) => {
            encode_primitive_value( dst, field, &primitive_type, value)?;
        }
        FieldType::Ref (ref_info) => {
            let connect_id = encode_id(schema, &schema.models[ref_info.model_index], value)?;
            dst.extend(connect_id);
        }
        _ => {
            return Err(EncodeError::UnavailableKeyFieldId(field.full_name.clone()))
        }
    }
    Ok(())
}

/// Записывает в offset текущий курсор на buf
fn write_header(dst: &mut [u8], offset_pos: usize) {
   let start = dst.len() as u32;
   dst[offset_pos..offset_pos + 4].copy_from_slice(&start.to_be_bytes());
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use crate::{json_parsers::{parse_insert, parsers::encode_list}, parse_schema, utils::{get_data, get_end, get_offsets}, write_op::{WriteDefault, WriteRelation}};

    #[test]
    fn encode_test() {
        let schema = parse_schema("
            model User {
                name        String
                age         UInt
                profile     Profile
            }
            model Profile {
                
            }
        ");  

        let model = &schema.models[0];

        let input = json!({
            "name": "Alice",
            "age": 30,
            "profile": { "id": 1 }
        });

        let encoded = parse_insert(&schema, model, &input).unwrap();

        // Проверяем версию
        assert_eq!(encoded.data[0], 1);
        match encoded.defaults[0] {
            WriteDefault::Key(offset, _) => assert_eq!(offset, 0),
            _ => panic!("Wrong default value")
        }

        // Читаем field_count
        let field_count = u16::from_be_bytes(encoded.data[1..3].try_into().unwrap());
        assert_eq!(field_count, model.payload_offset as u16);

        // Читаем смещения
        let offset_name = u32::from_be_bytes(encoded.data[3..7].try_into().unwrap()) as usize;
        let offset_age  = u32::from_be_bytes(encoded.data[7..11].try_into().unwrap()) as usize;
        let _offset_profile  = u32::from_be_bytes(encoded.data[11..15].try_into().unwrap()) as usize;

        assert_eq!(offset_name, model.payload_offset);

        // Проверяем, что смещения действительно указывают на данные
        // name: [len=5][bytes]
        let name_end = get_end(&encoded.data, 3, model.payload_offset);

        let name_value = &encoded.data[offset_name .. name_end];
        assert_eq!(name_value, b"Alice");
        
        // age: i64
        let age_bytes = &encoded.data[offset_age .. offset_age + 8];
        let age_value = i64::from_be_bytes(age_bytes.try_into().unwrap());
        assert_eq!(age_value, 30);
        
        assert_eq!(encoded.id, vec![0u8;8]);

        assert_eq!(encoded.defaults.len(), 1);
    }

    #[test]
    fn test_encode_struct_data() {
        let schema=  parse_schema("
            model User {
                name        String
            }

            model Project {
                name        String
                users       UserRole[]
            }

            struct UserRole {
                user        User          @id
                role        String
            }
        ");
        let model = &schema.models[1];

        let input = json!({
            "name": "First project",
            "users": [{ 
                "user": { "id": 1 },
                "role": "creator"
            }]
        });

        let encoded = parse_insert(&schema, model, &input).unwrap();
        
        // Проверяем версию
        assert_eq!(encoded.data[0], 1);

        assert_eq!(get_offsets(&encoded.data, model), vec![ model.payload_offset ]); // 3 bytes + 4 byte offset
        assert_eq!(encoded.refs.len(), 1);

        let WriteRelation::CreateMany { ops, .. } = &encoded.refs[0] else {
            panic!("Expected WriteRelation::CreateMany, found {:?}", encoded.refs[0]);
        };

        assert_eq!(ops.len(), 1);
        assert!(matches!(ops[0].defaults[0], WriteDefault::ParentId(0)));
        assert_eq!(&ops[0].id, &[ 0, 0, 0, 0, 0, 0, 0, 1]);
        assert_eq!(get_offsets(&ops[0].data, ops[0].entity), vec![ model.payload_offset ]);
        
        // assert_eq!()

    }   

    #[test]
    fn test_encode_relation_data() {
        let schema = parse_schema("
            model User {
                name        String
                posts       Post[]  @derived(Post.author)
            }

            model Post {
                title        String
                author       User?
            }
        ");

        let post_model = &schema.models[1];
        let input = json!({
            "title": "First Post",
            "author": { "id": 1 },
        });

        let encoded = parse_insert(&schema, post_model, &input).unwrap();
        assert_eq!(encoded.refs.len(), 1);
    }

    #[test]
    fn test_encode_enum() {
        let schema = parse_schema("
            model User {
                name        String
            }

            model Project {
                name        String
                users       UserRole[]
            }

            enum RoleKind {
                viewer
                admin {
                    features String[]
                }
            }

            struct UserRole {
                user        User          @id
                role        RoleKind
            }
        ");

        let project_model = &schema.models[1];

         // Проверяем, что field из enum на месте
        { 
            let input = json!({
                "name": "First project",
                "users": [{ 
                    "user": { "id": 1 },
                    "role": "admin",
                    "features": [ "root", "tester" ]
                }]
            });

            let encoded = parse_insert(&schema, project_model, &input).unwrap();
            assert_eq!(encoded.refs.len(), 1);
                
            let WriteRelation::CreateMany { ops, .. } = &encoded.refs[0] else {
                panic!("Wrong encoded ref type {:?}", encoded.refs[0]);
            };

            let enum_field = ops[0].entity.fields.iter().find(|i| i.name == "role").unwrap();
            assert_eq!(get_data(ops[0].entity, enum_field, &ops[0].id, &ops[0].data, &schema).unwrap(), &[ 0, 1 ]);
            
            let features_field = ops[0].entity.fields.iter().find(|i| i.name == "features").unwrap();

            let mut buf = vec![];
            
            let features_arr = json!([ "root", "tester" ]);
            encode_list(&mut buf, &features_arr, features_field, &crate::schema::PrimitiveFieldType::String, None).unwrap();

            assert_eq!(get_data(ops[0].entity, features_field, &ops[0].id, &ops[0].data, &schema).unwrap(), buf);
        }

        // Проверяем, что field из другого option не записывается в данные
        {
            let input = json!({
                "name": "First project",
                "users": [{ 
                    "user": { "id": 1 },
                    "role": "viewer",
                    "features": [ "root", "tester" ]
                }]
            });

            let encoded = parse_insert(&schema, project_model, &input).unwrap();
            assert_eq!(encoded.refs.len(), 1);

            let WriteRelation::CreateMany { ops, .. } = &encoded.refs[0] else {
                panic!("Wrong encoded ref type {:?}", encoded.refs[0]);
            };

            let enum_field = ops[0].entity.fields.iter().find(|i| i.name == "role").unwrap();
            assert_eq!(get_data(ops[0].entity, enum_field, &ops[0].id, &ops[0].data, &schema).unwrap(), &[ 0, 0 ]);

            let features_field = ops[0].entity.fields.iter().find(|i| i.name == "features").unwrap();
            assert_eq!(get_data(ops[0].entity, features_field, &ops[0].id, &ops[0].data, &schema), None);
        }
    }
}