use serde_json::Value;

use crate::{delete_op::{DeleteAction, DeleteIndex, DeleteOp, DependencyAction, DependencyActionType, RefToDelete}, index_utils::encode_index, json_parsers::{EncodeError, parsers::encode_id}, schema::{DeleteConstraint, Entity, FieldLocation, FieldType, RefBinding, Schema}, utils::get_next_id_value};

pub fn parse_delete<'a>(schema: &'a Schema, entity: &'a Entity, json_val: &Value) -> Result<DeleteOp<'a>, EncodeError> {
  let id = encode_id(schema, entity, json_val)?;
  Ok(DeleteOp { entity, action: parse_delete_internal(schema, entity, Some(&id)), id })
}

pub fn parse_delete_internal<'a>(schema: &'a Schema, entity: &'a Entity, id: Option<&[u8]>) -> DeleteAction<'a> {
  DeleteAction {
    indexes_to_delete: collect_indexes_to_delete(schema, entity, id),
    dependencies: collect_dependency_actions(schema, entity),
    refs_to_delete: collect_refs_to_delete(schema, entity)
  }
}

pub fn collect_indexes_to_delete<'a>(schema: &'a Schema, entity: &'a Entity, id: Option<&[u8]>) -> Vec<DeleteIndex<'a>> { 
  let mut indexes_to_delete = vec![];
  
  let mut offset = 0;
  
  for field in entity.fields.iter() {
    match field.location {
      FieldLocation::Key { index: id_idx } => {
        if let Some(id) = id {
          let value = get_next_id_value(field, &id, schema, offset);
          for index in field.indexes.iter() {
            indexes_to_delete.push(DeleteIndex::Value { index, key: [ &encode_index(field, index, value), id ].concat() });
          }
          offset += value.len();
        } else {
          for index in field.indexes.iter() {
            indexes_to_delete.push(DeleteIndex::KeyValue { index, field, id_idx });
          }
        }
      },
      FieldLocation::Body { offset } => {
        if field.indexes.is_empty() {
          continue;
        }
        for index in field.indexes.iter() {
          indexes_to_delete.push(DeleteIndex::BodyValue { index, field, offset_pos: offset });
        }
      },
      FieldLocation::Virtual => {},
    }
  }

  return indexes_to_delete;
}

pub fn collect_refs_to_delete<'a>(schema: &'a Schema, entity: &Entity) -> Vec<RefToDelete<'a>> {
  let mut resp = vec![];

  for field in entity.fields.iter() {
    let (FieldType::Ref(ref_info) | FieldType::RefList(ref_info)) = &field.ty else {
      continue;
    };

    match &ref_info.binding {
      RefBinding::CurrentId => {
        // Временный фикс - мы не должны удалять родителей у дочерних ключей
        if field.name == "@parent_id" {
          continue;
        }
        let ref_entity = &schema.models[ref_info.model_index];
        let action = parse_delete_internal(schema, ref_entity, None);
        resp.push(RefToDelete::ChildEntity { entity: &ref_entity, action });
      },
      RefBinding::FieldValue => continue,
      RefBinding::IndexTree(tree_name) => {
        resp.push(RefToDelete::Index { tree_name: tree_name.clone() });
      }
    }
  }

  return resp;
}

pub fn collect_dependency_actions<'a>(schema: &'a Schema, entity: &'a Entity) -> Vec<DependencyAction<'a>> { 
  
  let mut dependencies = vec![];

  for dep in entity.rev_dependencies.iter() {
    let rev_entity = &schema.models[dep.model_index];
    let rev_field = &rev_entity.fields[dep.field_index];

    let (FieldType::Ref(rev_ref_info) | FieldType::RefList(rev_ref_info)) = &rev_field.ty else {
      panic!("Entity rev dependency has wrong type {} {:?}", rev_field.full_name, rev_field.ty);
    };
    assert_eq!(&schema.models[rev_ref_info.model_index].name, &entity.name);

    let action_type = match dep.constraint {
      DeleteConstraint::SetNull => {
        match &rev_field.location {
          FieldLocation::Body { offset: offset_pos } => DependencyActionType::SetNull { offset_pos: *offset_pos },
          _ => panic!("Cannot set null on non-body values")
        }
      },
      DeleteConstraint::Restrict => DependencyActionType::Restrict,
      DeleteConstraint::Cascade => DependencyActionType::Delete(parse_delete_internal(schema, rev_entity, None))
    };

    // Если обратное поле существует - нам повезло, можно сразу найти элементы, которые связаны с элементом. Если нет - придется перебирать все элементы
    if let Some(field_idx) = rev_ref_info.rev_field_idx {
      let field = &entity.fields[field_idx];
      let (FieldType::Ref(ref_info) | FieldType::RefList(ref_info)) = &field.ty else {
        panic!("Entity dependency has wrong type {} {:?}", rev_field.full_name, rev_field.ty);
      }; 

      dependencies.push(DependencyAction { rev_entity, rev_field, rev_binding: &rev_ref_info.binding, binding: Some((&field, &ref_info.binding)), action_type });
    } else {
      dependencies.push(DependencyAction { rev_entity, rev_field, rev_binding: &rev_ref_info.binding, binding: None, action_type });
    }
  }

  return dependencies
}