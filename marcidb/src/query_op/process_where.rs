use memchr::memmem;

use crate::{query_op::{EnumListFilter, FieldCompare, FieldCompareRef, PrefixKey, TransationContext, Where, process_query::{get_ids_by_prefix, get_prefix}}, schema::{Entity}, utils::get_data};

pub fn process_where<'a, 'b, F>(id: &'b [u8], body: &'b [u8], ctx: &mut TransationContext<'a, F>, entity: &Entity, where_op: &Where<'a>) -> bool {

  match where_op {
    Where::True => true,
    Where::And(items) => items.iter().all(|f| process_where(id, body, ctx, entity, f)),
    Where::Or(items) => items.iter().any(|f| process_where(id, body, ctx, entity, f)),
    Where::Not(where_op) => !process_where(id, body, ctx, entity, where_op),
    Where::Field(field, field_compare) => {
      if let FieldCompare::Ref(ref_entity, prefix_key, field_compare_ref) = field_compare {
        match field_compare_ref {
          FieldCompareRef::Every(where_op) => {
            return has_items(ctx,  *ref_entity, where_op, prefix_key, entity, id, body, true)
          },
          FieldCompareRef::Some(where_op) => {
            return has_items(ctx,  *ref_entity, where_op, prefix_key, entity, id, body, false)
          },
          FieldCompareRef::None(where_op) => {
            return !has_items(ctx,  *ref_entity, where_op, prefix_key, entity, id, body, false)
          },
          FieldCompareRef::Eq(where_op) => {
            return has_one_item(ctx, *ref_entity, where_op, prefix_key, entity, id, body)
          },
          FieldCompareRef::Ne(where_op) => {
            return !has_one_item(ctx, *ref_entity, where_op, prefix_key, entity, id, body)
          },
          FieldCompareRef::Exists => {
            return has_one_item_exists(ctx, *ref_entity, prefix_key, entity, id, body)
          },
          FieldCompareRef::NotExists => {
            return !has_one_item_exists(ctx, *ref_entity, prefix_key, entity, id, body)
          }
        }
      }

      match field_compare {
        FieldCompare::EnumListSome(_filter) |
        FieldCompare::EnumListEvery(_filter) |
        FieldCompare::EnumListNone(_filter) => {
          let raw = get_data(entity, field, id, body, ctx.schema);

          // Пустой список: $every → true (пустое утверждение верно), остальные → false
          let Some(data) = raw else {
            return matches!(field_compare, FieldCompare::EnumListEvery(_));
          };

          let count = u32::from_be_bytes(data[0..4].try_into().unwrap()) as usize;
          if count == 0 {
            return matches!(field_compare, FieldCompare::EnumListEvery(_));
          }

          let outer_table = 4usize; // начало таблицы offset-ов во внешнем списке

          return match field_compare {
            FieldCompare::EnumListSome(f) => {
              (0..count).any(|i| {
                let item = get_enum_list_item(data, outer_table, i);
                enum_list_item_matches(item, f)
              })
            },
            FieldCompare::EnumListEvery(f) => {
              (0..count).all(|i| {
                let item = get_enum_list_item(data, outer_table, i);
                enum_list_item_matches(item, f)
              })
            },
            FieldCompare::EnumListNone(f) => {
              !(0..count).any(|i| {
                let item = get_enum_list_item(data, outer_table, i);
                enum_list_item_matches(item, f)
              })
            },
            _ => unreachable!()
          };
        },
        _ => {}
      }


      let Some(data) = get_data(entity, field, id, body, ctx.schema) else {
        return match field_compare {
          FieldCompare::EqNull => true,
          FieldCompare::NeNull => false,
          FieldCompare::Ne(_) => true,
          FieldCompare::In(_, has_null) => *has_null,
          FieldCompare::NotIn(_, has_null) => !(*has_null),
          _ => false
        }
      };

      match field_compare {
        FieldCompare::EqNull => false,
        FieldCompare::NeNull => true,

        FieldCompare::In(items, _) => items.iter().any(|f| f == data),
        FieldCompare::NotIn(items, _) => items.iter().all(|f| f != data),
        FieldCompare::Eq(f) => f == data,
        FieldCompare::Ne(f) => f != data,

        FieldCompare::Gt(num_value) => num_value.compare_with_bytes(data).map(|f| f.is_gt()).unwrap_or(false),
        FieldCompare::Gte(num_value) => num_value.compare_with_bytes(data).map(|f| f.is_ge()).unwrap_or(false),
        FieldCompare::Lt(num_value) => num_value.compare_with_bytes(data).map(|f| f.is_lt()).unwrap_or(false),
        FieldCompare::Lte(num_value) => num_value.compare_with_bytes(data).map(|f| f.is_le()).unwrap_or(false),

        FieldCompare::StringStartsWith(value) => data.starts_with(value),
        FieldCompare::StringIncludes(value) => memmem::find(data, value).is_some(),

        _ => false
      }
    },
  }
}

pub fn has_items<'a, F>(
  ctx: &mut TransationContext<'a, F>, 
  entity: &Entity, 
  where_op: &Where<'a>, 
  prefix_key: &PrefixKey, 
  parent_entity: &Entity,
  parent_id: &[u8], 
  parent_body: &[u8],
  check_all: bool
) -> bool {
  if let PrefixKey::ParentIndexTree(index_tree_name) = prefix_key {
    let index_tree = ctx.get_tree(index_tree_name);
    let ids = get_ids_by_prefix(&index_tree, parent_id);

    if ids.is_empty() {
      return check_all;
    }
    let tree = ctx.get_tree(&entity.name);
    if check_all {
      return ids.into_iter().all(|id| {
        let value = tree.get(&id).unwrap().unwrap();
        process_where(&id, &value, ctx, entity, where_op)
      });
    } else {
      return ids.into_iter().any(|id| {
        let value = tree.get(&id).unwrap().unwrap();
        process_where(&id, &value, ctx, entity, where_op)
      });
    }
  }

  let Some(prefix) = get_prefix(prefix_key, Some((parent_entity, parent_id, parent_body)), ctx.schema) else {
    return false;
  };
  
  let tree = ctx.get_tree(&entity.name);

  if check_all {
    tree.prefix(&prefix).unwrap().all(| item | {
      let (id, value) = item.unwrap();
      process_where(&id, &value, ctx, entity, where_op)
    })
  } else {
     tree.prefix(&prefix).unwrap().any(| item | {
      let (id, value) = item.unwrap();
      process_where(&id, &value, ctx, entity, where_op)
    })
  }
}

pub fn has_one_item<'a, F>(
  ctx: &mut TransationContext<'a, F>, 
  entity: &Entity, 
  where_op: &Where<'a>, 
  prefix_key: &PrefixKey, 
  parent_entity: &Entity,
  parent_id: &[u8], 
  parent_body: &[u8],
) -> bool {

  let Some(item_id) = get_prefix(prefix_key, Some((parent_entity, parent_id, parent_body)), ctx.schema) else {
    return false;
  };

  if where_op.only_id_required(entity) {
    return process_where(&item_id, &[], ctx, entity, where_op)
  }

  let tree = ctx.get_tree(&entity.name);
  let Some(value) = tree.get(item_id).unwrap() else {
    return false;
  };
  return process_where(&item_id, &value, ctx, entity, where_op)
}

pub fn has_one_item_exists<'a, F>(
  ctx: &mut TransationContext<'a, F>, 
  entity: &Entity, 
  prefix_key: &PrefixKey, 
  parent_entity: &Entity,
  parent_id: &[u8], 
  parent_body: &[u8],
) -> bool {
  let Some(item_id) = get_prefix(prefix_key, Some((parent_entity, parent_id, parent_body)), ctx.schema) else {
    return false;
  };

  let tree = ctx.get_tree(&entity.name);
  return tree.prefix_keys(&item_id).unwrap().next().is_some()
}

/// Извлекает срез байт для i-го элемента EnumList
#[inline]
fn get_enum_list_item<'a>(data: &'a [u8], outer_table: usize, i: usize) -> &'a [u8] {
  let off_a = u32::from_be_bytes(
    data[outer_table + i * 4..outer_table + i * 4 + 4].try_into().unwrap()
  ) as usize;
  let off_b = u32::from_be_bytes(
    data[outer_table + (i + 1) * 4..outer_table + (i + 1) * 4 + 4].try_into().unwrap()
  ) as usize;
  &data[off_a..off_b]
}

/// Проверяет, соответствует ли один item (срез байт) фильтру
fn enum_list_item_matches(item: &[u8], filter: &EnumListFilter<'_>) -> bool {
  let item_variant = u16::from_be_bytes(item[0..2].try_into().unwrap());

  // Проверяем вариант
  if let Some(expected) = filter.variant_idx {
    if item_variant != expected {
      return false;
    }
  }

  // Проверяем поля
  for ff in &filter.field_filters {
    // Поле принадлежит другому варианту — пропускаем
    if ff.variant_idx != item_variant {
      continue;
    }

    const INNER_TABLE: usize = 2; // после u16 variant
    let off_pos = INNER_TABLE + ff.field_idx * 4;
    let field_offset = u32::from_be_bytes(
      item[off_pos..off_pos + 4].try_into().unwrap()
    ) as usize;

    // Получаем данные поля
    let field_data: Option<&[u8]> = if field_offset == 0 {
      None
    } else {
      let field_end = ff.field.get_size()
          .map(|s| field_offset + s)
          .unwrap_or_else(|| {
            // Динамический размер: ищем следующий ненулевой offset в таблице
            let mut j = ff.field_idx + 1;
            while j < ff.num_variant_fields {
              let next_pos = INNER_TABLE + j * 4;
              let next_off = u32::from_be_bytes(
                item[next_pos..next_pos + 4].try_into().unwrap()
              ) as usize;
              if next_off != 0 {
                return next_off;
              }
              j += 1;
            }
            item.len()
          });
      Some(&item[field_offset..field_end])
    };

    let matches = compare_field_bytes(field_data, &ff.compare);
    if !matches {
      return false;
    }
  }

  true
}

/// Сравнивает байты поля с FieldCompare
fn compare_field_bytes(data: Option<&[u8]>, compare: &FieldCompare<'_>) -> bool {
  let Some(data) = data else {
    return match compare {
      FieldCompare::EqNull => true,
      FieldCompare::NeNull => false,
      FieldCompare::Ne(_) => true,
      FieldCompare::In(_, has_null) => *has_null,
      FieldCompare::NotIn(_, has_null) => !has_null,
      _ => false,
    };
  };

  match compare {
    FieldCompare::EqNull    => false,
    FieldCompare::NeNull    => true,
    FieldCompare::Eq(v)     => v.as_slice() == data,
    FieldCompare::Ne(v)     => v.as_slice() != data,
    FieldCompare::In(vs, _) => vs.iter().any(|v| v.as_slice() == data),
    FieldCompare::NotIn(vs, _) => vs.iter().all(|v| v.as_slice() != data),
    FieldCompare::Gt(nv)    => nv.compare_with_bytes(data).map(|o| o.is_gt()).unwrap_or(false),
    FieldCompare::Gte(nv)   => nv.compare_with_bytes(data).map(|o| o.is_ge()).unwrap_or(false),
    FieldCompare::Lt(nv)    => nv.compare_with_bytes(data).map(|o| o.is_lt()).unwrap_or(false),
    FieldCompare::Lte(nv)   => nv.compare_with_bytes(data).map(|o| o.is_le()).unwrap_or(false),
    _ => false,
  }
}