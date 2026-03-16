use crate::{query_op::{FieldCompare, TransationContext, Where}, schema::Entity, utils::get_data};

pub fn process_where<'a, 'b, F>(id: &'b [u8], body: &'b [u8], ctx: &mut TransationContext<'a, F>, entity: &Entity, where_op: &Where<'a>) -> bool {

  match where_op {
    Where::And(items) => items.iter().all(|f| process_where(id, body, ctx, entity, f)),
    Where::Or(items) => items.iter().any(|f| process_where(id, body, ctx, entity, f)),
    Where::Not(where_op) => !process_where(id, body, ctx, entity, where_op),
    Where::Field(field, field_compare) => {
      let Some(data) = get_data(entity, field, id, body, ctx.schema) else {
        return match field_compare {
          FieldCompare::EqNull => true,
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
      }
    },
  }
}