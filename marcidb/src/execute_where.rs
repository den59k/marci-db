use std::collections::HashSet;
use std::ops::Bound;

use serde_json::Value;

use crate::{
    marci_db::{find_by_direct, find_by_rev, MarciDB},
    marci_where::{
        check_condition, decode_bytes_to_value, encode_where_value, parse_operator_conditions,
        parse_where_json, FieldCondition, FieldConditionKind, MarciWhere, Operator,
        ParseWhereError, WhereNode, WhereValue,
    },
    schema::{Entity, Field},
    select::get_value_from_data,
};

pub fn execute_where(
    db: &MarciDB,
    model: &Entity,
    mw: &MarciWhere<'_>,
) -> Result<Option<Vec<Vec<u8>>>, ParseWhereError> {
    let ids = eval_node(db, model, &mw.node, None)?;
    Ok(ids.map(|s| s.into_iter().collect()))
}


pub fn parse_and_execute_where(
    db: &MarciDB,
    model: &Entity,
    body_json: &Value,
) -> Result<Option<Vec<Vec<u8>>>, ParseWhereError> {
    let Some(mw) = parse_where_json(model, &db.schema, body_json)? else {
        return Ok(None);
    };
    execute_where(db, model, &mw)
}

fn eval_node<'a>(
    db: &MarciDB,
    model: &Entity,
    node: &WhereNode<'a>,
    existing_ids: Option<&HashSet<Vec<u8>>>,
) -> Result<Option<HashSet<Vec<u8>>>, ParseWhereError> {
    match node {
        WhereNode::And(branches) => eval_and(db, model, branches, existing_ids),
        WhereNode::Or(branches) => eval_or(db, model, branches),
        WhereNode::Fields(conditions) => eval_fields(db, model, conditions, existing_ids),
    }
}





fn eval_and(
    db: &MarciDB,
    model: &Entity,
    branches: &[MarciWhere<'_>],
    existing_ids: Option<&HashSet<Vec<u8>>>,
) -> Result<Option<HashSet<Vec<u8>>>, ParseWhereError> {
    
    let mut with_prio: Vec<(u8, &MarciWhere)> = branches
        .iter()
        .map(|b| (node_priority(model, &b.node), b))
        .collect();
    with_prio.sort_by_key(|(p, _)| *p);

    let mut current: Option<HashSet<Vec<u8>>> = existing_ids.map(|s| s.clone());

    for (_, branch) in with_prio {
        let result = eval_node(db, model, &branch.node, current.as_ref())?;
        match result {
            None => {} 
            Some(ids) => {
                current = Some(match current {
                    None => ids,
                    Some(prev) => {
                        let inter: HashSet<_> = prev.intersection(&ids).cloned().collect();
                        if inter.is_empty() {
                            return Ok(Some(HashSet::new()));
                        }
                        inter
                    }
                });
            }
        }
    }
    Ok(current)
}

fn eval_or(
    db: &MarciDB,
    model: &Entity,
    branches: &[MarciWhere<'_>],
) -> Result<Option<HashSet<Vec<u8>>>, ParseWhereError> {
    let mut result: HashSet<Vec<u8>> = HashSet::new();
    let mut any_some = false;

    for branch in branches {
        
        if let Some(ids) = eval_node(db, model, &branch.node, None)? {
            any_some = true;
            result.extend(ids);
        }
    }

    if any_some { Ok(Some(result)) } else { Ok(None) }
}





fn eval_fields(
    db: &MarciDB,
    model: &Entity,
    conditions: &[FieldCondition<'_>],
    existing_ids: Option<&HashSet<Vec<u8>>>,
) -> Result<Option<HashSet<Vec<u8>>>, ParseWhereError> {
    
    
    
    
    
    
    let (special, scalar): (Vec<&FieldCondition>, Vec<&FieldCondition>) =
        conditions.iter().partition(|c| !matches!(c.kind, FieldConditionKind::Scalar(_)));

    
    
    if special.is_empty() && scalar.iter().all(|c| !is_indexed(c.field)) {
        return scan_all(db, model, conditions, existing_ids)
            .map(|ids| Some(ids.into_iter().collect()));
    }

    
    
    
    let mut ordered: Vec<&FieldCondition> = conditions.iter().collect();
    ordered.sort_by_key(|c| condition_priority_key(c));

    let mut current: Option<HashSet<Vec<u8>>> = existing_ids.map(|s| s.clone());

    for cond in ordered {
        let ids = eval_field_condition(db, model, cond, current.as_ref())?;
        match ids {
            None => {} 
            Some(ids) => {
                if ids.is_empty() {
                    return Ok(Some(HashSet::new()));
                }
                current = Some(match current {
                    None => ids,
                    Some(prev) => {
                        let inter: HashSet<_> = prev.intersection(&ids).cloned().collect();
                        if inter.is_empty() {
                            return Ok(Some(HashSet::new()));
                        }
                        inter
                    }
                });
            }
        }
    }

    Ok(current)
}



fn eval_field_condition(
    db: &MarciDB,
    model: &Entity,
    cond: &FieldCondition<'_>,
    existing_ids: Option<&HashSet<Vec<u8>>>,
) -> Result<Option<HashSet<Vec<u8>>>, ParseWhereError> {
    match &cond.kind {
        FieldConditionKind::Scalar(ops) => {
            if ops.is_empty() {
                return Ok(None);
            }
            let ids = get_ids_for_scalar(db, model, cond.field, ops, existing_ids)?;
            Ok(Some(ids.into_iter().collect()))
        }

        FieldConditionKind::StructWhere { st, inner } => {
            let result = eval_node(db, st, &inner.node, existing_ids)?;
            Ok(result)
        }

        
        FieldConditionKind::StructListAll { st, elements } => {
            if elements.is_empty() {
                return Ok(None);
            }
            let ids = eval_struct_list_all(db, model, st, elements, existing_ids)?;
            Ok(Some(ids.into_iter().collect()))
        }

        
        FieldConditionKind::ModelRefListAll(child_ids) => {
            if child_ids.is_empty() {
                return Ok(None);
            }
            let ids = eval_model_ref_list_all(db, cond.field, child_ids)?;
            Ok(Some(ids.into_iter().collect()))
        }

        
        FieldConditionKind::Injected {
            st,
            inner,
            parent_id_byte_start,
            ..
        } => {
            let keys_opt = eval_node(db, st, &inner.node, None)?;
            let keys = keys_opt.unwrap_or_default();

            let parent_ids: HashSet<Vec<u8>> = keys
                .into_iter()
                .map(|key| {
                    let start = *parent_id_byte_start;
                    key[start + 8..start + 16].to_vec()
                })
                .collect();

            Ok(Some(
                if let Some(existing) = existing_ids {
                    parent_ids.intersection(existing).cloned().collect()
                } else {
                    parent_ids
                },
            ))
        }

        
        
        
        
        
        FieldConditionKind::VectorSearch { .. } => Ok(None),
    }
}





fn scan_all(
    db: &MarciDB,
    model: &Entity,
    conditions: &[FieldCondition<'_>],
    existing_ids: Option<&HashSet<Vec<u8>>>,
) -> Result<Vec<Vec<u8>>, ParseWhereError> {
    let rx = db.db.begin_read().unwrap();
    let tree = rx.get_tree(model.name.as_bytes()).unwrap().unwrap();

    let ids_to_check: Vec<Vec<u8>> = if let Some(existing) = existing_ids {
        existing.iter().cloned().collect()
    } else {
        tree.keys()
            .unwrap()
            .map(|k| k.unwrap().to_vec())
            .collect()
    };

    let mut result = Vec::new();
    'outer: for id in ids_to_check {
        let Some(data) = tree.get(&id).unwrap() else { continue };

        for cond in conditions {
            
            let FieldConditionKind::Scalar(ops) = &cond.kind else { continue };

            let value_bytes = get_value_from_data(cond.field, &id, &data, cond.field.get_size());
            let decoded = match value_bytes {
                Some(b) => decode_bytes_to_value(cond.field, b)?,
                None => WhereValue::Null,
            };
            if !ops.iter().all(|(op, target)| check_condition(&decoded, *op, target)) {
                continue 'outer;
            }
        }
        result.push(id);
    }
    Ok(result)
}

fn get_ids_for_scalar(
    db: &MarciDB,
    model: &Entity,
    field: &Field,
    ops: &[(Operator, WhereValue)],
    existing_ids: Option<&HashSet<Vec<u8>>>,
) -> Result<Vec<Vec<u8>>, ParseWhereError> {
    if ops.iter().any(|(_, v)| matches!(v, WhereValue::Null)) {
        return scan_field_no_index(db, model, field, ops, existing_ids);
    }

    if let Some(index) = field.get_field_index() {
        return scan_index(
            db,
            field,
            ops,
            index.tree_name(),
            model.key_min_size(),
            existing_ids,
            /* value_is_suffix */ true,
        );
    }

    if let Some(index) = field.get_rev_index() {
        return scan_index(
            db,
            field,
            ops,
            index.tree_name(),
            model.key_min_size(),
            existing_ids,
            /* value_is_suffix */ true,
        );
    }

    
    if field.id_idx.is_some() {
        return scan_primary_key(db, model, field, ops, existing_ids);
    }

    
    scan_field_no_index(db, model, field, ops, existing_ids)
}


fn scan_index(
    db: &MarciDB,
    field: &Field,
    ops: &[(Operator, WhereValue)],
    tree_name: &[u8],
    key_len: usize,
    existing_ids: Option<&HashSet<Vec<u8>>>,
    _value_is_suffix: bool,
) -> Result<Vec<Vec<u8>>, ParseWhereError> {
    let (lower_bound, upper_bound, _has_range) = build_range_bounds(field, ops)?;

    if let (Some(lb), Some(ub)) = (&lower_bound, &upper_bound) {
        if lb > ub {
            return Ok(vec![]);
        }
    }

    let min_id = vec![0u8; key_len];
    let max_id = vec![255u8; key_len];

    let start = lower_bound
        .as_ref()
        .map(|lb| [lb.as_slice(), &min_id].concat())
        .unwrap_or_default();
    let end = upper_bound
        .as_ref()
        .map(|ub| [ub.as_slice(), &max_id].concat())
        .unwrap_or_default();

    let range = to_bound_pair(lower_bound.as_ref(), upper_bound.as_ref(), start, end);

    let rx = db.db.begin_read().unwrap();
    let tree = rx.get_tree(tree_name).unwrap().unwrap();

    let variable_len = field.get_size().is_none();
    let mut ids = Vec::new();

    for item in tree.range(range).unwrap() {
        let (key, _) = item.unwrap();
        let value_bytes = &key[..key.len() - key_len];
        let value_bytes = if variable_len && value_bytes.last() == Some(&0) {
            &value_bytes[..value_bytes.len() - 1]
        } else {
            value_bytes
        };
        let decoded = decode_bytes_to_value(field, value_bytes)?;
        if ops.iter().all(|(op, target)| check_condition(&decoded, *op, target)) {
            let id = key[key.len() - key_len..].to_vec();
            ids.push(id);
        }
    }

    if let Some(existing) = existing_ids {
        ids.retain(|id| existing.contains(id));
    }
    Ok(ids)
}


fn scan_primary_key(
    db: &MarciDB,
    model: &Entity,
    field: &Field,
    ops: &[(Operator, WhereValue)],
    existing_ids: Option<&HashSet<Vec<u8>>>,
) -> Result<Vec<Vec<u8>>, ParseWhereError> {
    let (lower_bound, upper_bound, has_range) = build_range_bounds(field, ops)?;
    if !has_range {
        return scan_field_no_index(db, model, field, ops, existing_ids);
    }
    if let (Some(lb), Some(ub)) = (&lower_bound, &upper_bound) {
        if lb > ub {
            return Ok(vec![]);
        }
    }

    let (start, end) = (
        lower_bound.clone().unwrap_or_default(),
        upper_bound.clone().unwrap_or_default(),
    );
    let range = to_bound_pair(lower_bound.as_ref(), upper_bound.as_ref(), start, end);

    let rx = db.db.begin_read().unwrap();
    let tree = rx.get_tree(model.name.as_bytes()).unwrap().unwrap();

    let mut ids = Vec::new();
    for item in tree.range(range).unwrap() {
        let (key, _) = item.unwrap();
        let decoded = decode_bytes_to_value(field, &key)?;
        if ops.iter().all(|(op, target)| check_condition(&decoded, *op, target)) {
            ids.push(key.to_vec());
        }
    }
    if let Some(existing) = existing_ids {
        ids.retain(|id| existing.contains(id));
    }
    Ok(ids)
}


fn scan_field_no_index(
    db: &MarciDB,
    model: &Entity,
    field: &Field,
    ops: &[(Operator, WhereValue)],
    existing_ids: Option<&HashSet<Vec<u8>>>,
) -> Result<Vec<Vec<u8>>, ParseWhereError> {
    let rx = db.db.begin_read().unwrap();
    let tree = rx.get_tree(model.name.as_bytes()).unwrap().unwrap();

    let ids_to_check: Vec<Vec<u8>> = if let Some(existing) = existing_ids {
        existing.iter().cloned().collect()
    } else {
        tree.keys()
            .unwrap()
            .map(|k| k.unwrap().to_vec())
            .collect()
    };

    let mut ids = Vec::new();
    for id in ids_to_check {
        let Some(data) = tree.get(&id).unwrap() else { continue };
        let value_bytes = get_value_from_data(field, &id, &data, field.get_size());
        let decoded = match value_bytes {
            Some(b) => decode_bytes_to_value(field, b)?,
            None => WhereValue::Null,
        };
        if ops.iter().all(|(op, target)| check_condition(&decoded, *op, target)) {
            ids.push(id);
        }
    }
    Ok(ids)
}





fn eval_model_ref_list_all(
    db: &MarciDB,
    field: &Field,
    child_ids: &[u64],
) -> Result<Vec<Vec<u8>>, ParseWhereError> {
    if let Some(rev_index) = field.get_rev_index() {
        
        let mut result_set: Option<HashSet<Vec<u8>>> = None;
        for &child_id in child_ids {
            let child_bytes = child_id.to_be_bytes();
            let rx = db.db.begin_read().unwrap();
            let parents: HashSet<Vec<u8>> =
                find_by_direct(&rx, rev_index.tree_name(), &child_bytes)
                    .into_iter()
                    .collect();
            result_set = Some(match result_set {
                None => parents,
                Some(prev) => prev.intersection(&parents).cloned().collect(),
            });
            if result_set.as_ref().map_or(false, |s| s.is_empty()) {
                return Ok(vec![]);
            }
        }
        return Ok(result_set.map(|s| s.into_iter().collect()).unwrap_or_default());
    }

    if let Some(direct_index) = field.get_direct_index() {
        let first_bytes = child_ids[0].to_be_bytes();
        let rx = db.db.begin_read().unwrap();
        let parents: HashSet<Vec<u8>> =
            find_by_rev(&rx, direct_index.tree_name(), &first_bytes, &db.schema)
                .into_iter()
                .collect();

        if parents.is_empty() {
            return Ok(vec![]);
        }

        let tree = rx.get_tree(direct_index.tree_name()).unwrap().unwrap();
        let mut result = parents;

        for &child_id in &child_ids[1..] {
            let child_bytes = child_id.to_be_bytes();
            let mut surviving = HashSet::new();
            for parent in &result {
                let mut key = parent.clone();
                key.extend_from_slice(&child_bytes);
                if tree.get(&key).unwrap().is_some() {
                    surviving.insert(parent.clone());
                }
            }
            result = surviving;
            if result.is_empty() {
                break;
            }
        }
        return Ok(result.into_iter().collect());
    }

    Err(ParseWhereError::TypeMismatch {
        field: field.full_name.clone(),
        expected: format!("field {} needs @derived or @index in schema for $all query (no index found)", field.full_name),
    })
}





fn eval_struct_list_all(
    db: &MarciDB,
    _model: &Entity,
    st: &Entity,
    elements: &[Value],
    existing_ids: Option<&HashSet<Vec<u8>>>,
) -> Result<Vec<Vec<u8>>, ParseWhereError> {
    
    let id_field = st
        .fields
        .iter()
        .find(|f| f.id_idx.is_some() && !f.name.starts_with('@'))
        .ok_or_else(|| ParseWhereError::TypeMismatch {
            field: st.name.clone(),
            expected: "struct must have an item @id field".into(),
        })?;

    
    let first_parents =
        get_parent_ids_for_struct_list_cond(db, st, &elements[0], existing_ids)?;
    let mut candidates: HashSet<Vec<u8>> = first_parents.into_iter().collect();

    if candidates.is_empty() {
        return Ok(vec![]);
    }

    
    let rx = db.db.begin_read().unwrap();
    let tree = rx.get_tree(st.name.as_bytes()).unwrap().unwrap();

    for elem in &elements[1..] {
        let ops = parse_operator_conditions(id_field, elem)?;
        if let Some((_, WhereValue::UInt64(child_id))) = ops.first() {
            let child_bytes = child_id.to_be_bytes();
            let mut surviving = HashSet::new();
            for parent_id in &candidates {
                let mut key = parent_id.clone();
                key.extend_from_slice(&child_bytes);
                if tree.get(&key).unwrap().is_some() {
                    surviving.insert(parent_id.clone());
                }
            }
            candidates = surviving;
        }
        if candidates.is_empty() {
            break;
        }
    }

    Ok(candidates.into_iter().collect())
}

fn get_parent_ids_for_struct_list_cond(
    db: &MarciDB,
    st: &Entity,
    cond: &Value,
    existing_ids: Option<&HashSet<Vec<u8>>>,
) -> Result<Vec<Vec<u8>>, ParseWhereError> {

    let condition_value = if cond.is_number() {
        let id_field = st
            .fields
            .iter()
            .find(|f| f.id_idx.is_some() && !f.name.starts_with('@'))
            .ok_or_else(|| ParseWhereError::TypeMismatch {
                field: st.name.clone(),
                expected: "struct must have an item @id field".into(),
            })?;
        let mut map = serde_json::Map::new();
        map.insert(id_field.name.clone(), cond.clone());
        Value::Object(map)
    } else {
        cond.clone()
    };

    let body = serde_json::json!({ "$where": condition_value });
    let Some(mw) = parse_where_json(st, &db.schema, &body)? else {
        return Ok(vec![]);
    };

    let existing_set: Option<HashSet<Vec<u8>>> = existing_ids.map(|s| s.clone());
    let keys_opt = eval_node(db, st, &mw.node, existing_set.as_ref())?;
    let keys = keys_opt.unwrap_or_default();

    let parent_ids: HashSet<Vec<u8>> = keys
        .into_iter()
        .map(|key| key[..8].to_vec())
        .collect();

    Ok(parent_ids.into_iter().collect())
}

fn build_range_bounds(
    field: &Field,
    ops: &[(Operator, WhereValue)],
) -> Result<(Option<Vec<u8>>, Option<Vec<u8>>, bool), ParseWhereError> {
    let mut lower: Option<Vec<u8>> = None;
    let mut upper: Option<Vec<u8>> = None;
    let mut has_range = false;

    for (op, val) in ops {
        match op {
            Operator::Eq | Operator::Gt | Operator::Ge | Operator::Lt | Operator::Le => {
                has_range = true;
                let encoded = encode_where_value(field, val)?;
                match op {
                    Operator::Eq => {
                        lower = Some(encoded.clone());
                        upper = Some(encoded);
                    }
                    Operator::Gt | Operator::Ge => {
                        if lower.as_ref().map_or(true, |lb| encoded > *lb) {
                            lower = Some(encoded);
                        }
                    }
                    Operator::Lt | Operator::Le => {
                        if upper.as_ref().map_or(true, |ub| encoded < *ub) {
                            upper = Some(encoded);
                        }
                    }
                    _ => {}
                }
            }
            Operator::Ne => {}
        }
    }

    Ok((lower, upper, has_range))
}

fn to_bound_pair(
    lower: Option<&Vec<u8>>,
    upper: Option<&Vec<u8>>,
    start: Vec<u8>,
    end: Vec<u8>,
) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
    match (lower, upper) {
        (Some(_), Some(_)) => (Bound::Included(start), Bound::Included(end)),
        (Some(_), None) => (Bound::Included(start), Bound::Unbounded),
        (None, Some(_)) => (Bound::Unbounded, Bound::Included(end)),
        (None, None) => unreachable!("build_range_bounds guarantees at least one bound"),
    }
}

fn condition_priority_key(cond: &FieldCondition) -> u8 {
    match &cond.kind {
        FieldConditionKind::Scalar(_) if is_indexed(cond.field) => field_priority(cond.field),
        FieldConditionKind::Scalar(_) => 4,
        _ => 3,
    }
}

fn field_priority(field: &Field) -> u8 {
    if field.get_field_index().is_some() { 0 }
    else if field.get_rev_index().is_some() { 1 }
    else if field.id_idx.is_some() { 2 }
    else if field.get_direct_index().is_some() { 2 }
    else if field.injected_fields.is_some() { 2 }
    else { 3 }
}

fn is_indexed(field: &Field) -> bool {
    field_priority(field) < 3
}

fn node_priority(model: &Entity, node: &WhereNode) -> u8 {
    match node {
        WhereNode::And(branches) => branches
            .iter()
            .map(|b| node_priority(model, &b.node))
            .min()
            .unwrap_or(3),
        WhereNode::Or(_) => 3,
        WhereNode::Fields(conds) => conds
            .iter()
            .map(|c| field_priority(c.field))
            .min()
            .unwrap_or(3),
    }
}