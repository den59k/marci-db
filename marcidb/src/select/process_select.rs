use std::{collections::HashMap, time::Instant};

use canopydb::{ReadTransaction, Tree};

use crate::{marci_db::{find_by_direct, get_offset_from_field}, schema::{Entity, FieldType}, select::{DecodeCtx, IncludeResult, Injected, MarciSelect, MarciSelectBinding, MarciSelectInclude, get_value_from_data}};

pub struct ProcessDataContext<'a, U, F> where 
  U: Clone,
  F: Fn(DecodeCtx<'a, U>) -> U, 
{
  pub trees: HashMap<&'a str, Tree<'a>>,
  pub rx: &'a ReadTransaction,
  pub f: F,
  pub include_cache: Vec<HashMap<Vec<u8>, U>>
}

impl<'a, U, F> ProcessDataContext<'a, U, F>
where
  U: Clone,
  F: Fn(DecodeCtx<'a, U>) -> U,
{
    pub fn new(rx: &'a ReadTransaction, f: F, select: &'a MarciSelect) -> Self {
        Self {
            trees: HashMap::new(),
            rx,
            f,
            include_cache: select.includes.iter().map(|_| HashMap::new()).collect(),
        }
    }

    pub fn get_tree(&mut self, key: &'a str) -> &Tree<'a> {
      self.trees
        .entry(key)
        .or_insert_with(|| self.rx.get_tree(key.as_bytes()).unwrap().unwrap())
    }
}


pub fn process_data<'a,'b, U, F>(
    id: &'b [u8],
    data: &'b [u8],
    select: &'a MarciSelect,
    entity: &'a Entity,
    ctx: &mut ProcessDataContext<'a, U, F>,
    mut inject: Option<U>,
) -> U
where
    U: Clone,
    F: Fn(DecodeCtx<U>) -> U,
{

  let mut includes: Vec<IncludeResult<U>> = Vec::with_capacity(select.includes.len());

  for (idx, include) in select.includes.iter().enumerate() {
    includes.push(parse_include(include, id, data, ctx, idx));
  }

  for (field_index, variants_map) in &select.enum_selects {
    let field = &entity.fields[*field_index];
    let FieldType::Enum(en) = &field.ty else {
      panic!("Field type is not enum");
    };
    let offset = get_offset_from_field(data, field);
    if offset == 0 { continue; }

    let variant = &u16::from_be_bytes(data[offset..offset+2].try_into().unwrap());
    if let Some(variant_select) = variants_map.get(variant) {
      let variant_resp = process_data(&[], &data[offset..], variant_select, &en.variants[*variant as usize], ctx, inject.take());
      inject = Some(variant_resp);
    }
  }
  
  return (ctx.f)(DecodeCtx { id, data, entity, select: &select.mask, includes, inject, aliases: select.aliases });
}

pub fn parse_include<'a,'b, U, F>(
  include: &'a MarciSelectInclude<'a>,
  id: &'b [u8],
  data: &'b [u8],
  ctx: &mut ProcessDataContext<'a, U, F>,
  include_idx: usize
) -> IncludeResult<'a, U> 
where 
    U: Clone,
    F: Fn(DecodeCtx<U>) -> U, 
{
  match include.binding {
    MarciSelectBinding::One() => {
      let Some(item_id) = get_value_from_data(include.field, id, data, 8) else {
        return IncludeResult::None(include.field);
      };

      let new_ctx = ProcessDataContext::new(ctx.rx, &ctx.f, &include.select);

      // let injected_tree = include.injected.as_ref()
      //   .and_then(|i| Some((i, ctx.get_tree(&i.st.name))));

      if include.select_only_id {
        // let injected_data = get_injected_data(item_id, injected_tree, ctx);
        // We send empty data because only ID bytes is using
        let item = process_data(item_id, &[], &include.select, include.model, ctx, None); 
        return IncludeResult::One(include.field, item);
      }
      let nested_tree = ctx.get_tree(&include.model.name);
      let Some(data) = nested_tree.get(item_id).unwrap() else {
        println!("Warning: not found entry for key {:?}", item_id);
        return IncludeResult::None(include.field);
      };
      // let injected_data: Option<U> = get_injected_data(item_id, injected_tree, ctx);
      let item = ctx.include_cache[include_idx]
        .entry(item_id.to_vec())
        .or_insert_with(|| process_data(item_id, data.as_ref(), &include.select, include.model, ctx, None));
       
      return IncludeResult::One(include.field, item.clone());
    },
    MarciSelectBinding::Many(tree_name) => {
      let keys = find_by_direct(ctx.rx, tree_name, id);
      
      if keys.is_empty() {
        return IncludeResult::Many(include.field, vec![]);
      }
      
      // let injected_tree = include.injected.as_ref()
      //   .and_then(|i| Some((i, ctx.get_tree(&i.st.name))));

      if include.select_only_id {
        let items = keys.iter().map(|key| {
          // let injected_data = get_injected_data(key, injected_tree, ctx);
          return process_data(key, &[], &include.select, include.model, ctx, None);
        }).collect();

        return IncludeResult::Many(include.field, items);
      }

      let items = keys.iter().map(|key| {
        let nested_tree = ctx.get_tree(&include.model.name);
        let Some(data) = nested_tree.get(&key[..8]).unwrap() else {
          panic!("Not found value in tree {}. Key: {:?}", str::from_utf8(include.model.name.as_bytes()).unwrap(), key);
        };
        // let injected_data = get_injected_data(key, injected_tree, ctx);
        return process_data(key, data.as_ref(), &include.select, include.model, ctx, None);
      }).collect();

      return IncludeResult::Many(include.field, items);
    },
    MarciSelectBinding::OneStruct() => {
      let st_tree = ctx.rx.get_tree(include.model.name.as_bytes()).unwrap().unwrap();
      let Some(data) = st_tree.get(id).unwrap() else {
        return IncludeResult::None(include.field);
      };
      let item = process_data(id, data.as_ref(), &include.select, include.model, ctx, None);
      return IncludeResult::One(include.field, item);
    },
    MarciSelectBinding::ManyStruct() => {

      let st_tree = ctx.rx.get_tree(include.model.name.as_bytes()).unwrap().unwrap();

      if include.select_only_id {
        let items = st_tree.prefix_keys(&id).unwrap().map(|item| {
          let key = item.unwrap();
          return process_data(&key, &[], &include.select, include.model, ctx, None);
        }).collect();
        return IncludeResult::Many(include.field, items);
      }

      let items = st_tree.prefix(&id).unwrap().map(|item| {
        let (key, data) = item.unwrap();
        return process_data(&key, data.as_ref(), &include.select, include.model, ctx, None);
      }).collect();

      return IncludeResult::Many(include.field, items);
    },
  }
}

pub fn get_injected_data<'a,U,F>(
  id: &[u8], 
  injected_tree: Option<(&'a Injected<'_>, &'a Tree<'_>)>, 
  ctx: &mut ProcessDataContext<'a, U, F>
) -> Option<U> 
where 
    U: Clone,
    F: Fn(DecodeCtx<U>) -> U
{
  let Some((injected, tree)) = injected_tree else { return None };

  let Some(data) = tree.get(id).unwrap() else {
    panic!("Not found key {:?} in tree {}", id, injected.st.name)
  };

  return Some(process_data(id, &data, &injected.select, injected.st, ctx, None));
}


