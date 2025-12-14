use std::{collections::HashMap, sync::Arc};

use canopydb::{ReadTransaction, Tree};

use crate::{marci_db::{find_by_direct, get_offset_from_field}, schema::{Entity, FieldType}, select::{DecodeCtx, IncludeResult, MarciSelect, MarciSelectBinding, MarciSelectInclude, get_value_from_data}};

pub struct TransationContext<'a, F> {
  pub trees: HashMap<&'a str, Tree<'a>>,
  pub rx: &'a ReadTransaction,
  pub f: F
}

impl<'a, F> TransationContext<'a, F> {
    pub fn new(rx: &'a ReadTransaction, f: F) -> Self {
        Self {
            trees: HashMap::new(),
            rx,
            f
        }
    }

    pub fn get_tree(&mut self, key: &'a str) -> &Tree<'_> {
      // let tree= self.rx.get_tree(key.as_bytes()).unwrap().unwrap();
      // return tree;
      self.trees
        .entry(key)
        .or_insert_with(|| {
          // let now = Instant::now();
          let tree = self.rx.get_tree(key.as_bytes()).unwrap().unwrap();
          // println!("Get tree {} time: {:?}", key, now.elapsed());
          return tree;
        })
    }
}

pub struct ProcessDataContext<'a, U> {
  pub select: &'a MarciSelect<'a>,
  pub include_cache: Vec<HashMap<Vec<u8>, Arc<U>>>
}

impl<'a, U> ProcessDataContext<'a, U> {
  pub fn new(select: &'a MarciSelect) -> Self {
    Self { 
      select, 
      include_cache: select.includes.iter().map(|_| HashMap::new()).collect(),
    }
  }
} 

pub fn process_data<'a,'b, U, F>(
    id: &'b [u8],
    data: &'b [u8],
    entity: &'a Entity,
    tctx: &mut TransationContext<'a, F>,
    ctx: &mut ProcessDataContext<'a, U>,
    mut inject: Option<U>,
) -> U
where
    // U: Clone,
    F: Fn(DecodeCtx<U>) -> U,
{

  let mut includes: Vec<IncludeResult<U>> = Vec::with_capacity(ctx.select.includes.len());

  for (idx, include) in ctx.select.includes.iter().enumerate() {
    includes.push(parse_include(include, id, data, tctx, ctx, idx));
  }

  for (field_index, variants_map) in &ctx.select.enum_selects {
    let field = &entity.fields[*field_index];
    let FieldType::Enum(en) = &field.ty else {
      panic!("Field type is not enum");
    };
    let offset = get_offset_from_field(data, field);
    if offset == 0 { continue; }

    let variant = &u16::from_be_bytes(data[offset..offset+2].try_into().unwrap());
    if let Some(variant_select) = variants_map.get(variant) {

      let mut ctx = ProcessDataContext::new(variant_select);

      let variant_resp = process_data(&[], &data[offset..], &en.variants[*variant as usize], tctx, &mut ctx, inject.take());
      inject = Some(variant_resp);
    }
  }
  
  return (tctx.f)(DecodeCtx { id, data, entity, select: &ctx.select.mask, includes, inject, aliases: ctx.select.aliases });
}

pub fn parse_include<'a,'b, U, F>(
  include: &'a MarciSelectInclude<'a>,
  id: &'b [u8],
  data: &'b [u8],
  tctx: &mut TransationContext<'a, F>,
  ctx: &mut ProcessDataContext<'a, U>,
  include_idx: usize
) -> IncludeResult<'a, U> 
where 
    // U: Clone,
    F: Fn(DecodeCtx<U>) -> U
{
  match include.binding {
    MarciSelectBinding::One() => {
      let Some(item_id) = get_value_from_data(include.field, id, data, Some(8)) else {
        return IncludeResult::None(include.field);
      };

      let mut new_ctx = ProcessDataContext::new(&include.select);

      if let Some(injected) = &include.injected {
        let mut injected_ctx = ProcessDataContext::new(&injected.select);
        let injected_data = get_injected_data(item_id, injected.st, tctx, &mut injected_ctx);

        if include.select_only_id {
          // We send empty data because only ID bytes is using
          let item = process_data(item_id, &[], include.model, tctx, &mut new_ctx, Some(injected_data)); 
          return IncludeResult::One(include.field, Arc::new(item));
        }

        let nested_tree = tctx.get_tree(&include.model.name);
        let Some(data) = nested_tree.get(item_id).unwrap() else {
          println!("Warning: not found entry for key {:?}", item_id);
          return IncludeResult::None(include.field);
        };

        let item = ctx.include_cache[include_idx]
                .entry(item_id.to_vec())
                .or_insert_with(|| {
                  Arc::new(process_data(item_id, data.as_ref(), include.model, tctx, &mut new_ctx, Some(injected_data)))
                })
                .clone();

        return IncludeResult::One(include.field, item);       
      }

      if include.select_only_id {
        // We send empty data because only ID bytes is using
        let item = process_data(item_id, &[], include.model, tctx, &mut new_ctx, None); 
        return IncludeResult::One(include.field, Arc::new(item));
      }
      let nested_tree = tctx.get_tree(&include.model.name);
      let Some(data) = nested_tree.get(item_id).unwrap() else {
        println!("Warning: not found entry for key {:?}", item_id);
        return IncludeResult::None(include.field);
      };
      let item = ctx.include_cache[include_idx]
        .entry(item_id.to_vec())
        .or_insert_with(|| {
          Arc::new(process_data(item_id, data.as_ref(), include.model, tctx, &mut new_ctx, None))
        })
        .clone();
       
      return IncludeResult::One(include.field, item);
    },
    MarciSelectBinding::Many(tree_name) => {
      let keys = find_by_direct(tctx.rx, tree_name, id);
      
      if keys.is_empty() {
        return IncludeResult::Many(include.field, vec![]);
      }

      let mut new_ctx = ProcessDataContext::new(&include.select);

      if let Some(injected) = &include.injected {
        let mut injected_ctx = ProcessDataContext::new(&injected.select);
        
        if include.select_only_id {
          let items = keys.iter().map(|key| {
            let injected_data = get_injected_data(key, injected.st, tctx, &mut injected_ctx);
            return Arc::new(process_data(key, &[], include.model, tctx, &mut new_ctx, Some(injected_data)));
          }).collect();

          return IncludeResult::Many(include.field, items);
        }

        let items = keys.iter().map(|key| {
          ctx.include_cache[include_idx]
            .entry(key.clone())
            .or_insert_with(|| {
              let nested_tree = tctx.get_tree(&include.model.name);
              let Some(data) = nested_tree.get(&key[..8]).unwrap() else {
                panic!("Not found value in tree {}. Key: {:?}", str::from_utf8(include.model.name.as_bytes()).unwrap(), key);
              };
              let injected_data = get_injected_data(key, injected.st, tctx, &mut injected_ctx);
              Arc::new(process_data(key, data.as_ref(), include.model, tctx, &mut new_ctx, Some(injected_data)))
            })
            .clone()
        }).collect();

        return IncludeResult::Many(include.field, items);
      }

      if include.select_only_id {
        let items = keys.iter().map(|key| {
          return Arc::new(process_data(key, &[], include.model, tctx, &mut new_ctx, None));
        }).collect();

        return IncludeResult::Many(include.field, items);
      }

      let items = keys.iter().map(|key| {
        ctx.include_cache[include_idx]
          .entry(key.clone())
          .or_insert_with(|| {
            let nested_tree = tctx.get_tree(&include.model.name);
            let Some(data) = nested_tree.get(&key[..8]).unwrap() else {
              panic!("Not found value in tree {}. Key: {:?}", str::from_utf8(include.model.name.as_bytes()).unwrap(), key);
            };
            Arc::new(process_data(key, data.as_ref(), include.model, tctx, &mut new_ctx, None))
          })
          .clone()
      }).collect();

      return IncludeResult::Many(include.field, items);
    },
    MarciSelectBinding::OneStruct() => {
      let st_tree = tctx.get_tree(&include.model.name);
      let Some(data) = st_tree.get(id).unwrap() else {
        return IncludeResult::None(include.field);
      };

      let mut new_ctx = ProcessDataContext::new(&include.select);

      let item = Arc::new(process_data(id, data.as_ref(), include.model, tctx, &mut new_ctx, None));
      return IncludeResult::One(include.field, item);
    },
    MarciSelectBinding::ManyStruct() => {

      let mut new_ctx = ProcessDataContext::new(&include.select);

      let st_tree = tctx.rx.get_tree(&include.model.name.as_bytes()).unwrap().unwrap();

      if include.select_only_id {
        let items = st_tree.prefix_keys(&id).unwrap().map(|item| {
          let key = item.unwrap();
          return Arc::new(process_data(&key, &[], include.model, tctx, &mut new_ctx, None));
        }).collect();
        return IncludeResult::Many(include.field, items);
      }

      let items = st_tree.prefix(&id).unwrap().map(|item| {
        let (key, data) = item.unwrap();
        return Arc::new(process_data(&key, data.as_ref(), include.model, tctx, &mut new_ctx, None));
      }).collect();

      return IncludeResult::Many(include.field, items);
    },
  }
}

pub fn get_injected_data<'a,U,F>(
  id: &[u8], 
  st: &'a Entity,
  tctx: &mut TransationContext<'a, F>,
  ctx: &mut ProcessDataContext<'a, U>
) -> U
where 
    // U: Clone,
    F: Fn(DecodeCtx<U>) -> U
{
  let tree = tctx.get_tree(&st.name);

  let Some(data) = tree.get(id).unwrap() else {
    panic!("Not found key {:?} in tree {}", id, st.name)
  };

  // &injected.select, 

  return process_data(id, &data, st, tctx, ctx, None);
}


