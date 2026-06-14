use std::{collections::HashMap};

use crate::{schema::{Entity, EntityDependency, FieldIndex, RefInfo, Schema, SchemaError, schema_attributes::{Attribute, DeleteConstraint}, schema_default_value::{FieldDefault, resolve_default_value}, schema_enum::{EnumDef, parse_enum_block}, schema_field::{EnumInfo, Field, FieldExistsCondition, FieldLocation, FieldType, RefBinding, parse_field_raw}, schema_resolve_bindings::{resolve_bind_refs, resolve_ref_bindings}}};

/// Parses schema text into raw blocks (models/structs/enums) BEFORE resolving refs,
/// offsets, indexes, etc. Used by both `parse_schema` and the migration engine (the diff
/// prefers raw models: the type is still `RefUnresolved(name)`, without synthetic struct models)
pub fn collect_blocks(input: &str) -> Result<(Vec<Entity>, HashMap<String, Entity>, HashMap<String, EnumDef>), SchemaError> {
    let mut models = Vec::new();
    let mut structs: HashMap<String, Entity> = HashMap::new();
    let mut enums: HashMap<String,EnumDef> = HashMap::new();
    let mut lines = input.lines().peekable();

    while let Some(line) = lines.next() {
        let line = line.trim();
        if !line.starts_with("model ") && !line.starts_with("struct ") && !line.starts_with("enum ") {
            continue;
        }
        let (kind, rest) = line.trim().split_once(' ').unwrap();
        let name = rest.trim_end_matches('{').trim().to_string();

        match kind.trim() {
            "model" => {
                models.push(parse_model_block(name, &mut lines)?);
            },
            "struct" => {
                structs.insert(name, parse_struct_block(&mut lines)?);
            },
            "enum" => {
                enums.insert(name.clone(), parse_enum_block(&mut lines)?);
            }
            _ => {}
        }
    }

    Ok((models, structs, enums))
}

/// Fallible schema text parsing: on an invalid schema returns [`SchemaError`] with a human-readable
/// message (→ HTTP 400) instead of panicking. Use on UNtrusted input (schema from `$sync`).
/// For already-validated/internal input there is the infrastructural [`parse_schema`]
pub fn try_parse_schema(input: &str) -> Result<Schema, SchemaError> {
    let (mut models, structs, enums) = collect_blocks(input)?;

    // Add a mandatory id to every model if it doesn't have one
    for model in models.iter_mut() {
      resolve_model_id(model);
    }

    let mut model_structs: Vec<Entity> = vec![];
    for model in models.iter_mut() {
        resolve_structs_and_enums(model, &structs, &enums, &mut model_structs)?;
    }
    models.extend(model_structs);

    for model in models.iter_mut() {
        for field in model.fields.iter_mut() {
            resolve_default_value(field)?;
        }
    }

    // At this point we have the final list of models with the final list of Fields. It only remains to adjust the refs
    let model_by_name = models.iter().enumerate()
        .map(|(i, m)| (m.name.clone(), i))
        .collect();

    for model in models.iter_mut() {
        resolve_refs(model, &model_by_name)?;
    }

    for model in models.iter_mut() {
        resolve_indexes(model)?;
    }

    for model in models.iter_mut() {
        resolve_field_offsets(model);
    }

    let mut counter_id = 0;
    for model in models.iter_mut() {
        resolve_counter_idx(model, &mut counter_id);
    }

    resolve_bind_refs(&mut models)?;
    resolve_ref_bindings(&mut models);

    resolve_ref_constraints(&mut models)?;

    Ok(Schema { models })
}

/// Infrastructural schema parsing: panics on an invalid schema. For internal/trusted input
/// (tests, `MarciDB::new`, reconstruction from storage in `open`). For external untrusted input use
/// [`try_parse_schema`], which returns [`SchemaError`] instead of panicking
pub fn parse_schema(input: &str) -> Schema {
    try_parse_schema(input).unwrap_or_else(|e| panic!("invalid schema: {}", e))
}

/// Rebuilds the computed caches of already-flat models: `default` bytes, indexes (`FieldIndex`),
/// `counter_idx`, `rev_dependencies`. WITHOUT desugaring and WITHOUT reassigning slots/offsets —
/// for loading a `Schema` from a materialized snapshot ([`crate::snapshot::parse_snapshot`]), where
/// the structure/slots/bindings are already pinned and only the caches need to be restored.
pub(crate) fn rebuild_caches(models: &mut [Entity]) -> Result<(), SchemaError> {
    for model in models.iter_mut() {
        for field in model.fields.iter_mut() {
            resolve_default_value(field)?;
        }
    }
    for model in models.iter_mut() {
        resolve_indexes(model)?;
    }
    let mut counter_id = 0;
    for model in models.iter_mut() {
        resolve_counter_idx(model, &mut counter_id);
    }
    resolve_ref_constraints(models)?;
    Ok(())
}

pub fn parse_model_block(name: String, lines: &mut std::iter::Peekable<std::str::Lines<'_>>) -> Result<Entity, SchemaError> {

    let fields = parse_fields(lines)?;
    // update_key_fields(&mut fields);

    return Ok(Entity::new(name, fields));
}

pub fn parse_struct_block(lines: &mut std::iter::Peekable<std::str::Lines<'_>>) -> Result<Entity, SchemaError> {
    let fields = parse_fields(lines)?;
    let mut entity = Entity::new(String::new(), fields);
    entity.autoinsert = true;
    return Ok(entity);
}

pub fn parse_fields(lines: &mut std::iter::Peekable<std::str::Lines<'_>>) -> Result<Vec<Field>, SchemaError> {
    let mut fields = Vec::new();

    for line in lines {
        let line = line.trim();
        if line.starts_with("//") {
            continue;
        }
        if line == "}" { break }
        if line.is_empty() { continue; }
        fields.push(parse_field_raw(line)?);
    }

    return Ok(fields);
}

// Adds a generated_id to the model if it has no own id
fn resolve_model_id(entity: &mut Entity) {
  if !entity.fields.iter().any(|f| f.is_id()) {
    entity.fields.insert(0, Field::new_id());
  }
}

// Finds structs and turns them into models. Also sets field.full_name for all fields
// Since we can have enums nested in a struct, as well as a struct in an enum, we resolve both types at once in this function
// This function does not assign the structs themselves, they stay as RefUnresolved, only with a different name
fn resolve_structs_and_enums(entity: &mut Entity, structs: &HashMap<String, Entity>, enums: &HashMap<String, EnumDef>, model_structs: &mut Vec<Entity>) -> Result<(), SchemaError> {

    for field in entity.fields.iter_mut(){
        let field_full_name: String = format!("{}.{}", &entity.name, &field.name);
        field.full_name = field_full_name.clone();
    }

    let mut i = 0;
    loop {
        if i >= entity.fields.len() {
            break;
        }

        let field = &mut entity.fields[i];

        match &field.ty {
            FieldType::RefUnresolved(name) | FieldType::RefListUnresolved(name) => {
                if let Some(st) = structs.get(name) {
                    let mut entity_model = create_entity_model(field, st, &entity.name);
                    field.location = FieldLocation::Virtual;
                    if let FieldType::RefUnresolved(name) | FieldType::RefListUnresolved(name) = &mut field.ty {
                        *name = entity_model.name.clone();
                    }
                    resolve_structs_and_enums(&mut entity_model, structs, enums, model_structs)?;
                    model_structs.push(entity_model);
                    i += 1;
                    continue;
                };

                if let Some(enum_def) = enums.get(name) {
                    if matches!(&field.ty, FieldType::RefListUnresolved(_)) {
                        return Err(SchemaError(format!(
                            "Field '{0}': list of enum '{1}' is not supported. An enum injects its variant fields directly into the model, so an enum list has nowhere to store them. Instead make a list of a model that wraps the enum — e.g. `model {1}Item {{ value {1} }}` then `{0} {1}Item[]`.",
                            field.name, name
                        )));
                    }

                    let field_name = field.name.clone();
                    let enum_info = create_enum_info(enum_def, entity, &field_name, i)?;

                    entity.fields[i].ty = FieldType::Enum(enum_info);

                    i += 1;
                    continue;
                }
            },
            _ => {}
        }

        i += 1;
    }

    Ok(())
}

// Creates a new model from a Struct
fn create_entity_model(field: &Field, st: &Entity, parent_name: &String) -> Entity {
    let mut entity_model = st.clone();
    entity_model.name = field.full_name.clone();

    if matches!(&field.ty, FieldType::RefListUnresolved(_)) {
        resolve_model_id(&mut entity_model);
    }

    let mut parent_id = Field::new_id();
    parent_id.name = "@parent_id".to_string();
    parent_id.ty = FieldType::RefUnresolved(parent_name.to_string());
    parent_id.default_value = None;
    // @parent_id is a Ref to the parent, it only has bind (the @default inherited from new_id is not needed)
    parent_id.attributes = vec![Attribute::BindUnresolved(field.name.clone())];
    entity_model.fields.insert(0, parent_id);

    entity_model
}

/// Adds the Enum's fields into the current Entity.
/// A field shared by several variants is added once — its index goes into the lists of all of its variants
fn create_enum_info(enum_def: &EnumDef, entity: &mut Entity, field_name: &String, field_index: usize) -> Result<EnumInfo, SchemaError> {
    let variants_names_map = create_variants_names_map(&enum_def.variants_map);

    let mut variants: HashMap<u16,Vec<usize>> = enum_def.variants_map.values().map(|variant| (*variant, vec![])).collect();

    for field_def in enum_def.fields.iter() {
        let variant_names: Vec<&str> = field_def.variants.iter()
            .map(|variant| variants_names_map.get(variant).unwrap().as_str())
            .collect();
        let base_name = format!("{}[{}={}]", entity.name, field_name, variant_names.join("|"));

        let mut field = field_def.field.clone();
        field.full_name = format!("{}.{}", base_name, field.name);
        field.condition = FieldExistsCondition::EnumValue { field_index, variants: field_def.variants.clone() };

        if let Some(exists_field) = entity.fields.iter().find(|f| f.name == field.name) {
            return Err(SchemaError(format!("Cannot add enum field {} to {}. Field {} already exists", field.name, entity.name, exists_field.full_name)));
        }

        for variant in field_def.variants.iter() {
            variants.get_mut(variant).unwrap().push(entity.fields.len());
        }
        entity.fields.push(field);
    }

    Ok(EnumInfo {
        variants,
        variants_map: enum_def.variants_map.clone(),
        variants_names_map
    })
}

fn create_variants_names_map(variants_map: &HashMap<String,u16>) -> HashMap<u16,String> {
    let mut inverted_map = HashMap::new();
    for (key, value) in variants_map {
        inverted_map.insert(*value, key.clone());
    }
    inverted_map
}

/// Finds the proper models and structs for RefUnresolved and RefListUnresolved refs
fn resolve_refs(entity: &mut Entity, model_by_name: &HashMap<String, usize>) -> Result<(), SchemaError> {
    for field in entity.fields.iter_mut(){
        match &field.ty {
            FieldType::RefUnresolved(name) => {
                let Some(model_index) = model_by_name.get(name) else {
                    return Err(SchemaError(format!("Unknown type {} (field {})", name, field.full_name)));
                };
                field.ty = FieldType::Ref(RefInfo::new(*model_index));
            }
            FieldType::RefListUnresolved(name) => {
                let Some(model_index) = model_by_name.get(name) else {
                    return Err(SchemaError(format!("Unknown type {} (field {})", name, field.full_name)));
                };
                field.ty = FieldType::RefList(RefInfo::new(*model_index));
            }
            _ => {}
        }
    }
    Ok(())
}

// Assigns the correct index for Key fields and offset for Body fields
fn resolve_field_offsets(entity: &mut Entity) {
  let mut offset_index: usize = 0;
  let mut key_index: usize = 0;

  let pre_header_size = 4;

  for field in entity.fields.iter_mut(){
    match &mut field.location {
        FieldLocation::Key { index } => {
          *index = key_index;
          key_index += 1;
        },
        FieldLocation::Body { offset_pos } => {
          *offset_pos = pre_header_size + offset_index * 4;
          offset_index += 1;
        },
        FieldLocation::Virtual => {}
    }
  }

  entity.payload_offset = pre_header_size + offset_index * 4;
}

// Sets the correct counter_idx for all models
fn resolve_counter_idx(entity: &mut Entity, counter_id: &mut usize) {
  for field in entity.fields.iter_mut() {
    if let Some(FieldDefault::Counter(counter_idx)) = &mut field.default_value {
      *counter_idx = *counter_id;
      *counter_id += 1;
    }
  }
}

/// Sets RefConstraints for Ref and RefList
fn resolve_ref_constraints(models: &mut [Entity]) -> Result<(), SchemaError> {
    let mut refs: Vec<Vec<EntityDependency>> = vec![Vec::new(); models.len()];

    for (model_index, entity) in models.iter_mut().enumerate() {
        for (field_index, field) in entity.fields.iter_mut().enumerate() {
            match &mut field.ty {
                FieldType::Ref(ref_info) => {
                    let constraint: DeleteConstraint;

                    if let Some(delete_constraint) = get_delete_constraint(&field.attributes) {
                        constraint = delete_constraint.clone();
                    } else {
                        constraint = match &ref_info.binding {
                            RefBinding::CurrentId => {
                                // If this is a child object, it cannot affect the parent in any way, so we skip it
                                continue;
                            },
                            RefBinding::FieldValue => DeleteConstraint::SetNull,
                            RefBinding::IndexTree(_) => { continue; }
                       };
                    }

                    refs[ref_info.model_index].push(EntityDependency { 
                        model_index: model_index, 
                        field_index: field_index, 
                        constraint
                    });
                }
                FieldType::RefList(ref_info) => {
                    match &ref_info.binding {
                        RefBinding::CurrentId => {
                            // If this is a child object, it cannot affect the parent in any way, so we skip it
                            continue;
                        },
                        RefBinding::FieldValue => {
                            return Err(SchemaError(format!("Cannot use @bind FieldValue for a list relation: {:?}", ref_info.binding)));
                        },
                        RefBinding::IndexTree(_) => { 
                            refs[ref_info.model_index].push(EntityDependency { 
                                model_index: model_index, 
                                field_index: field_index, 
                                constraint: DeleteConstraint::RemoveItem
                            });
                        }
                    }
                },
                _ => {}
            }
        }
    }

    for (model_index, deps) in refs.into_iter().enumerate() {
        models[model_index].rev_dependencies = deps;
    }
    Ok(())
}

fn get_delete_constraint(attributes: &[Attribute]) -> Option<&DeleteConstraint> {
    attributes.iter().find_map(|attr| {
        if let Attribute::OnDelete(dc) = attr { Some(dc) } else { None }
    })
}

fn resolve_indexes(model: &mut Entity) -> Result<(), SchemaError> {
    for field in model.fields.iter_mut() {
        let mut has_index = false;
        let mut unique = false;
        for attr in field.attributes.iter() {
            match attr {
                Attribute::Index => {
                    has_index = true;
                    break
                },
                Attribute::Unique => {
                    unique = true;
                    has_index = true;
                    break
                }
                _ => {}
            }
        }

        if has_index {
            match &mut field.ty {
                FieldType::Primitive(ty) => {
                    if let Some(ty) = ty.get_num_type() {
                        field.indexes.push(FieldIndex::Number { tree_name: format!("index_{}", field.full_name), unique, ty });
                    } else {
                        field.indexes.push(FieldIndex::Value { tree_name: format!("index_{}", field.full_name), unique });
                    }
                },
                FieldType::Enum(_) | FieldType::PrimitiveList(_, _) => {
                    field.indexes.push(FieldIndex::Value { tree_name: format!("index_{}", field.full_name), unique });
                }
                FieldType::Ref(ref_info) => {
                    // @unique on a relation is the one-to-one constraint; a plain @index on a relation
                    // is not supported — a relation is indexed through its reverse side, not a value tree.
                    if !unique {
                        return Err(SchemaError(format!(
                            "@index on relation field '{}' is not supported. Index a relation through its reverse collection (declare the back-reference list with @bind on the target), or use @unique for a one-to-one relation.",
                            field.full_name
                        )));
                    }
                    ref_info.is_unique = unique;
                },
                _ => return Err(SchemaError(format!("Wrong type for index in field {}", field.full_name)))
            }
        }

        // Module (`@custom`) indexes: materialize each into a FieldIndex::Custom. The schema layer stays
        // provider-agnostic — validation of field type/args is the engine's job (IndexProvider::validate).
        let customs: Vec<(String, String)> = field.attributes.iter().filter_map(|a| {
            if let Attribute::Custom { name, args } = a { Some((name.clone(), args.clone())) } else { None }
        }).collect();
        for (name, args) in customs {
            let tree_name = format!("custom_{}_{}", name, field.full_name);
            field.indexes.push(FieldIndex::Custom { tree_name, name, args });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::schema::{FieldLocation, FieldType, RefBinding, parse_schema, try_parse_schema};

    /// Invalid schema → SchemaError (not a panic). One case per resolver
    #[test]
    fn try_parse_schema_rejects_invalid() {
        assert!(try_parse_schema("model User {\n  name String\n}").is_ok());

        let bad = [
            "model A {\n  x B\n}",                                   // unknown type
            "model A {\n  x String @bogus\n}",                      // unknown attribute
            "model A {\n  x\n}",                                    // no field type
            "model A {\n  x UInt @default(abc)\n}",                 // bad default
            "model A {\n  x Int @index @default(zzz)\n}",           // bad default on an indexed field
            "model A {\n  t E\n}\nenum E {\n  a {\n    s String\n  }\n  b {\n    s String\n  }\n}", // duplicate enum field
            "model A {\n  b B @bind(B.nope)\n}\nmodel B {\n  x String\n}", // bad @bind
        ];
        for schema in bad {
            assert!(try_parse_schema(schema).is_err(), "expected an error for:\n{}", schema);
        }
    }

    #[test]
    fn parse_materializes_custom_index() {
        use crate::schema::FieldIndex;

        let schema = try_parse_schema("model Doc {\n  embedding Float[8] @custom(vector, cosine)\n}").unwrap();
        let f = schema.models[0].fields.iter().find(|f| f.name == "embedding").unwrap();
        assert!(matches!(f.ty, FieldType::PrimitiveList(_, Some(8))));
        let custom = f.indexes.iter().find_map(|i| match i {
            FieldIndex::Custom { name, args, tree_name } => Some((name.as_str(), args.as_str(), tree_name.as_str())),
            _ => None,
        }).expect("custom index not materialized");
        assert_eq!(custom, ("vector", "cosine", "custom_vector_Doc.embedding"));

        // @custom with no args ⇒ empty args
        let s2 = try_parse_schema("model Doc {\n  body String @custom(fulltext)\n}").unwrap();
        let b = s2.models[0].fields.iter().find(|f| f.name == "body").unwrap();
        assert!(b.indexes.iter().any(|i| matches!(i, FieldIndex::Custom { name, args, .. } if name == "fulltext" && args.is_empty())));

        // @custom() with no provider name is rejected
        assert!(try_parse_schema("model Doc {\n  x String @custom()\n}").is_err());
    }

    #[test]
    fn test_parse_schema() {
        let schema = parse_schema("
        model User {
            name            String
            projects        Project[]       @bind(Project.users.user)
            projectOwners   Project[]       @bind(Project.author)
        }

        model Project {
            name        String
            users       UserRole[]
            info        ProjectInfo?
            author      User
        }

        struct UserRole {
            user        User          @id
            role        String
        }

        struct ProjectInfo {
            description String
        }

        ");

        assert_eq!(schema.models.len(), 4);
        // assert_eq!(schema.models[0].fields.len(), 3);

        // The Project.users struct is formed, it has 3 fields:
        // @parent_id
        // user
        // role
        assert_eq!(schema.models[2].fields.len(), 3);

        // Check Project.users.user field type
        match &schema.models[2].fields[1].ty {
            FieldType::Ref (ref_info) => {
                assert_eq!(schema.models[ref_info.model_index].name, "User");
                assert_eq!(schema.models[ref_info.model_index].fields[ref_info.rev_field_idx.unwrap()].name, "projects");
                assert_eq!(ref_info.parent_index, None);

                assert_eq!(ref_info.binding, RefBinding::FieldValue);
            },
            _ => panic!("Wrong schema field {} type", schema.models[0].fields[2].name)
        }

        // Check Project.author type
        match &schema.models[1].fields[4].ty {
            FieldType::Ref (ref_info) => {
                assert_eq!(schema.models[ref_info.model_index].name, "User");
                assert_eq!(ref_info.binding, RefBinding::FieldValue);
            },
            _ => panic!("Wrong schema field {} type", schema.models[0].fields[2].name)
        }
        
        // Check User.projects type
        match &schema.models[0].fields[2].ty {
            FieldType::RefList(ref_info) => {
                assert_eq!(schema.models[ref_info.model_index].name, "Project.users");
                assert_eq!(schema.models[ref_info.model_index].fields[ref_info.rev_field_idx.unwrap()].name, "user");
                assert_eq!(schema.models[ref_info.parent_index.unwrap()].name, "Project");

                assert_eq!(ref_info.binding, RefBinding::IndexTree("User.projects->Project.users".to_string()));
            },
            _ => panic!("Wrong schema field {} type", schema.models[0].fields[2].name)
        }
        
        // assert_eq!(schema.models[0].fields[2].indexes.direct, Some(InsertedIndex { tree_name: "User.projects->Project.users".to_string() }));
        // assert_eq!(schema.models[0].fields[2].indexes.rev, Some(InsertedIndex { tree_name: "Project.users.user->User".to_string() }));

        // assert_eq!(schema.models[2].fields[1].indexes.direct, Some(InsertedIndex { tree_name: "Project.users.user->User".to_string() }));
        // assert_eq!(schema.models[2].fields[1].indexes.rev, Some(InsertedIndex { tree_name: "User.projects->Project.users".to_string() }));


        // assert!(matches!(schema.models[1].fields[3].location, FieldLocation::Virtual));
        // assert_eq!(schema.models[1].fields[3].indexes.direct, None );
        

        // assert_eq!(schema.models[1].fields[4].indexes.direct, None );
    }

    #[test]
    fn test_parse_schema_ref() {
        let schema = parse_schema("
        model User {
            name            String
            posts           Post[]      @bind(Post.author)
            passport        Passport    @bind(Passport.user)
        }

        model Post {
            author          User
        }

        model Passport {
            id          String      @id
            user        User        @unique
        }
        ");

        assert_eq!(schema.models.len(), 3);

        match schema.models[1].fields[1].location {
            FieldLocation::Body { offset_pos } => {
                assert_eq!(offset_pos, 4);
            },
            _ => panic!("Wrong schema field location {}", schema.models[1].fields[1].full_name)
        }

        match schema.models[0].fields[3].location {
            FieldLocation::Virtual => {
                
            },
            _ => panic!("Wrong schema field location {}. Expected FieldLocation::Virtual", schema.models[0].fields[3].full_name)
        }
    }

    #[test]
    fn test_parse_schema_enum_union() {
        let schema = parse_schema("
        model Account {
            name        String
            type        AccountType
        }

        enum AccountType {
            basic
            pro | business {
                sign    String
            }
            business {
                company String
            }
        }
        ");

        let entity = &schema.models[0];
        // id, name, type, sign, company — sign is added once, despite two variants
        assert_eq!(entity.fields.len(), 5);
        assert_eq!(entity.fields[3].name, "sign");
        assert_eq!(entity.fields[4].name, "company");

        match &entity.fields[2].ty {
            FieldType::Enum(enum_info) => {
                assert_eq!(enum_info.variants_map.get("basic"), Some(&0));
                assert_eq!(enum_info.variants_map.get("pro"), Some(&1));
                assert_eq!(enum_info.variants_map.get("business"), Some(&2));

                // Both variants reference the same physical field sign
                assert_eq!(enum_info.variants.get(&0).unwrap(), &Vec::<usize>::new());
                assert_eq!(enum_info.variants.get(&1).unwrap(), &vec![3]);
                assert_eq!(enum_info.variants.get(&2).unwrap(), &vec![3, 4]);
            },
            _ => panic!("Wrong field type {:?}", entity.fields[2].ty)
        }

        match &entity.fields[3].condition {
            crate::schema::FieldExistsCondition::EnumValue { variants, .. } => {
                assert_eq!(variants, &vec![1, 2]);
            },
            _ => panic!("Wrong condition for field {}", entity.fields[3].full_name)
        }
    }

    #[test]
    fn test_parse_schema_enum() {
        let schema = parse_schema("
        model Project {
            name        String            
            type        ProjectType
        }

        model User {
            name        String
        }

        enum ProjectType {
            base
            shared {
                users   User[]
            }
        }
        ");

        assert_eq!(schema.models.len(), 2);

        assert_eq!(schema.models[0].fields.len(), 4);
        
        match &schema.models[0].fields[2].ty {
            FieldType::Enum(enum_def) => {
                assert_eq!(enum_def.variants.len(), 2);

                assert_eq!(enum_def.variants.get(&0).unwrap(), &Vec::<usize>::new());
                assert_eq!(enum_def.variants.get(&1).unwrap(), &vec![3]);
                assert_eq!(enum_def.variants_map.len(), 2);
            },
            _ => panic!("Wrong field type {:?}", schema.models[0].fields[3].ty)
        }
    }
}
