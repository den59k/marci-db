//! Binary result encoder — the read-path counterpart of [`json_decoder::decode_document`].
//!
//! The query machinery is generic over the decode output `U` (the closure `F: Fn(DecodeCtx<U>) -> U`).
//! The JSON path uses `U = String`; this module plugs in `U = Vec<u8>`, so the *same* slot walk, include
//! traversal and include cache run — only the sink changes (binary instead of a JSON string). That also
//! removes the JSON double-serialize the FFI envelope used to pay (decode → string → `Value` → re-serialize).
//!
//! ## Wire format (a result buffer)
//!
//! ```text
//! [u8 version][u32 row_count][row 0][row 1] … [row N-1]      (all integers little-endian)
//! ```
//!
//! A **row** is the selected fields in **slot order** (the order `entity.fields` is iterated), then the
//! includes in field order — exactly what [`decode_document`] emits, so the TS decoder reads in the same
//! order. Per field:
//!
//! * **Key field** (`@id`): the value, no presence tag (ids are always present).
//! * **Body field**: a 1-byte presence tag — [`TAG_NULL`] or [`TAG_PRESENT`] — then, if present, the value.
//! * **To-one relation**: 1 byte `0` (null) or `1` (present) then, if present, the related row inline.
//! * **To-many relation**: `u32 count` then that many related rows inline (Part 1 repeats shared rows).
//!
//! Scalar value encodings (chosen for exact parity with the JSON→JS path; see [`encode_primitive`]):
//! fixed-width LE for numbers, `u32 len` + UTF-8 for strings.
//!
//! ## Scope (Part 1, MVP)
//!
//! Supported shapes are gated by [`shape_supported`]; anything outside falls back to JSON transparently
//! (the FFI reports "unsupported" and the client re-issues over the JSON path). v1 covers the eight
//! primitives + nested to-one / to-many relations. Formats (uuid/hex), enums, primitive lists, and
//! composite/ref keys are intentionally deferred — they stay on the JSON path until a later slice.

use serde_json::Value;

use crate::{
  MarciDB, OpError, QueryError,
  json_parsers::{DecodeError, get_offset_checked, parse_query},
  query_op::{DecodeCtx, IncludeQuery, IncludeResult, QueryOp},
  schema::{FieldLocation, FieldType, PrimitiveFieldType, Schema},
  utils::{check_exists_condition, get_end_optimized, get_id_field_size},
};

/// Wire format version. Bump on any layout change; the decoder hard-errors on a mismatch.
pub const BINARY_VERSION: u8 = 1;

/// Body-field presence tags (key fields carry no tag — they are always present).
const TAG_NULL: u8 = 0;
const TAG_PRESENT: u8 = 1;

#[inline]
fn put_u32(out: &mut Vec<u8>, v: usize) {
  out.extend_from_slice(&(v as u32).to_le_bytes());
}

/// Whether the binary encoder/decoder can handle this query's result shape. The engine encoder and the TS
/// decoder cover the same subset (both derive from the schema), so this is the single authority: if it
/// returns `false` the caller falls back to the JSON path. Checked once per query (not per row).
pub fn shape_supported(query: &QueryOp) -> bool {
  for (i, field) in query.entity.fields.iter().enumerate() {
    if !query.mask[i] {
      continue;
    }
    // Only plain (unformatted) primitive scalars in v1; formats/enums/lists/ref-keys → JSON fallback.
    if field.format.is_some() {
      return false;
    }
    if !matches!(field.ty, FieldType::Primitive(_)) {
      return false;
    }
    // A variant (enum-payload) field is *absent* in JSON when its enum isn't on the matching value, which
    // binary's present/null tag can't reproduce — keep these on the JSON path.
    if !matches!(field.condition, crate::schema::FieldExistsCondition::None) {
      return false;
    }
  }

  for include in query.includes.iter() {
    match &include.query {
      // Nested select: the related shape must itself be supported (recursive).
      IncludeQuery::Query(sub) => {
        if !shape_supported(sub) {
          return false;
        }
      }
      // Aggregate-in-include is formatted at the JSON layer — JSON fallback.
      IncludeQuery::Aggregate(_) => return false,
    }
  }

  true
}

/// The binary counterpart of [`decode_document`]: encodes one row (with its already-encoded includes) into
/// the wire format above. Infallible types are gated by [`shape_supported`]; a `DecodeError` here means a
/// genuinely corrupt row (the JSON path would `unwrap()` on the same input).
pub fn encode_document(ctx: DecodeCtx<Vec<u8>>) -> Result<Vec<u8>, DecodeError> {
  let DecodeCtx { data, entity, id, mask, includes, schema } = ctx;

  // Header length is taken from the row itself (bytes 2..4), exactly like decode_document, so a row written
  // under an older (shorter) schema reads its missing tail fields as absent rather than over-reading.
  let row_header = if data.is_empty() {
    0
  } else {
    if data.len() < 4 {
      return Err(DecodeError::BufferTooSmall);
    }
    if data[0] != 1 {
      return Err(DecodeError::WrongVersion);
    }
    let row_header = u16::from_be_bytes([data[2], data[3]]) as usize;
    if data.len() < row_header {
      return Err(DecodeError::BufferTooSmall);
    }
    row_header
  };

  let mut out = Vec::with_capacity(64);

  let mut id_offset = 0;
  for (field_index, field) in entity.fields.iter().enumerate() {
    match field.location {
      FieldLocation::Key { .. } => {
        let size = get_id_field_size(id, field, id_offset, schema);
        if mask[field_index] {
          encode_key_field(&mut out, &id[id_offset..id_offset + size], &field.ty, schema)?;
        }
        id_offset += size;
      }
      FieldLocation::Body { offset_pos } => {
        if !mask[field_index] {
          continue;
        }
        // A variant field whose enum isn't on the matching value is absent in the JSON output. v1 gates
        // such fields out of binary (shape_supported), but keep the guard so the contract is faithful:
        // emit a NULL tag so the row stays decodable in slot order.
        if !check_exists_condition(entity, &field.condition, id, data, schema) {
          out.push(TAG_NULL);
          continue;
        }
        encode_body_field(&mut out, data, offset_pos, field, row_header)?;
      }
      FieldLocation::Virtual => {}
    }
  }

  // Includes come after the body, in field order — same as decode_document. The nested rows are already
  // encoded by this very closure (the query machinery ran it recursively), so they inline verbatim.
  for include in includes {
    match include {
      IncludeResult::None(_) => out.push(0), // to-one with no match → null
      IncludeResult::One(_, buf) => {
        out.push(1);
        out.extend_from_slice(&buf);
      }
      IncludeResult::Many(_, bufs) => {
        put_u32(&mut out, bufs.len());
        for buf in bufs {
          out.extend_from_slice(&buf);
        }
      }
      IncludeResult::Aggregate(..) => {
        // Gated out by shape_supported — reaching here is a contract bug.
        return Err(DecodeError::TypeMismatch("aggregate include is not supported by the binary encoder".into()));
      }
    }
  }

  Ok(out)
}

/// A key (`@id`) value: present unconditionally, no tag. v1 supports primitive keys only.
fn encode_key_field(out: &mut Vec<u8>, data: &[u8], ty: &FieldType, _schema: &Schema) -> Result<(), DecodeError> {
  match ty {
    FieldType::Primitive(primitive) => encode_primitive(out, primitive, data),
    _ => Err(DecodeError::TypeMismatch("only primitive keys are supported by the binary encoder".into())),
  }
}

/// A body value: a presence tag, then the value if present. Mirrors [`decode_body_field`]'s null logic.
fn encode_body_field(out: &mut Vec<u8>, data: &[u8], offset_pos: usize, field: &crate::Field, payload_offset: usize) -> Result<(), DecodeError> {
  // Slot beyond the row header — added by a later migration → absent on this row → null.
  if offset_pos + 4 > payload_offset {
    out.push(TAG_NULL);
    return Ok(());
  }
  let offset_start = get_offset_checked(data, offset_pos)?;
  if offset_start == 0 {
    out.push(TAG_NULL);
    return Ok(());
  }

  out.push(TAG_PRESENT);

  match &field.ty {
    FieldType::Primitive(primitive) => {
      let offset_end = get_end_optimized(data, field, offset_start, offset_pos, payload_offset);
      encode_primitive(out, primitive, &data[offset_start..offset_end])
    }
    _ => Err(DecodeError::TypeMismatch("only primitive body fields are supported by the binary encoder".into())),
  }
}

/// Encodes one primitive value. Encodings are chosen so the bytes the TS side reads produce a value
/// **identical** to what the JSON path would (`decode_primitive_value` → JSON number/string → `JSON.parse`):
///
/// * `Int64`/`UInt64`/`DateTime` → 8 bytes LE; TS reads BigInt then `Number()` (same f64 as parsing the
///   decimal string — and the same 2^53 precision ceiling the JSON path already has).
/// * `Double` → 8 bytes LE f64; bit-identical to the shortest-round-trip JSON path.
/// * `Float` → **widened to f64 via the decimal string** then 8 bytes LE. `0.1_f32 as f64` ≠ `0.1`, but the
///   JSON path prints `f32::to_string()` ("0.1") which JS parses to the f64 nearest 0.1; going through the
///   string here reproduces exactly that f64, so binary and JSON agree.
/// * `Bool` → 1 byte 0/1; `Byte` → 1 byte; `String` → `u32 len` + UTF-8 (raw, no JSON escaping).
fn encode_primitive(out: &mut Vec<u8>, ty: &PrimitiveFieldType, data: &[u8]) -> Result<(), DecodeError> {
  let fixed = |data: &[u8]| -> Result<[u8; 8], DecodeError> {
    data.try_into().map_err(|_| DecodeError::InvalidLength { expected: 8, got: data.len() })
  };
  match ty {
    PrimitiveFieldType::String => {
      // Validate UTF-8 (the JSON path does too via from_utf8); the wire carries raw bytes.
      std::str::from_utf8(data).map_err(|_| DecodeError::Utf8Error)?;
      put_u32(out, data.len());
      out.extend_from_slice(data);
    }
    PrimitiveFieldType::Int64 | PrimitiveFieldType::DateTime => {
      let n = i64::from_be_bytes(fixed(data)?);
      out.extend_from_slice(&n.to_le_bytes());
    }
    PrimitiveFieldType::UInt64 => {
      let n = u64::from_be_bytes(fixed(data)?);
      out.extend_from_slice(&n.to_le_bytes());
    }
    PrimitiveFieldType::Double => {
      let n = f64::from_be_bytes(fixed(data)?);
      out.extend_from_slice(&n.to_le_bytes());
    }
    PrimitiveFieldType::Float => {
      let n = f32::from_be_bytes(data.try_into().map_err(|_| DecodeError::InvalidLength { expected: 4, got: data.len() })?);
      // Widen through the decimal string so the f64 matches JSON.parse(f32::to_string()) exactly.
      let widened: f64 = n.to_string().parse().map_err(|_| DecodeError::TypeMismatch("float widen".into()))?;
      out.extend_from_slice(&widened.to_le_bytes());
    }
    PrimitiveFieldType::Bool => out.push(if data[0] != 0 { 1 } else { 0 }),
    PrimitiveFieldType::Byte => out.push(data[0]),
  }
  Ok(())
}

// ───────────────────────────────── engine entries ─────────────────────────────────

/// The binary closure passed to `find_many`/`find_first`. Shapes are pre-validated by [`shape_supported`],
/// so an `Err` here is a corrupt row — the JSON path `unwrap()`s on the same input, so we mirror that.
fn encode_row(ctx: DecodeCtx<Vec<u8>>) -> Vec<u8> {
  encode_document(ctx).expect("binary encode of a supported, well-formed row")
}

/// Runs a `findMany` and encodes the result set into the wire buffer (`[version][row_count][rows…]`).
pub fn query_binary_many(db: &MarciDB, query: &QueryOp) -> Result<Vec<u8>, QueryError> {
  let rows = db.find_many(query, encode_row)?;
  let mut out = Vec::with_capacity(5 + rows.iter().map(Vec::len).sum::<usize>());
  out.push(BINARY_VERSION);
  put_u32(&mut out, rows.len());
  for row in rows {
    out.extend_from_slice(&row);
  }
  Ok(out)
}

/// Runs a `findFirst` and encodes 0-or-1 rows into the same wire buffer.
pub fn query_binary_one(db: &MarciDB, query: &QueryOp) -> Result<Vec<u8>, QueryError> {
  let row = db.find_first(query, encode_row)?;
  let mut out = Vec::with_capacity(5 + row.as_ref().map_or(0, Vec::len));
  out.push(BINARY_VERSION);
  match row {
    Some(row) => {
      put_u32(&mut out, 1);
      out.extend_from_slice(&row);
    }
    None => put_u32(&mut out, 0),
  }
  Ok(out)
}

/// Outcome of [`execute_query_binary`]. `Unsupported` means "not a binary-eligible read" (wrong action, or
/// a shape the binary path doesn't cover yet) — the caller transparently re-issues over the JSON path.
pub enum QueryBinaryOutcome {
  Supported(Vec<u8>),
  Unsupported,
}

/// Binary read dispatcher — the binary counterpart of [`execute_op`](crate::execute_op) for the two read
/// actions. Parses the `{ model, action: "findMany"|"findFirst", query }` op, checks the shape, and encodes.
/// Non-read actions and unsupported shapes return [`QueryBinaryOutcome::Unsupported`] (→ JSON fallback);
/// only genuine client/storage faults return `Err`.
pub fn execute_query_binary(db: &MarciDB, op: &Value) -> Result<QueryBinaryOutcome, OpError> {
  let obj = op.as_object().ok_or(OpError::NotAnObject)?;
  let model = obj.get("model").and_then(|m| m.as_str()).ok_or(OpError::MissingField("model"))?;
  let action = obj.get("action").and_then(|a| a.as_str()).ok_or(OpError::MissingField("action"))?;

  let many = match action {
    "findMany" => true,
    "findFirst" => false,
    _ => return Ok(QueryBinaryOutcome::Unsupported),
  };

  let entity = db.get_model(model).ok_or_else(|| OpError::UnknownModel(model.to_string()))?;
  let query_val = obj.get("query").ok_or(OpError::MissingField("query"))?;
  let query = parse_query(&db.schema, entity, query_val).map_err(|e| OpError::Parse(format!("{:?}", e)))?;

  if !shape_supported(&query) {
    return Ok(QueryBinaryOutcome::Unsupported);
  }

  let bytes = if many { query_binary_many(db, &query) } else { query_binary_one(db, &query) }
    .map_err(OpError::Query)?;
  Ok(QueryBinaryOutcome::Supported(bytes))
}
