//! JSONB — a binary document format for the `Json` field type, the read/write counterpart for a JSON value
//! stored as one body field (see [`encode_primitive_value`](super::parsers::encode_primitive_value) and
//! [`decode_primitive_value`](super::json_decoder)).
//!
//! The blob is self-describing and laid out for **random access**: an object's keys live in a sorted offset
//! table so a key can be found by binary search, and an array's elements live in an offset table for O(1)
//! indexing — a nested field can be reached without parsing the whole document. That is the property that
//! makes the future "filter by JSON field" feature cheap; Phase 1 only exercises the full encode/decode
//! round-trip, but the layout is the durable on-disk contract.
//!
//! All integers are **big-endian** and offset tables are **start-offsets plus a trailing sentinel**, matching
//! the row format and [`encode_list_dynamic`](super::parsers::encode_list_dynamic).
//!
//! ## Wire format
//!
//! Every value is `[tag: u8][payload]`:
//!
//! ```text
//! 0x00 NULL     (no payload)
//! 0x01 FALSE    (no payload)
//! 0x02 TRUE     (no payload)
//! 0x03 INT      i64 BE                                  ; a serde Number that fits i64
//! 0x04 UINT     u64 BE                                  ; a serde Number in (i64::MAX, u64::MAX]
//! 0x05 DOUBLE   f64 BE                                  ; anything else numeric (always finite)
//! 0x06 STRING   [u32 len BE][utf8 bytes]
//! 0x07 ARRAY    [u32 n BE][ (n+1) u32 BE offsets ][ elem0 ][ elem1 ]…     ; each elem is a full value
//! 0x08 OBJECT   [u32 n BE][ (n+1) u32 BE key-offsets ][ (n+1) u32 BE val-offsets ][ keys… ][ values… ]
//! ```
//!
//! In a container the offsets are relative to the start of *their own* data region (the element bytes for an
//! array; the key region and, separately, the value region for an object). Object keys are stored as raw
//! UTF-8 (never tagged — they are always strings) sorted bytewise, so the layout is canonical: two objects
//! with the same entries encode to identical bytes regardless of source key order.

use serde_json::{Map, Value};

use crate::json_parsers::json_decoder::DecodeError;

const TAG_NULL: u8 = 0;
const TAG_FALSE: u8 = 1;
const TAG_TRUE: u8 = 2;
const TAG_INT: u8 = 3;
const TAG_UINT: u8 = 4;
const TAG_DOUBLE: u8 = 5;
const TAG_STRING: u8 = 6;
const TAG_ARRAY: u8 = 7;
const TAG_OBJECT: u8 = 8;

// ───────────────────────────────── encode ─────────────────────────────────

/// Encodes a JSON value into a fresh JSONB blob. The write path uses [`encode_into`] to append directly to
/// the row body; this convenience wrapper is used by tests and the upcoming filter/transport phases.
#[allow(dead_code)]
pub fn encode(value: &Value) -> Vec<u8> {
    let mut buf = Vec::with_capacity(32);
    encode_into(&mut buf, value);
    buf
}

/// Appends a JSON value's JSONB encoding to `buf` (the write path stores the blob inline in the row body,
/// so this avoids the intermediate allocation [`encode`] would make).
pub fn encode_into(buf: &mut Vec<u8>, value: &Value) {
    match value {
        Value::Null => buf.push(TAG_NULL),
        Value::Bool(false) => buf.push(TAG_FALSE),
        Value::Bool(true) => buf.push(TAG_TRUE),
        Value::Number(n) => encode_number(buf, n),
        Value::String(s) => {
            buf.push(TAG_STRING);
            put_str(buf, s);
        }
        Value::Array(arr) => encode_array(buf, arr),
        Value::Object(obj) => encode_object(buf, obj),
    }
}

fn encode_number(buf: &mut Vec<u8>, n: &serde_json::Number) {
    // Prefer the narrowest lossless tag: signed integers, then large unsigned (> i64::MAX), then float.
    // serde_json numbers are always finite, so the f64 branch never sees NaN/Inf.
    if let Some(i) = n.as_i64() {
        buf.push(TAG_INT);
        buf.extend_from_slice(&i.to_be_bytes());
    } else if let Some(u) = n.as_u64() {
        buf.push(TAG_UINT);
        buf.extend_from_slice(&u.to_be_bytes());
    } else {
        buf.push(TAG_DOUBLE);
        buf.extend_from_slice(&n.as_f64().unwrap_or(0.0).to_be_bytes());
    }
}

fn put_str(buf: &mut Vec<u8>, s: &str) {
    buf.extend_from_slice(&(s.len() as u32).to_be_bytes());
    buf.extend_from_slice(s.as_bytes());
}

fn encode_array(buf: &mut Vec<u8>, arr: &[Value]) {
    buf.push(TAG_ARRAY);
    let n = arr.len();
    buf.extend_from_slice(&(n as u32).to_be_bytes());

    // Reserve the offset table (n+1 slots), then append elements and backpatch each slot — single pass,
    // no temporary buffers (same technique as encode_list_dynamic).
    let table = buf.len();
    buf.resize(table + (n + 1) * 4, 0);
    let region = buf.len();
    for (i, item) in arr.iter().enumerate() {
        put_offset(buf, table + i * 4, region);
        encode_into(buf, item);
    }
    put_offset(buf, table + n * 4, region); // sentinel = end of the last element
}

fn encode_object(buf: &mut Vec<u8>, obj: &Map<String, Value>) {
    buf.push(TAG_OBJECT);
    let n = obj.len();
    buf.extend_from_slice(&(n as u32).to_be_bytes());

    // Sort by key bytes so the blob is canonical and supports binary-search lookup on read. serde_json's
    // default Map is a BTreeMap (already sorted), but sort explicitly so we don't silently depend on that.
    let mut entries: Vec<(&String, &Value)> = obj.iter().collect();
    entries.sort_unstable_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));

    let key_table = buf.len();
    buf.resize(key_table + (n + 1) * 4, 0);
    let val_table = buf.len();
    buf.resize(val_table + (n + 1) * 4, 0);

    let key_region = buf.len();
    for (i, (k, _)) in entries.iter().enumerate() {
        put_offset(buf, key_table + i * 4, key_region);
        buf.extend_from_slice(k.as_bytes());
    }
    put_offset(buf, key_table + n * 4, key_region);

    let val_region = buf.len();
    for (i, (_, v)) in entries.iter().enumerate() {
        put_offset(buf, val_table + i * 4, val_region);
        encode_into(buf, v);
    }
    put_offset(buf, val_table + n * 4, val_region);
}

/// Writes `(buf.len() - region)` — the current end relative to a data region — into the 4-byte slot at `at`.
fn put_offset(buf: &mut [u8], at: usize, region: usize) {
    let off = (buf.len() - region) as u32;
    buf[at..at + 4].copy_from_slice(&off.to_be_bytes());
}

// ───────────────────────────────── decode ─────────────────────────────────

/// Decodes a JSONB blob into JSON text. The output is already valid JSON, so the read path inserts it
/// verbatim (via `insert_value`, not `insert_string` — no re-escaping). A malformed blob yields a
/// [`DecodeError`] rather than panicking.
pub fn decode(data: &[u8]) -> Result<String, DecodeError> {
    let mut out = String::with_capacity(data.len());
    decode_value(data, &mut out)?;
    Ok(out)
}

/// Decodes one value occupying exactly `data` (container children are sliced to their exact bounds via the
/// offset tables, so every recursive call receives a tight slice).
fn decode_value(data: &[u8], out: &mut String) -> Result<(), DecodeError> {
    let tag = *data.first().ok_or(DecodeError::BufferTooSmall)?;
    match tag {
        TAG_NULL => out.push_str("null"),
        TAG_FALSE => out.push_str("false"),
        TAG_TRUE => out.push_str("true"),
        TAG_INT => out.push_str(&i64::from_be_bytes(read_8(data, 1)?).to_string()),
        TAG_UINT => out.push_str(&u64::from_be_bytes(read_8(data, 1)?).to_string()),
        TAG_DOUBLE => {
            let f = f64::from_be_bytes(read_8(data, 1)?);
            let num = serde_json::Number::from_f64(f)
                .ok_or_else(|| DecodeError::TypeMismatch("non-finite float in jsonb".into()))?;
            out.push_str(&num.to_string());
        }
        TAG_STRING => push_json_string(out, read_str(data, 1)?),
        TAG_ARRAY => decode_array(data, out)?,
        TAG_OBJECT => decode_object(data, out)?,
        other => return Err(DecodeError::TypeMismatch(format!("unknown jsonb tag {}", other))),
    }
    Ok(())
}

fn decode_array(data: &[u8], out: &mut String) -> Result<(), DecodeError> {
    let n = read_u32(data, 1)?;
    let table = 5; // tag(1) + count(4)
    let region = table + (n + 1) * 4;

    out.push('[');
    for i in 0..n {
        if i > 0 {
            out.push(',');
        }
        let start = region + read_u32(data, table + i * 4)?;
        let end = region + read_u32(data, table + (i + 1) * 4)?;
        decode_value(slice(data, start, end)?, out)?;
    }
    out.push(']');
    Ok(())
}

fn decode_object(data: &[u8], out: &mut String) -> Result<(), DecodeError> {
    let n = read_u32(data, 1)?;
    let key_table = 5; // tag(1) + count(4)
    let val_table = key_table + (n + 1) * 4;
    let key_region = val_table + (n + 1) * 4;
    let val_region = key_region + read_u32(data, key_table + n * 4)?; // key_region + total key bytes

    out.push('{');
    for i in 0..n {
        if i > 0 {
            out.push(',');
        }
        // Key — raw UTF-8, relative to the key region.
        let ks = key_region + read_u32(data, key_table + i * 4)?;
        let ke = key_region + read_u32(data, key_table + (i + 1) * 4)?;
        let key = std::str::from_utf8(slice(data, ks, ke)?).map_err(|_| DecodeError::Utf8Error)?;
        push_json_string(out, key);
        out.push(':');
        // Value — a full tagged value, relative to the value region.
        let vs = val_region + read_u32(data, val_table + i * 4)?;
        let ve = val_region + read_u32(data, val_table + (i + 1) * 4)?;
        decode_value(slice(data, vs, ve)?, out)?;
    }
    out.push('}');
    Ok(())
}

/// Appends `s` as a correctly-escaped, quoted JSON string (mirrors the `String` path in
/// `decode_primitive_value`, which also leans on serde_json for escaping).
fn push_json_string(out: &mut String, s: &str) {
    out.push_str(&serde_json::to_string(s).expect("string serialization is infallible"));
}

fn read_u32(data: &[u8], pos: usize) -> Result<usize, DecodeError> {
    let bytes = data.get(pos..pos + 4).ok_or(DecodeError::BufferTooSmall)?;
    Ok(u32::from_be_bytes(bytes.try_into().unwrap()) as usize)
}

fn read_8(data: &[u8], pos: usize) -> Result<[u8; 8], DecodeError> {
    let bytes = data
        .get(pos..pos + 8)
        .ok_or(DecodeError::InvalidLength { expected: 8, got: data.len().saturating_sub(pos) })?;
    Ok(bytes.try_into().unwrap())
}

fn read_str(data: &[u8], pos: usize) -> Result<&str, DecodeError> {
    let len = read_u32(data, pos)?;
    let bytes = data.get(pos + 4..pos + 4 + len).ok_or(DecodeError::BufferTooSmall)?;
    std::str::from_utf8(bytes).map_err(|_| DecodeError::Utf8Error)
}

/// Bounds-checked sub-slice; rejects both `start > end` and `end` past the buffer.
fn slice(data: &[u8], start: usize, end: usize) -> Result<&[u8], DecodeError> {
    data.get(start..end).ok_or(DecodeError::OffsetOutOfRange)
}

#[cfg(test)]
mod tests {
    use super::{decode, encode};
    use serde_json::{json, Value};

    /// encode → decode (to text) → re-parse must reproduce the original value.
    fn round_trip(v: Value) {
        let blob = encode(&v);
        let text = decode(&blob).unwrap();
        let back: Value =
            serde_json::from_str(&text).unwrap_or_else(|e| panic!("decoded text is not valid JSON: {} ({})", text, e));
        assert_eq!(back, v, "round-trip mismatch; text was {}", text);
    }

    #[test]
    fn scalars() {
        for v in [
            json!(null),
            json!(true),
            json!(false),
            json!(0),
            json!(42),
            json!(-7),
            json!(i64::MAX),
            json!(i64::MIN),
            json!(u64::MAX), // forces the UINT tag (> i64::MAX)
            json!(1.5),
            json!(-0.25),
            json!(""),
            json!("hello"),
            json!("quote \" backslash \\ newline \n tab \t unicode ☃"),
        ] {
            round_trip(v);
        }
    }

    #[test]
    fn arrays_and_objects() {
        round_trip(json!([]));
        round_trip(json!({}));
        round_trip(json!([1, 2, 3]));
        round_trip(json!(["a", "b", "c"]));
        round_trip(json!([1, "two", true, null, 3.5]));
        round_trip(json!({ "a": 1, "b": "two", "c": [1, 2, 3], "d": { "e": true } }));
    }

    #[test]
    fn nested_deep() {
        round_trip(json!({
            "user": { "name": "Bob", "tags": ["x", "y"], "meta": { "age": 30, "vip": false } },
            "items": [ { "id": 1, "p": [1.5, 2.5] }, { "id": 2, "p": [] } ],
            "n": null
        }));
    }

    #[test]
    fn unicode_keys_and_values() {
        round_trip(json!({ "ключ": "значение", "☃": [1, 2], "emoji 🎉": true }));
    }

    /// Keys are emitted sorted, so the encoding is canonical: same entries, different source order →
    /// identical bytes. (This is also what would let an equality filter compare blobs directly later.)
    #[test]
    fn object_encoding_is_canonical() {
        let a = encode(&json!({ "b": 1, "a": 2, "c": 3 }));
        let b = encode(&json!({ "c": 3, "a": 2, "b": 1 }));
        assert_eq!(a, b);
    }

    /// A truncated/garbage blob must error, never panic.
    #[test]
    fn malformed_blobs_error() {
        assert!(decode(&[]).is_err());
        assert!(decode(&[TAG_BAD]).is_err());
        assert!(decode(&[super::TAG_INT, 1, 2, 3]).is_err()); // INT needs 8 bytes
        assert!(decode(&[super::TAG_STRING, 0, 0, 0, 9, b'a']).is_err()); // len 9, 1 byte
        assert!(decode(&[super::TAG_ARRAY, 0, 0, 0, 1]).is_err()); // count 1, no offset table
    }

    const TAG_BAD: u8 = 99;
}
