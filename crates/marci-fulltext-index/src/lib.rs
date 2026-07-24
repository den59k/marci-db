//! `@custom(fulltext, <lang>)` index provider: a basic full-text search over a `String` field.
//!
//! Analyzer: Unicode word tokenizer (`is_alphanumeric` boundaries) → lowercase → Snowball stemmer. Language
//! is `multi` by default — each token is stemmed by script (Cyrillic → Russian, otherwise English), so a
//! single field handles mixed Russian/English text. `@custom(fulltext, russian)` / `english` force one.
//!
//! Index layout (one KV tree per field, multiplexed by a tag byte):
//!   posting:  `0x01 <term> 0x00 <id>`  →  term frequency (u32 BE)
//!   stats:    `0x00 'N'`               →  document count (u64 BE)
//! `<term>` never contains `0x00` (UTF-8 alphanumerics only), so the separator delimits term from id and
//! keeps a short term's prefix scan from matching a longer term.
//!
//! Ranking: OR over query terms, scored by `Σ tf · idf` (BM25 idf), best first. The index is maintained
//! **live**: inserts/updates/deletes flow through the `on_*` hooks (see [`IndexProvider::maintains_incrementally`]),
//! so a `$search` reflects writes immediately. `$reindex` is still used to backfill pre-existing rows when the
//! index is first added (or after an analyzer/args change), and to rebuild from scratch.

use std::collections::{HashMap, HashSet};

use marcidb::{Field, FieldType, IndexProvider, IndexTree, PrimitiveFieldType, ProviderError, RowRef, RowScan, SearchHit};
use rust_stemmers::{Algorithm, Stemmer};
use serde_json::Value;

const TAG_POSTING: u8 = 0x01;
const TAG_STATS: u8 = 0x00;
const SEP: u8 = 0x00;
const MIN_TOKEN_CHARS: usize = 2;
const DEFAULT_LIMIT: usize = 100;

pub struct FullTextProvider;

impl FullTextProvider {
    pub fn new() -> Self { FullTextProvider }
}

impl Default for FullTextProvider {
    fn default() -> Self { Self::new() }
}

impl IndexProvider for FullTextProvider {
    fn name(&self) -> &str { "fulltext" }

    fn validate(&self, field: &Field, args: &str) -> Result<(), ProviderError> {
        Lang::parse(args)?;
        match field.ty {
            FieldType::Primitive(PrimitiveFieldType::String) => Ok(()),
            _ => Err(ProviderError::Invalid(format!(
                "@custom(fulltext) requires a String field, but '{}' is not one", field.full_name
            ))),
        }
    }

    fn rebuild(&self, _field: &Field, args: &str, scan: RowScan<'_>, store: &mut IndexTree<'_>) -> Result<(), ProviderError> {
        let analyzer = Analyzer::new(Lang::parse(args)?);
        let mut doc_count: u64 = 0;

        for row in scan {
            let (id, value) = row?;
            let Some(value) = value else { continue }; // null text — not indexable
            let text = std::str::from_utf8(&value)
                .map_err(|_| ProviderError::Invalid("fulltext field is not valid UTF-8".into()))?;

            let counts = analyzer.term_counts(text);
            if counts.is_empty() { continue; }
            doc_count += 1;
            for (term, tf) in counts {
                store.insert(&posting_key(&term, &id), &tf.to_be_bytes())?;
            }
        }

        store.insert(&stats_key(), &doc_count.to_be_bytes())?;
        Ok(())
    }

    fn search(&self, _field: &Field, args: &str, payload: &Value, store: &IndexTree<'_>) -> Result<Vec<SearchHit>, ProviderError> {
        let analyzer = Analyzer::new(Lang::parse(args)?);
        let (query, limit) = parse_payload(payload)?;

        let terms = analyzer.unique_terms(&query);
        if terms.is_empty() { return Ok(vec![]); }

        let n = store.get(&stats_key())?
            .map(|b| u64::from_be_bytes(b.try_into().unwrap_or([0; 8])))
            .unwrap_or(0) as f32;

        // OR over terms, accumulating tf·idf per document.
        let mut scores: HashMap<Vec<u8>, f32> = HashMap::new();
        for term in &terms {
            let prefix = term_prefix(term);
            let mut postings: Vec<(Vec<u8>, u32)> = Vec::new();
            for entry in store.prefix(&prefix)? {
                let (key, val) = entry?;
                let id = key[prefix.len()..].to_vec();
                let tf = u32::from_be_bytes(val.try_into().unwrap_or([0; 4]));
                postings.push((id, tf));
            }
            let df = postings.len() as f32;
            if df == 0.0 { continue; }
            let idf = ((n - df + 0.5) / (df + 0.5) + 1.0).ln(); // BM25 idf, always ≥ 0
            for (id, tf) in postings {
                *scores.entry(id).or_insert(0.0) += tf as f32 * idf;
            }
        }

        let mut hits: Vec<SearchHit> = scores.into_iter().map(|(id, score)| SearchHit { id, score }).collect();
        // Best (highest relevance) first; id as a deterministic tie-break.
        hits.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal).then(a.id.cmp(&b.id)));
        hits.truncate(limit);
        Ok(hits)
    }

    // ───────────────────────────── incremental maintenance ─────────────────────────────
    // The hooks run inside the row's write transaction, so postings and the row commit/roll back together.
    // The analyzer is deterministic and stable for the life of an index (an args change is a drop+add →
    // `$reindex`), so a delete removes exactly the terms a prior insert added. `df` is derived at query time
    // from the posting prefix, so only the postings and the document count `N` are maintained here.

    fn maintains_incrementally(&self) -> bool { true }

    fn on_insert(&self, _field: &Field, args: &str, row: RowRef<'_>, new: Option<&[u8]>, store: &mut IndexTree<'_>) -> Result<(), ProviderError> {
        let analyzer = Analyzer::new(Lang::parse(args)?);
        let counts = term_counts_opt(&analyzer, new)?;
        for (term, tf) in &counts {
            store.insert(&posting_key(term, row.id), &tf.to_be_bytes())?;
        }
        // A doc with no indexable terms (null/empty/punctuation-only text) doesn't count toward `N`.
        adjust_doc_count(store, if counts.is_empty() { 0 } else { 1 })
    }

    fn on_delete(&self, _field: &Field, args: &str, row: RowRef<'_>, old: Option<&[u8]>, store: &mut IndexTree<'_>) -> Result<(), ProviderError> {
        let analyzer = Analyzer::new(Lang::parse(args)?);
        let counts = term_counts_opt(&analyzer, old)?;
        for term in counts.keys() {
            store.remove(&posting_key(term, row.id))?;
        }
        adjust_doc_count(store, if counts.is_empty() { 0 } else { -1 })
    }

    fn on_update(&self, _field: &Field, args: &str, row: RowRef<'_>, old: Option<&[u8]>, new: Option<&[u8]>, store: &mut IndexTree<'_>) -> Result<(), ProviderError> {
        let analyzer = Analyzer::new(Lang::parse(args)?);
        let old_counts = term_counts_opt(&analyzer, old)?;
        let new_counts = term_counts_opt(&analyzer, new)?;

        // Remove the old postings, then write the new ones. A term present in both is removed and re-inserted
        // with its new tf; a term only in `old` is dropped; a term only in `new` is added.
        for term in old_counts.keys() {
            store.remove(&posting_key(term, row.id))?;
        }
        for (term, tf) in &new_counts {
            store.insert(&posting_key(term, row.id), &tf.to_be_bytes())?;
        }

        // `N` tracks only the presence transition: a doc that gains its first term (+1) or loses its last (-1).
        let delta = match (!old_counts.is_empty(), !new_counts.is_empty()) {
            (false, true) => 1,
            (true, false) => -1,
            _ => 0,
        };
        adjust_doc_count(store, delta)
    }
}

// ───────────────────────────────── analyzer ─────────────────────────────────

#[derive(Clone, Copy)]
enum Lang { Multi, English, Russian }

impl Lang {
    fn parse(args: &str) -> Result<Lang, ProviderError> {
        match args.trim().to_lowercase().as_str() {
            "" | "multi" | "auto" => Ok(Lang::Multi),
            "english" | "en" => Ok(Lang::English),
            "russian" | "ru" => Ok(Lang::Russian),
            other => Err(ProviderError::Invalid(format!(
                "@custom(fulltext, {}): unknown language — use 'multi', 'english', or 'russian'", other
            ))),
        }
    }
}

struct Analyzer {
    lang: Lang,
    english: Stemmer,
    russian: Stemmer,
}

impl Analyzer {
    fn new(lang: Lang) -> Self {
        Analyzer { lang, english: Stemmer::create(Algorithm::English), russian: Stemmer::create(Algorithm::Russian) }
    }

    /// Lowercased, stemmed tokens (with repeats) — split on non-alphanumeric, drop tokens shorter than
    /// `MIN_TOKEN_CHARS`. Cyrillic and Latin words both tokenize via Unicode `is_alphanumeric`.
    fn tokens(&self, text: &str) -> Vec<String> {
        text.split(|c: char| !c.is_alphanumeric())
            .filter(|t| t.chars().take(MIN_TOKEN_CHARS).count() >= MIN_TOKEN_CHARS)
            .map(|t| self.stem(&t.to_lowercase()))
            .collect()
    }

    fn stem(&self, token: &str) -> String {
        let stemmer = match self.lang {
            Lang::English => &self.english,
            Lang::Russian => &self.russian,
            // Per-token: a token with any Cyrillic letter is Russian, otherwise English.
            Lang::Multi => if token.chars().any(is_cyrillic) { &self.russian } else { &self.english },
        };
        stemmer.stem(token).into_owned()
    }

    fn term_counts(&self, text: &str) -> HashMap<String, u32> {
        let mut counts = HashMap::new();
        for term in self.tokens(text) {
            *counts.entry(term).or_insert(0) += 1;
        }
        counts
    }

    fn unique_terms(&self, text: &str) -> Vec<String> {
        let mut seen = HashSet::new();
        self.tokens(text).into_iter().filter(|t| seen.insert(t.clone())).collect()
    }
}

fn is_cyrillic(c: char) -> bool {
    matches!(c, '\u{0400}'..='\u{04FF}' | '\u{0500}'..='\u{052F}')
}

// ───────────────────────────────── key codec & payload ─────────────────────────────────

fn posting_key(term: &str, id: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(2 + term.len() + id.len());
    key.push(TAG_POSTING);
    key.extend_from_slice(term.as_bytes());
    key.push(SEP);
    key.extend_from_slice(id);
    key
}

fn term_prefix(term: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(2 + term.len());
    key.push(TAG_POSTING);
    key.extend_from_slice(term.as_bytes());
    key.push(SEP);
    key
}

fn stats_key() -> Vec<u8> {
    vec![TAG_STATS, b'N']
}

/// Tokenize + count an optional field value; null text yields an empty map. Errors on non-UTF-8 bytes.
fn term_counts_opt(analyzer: &Analyzer, value: Option<&[u8]>) -> Result<HashMap<String, u32>, ProviderError> {
    let Some(value) = value else { return Ok(HashMap::new()); };
    let text = std::str::from_utf8(value)
        .map_err(|_| ProviderError::Invalid("fulltext field is not valid UTF-8".into()))?;
    Ok(analyzer.term_counts(text))
}

/// Read-modify-write the document count `N`. Always rewrites the stats key, so a freshly-created index gains
/// an explicit `N` on its first live write — keeping incremental maintenance byte-identical to a `$reindex`.
/// Writes are single-writer (exclusive write lock), so this needs no extra synchronization. `N` floors at 0.
fn adjust_doc_count(store: &mut IndexTree<'_>, delta: i64) -> Result<(), ProviderError> {
    let n = store.get(&stats_key())?
        .map(|b| u64::from_be_bytes(b.try_into().unwrap_or([0; 8])))
        .unwrap_or(0);
    let n = if delta >= 0 { n + delta as u64 } else { n.saturating_sub((-delta) as u64) };
    store.insert(&stats_key(), &n.to_be_bytes())?;
    Ok(())
}

/// `$search` / `$near` payload: a bare query string, or `{ "query": "...", "limit": N }`.
fn parse_payload(payload: &Value) -> Result<(String, usize), ProviderError> {
    match payload {
        Value::String(s) => Ok((s.clone(), DEFAULT_LIMIT)),
        Value::Object(obj) => {
            let query = obj.get("query").and_then(|v| v.as_str())
                .ok_or_else(|| ProviderError::Invalid("$search requires a string or { query: string, limit?: number }".into()))?
                .to_string();
            let limit = obj.get("limit").and_then(|v| v.as_u64()).map(|n| n as usize).unwrap_or(DEFAULT_LIMIT).max(1);
            Ok((query, limit))
        }
        _ => Err(ProviderError::Invalid("$search payload must be a string or { query, limit? }".into())),
    }
}
