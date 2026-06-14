//! Authoring layer for MarciDB: computing migrations between two materialized schemas — `diff` (the ops),
//! `reconcile` (carry slots + enum ids from history), and `serialize_migration` (render `.march` actions).
//! This is the "smart" build-time tooling kept OUT of the runtime engine (`marcidb`), which only applies
//! pre-computed actions. Used by `marcidb-migrate` (CLI) and `marcidb-server` (`$sync`).
//!
//! Note: the `.marci` DSL parser (`parse_schema`) stays in `marcidb` itself — the engine's own unit tests
//! depend on it, and a dev-dependency cycle can't share types across the `--test` boundary.

mod diff;

pub use crate::diff::{diff, reconcile, serialize_migration};
