# MarciDB Migrations

Status: **implemented** (snapshot-based engine). This document describes how migrations actually work:
the snapshot model, the engine (`snapshot.rs` + `migrate.rs`), the row format that makes the common
cases O(1), the two HTTP frontends, and the CLI.

## Core idea: the snapshot IS the migration unit

The source of truth is the **materialized snapshot** — the flat `Schema.models` array exactly as MarciDB
holds it in memory: `struct` already expanded into models (`Parent.field` with an injected `@parent_id`
key), `enum` already injected into the owning model (discriminant field + per-variant payload fields),
refs carrying their resolved binding. There is no `struct`/`enum` sugar and no nesting in a snapshot.

`schema.marci` (with sugar) stays the human-edited source. The engine **materializes** it
(`parse_schema`), and from there everything — diff, apply, the stored schema, migration files — operates
on the flat snapshot. Two consequences:

- Migrations never special-case `struct`/`enum`: in the flat form a struct is just a model and an enum is
  just fields. `diff` is a per-name comparison of two flat schemas.
- What you diff is exactly what runs, so applying a migration holds no surprises. A snapshot also makes it
  obvious where a bug lives: if the snapshot is wrong it's the parser; if apply is wrong it's the engine.

## Artifacts

```
schema.marci                       # human-edited source of truth (with struct/enum sugar)
migrations/
  meta/
    snapshot                       # the latest materialized snapshot — what `generate` diffs against
  0000_init.snapshot               # one full materialized snapshot per version
  0001_add_users.snapshot
  ...
```

A migration file is a **full materialized snapshot** of the schema at that version — not an incremental
op list. The engine recovers "what changed" by diffing consecutive snapshots, so there is no separate op
DSL and no replay/evolve step. Files are verbose (each is the whole schema) but fully reviewable: you see
the exact flat schema at every version.

Inside each database, the reserved tree `__marci_meta__` holds:

- `schema` — the current materialized snapshot (text). `MarciDB::open` reconstructs the in-memory schema
  from it via `parse_snapshot`, with no re-expansion of sugar and the same slots — so reopening is
  identical to the state the migration left behind.
- `version` — `u64` BE, bumped per applied migration.
- `applied` — ledger of applied migration ids (`\n`-joined), for the imperative frontend.

## Commands (`marci-migrate`)

| Command | What |
|---|---|
| `marci-migrate snapshot <schema.marci> [out]` | materialize a schema → flat snapshot (stdout or file) |
| `marci-migrate generate <schema.marci> [dir] [name]` | diff schema vs `meta/snapshot` → new `NNNN_name.snapshot` + update `meta/snapshot` |

The npm wrapper (`marcidb` CLI) drives both binaries: `marci-generate` for TS types, `marci-migrate` for
migrations. `marcidb generate` produces types + a migration file; `marcidb migrate push <db>` sends all
`.snapshot` files to a server.

## Row format (the foundational decision)

A row is `[ version:u8 | reserved:u8 | header_len:u16 | offset_table | payload ]`. Each Body field has a
**fixed byte slot** in the offset table (`offset_pos`); the header length is stored per row.

- Reading field at `offset_pos`: if `offset_pos + 4 > header_len` the field was added after this row was
  written → treated as **absent** (resolves to `@default` if present, else `null` — *default-on-read*, no
  backfill). Otherwise the offset gives the payload start; the end is the next non-zero slot.
- **Invariant:** Body fields are laid out in slot order. The writer writes them in array order and the
  reader derives a field's end from the next slot, so the entity's field array must keep Body fields in
  `offset_pos` order. `reconcile` enforces this via `canonicalize_field_order`.

What this buys (assuming the engine never reuses a retired slot):

| Migration | Cost |
|---|---|
| add nullable field | **O(1)** — old rows read it as absent/null |
| add field with `@default` | **O(1)** — default-on-read, no backfill |
| reorder fields in `schema.marci` | **O(1)** — declaration order is cosmetic; slots are carried |
| add index / unique | scan (backfill the index tree from existing rows) |

## Engine

Pipeline for a declarative migration (`migrate_to` / `$sync`):

```
new = parse_schema(new_text)          # materialize: expand sugar, assign provisional slots/ids by order
reconcile(&mut new, &old_snapshot)    # carry slots + enum variant ids from history; canonicalize order
ops = diff(&old_snapshot, &new)       # per-name comparison of two flat schemas
apply(tx, &old_snapshot, &new, &ops)  # physical: create/drop trees, build/drop indexes
store serialize_snapshot(&new)        # __marci_meta__/schema
```

- **`reconcile(new, old)`** moves everything order-dependent from the old snapshot into the freshly
  parsed schema, because the parser assigns these by declaration order (unstable):
  - **slots**: a field matched by name keeps its old `offset_pos`; a new field gets the next free slot
    (append-only, above the old high-water mark). This is why inserting a field mid-model no longer
    breaks old rows — slot ownership lives in the migration layer, not the parser.
  - **enum variant ids**: a variant matched by name keeps its old `u16` id; a new variant gets the next
    free id. Stored discriminants stay valid even if variants are reordered in `schema.marci`.
  - then `canonicalize_field_order` sorts each entity's fields (keys by index, Body by slot, then
    virtual) and remaps index references (`condition.field_index`, enum variant→field maps,
    `rev_field_idx`).
- **`diff(old, new)`** emits `MigrateOp` by comparing entities and fields by name. Type comparison is by
  name for refs/primitives; enums compare variant maps (adding variants is allowed, removing/renumbering
  is rejected).
- **`apply`** executes ops in one write transaction. add/alter field are metadata-only (the slot is
  already in `new`, old rows stay forward-compatible).

`parse_snapshot` reverses `serialize_snapshot`: it reads the flat text, wires names → indices, and
rebuilds the computed caches (default bytes, index trees, counters, reverse-dependencies) via
`rebuild_caches` — without re-expanding sugar or re-assigning slots/ids (those are pinned in the text).

## Op-set (`MigrateOp`)

| op | apply does | rewrite? | destructive |
|---|---|---|---|
| `CreateEntity` | create the entity tree + its index/relation trees | no (empty) | no |
| `DropEntity` | delete the entity tree + its index/relation trees | no | **yes** |
| `AddField` | nothing (slot is in `new`; default-on-read) | **no** | no |
| `AlterField` | nothing (nullable/default/format/added enum variants — metadata) | no | no |
| `DropField` | — **rejected** (slot tombstone not implemented yet) | — | **yes** |
| `AddIndex` / `DropIndex` | build the index tree from existing rows / delete it | scan / no | no |

**Rejected by `diff`** (need data transformation, not done): a field **type** change
(`UnsupportedTypeChange`), an `@id`/key change (`UnsupportedKeyChange`), a slot that moved without being
reconciled (`UnsupportedLayoutChange`), removing or renumbering an enum variant
(`UnsupportedEnumChange`).

## Frontends

Both wrap the same engine; do not mix them on one database (`$sync` ignores the ledger).

- **`POST /:db/$sync`** (declarative) — body is `schema.marci` text. The server materializes, reconciles
  against its stored snapshot, diffs and applies. For databases not managed by migration files (CI,
  direct HTTP). `MarciDB::migrate_to`.
- **`POST /:db/$migrate`** (imperative, ledger) — body is `[{ id, ops }]` where `ops` is the version's
  **snapshot** (not a DSL). The server applies only ids after those in its ledger, diffing each snapshot
  against the running one in a single transaction. Applied ids must be a prefix of the incoming list,
  else `HistoryDiverged` (400). Idempotent. `MarciDB::apply_migrations`. Chosen for prod reproducibility:
  apply exactly what was reviewed and committed.

## Deferred

- `drop field` (needs slot tombstones so retired slots are never reused).
- `changeFieldType` with data transforms; re-keying; `vacuum` to reclaim tombstoned slots.
- Interactive rename detection (a removed+added pair is currently two ops).
