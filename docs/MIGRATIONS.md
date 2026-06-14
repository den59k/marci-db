# MarciDB Migrations

Status: **implemented** (snapshot-based engine). This document describes how migrations actually work:
the snapshot model, the engine (`snapshot.rs` + `migrate.rs`), the row format that makes the common
cases O(1), the two HTTP frontends, and the CLI.

## The materialized snapshot

The state representation is the **materialized snapshot** — the flat `Schema.models` array exactly as
MarciDB holds it in memory: `struct` already expanded into models (`Parent.field` with an injected
`@parent_id` key), `enum` already injected into the owning model (discriminant field + per-variant payload
fields), refs carrying their resolved binding. No `struct`/`enum` sugar, no nesting.

`schema.marci` (with sugar) stays the human-edited source. The engine **materializes** it (`parse_schema`),
and from there everything — diff, apply, the stored schema, migration files — operates on the flat
snapshot. Migrations never special-case `struct`/`enum`: in the flat form a struct is just a model and an
enum is just fields, so `diff` is a per-name comparison of two flat schemas.

## A migration file = self-contained actions

A migration file (`.march`) is a list of **self-contained actions, one per line** — what changes, each
carrying its own field definition (the snapshot line). There is **no snapshot section**: you review the
*changes* here, and look at the full schema in `schema.marci`.

```
# 0001_add_age
add field User.age UInt @slot(12)
add index User.email
```

`create entity X` is a bare line; the entity's fields follow as `add field` actions — the same definition
surface used everywhere — so a baseline / first migration reads:

```
# 0000_init
create entity User
add field User.id UInt @id @default(autoincrement())
add field User.name String @slot(4)
add field User.email String @slot(8) @unique
add unique User.email
```

Because each action carries its definition, the server can apply a migration without seeing the whole
schema — it just lays the actions onto its current state. An accidental `drop field` is visible in review.

## Artifacts

```
schema.marci                       # human-edited source of truth (with struct/enum sugar)
migrations/
  0000_init.march                  # self-contained actions, one file per version
  0001_add_age.march
  ...
```

`generate` replays the existing `.march` files from empty (via `evolve`) to recover the previous state,
then diffs `schema.marci` against it — no separate `meta/` snapshot pointer. Inside each database, the
reserved tree `__marci_meta__` holds:

- `schema` — the current materialized snapshot (text). `MarciDB::open` reconstructs the in-memory schema
  from it via `parse_snapshot`, with no re-expansion of sugar and the same slots — so reopening is
  identical to the state the migration left behind.
- `version` — `u64` BE, bumped per applied migration. (No applied-id ledger — see *Frontends*.)

## Commands (`marci-migrate`)

| Command | What |
|---|---|
| `marci-migrate snapshot <schema.marci> [out]` | materialize a schema → flat snapshot (stdout or file) |
| `marci-migrate generate <schema.marci> [dir] [name]` | diff schema vs replayed history → new `NNNN_name.march` (self-contained actions) |
| `marci-migrate plan [dir]` | read the server's snapshot on STDIN → print the actions not yet applied |

The npm wrapper (`marcidb` CLI) drives both binaries: `marci-generate` for TS types, `marci-migrate` for
migrations. `marcidb generate` produces types + a migration file; `marcidb migrate push <db>` plans the
pending actions and applies them; `marcidb migrate check <db>` reports whether the server is up to date.

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

For `$migrate` (the file-based frontend) the server is **dumb**: it `evolve`s its current snapshot with the
actions it was sent (each action carries its definition), parses the result, and applies — no diff, no
ledger, no deciding what to skip. Choosing *which* actions to send is the client's job (`marci-migrate
plan`, see *Frontends*). `$sync` has no file, so it diffs as shown above.

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
(`UnsupportedEnumChange`), and a change to a relation's **storage binding**
(`UnsupportedBindingChange`) — e.g. adding `@bind` to a composite-key relation flips it from `index_tree`
to `current_id`, which moves where the relation lives. These surface as an explicit error rather than a
silent no-op; apply the schema to a fresh database via `$sync` instead.

## Frontends

Both wrap the same engine. The split is **smart `$sync` vs dumb `$migrate`**: the server thinks for
`$sync`, the client thinks for `$migrate`.

- **`POST /:db/$sync`** (declarative, smart) — body is `schema.marci` text. The server materializes,
  reconciles against its stored snapshot, diffs and applies. For databases not managed by migration files
  (CI, direct HTTP). `MarciDB::migrate_to`.
- **`POST /:db/$migrate`** (imperative, dumb) — body is the text of migration actions. The server lays them
  onto its current state and applies — **no ledger**, no deciding what to skip. `MarciDB::apply_migration`.
  Coordination lives in the client:
  - **`GET /:db/$snapshot`** returns the server's current materialized snapshot.
  - **`marci-migrate plan`** replays the local `.march` history from empty until a step matches the
    server's snapshot, then emits the unapplied tail. `marcidb migrate push` sends that tail to `$migrate`;
    `marcidb migrate check` reports up-to-date / behind / drift. This makes push idempotent and ordered
    *client-side* — the server stays a pure executor.

## Deferred

- `rename` of fields/entities — self-contained actions make this expressible (an explicit `rename` action
  carrying the slot, so data is preserved); detection in `generate` is the next step.
- **Squash / baseline** ("migration = snapshot") — collapsing history into one baseline migration is
  already expressible (a baseline is just `create entity` + `add field` actions); a `generate --squash` is
  the next step.
- `drop field` (needs slot tombstones so retired slots are never reused).
- `changeFieldType` with data transforms; re-keying; `vacuum` to reclaim tombstoned slots.
