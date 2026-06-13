# MarciDB Migrations — design (proposal, not yet implemented)

Status: **design**. This document specifies the op-set, the migration-file format, the row-format
change that makes the common cases cheap, and what `apply` does per operation. It is the
implementation guide for the migration engine.

## Goals

- `schema.marci` is the source of truth (declarative). You edit the schema; the tool computes the diff.
- One **engine** — `(old_schema, ops) → apply` — shared by the embeddable library (direct call) and the
  server (HTTP push). Transport differs, engine is the same.
- Most migrations are **O(1) metadata** (see the row-format change below); only genuinely heavy changes
  (type transforms, re-keying) cost a data rewrite, and those are **rejected in v1** rather than half-done.
- Never silently lose data: destructive ops are gated; data conflicts (unique, failed transforms) are
  reported up front, before anything is written.

## Artifacts

```
schema.marci                      # source of truth (dev-side)
migrations/
  meta/
    snapshot.json                 # schema as of the last migration — used to diff offline at `generate`
    journal.json                  # ordered [{ id, name, schemaHash }] — the migration chain
  0000_init.mig
  0001_add_user_bio.mig
  ...
```

Inside every database, a reserved tree `__marci_meta__` holds:

```
{ format_version, schema_snapshot, applied: [migration ids], schema_version: <last applied id/hash> }
```

`schema_snapshot` is the **materialized current schema** — the running server reconstructs its in-memory
layout from here (it has no `schema.marci`). `format_version` is the version of *our* binary row format,
independent of the user schema (lets us upgrade the format itself later).

## Commands

| Command | When | What |
|---|---|---|
| `marcidb generate [name]` | dev, offline | diff `meta/snapshot.json` ↔ `schema.marci` → new migration file + update snapshot/journal |
| `marcidb migrate [--dry-run]` | embedded / local | apply migrations after the DB's `schema_version`; `--dry-run` prints the plan |
| `marcidb migrate push <db>` | server | send pending migration files to a running server; it applies them to the named DB |
| `marcidb push` | dev only | diff `schema.marci` ↔ the DB's `schema_snapshot` and apply directly, no files (prototyping) |

Rename detection (interactive "is `a` renamed to `b`?") is **v2** — see Open decisions.

## Row-format change (the foundational decision)

The current row is `[ offset table | payload ]` where the offset table has a fixed number of 4-byte slots
(`payload_offset`, a per-schema constant). Adding a field grows the table → every existing row has the
wrong layout → **full rewrite**. That makes the most common migration (add a field) O(rows).

**Proposal — logical append-only slots + a per-row header length + default-on-read:**

```
v1 (now):   [ off0 off1 off2 ]                  [ payload ]
            payload_offset = 12 (3 slots), constant per schema

v2 (new):   [ nslots:u16 ][ off0 off1 off2 off3 ] [ payload ]
            header length is read from the row, not the schema
```

- Each body field maps to a **fixed logical slot index**, stored in the schema and assigned
  **append-only** (a new field always gets `max_slot + 1`; declaration order in `schema.marci` is
  cosmetic). Dropped slots are tombstoned, never reused.
- Reading field at slot `i`: if `i < nslots` read `off_i` (0 = null); if `i >= nslots` the field was added
  after this row was written → treat as **absent**. Absent resolves to the field's `@default` if it has
  one, otherwise `null` (**default-on-read** — the default lives in the schema, no backfill needed).

What this buys:

| Migration | Cost with v2 format |
|---|---|
| add nullable field | **O(1)** — old rows read it as null, new rows write the extra slot |
| add field with `@default` | **O(1)** — default-on-read; no backfill |
| drop field | **O(1)** — tombstone the slot; old bytes stay unread, new rows write 0 there |
| reorder fields in schema | **O(1)** — declaration order is cosmetic, slots are stable |

So with the v2 format, **v1 `apply` does almost no row rewriting** — it's mostly metadata updates plus
index/relation scans. The scary byte-rewrite path shrinks to v2-only changes (type transforms, re-key,
optional `vacuum` to reclaim tombstoned slots). This is why the format decision must be made first.

There is no production data yet, so v2 is adopted as **the** format directly — no v1→v2 upgrader is
written. The `format_version` field stays in `__marci_meta__` so a *future* format change can ship an
upgrader without a flag day.

## Op-set

Each op is a JSON object `{ "op": "...", ... }`. "Rewrite?" assumes the v2 format above.

"Rewrite?" assumes the v2 row format above.

| line | apply does | rewrite? | destructive |
|---|---|---|---|
| `create model M { … }` | create model tree (+ index/relation trees), assign slots | no (empty) | no |
| `drop model M` | delete model tree + its index/relation trees | no | **yes** |
| `rename model A -> B` | `rename_tree` for the model + dependent trees; update refs | no | no |
| `add field M.f <spec>` | assign next slot; update schema | **no** | no |
| `alter field M.f <spec>` | update default / format / nullable; `false` nullable → check no nulls | no | narrowing only |
| `drop field M.f` | tombstone the slot in schema | **no** | **yes** |
| `rename field M.f -> g` | update name → slot mapping (layout unchanged) | no | no |
| `add index M.f` / `add unique M.f` | scan rows, encode keys, fill index tree; `unique` → dup check | scan | no |
| `drop index M.f` / `drop unique M.f` | delete the index tree | no | no |
| `add relation M.f -> T.r` | create relation index tree(s), backfill from existing refs | scan | no |
| `drop relation M.f` | delete the relation index tree(s) | no | no |
| `add variant E.v` | update the discriminant map in schema | no | no |
| `drop variant E.v` | update schema; rows of that variant are gated | no | **yes** |

**Rejected by `generate` in v1** (clear "edit manually" message): a field **type** change (needs a
transform), `@id` / key changes (re-key), field-slot `vacuum`.

## Migration file format

Migration files are **line-based and human-readable** — one action per line, reusing the `.marci` schema
vocabulary (so `add field User.bio String?` is the same field syntax you already write in the schema). A
new model is a schema-style block; every incremental change is a single line.

```
# 0001_add_user_bio   (from 0000)

add field User.bio String?
add index Post.views
add unique User.email
```

Baseline `0000_init.mig` is just the schema as `create model` blocks:

```
# 0000_init

create model User {
  id     Byte[16]  @id @format(uuid)
  name   String
  email  String?   @unique
  posts  Post[]    @bind(Post.author)
}
create model Post {
  title  String
  views  UInt      @index
  author User?
}
```

Chain metadata (`from`, resulting `schemaHash`, order) lives in machine-managed `meta/journal.json`, not in
the file — the file stays purely the list of actions (the `#` header line is cosmetic). The resulting
schema is recomputed by replaying the actions; `apply` verifies its hash against the journal, and refuses
to apply unless the DB is at `from`.

### Grammar

```
# models
create model <Model> { <schema-style field block> }
drop   model <Model>
rename model <Old> -> <New>

# fields   (<spec> = type + nullable + @default/@format, exactly as in schema.marci)
add    field <Model>.<name> <spec>
alter  field <Model>.<name> <spec>      # default / format / nullable; a type change is rejected in v1
drop   field <Model>.<name>
rename field <Model>.<old> -> <new>

# indexes
add  index  <Model>.<field>
add  unique <Model>.<field>
drop index  <Model>.<field>
drop unique <Model>.<field>

# relations  (mirrors @bind)
add  relation <Model>.<field> -> <Target>.<reverseField>
drop relation <Model>.<field>

# enum variants
add  variant <Enum>.<variant>
drop variant <Enum>.<variant>
```

The format carries **semantic actions, not byte offsets** — `apply` computes the byte mechanics at run
time from the DB's stored old schema + the action. Files stay readable and survive format-version changes.

## `apply` algorithm

```
apply(db, migration):
  assert db.schema_version == migration.from           # chain guard, else error
  old = db.schema_snapshot
  new = replay(old, migration.ops)                      # pure: ops → new schema
  assert hash(new) == migration.schemaHash
  in one write transaction (build-then-swap):
    for op in migration.ops:
      run op against trees (per the op-set table)       # metadata, tree create/drop/rename, index scans
      on conflict (unique dup, narrowing with nulls, …) → abort tx, return the offending rows
    write __marci_meta__ { schema_snapshot = new, schema_version = migration.id, applied += id }
    commit                                              # version bumps only here
```

- **Atomic**: a crash or conflict mid-migration rolls the whole thing back; the DB stays on `from`.
- **Online (server)**: the write lock is exclusive, so writes to that DB stall for the migration's
  duration (reads continue on the old snapshot). With the v2 format most ops are O(1)/metadata, so the
  stall is short; index builds are O(rows). In-flight reads keep the *old* in-memory schema until they
  finish; new transactions pick up `new` after the swap. v1 keeps the swap simple: briefly quiesce new
  transactions, drain in-flight, swap, resume.
- For very large index builds we can later chunk + checkpoint; v1 does it in one transaction.

## `generate` algorithm

```
generate(name):
  old = read migrations/meta/snapshot.json   # or empty for 0000_init
  new = parse(schema.marci)
  ops = diff(old, new)                        # see below
  if ops contains a v2-only change: error with "unsupported in v1, edit manually"
  write migrations/NNNN_name.mig                      # the action lines
  record { id, name, from, schemaHash: hash(new) } in meta/journal.json
  update meta/snapshot.json = new
```

`diff(old, new)` per element:
- model in new not in old → `createModel`; in old not in new → `dropModel` (destructive, gated).
- field added / removed / default / format / nullable change → the matching op.
- index/relation/enum-variant added or removed → the matching op.
- v1 has **no rename detection**: a removed+added pair is emitted as drop+add (destructive). v2 adds the
  interactive "renamed `a`→`b`?" prompt that rewrites the pair into a `renameField`/`renameModel`.

Destructive ops require `--allow-destructive` (CLI) / `OpenOptions { allow_destructive }` (embedded).

## v1 scope

- **In:** the v2 row format; `generate` (diff → file) without rename detection; `apply` for all ops in the
  op-set table above; `migrate` (local) and `push` (dev); chain guard; destructive gate; conflict
  reporting; one-time format v1→v2 upgrade.
- **Deferred (v2+):** interactive rename detection; `changeFieldType` with transforms; re-keying;
  `vacuum`; server `migrate push` + multi-DB + auth (separate server-layer milestone — the engine is
  ready to be wrapped by it).

## Open decisions

1. **Row format v2 (logical slots + header length + default-on-read)** — recommended; it is what makes v1
   `apply` tractable. Must be decided before freezing v1.
2. Migration files as a line-based DSL (one action per line) — proposed above.
3. `migrations/` layout & file naming (`NNNN_name.json`) — proposed; bikeshed welcome.
4. Rename detection deferred to v2 — confirmed direction.
