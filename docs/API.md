# MarciDB TypeScript API

Schema definition and the typed client (`marcidb-client`). For the raw HTTP protocol see [HTTP-API.md](HTTP-API.md).

## Schema (`schema.marci`)

```
model User {
    id          Byte[16]      @id   @format(uuid)
    name        String
    email       String?       @unique
    age         UInt          @index
    createdAt   DateTime      @default(now())
    info        UserInfo?
    posts       Post[]        @bind(Post.author)
}

struct UserInfo {
    bio         String
}

model Post {
    title       String
    author      User?
}

enum Role {
    viewer
    admin {
        level   Int
    }
    admin | moderator {
        sign    String
    }
}
```

### Types

| Schema type | TS type | Notes |
|---|---|---|
| `String` | `string` | |
| `Int` / `i64` | `number` | 64-bit signed |
| `UInt` / `u64` | `number` | 64-bit unsigned |
| `Float` / `f32`, `Double` / `f64` | `number` | |
| `Bool` / `Boolean` | `boolean` | |
| `Byte` / `u8` | `number` | |
| `DateTime` | `Date \| number` | stored as epoch millis |
| `Json` | `JsonValue` | any JSON value, stored as a compact binary blob; filter by path — see [JSON fields](#json-fields) |
| `T[]`, `T[n]` | `T[]` | primitive lists, fixed size optional |
| `Byte[16] @format(uuid)` | `string` | uuid in JSON, 16 bytes in storage |
| `Byte[] @format(hex)` | `string` | hex in JSON |

If a model has no `@id` field, an autoincrement `id UInt` is added implicitly.

### Attributes

- `@id` — primary key field (composite keys: several `@id` fields)
- `@unique` — unique secondary index on a scalar; on a relation field it is the one-to-one constraint
- `@index` — secondary index on a scalar/enum/list (used by `$where`, `$order` and aggregations). **Not** valid on a relation field — index a relation through its reverse collection (`@bind`) instead
- `@default(value)` — also `autoincrement()`, `now()`
- `@bind(Model.field)` — declares the reverse side of a relation
- `@list` — on a relation list (`Model[]`): stores the related ids **inline in the row as an ordered array** instead of a virtual index-tree relation — see [Ordered relation lists](#ordered-relation-lists-list)
- `@format(uuid | hex)` — JSON representation of byte fields
- `@onDelete(...)` — delete constraint for relations (`Cascade`, `SetNull`, `Restrict`)
- `@vector(cosine | euclidean)` / `@fulltext(multi | english | russian)` — module-provided indexes: nearest-neighbour on a `Float[N]` field, ranked text search on a `String` field. Any attribute that matches no built-in is parsed as a module index named after the keyword (`@<provider>(args)`); `@custom(<provider>, args)` is the explicit equivalent. Queried with `$near` / `$search`; full-text is maintained live on writes, vector is built by `reindex()` (also used to backfill) — see [Vector & full-text search](#vector--full-text-search)

### Structs, relations, enums

- `struct` — nested entity stored under the parent's key prefix; inserted/updated inline with the parent.
- `Model` / `Model[]` fields — relations by id; lists require a `@bind` on the opposite side.
- **Composite-key relations** — a join table (`model ChatUser { chat Chat @id  user User @id }`) or a child keyed by `parent + autoincrement` (`model Message { chat Chat @id  id UInt @id @default(autoincrement()) }`) is fully supported, including `@onDelete(Cascade)`: deleting the parent removes the owned children (and their children), while merely-referenced rows are left untouched. The composite-key ref must carry `@onDelete(Cascade)` (a key cannot be set null).
- `enum` variants may carry **payload fields** which are injected into the model itself. In TS this is a discriminated union: `{ role: "viewer" } | { role: "admin", level: number, sign: string } | { role: "moderator", sign: string }`. Switching the variant on update requires the full payload of the new variant and clears fields of the old one.
- Every enum line is `name1 | name2 [{ fields }]`: a variant is declared on first mention, and a block attaches its fields to all listed variants. `admin | moderator { sign String }` makes `sign` a single physical field shared by both variants — switching between them keeps it (the required payload overwrites it anyway), while switching outside the group clears it. Blocks mentioning the same variant merge; declaring the same field name twice is a schema error.

### Ordered relation lists (`@list`)

```
model Gallery {
    name      String
    images    Image[]    @list
}

model Image {
    url        String
    galleries  Gallery[]  @bind(Gallery.images)   // optional back-reference (read-only)
}
```

A plain many-to-many keeps its links in index trees, so related rows always come back ordered by id — and each pair can exist only once. `@list` stores the related **ids inline in the row body as an ordered array** — the insert/`$set` order is the result order, and the array is a *sequence*, so the same item may appear several times. That makes it the right tool wherever the user arranges the collection by hand: image galleries, playlists, kanban columns, pinned items.

```ts
await db.gallery.create({ name: "Trip", images: [img3, img1, img2, img3] })  // order is meaningful, repeats allowed

await db.gallery.update(id, { images: { $set: [img1, img3] } })       // replace — also the reorder op
await db.gallery.update(id, { images: { $connect: img4 } })           // append at the end
await db.gallery.update(id, { images: { $connectUnique: img4 } })     // append only if absent
await db.gallery.update(id, { images: { $remove: img3 } })            // remove every occurrence
```

Everything else reads the same as a tree-backed relation: nested selects (`images: { url: true }`, in array order), `$some`/`$every`/`$none` filters, and `$count` (which is O(1) — the stored array length). Deletes stay consistent in both directions: deleting an `Image` splices it out of every gallery, deleting a `Gallery` cleans up its links.

Rules and trade-offs:

- The target model must have a **fixed-size id** (autoincrement `UInt`, `Byte[16] @format(uuid)`, composite fixed keys) — not `String @id`.
- **Duplicates are first-class**: the same id may appear several times (something a tree-backed relation physically cannot represent). `$count` counts occurrences; `$connect` always appends (it is *not* idempotent — `$connectUnique` is the set-flavored variant that appends only absent ids); `$remove` severs the relation — every occurrence goes. For positional edits (remove one of several occurrences, insert in the middle) use `$set` with the exact new array.
- The back-reference (`Image.galleries`) is optional and **read-only**: it answers "which galleries contain this image" (backed by a reverse index tree, ordered by owner id — each owner listed once, however many times the item repeats in it), but `$connect`/`$remove`/insert on it are rejected — membership is changed through the `@list` side. Without a declared back-reference the reverse tree is still maintained internally for delete integrity.
- The array lives in the row: every membership change rewrites the row, and every row read carries the array. Use `@list` for hand-ordered collections of up to roughly a thousand members; for unbounded sets (chat members, followers) keep the plain index-tree relation.
- `@list` cannot target a `struct` (owned children have no standalone ids), and converting an existing relation between `@list` and the tree-backed form is rejected by migrations — add a new field and backfill instead.

## Queries

### findMany / findFirst

```ts
const users = await db.user.findMany({
  name: true,                        // field selection
  posts: { title: true },            // nested relation select
  $where: { age: { $gte: 18 } },
  $order: { age: "desc" },
  $limit: 20,
  $skip: 0,
  $cursor: { id: lastSeenId },       // keyset pagination
})
```

The result type is inferred from the select shape. Keys that are not selected do not exist in the result.

### `$where` operators

| Operator | Types | Example |
|---|---|---|
| value / `$eq`, `$not` | all | `{ name: "Alice" }`, `{ age: { $not: 30 } }` |
| `null` / `{ $not: null }` | nullable | `{ email: null }` |
| `$in`, `$notIn` | all | `{ age: { $in: [20, 30] } }` |
| `$gt`, `$gte`, `$lt`, `$lte` | numbers, DateTime | `{ age: { $gt: 18 } }` |
| `$startsWith`, `$includes` | strings | `{ name: { $startsWith: "Al" } }` |
| `$some`, `$every`, `$none` | list relations | `{ posts: { $some: { title: "x" } } }` |
| nested where | single relations | `{ author: { name: "Alice" } }` |
| path filters | `Json` fields | `{ meta: { "address.city": "Tokyo" } }` — see [JSON fields](#json-fields) |

**Operators on one field are ANDed**, so a two-sided range is just both bounds together:

```ts
$where: { age: { $gte: 18, $lt: 65 } }        // 18 ≤ age < 65
$where: { age: { $gte: 18, $ne: 30 } }        // ≥ 18, but not exactly 30
$where: { startsAt: { $gte: from, $lt: to } } // DateTime works the same way
```

On an indexed numeric field the planner fuses a lower and an upper bound into a **single bounded index scan** instead of scanning from one bound to the end of the index. The equivalent `$and` spelling is fused the same way, so `{ age: { $gte: 18, $lt: 65 } }` and `$and: [{ age: { $gte: 18 } }, { age: { $lt: 65 } }]` plan identically — the compact form is just shorter.

### Combining conditions — `$and` / `$or` / `$not`

Fields listed side by side are ANDed. For anything else, wrap the conditions in a combinator:

```ts
$where: { $and: [{ capacity: { $gte: 2 } }, { title: "spin" } ] }
$where: { $or:  [{ capacity: 1 }, { capacity: 2 }] }
$where: { $not: { capacity: 1 } }
```

**Every condition goes *inside* the array.** A combinator is resolved as soon as it is found, so a field sitting next to one is not merged — it is dropped:

```ts
$where: { $and: [{ capacity: { $gte: 2 } }], capacity: 999 }   // ✗ `capacity: 999` is ignored
$where: { $and: [{ capacity: { $gte: 2 } }, { capacity: 999 }] } // ✓
```

The generated types reject this, so it doesn't compile. Combinators nest freely and work inside a nested relation filter (`{ trainer: { $or: [...] } }`) as well as at the top level.

The planner picks the most selective indexed condition (exact id → unique eq → eq → startsWith → two-sided range → half-open range); all other conditions are re-checked per row, so the index choice never affects correctness.

### JSON fields

A `Json` field stores **any** JSON value (object, array, or scalar) in a compact binary format — use it for schemaless or variable-shape data without declaring a sub-schema.

```
model Event {
    name    String
    payload Json
    meta    Json?
}
```

Insert and update take the value directly; reads return it as-is (typed `JsonValue`):

```ts
await db.event.insert({ name: "signup", payload: { plan: "pro", seats: 5, tags: ["beta"] } })
```

**Filter by path.** Under a `Json` field, keys are dot-paths into the document and a bare value is shorthand for `$eq`:

```ts
await db.event.findMany({
  name: true,
  $where: {
    payload: {
      "plan": "pro",                 // = $eq
      "seats": { $gt: 3 },
      "tags": { $contains: "beta" },
      "coupon": { $exists: false },
    },
  },
})
```

Multiple paths under one field are ANDed, as are multiple operators on one path (`{ "seats": { $gte: 2, $lt: 8 } }`); combine fields with `$or` / `$and` / `$not` as usual. A numeric path segment indexes an array (`"items.0.id"`).

| Operator | Meaning |
|---|---|
| value / `$eq`, `$ne` (`$not`) | leaf equals / differs from a JSON value (a plain object compares the whole subtree) |
| `$gt`, `$gte`, `$lt`, `$lte` | numeric, or lexicographic between two strings |
| `$in`, `$notIn` | leaf is (not) one of a set |
| `$startsWith`, `$includes` | string prefix / substring |
| `$contains` | leaf is an array containing the value |
| `$exists` | `true` / `false` — whether the path resolves |
| `$type` | `"string"` \| `"number"` \| `"boolean"` \| `"object"` \| `"array"` \| `"null"` |

Semantics for schemaless data: a **missing path or a type mismatch is simply "no match"** (e.g. `$gt` against a string leaf), except `$ne` / `$notIn`, which also match a missing path. Numbers compare by value (`5` equals `5.0`).

To compare against the **whole** value instead of a path, put a `$`-operator (or a bare value) directly on the field: `{ payload: { $eq: { plan: "pro" } } }`.

> JSON path filters run as a residual scan (no index in this version), though each row reads only the bytes along the path. Keys beginning with `$` are interpreted as operators, so they can't be addressed as paths.

### `$order`, `$limit`, `$skip`, `$cursor`

- `$order: { field: "asc" | "desc" }` — single field. Indexed non-nullable fields are served by an index scan; everything else is sorted in memory. Nulls go last for `asc`, first for `desc`.
- `$limit` / `$skip` — applied after filtering. `$skip` is O(n); prefer `$cursor` for pagination.
- `$cursor: { id }` — **exclusive** keyset cursor: results strictly after the row with this id, in the current `$order`. Without `$order` the order is fixed to primary-key order. Take the id from the last row of the previous page. If the cursor row was deleted: id-ordered queries continue seamlessly; value-ordered queries return an empty page.

All of the above also work inside nested selects: `posts: { title: true, $order: { id: "desc" }, $limit: 5 }`.

### Vector & full-text search

A field with a module index (`@vector` / `@fulltext`, or the generic `@custom`) is searched with `$near` (alias `$search`) inside `$where`. **Full-text is maintained live** — inserts/updates/deletes keep it current, so a `$search` reflects writes immediately. **Vector is `reindex()`-only** (its clustering can't be updated per-point). Either way, call `reindex()` once to backfill rows that predate the index, after a bulk import that bypasses the API, or after a language/args change.

**Vector** — `embedding Float[1536] @vector(cosine)` (or `euclidean`):

```ts
await db.doc.findMany({
  title: true,
  $where: { embedding: { $near: { vector: queryEmbedding, k: 10, threshold: 0 } } },
})
```

`vector` (length = the field's `N`) is required; `k` (default 10) caps the neighbours; `threshold` (default 0, off) is a relative distance-gap cutoff. Cosine L2-normalizes vectors at index and query time.

**Full-text** — `body String @fulltext`. The default `multi` analyzer stems each token by script (Cyrillic → Russian, otherwise English), so one field handles mixed text; force a single language with `@fulltext(english)` / `@fulltext(russian)`:

```ts
await db.doc.findMany({
  title: true,
  $where: { body: { $search: "быстрый поиск" } },        // or { query: "...", limit: 20 }
})
```

`$search` takes a query string (or `{ query, limit }`). It's OR over terms, ranked by tf·idf.

Results come back **ranked best-first**; any other `$where` conditions are applied as a post-filter over the candidate set. In v1 a `$near`/`$search` runs standalone — it can't be combined with `$order` (results stay in rank order), and it is rejected inside `$or` / `$not`.

```ts
await db.doc.reindex()   // rebuild this model's @custom indexes → { ok: true, indexed: <count> }
```

> The vector and full-text modules are compiled into the server behind cargo features (`--features vector`, `--features fulltext`); the vector module needs a nightly toolchain. A database with a module index still **opens** and serves normal CRUD even if the module isn't enabled — but **migrating** a schema that uses it (and `reindex()` / `$near`) requires the module, so a `$sync`/`$migrate` with an unregistered (or mistyped) `@<provider>` is rejected with a clear error.

## Mutations

```ts
const { id } = await db.user.insert({
  name: "Alice",
  info: { bio: "hi" },              // struct — created together with the parent
  posts: [{ id: 1 }],               // relation — connect by id
})

await db.user.update({ id }, {
  name: "Alicia",
  age: { $increment: 1 },
  tags: { $push: "vip" },                // primitive arrays take in-place operators too
  info: { $update: { bio: "hello" } },   // struct — content ops: $update | $ensure | $set | null
  posts: { $connect: { id: 2 }, $remove: { id: 1 } },  // model relation — link ops
})

await db.user.delete({ id })
```

**Key fields cannot be updated** — `@id` fields (and each part of a composite key) are absent from the update type. Write the new row and delete the old one if you need to move a record's identity.

A relation-list field takes **one object**, not a list of them: `{ $connect: …, $remove: … }` applies every operator it contains in a single update. A primitive-array field takes one operator per update (or a bare array, which replaces the value).

### Relation & array operators

Every operator belongs to one of two families, and a field's kind decides which family it speaks. **Content ops** treat the elements as data the parent owns: primitive values, or `struct` children that live and die with the parent. **Link ops** treat the elements as references to independent rows: they change the relation, never the rows themselves. An operator from the wrong family is rejected — by the TypeScript types and by the engine.

| operator | primitive array (`String[]`) | struct list (owned) | model list (m2m tree) | model list (`@list`, ordered) |
|---|---|---|---|---|
| `$set: [...]` | replace the array | replace the children (delete + create) | replace link **membership** (diff — rows untouched) | replace / reorder the id array |
| `$push` | append value(s), duplicates kept | create child(ren) | — | — (use `$connect`) |
| `$pushUnique` | append only absent values | — | — | — |
| `$connect` | — | — | link (idempotent) | append an occurrence |
| `$connectUnique` | — | — | — (plain `$connect` is already idempotent) | append only if absent |
| `$remove` | remove a value, every occurrence | **delete** children by id | unlink | unlink, every occurrence |
| `$update` | — (positional edit → `$set`) | edit one child: `{ ...childId, data }` | — (update via its own collection) | — |

The same word can appear in both families — ownership decides the consequence, exactly as `field: null` on a to-one relation already does (struct → the child is deleted, model → disconnected). For a `struct` there is no second meaning to confuse: an owned child cannot exist outside its parent, so "remove from the collection" can only mean delete.

```ts
// Owned (struct) list — children are addressed by their own id fields,
// the same shape query results return them in:
const board = await db.board.findFirst({ tasks: { id: true, title: true } })
await db.board.update(id, {
  tasks: {
    $update: { id: board.tasks[0].id, data: { title: "Renamed" } },  // single or an array of these
    $push: { title: "New task" },
    $remove: { id: board.tasks[1].id },                              // deletes the child
  }
})

// Model m2m — $set replaces which rows are linked, never the rows themselves:
await db.post.update(id, { viewers: { $set: [userA, userB] } })

// Primitive array — same sequence semantics as @list:
await db.post.update(id, { tags: { $pushUnique: "featured" } })
await db.post.update(id, { tags: { $remove: "draft" } })
```

Deliberately **not** supported:

- **Positional ops** (`$insertAt` / `$removeAt`): an index is only meaningful against the array version the client last read — racy by construction. Positional edits send the full array via `$set`; these are hand-ordered collections, the client has it.
- **Nested creation of independent models** (`$push`/`$ensure` on a non-struct relation): an independent row is created through its own collection. To create-then-connect atomically, use a [transaction](#transactions) with `ref("0.id")`. This keeps the line crisp: `struct` = created through the parent, model = referenced by id.
- **Fixed-size lists** (`Byte[8]`): length cannot change, so no in-place operators — assign a whole new value.

A missing child id in a struct-list `$update` is an error and rolls the update back; `$remove` of an absent child is a no-op (the requested end state already holds). Children are addressed relative to their parent, so another parent's children are unreachable by construction.

A `Byte` field with `@format(uuid | hex)` accepts either the formatted string or the raw byte array anywhere it is written — insert, `$where`, or update. Reads always return the formatted string.

## Aggregations

```ts
await db.user.count({ $where: { age: { $gte: 18 } } })   // → number

await db.user.aggregate({
  $where: { email: { $not: null } },
  $count: true,
  $sum: "age",      // numeric fields only
  $avg: "age",
  $min: "name",     // any primitive field
  $max: "age",
})
// → { count, sum, avg, min, max } — only the requested keys
```

Empty set: `count: 0`, the rest are `null` (SQL semantics). Null values are excluded from `$sum`/`$avg`.

### Aggregates over relations

Aggregate keys inside a relation select replace the array with an aggregate object:

```ts
await db.user.findMany({
  name: true,
  posts: { $count: true, $max: "views", $where: { published: true } },
})
// → { name, posts: { count: number, max: number | null } }
```

`$count` without `$where` is served from index keys without reading the related rows.

### Fast paths

- `count()` without filter → tree size, O(1)
- `count` with a single indexed condition → index range key count, no row reads
- `count` with `{ field: null }` / `{ $not: null }` on an indexed field → difference of tree sizes, O(1)
- `$min`/`$max` on indexed non-nullable fields without filter → first/last index key, O(log n)

## Transactions

`db.$transaction([...])` runs a list of operations in a single atomic transaction — all commit or none do. The results come back as a tuple, typed per operation:

```ts
import { marcidb, ref } from "marcidb-client"

const [user, post] = await db.$transaction([
  db.user.insert({ name: "Alice", email: "a@example.com" }),
  db.post.insert({ title: "Hi", author: { id: ref("0.id") } }),
])
// user: { id: ... }, post: { id: ... }
```

Methods are **lazy**: `db.<model>.<method>(...)` builds an operation without running it. Awaited on its own it executes as a single request; passed to `$transaction` it is collected into the batch. (So don't both `await` a call and pass it to `$transaction` — that would run it twice.)

`ref("<i>.<path>")` references the result of operation `i`, resolved server-side before that operation runs — typically a generated id: `ref("0.id")`, `ref("1.author.id")`.

Reads inside the transaction see the writes of earlier operations (read-your-writes). On any failure the whole transaction is rolled back and `$transaction` rejects.

Reach for it when several **independent** writes must be atomic (e.g. a balance transfer). The "create parent + children" case doesn't need it — nested writes (`insert({ ..., posts: [...] })`) are already atomic in a single operation.

## Client setup

```ts
import { marcidb } from "marcidb-client"

// the database name is the last path segment of the URL
const db = marcidb("http://localhost:3000/myapp")
```

`npx marcidb generate` produces both the typed client (from `schema.marci`) and a `.march` migration file (a reviewable action list + a snapshot of the resulting schema); `npx marcidb migrate push myapp` ships your migration files and the server applies the new ones (creating the database on first push). The client talks to the server over the [HTTP API](HTTP-API.md). Every model gets the same set of methods:

```ts
db.<model>.findMany(query)        // Promise<Result[]>
db.<model>.findFirst(query)       // Promise<Result | null>
db.<model>.insert(data)           // Promise<Id>
db.<model>.update(id, data)       // Promise<void>
db.<model>.updateMany(query, data) // Promise<number> — rows matched
db.<model>.delete(id)             // Promise<void>
db.<model>.count(query?)          // Promise<number>
db.<model>.aggregate(query)       // Promise<AggregateResult>
db.<model>.reindex()              // Promise<{ ok, indexed }> — only on models with a @custom index

db.$transaction([ ...ops ])       // Promise<[...results]> — atomic, see Transactions
```

(The per-model methods return a lazy `Op<T>`, which is awaitable like a `Promise<T>` and can also be passed to `$transaction`.)

### updateMany

Applies one update to every row matching `$where`, in a single transaction — either all of them land or none do:

```ts
const n = await db.user.updateMany({ $where: { age: { $lt: 18 } } }, { active: false })
```

The returned number counts rows **matched**, not rows whose stored bytes changed — re-applying a value a row already holds still counts, as in SQL. An empty query (`{}`) matches every row.

Matching ids are resolved before any row is written, so the filter sees the pre-update state: a row that stops matching part-way through is still updated, and one that starts matching is not.

Three things are rejected rather than silently ignored:

- **`$limit` / `$skip` / `$cursor`** — without a total order these would update an arbitrary subset. Narrow the `$where` instead.
- **`$near` / `$search`** — module-index search is not available on this path.
- **relation operations** (`$connect`, `$push`, `$update`, …) in `data` — these recurse per matched row, so the work is unbounded. Use per-row `update` for those.

Field selections in the query are ignored: `updateMany` only ever resolves ids.

### Atomic counters — `$increment`

A numeric field can be updated relative to its current value instead of assigned:

```ts
await db.user.update({ id }, { depositRub: { $increment: -800 } })
```

The read, the addition and the write all happen inside the update's own transaction, so concurrent writers cannot lose an update the way a read-then-write from application code can.

Combined with a guard in `$where`, `updateMany` becomes a **compare-and-set** — the returned count tells you whether it applied:

```ts
const n = await db.user.updateMany(
  { $where: { id, depositRub: { $gte: 800 } } },
  { depositRub: { $increment: -800 } },
)
if (n === 0) throw new Error("insufficient funds")
```

The predicate is evaluated in the same transaction as the write, so two concurrent debits cannot both succeed against one balance. This is the DB-level alternative to serializing the operation in application code.

Notes:

- **The delta is signed on every numeric type, including unsigned ones** — a `UInt` field can be decremented. The storage stays unsigned; only the delta is signed.
- **A result outside the field's range is rejected, never wrapped.** Taking a `UInt` below zero, or any type past its min/max, fails the write and rolls the whole transaction back — so a batch or an `updateMany` that hits it commits nothing. Guard with `$where` (as above) if you want "skip" rather than "fail".
- **A `DateTime` delta is a plain number of milliseconds**, not a date.
- **Incrementing a field that is currently null is a no-op**, not an error — the row is left unchanged and still counts as matched.
