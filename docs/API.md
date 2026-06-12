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
| `T[]`, `T[n]` | `T[]` | primitive lists, fixed size optional |
| `Byte[16] @format(uuid)` | `string` | uuid in JSON, 16 bytes in storage |
| `Byte[] @format(hex)` | `string` | hex in JSON |

If a model has no `@id` field, an autoincrement `id UInt` is added implicitly.

### Attributes

- `@id` — primary key field (composite keys: several `@id` fields)
- `@unique` — unique secondary index
- `@index` — secondary index (used by `$where`, `$order` and aggregations)
- `@default(value)` — also `autoincrement()`, `now()`
- `@bind(Model.field)` — declares the reverse side of a relation
- `@format(uuid | hex)` — JSON representation of byte fields
- `@onDelete(...)` — delete constraint for relations

### Structs, relations, enums

- `struct` — nested entity stored under the parent's key prefix; inserted/updated inline with the parent.
- `Model` / `Model[]` fields — relations by id; lists require a `@bind` on the opposite side.
- `enum` variants may carry **payload fields** which are injected into the model itself. In TS this is a discriminated union: `{ role: "viewer" } | { role: "admin", level: number, sign: string } | { role: "moderator", sign: string }`. Switching the variant on update requires the full payload of the new variant and clears fields of the old one.
- Every enum line is `name1 | name2 [{ fields }]`: a variant is declared on first mention, and a block attaches its fields to all listed variants. `admin | moderator { sign String }` makes `sign` a single physical field shared by both variants — switching between them keeps it (the required payload overwrites it anyway), while switching outside the group clears it. Blocks mentioning the same variant merge; declaring the same field name twice is a schema error.

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

The planner picks the most selective indexed condition (exact id → unique eq → eq → startsWith → range); all other conditions are re-checked per row, so the index choice never affects correctness.

### `$order`, `$limit`, `$skip`, `$cursor`

- `$order: { field: "asc" | "desc" }` — single field. Indexed non-nullable fields are served by an index scan; everything else is sorted in memory. Nulls go last for `asc`, first for `desc`.
- `$limit` / `$skip` — applied after filtering. `$skip` is O(n); prefer `$cursor` for pagination.
- `$cursor: { id }` — **exclusive** keyset cursor: results strictly after the row with this id, in the current `$order`. Without `$order` the order is fixed to primary-key order. Take the id from the last row of the previous page. If the cursor row was deleted: id-ordered queries continue seamlessly; value-ordered queries return an empty page.

All of the above also work inside nested selects: `posts: { title: true, $order: { id: "desc" }, $limit: 5 }`.

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
  info: { $update: { bio: "hello" } },   // $update | $ensure | $set
  posts: { $connect: { id: 2 }, $remove: { id: 1 } },  // also $push, $set for struct lists
})

await db.user.delete({ id })
```

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

## Client setup

```ts
import { marcidb } from "marcidb-client"

const db = marcidb("http://localhost:3000")
```

The client is generated from `schema.marci` by `npx marcidb generate` and talks to the server over the [HTTP API](HTTP-API.md). Every model gets the same set of methods:

```ts
db.<model>.findMany(query)        // Promise<Result[]>
db.<model>.findFirst(query)       // Promise<Result | null>
db.<model>.insert(data)           // Promise<Id>
db.<model>.update(id, data)       // Promise<void>
db.<model>.delete(id)             // Promise<void>
db.<model>.count(query?)          // Promise<number>
db.<model>.aggregate(query)       // Promise<AggregateResult>
```
