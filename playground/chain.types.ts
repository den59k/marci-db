/**
 * Type-level check of the query builder over the generated playground client (index.d.ts) — compiled, never
 * run: `bunx tsc --noEmit --strict playground/chain.types.ts`. Positive cases must compile; negative cases are
 * `@ts-expect-error` lines (an unused directive is itself an error).
 */
import { marcidb, ref, type Sub } from "./index"

type Equal<A, B> = (<T>() => T extends A ? 1 : 2) extends (<T>() => T extends B ? 1 : 2) ? true : false
const assertEqual = <A, B>(_ok: Equal<A, B>): void => {}
const db = marcidb("http://localhost:3000/app")

export const _compiles = async () => {
  // ── the root query: no select → id + scalars (struct/relation fields are not scalars) ──
  const all = await db.user
  assertEqual<typeof all, { id: string; name: string }[]>(true)
  const scalars = await db.user.where({ name: { $startsWith: "A" } }).order("name", "desc").limit(10).skip(2).after("u1")
  assertEqual<typeof scalars, { id: string; name: string }[]>(true)
  const one = await db.user.where({ id: "u1" }).first()
  assertEqual<typeof one, { id: string; name: string } | null>(true)

  // ── select shapes: nested plain shape, sub-query, count sub-query ──
  const users = await db.user.select({
    name: true,
    info: { bio: true },
    posts: db.post.order("id", "desc").limit(3).select({ title: true }),
  })
  assertEqual<typeof users, { name: string; info: { bio: string } | null; posts: { title: string }[] }[]>(true)

  const withCounts = await db.user.select({ id: true, posts: db.post.where({ title: { $startsWith: "x" } }).count() })
  assertEqual<typeof withCounts, { id: string; posts: { count: number } }[]>(true)

  const withAgg = await db.user.select({ id: true, posts: db.post.aggregate({ $count: true, $max: "id" }) })
  assertEqual<typeof withAgg, { id: string; posts: { count: number } & { max: number | null } }[]>(true)

  // sub-query without a select → the relation's scalars
  const subScalars = await db.user.select({ name: true, posts: db.post.limit(5) })
  assertEqual<typeof subScalars, { name: string; posts: { id: number; title: string }[] }[]>(true)

  // sub-query on a to-one relation
  const authors = await db.post.select({ title: true, author: db.user.select({ name: true }) })
  assertEqual<typeof authors, { title: string; author: { name: string } | null }[]>(true)

  // select() with only clauses = scalars; select() empty = scalars
  const clausesOnly = await db.post.select({ $where: { title: "x" }, $limit: 1 })
  assertEqual<typeof clausesOnly, { id: number; title: string }[]>(true)
  const empty = await db.post.select()
  assertEqual<typeof empty, { id: number; title: string }[]>(true)

  // ── the object form still works and merges with the chain; empty object = scalars ──
  const objForm = await db.post.where({ id: 1 }).findMany({ title: true, author: { name: true } })
  assertEqual<typeof objForm, { title: string; author: { name: string } | null }[]>(true)
  const objEmpty = await db.post.findMany({ $limit: 2 })
  assertEqual<typeof objEmpty, { id: number; title: string }[]>(true)
  const objFirst = await db.post.findFirst()
  assertEqual<typeof objFirst, { id: number; title: string } | null>(true)

  // ── count / aggregate at the top level ──
  const n = await db.post.where({ title: "a" }).count()
  assertEqual<typeof n, number>(true)
  const agg = await db.post.aggregate({ $count: true, $min: "title" })
  assertEqual<typeof agg, { count: number } & { min: string | null }>(true)

  // ── queries compose and batch ──
  const recent = db.post.order({ id: "desc" }).limit(5)
  const [rows, first, count] = await db.$transaction([recent, recent.first(), recent.count()])
  assertEqual<typeof rows, { id: number; title: string }[]>(true)
  assertEqual<typeof first, { id: number; title: string } | null>(true)
  assertEqual<typeof count, number>(true)
  await db.$transaction([db.user.insert({ id: "u9", name: "A" }), db.post.insert({ title: "p", author: { id: ref("0.id") } })])
  await db.post.where({ title: "old" }).updateMany({}, { title: "new" })

  // a query is a Sub of its model
  const sub: Sub<"Post"> = db.post.limit(1)
  void sub
}

export const _rejects = async () => {
  // @ts-expect-error — a sub-query of the wrong model
  await db.user.select({ posts: db.user.limit(1) })
  // @ts-expect-error — unknown field in a select
  await db.post.select({ nope: true })
  // @ts-expect-error — unknown order field
  db.post.order("nope")
  // @ts-expect-error — where operator of the wrong type
  db.post.where({ title: { $gt: 1 } })
  // @ts-expect-error — a field next to a combinator is ignored by the engine → rejected
  db.post.where({ $and: [{ title: "a" }], title: "b" })
  // @ts-expect-error — no reindex without a @custom index
  db.post.reindex()
}
