// The generic query layer of the marcidb client: the query-language types every generated client is built
// on (where / update operators, `GetResult`, `Op`, the query builder `Query<T>`) and the builder runtime
// (`createQueryLayer`). Everything here is schema-agnostic — the codegen (crates/marcidb-ts) emits the
// per-model type bags and MODELS metadata this file is parametrised by.
//
// Keep it dependency-free: besides shipping in `marcidb-client/runtime`, this file is vendored verbatim
// into the lecodes SDK (`packages/sdk/src/server/db/marci/query.ts`, `bun run sync:marcidb` there), which
// derives its model types from TS builders instead of codegen and plugs them into the same generics.

// ───────────────────────────── query language types ─────────────────────────────

export type ServiceKeys = "$where" | "$order" | "$limit" | "$skip" | "$cursor";

// A `Json` field: any JSON value. Stored as a JSONB binary blob server-side; on the wire it is a plain
// decoded JSON value.
export type JsonValue = string | number | boolean | null | JsonValue[] | { [key: string]: JsonValue };

// Aggregate query over related records inside select
export type AggregateKeys = { $count: true } | { $sum: string } | { $avg: string } | { $min: string } | { $max: string }

// Distributive over union models (discriminated union for payload enums):
// keys not present in the current union branch are dropped
export type GetResult<TModel, TSelect> = TModel extends any ? {
  -readonly [K in keyof Omit<TSelect, ServiceKeys> as TSelect[K] extends false | undefined ? never : K extends keyof TModel ? K : never]:
    K extends keyof TModel
      ? TSelect[K] extends true
        ? TModel[K]                          // selected the whole field
        : TSelect[K] extends Record<string, any>
          ? TModel[K] extends readonly object[]
            ? TSelect[K] extends AggregateKeys
              ? AggregateResult<NonNullable<TModel[K][number]>, TSelect[K]>   // aggregate over the relation
              : GetResult<NonNullable<TModel[K][number]>, TSelect[K]>[]
            : GetResult<NonNullable<TModel[K]>, TSelect[K]> | Extract<TModel[K], null>
          : TModel[K]                        // fallback
      : never
} : never

export type RefUpdate<I> = {
  "$connect"?: I
}

export type RefUpdateStruct<I,U> = {
  "$update"?: U,
  "$ensure"?: I,
  "$set"?: I
}

/** Relation to independent rows — link operations only, the rows themselves are never created
 * or deleted. `$set` replaces link membership with exactly the given set (missing links are
 * disconnected, new ones connected); `$connect` links (idempotent); `$remove` unlinks. */
export type RefListUpdate<I> = {
  "$set"?: I[],
  "$connect"?: I | I[],
  "$remove"?: I | I[],
}

/** `@list` relation: an ordered inline id array — a sequence, so the same id may appear several
 * times. `$set` replaces the whole array (also the reorder operation); `$connect` appends at the
 * end (an already-present id gains another occurrence); `$connectUnique` appends only ids not
 * already present; `$remove` removes every occurrence. */
export type RefListUpdateOrdered<I> = {
  "$set"?: I[],
  "$connect"?: I | I[],
  "$connectUnique"?: I | I[],
  "$remove"?: I | I[],
}

/** Owned (struct) list: the children live and die with the parent. `$push` creates children,
 * `$update` edits single children in place (each item is the child's id fields — the shape
 * query results return — plus the changes under `data`), `$remove` deletes children by id,
 * `$set` replaces all children (deletes the current ones, creates the new). */
export type RefListUpdateStruct<I,U,Id> = {
  "$push"?: I | I[],
  "$update"?: (Id & { data: U }) | (Id & { data: U })[],
  "$remove"?: Id | Id[],
  "$set"?: I[]
}

/** Variable-length primitive array — a sequence, so the same value may appear several times.
 * `$push` appends at the end (duplicates kept); `$pushUnique` appends only values not already
 * present; `$remove` removes every occurrence; `$set` replaces the whole array (also the
 * positional-edit path — send the full new array). One operator per update. */
export type PrimitiveListUpdate<T> = {
  "$set"?: T[],
  "$push"?: T | T[],
  "$pushUnique"?: T | T[],
  "$remove"?: T | T[],
}

// Marks a set of keys as forbidden. Needed because TypeScript's excess-property check against a *union*
// permits any key present in some member: a plain union of single-key objects accepts `{ $gte, $lt }`,
// and a plain `T | { $and: T[] }` accepts a field sitting next to `$and`. Both are runtime errors or
// silently-wrong queries, so every branch below spells out the keys it excludes.
export type Never<T> = { [K in keyof T]?: never }

// Every field-level operator, so a field's condition can exclude the ones its type doesn't support.
export type FieldOps = {
  "$eq": unknown, "$ne": unknown, "$not": unknown, "$in": unknown, "$notIn": unknown,
  "$gt": unknown, "$gte": unknown, "$lt": unknown, "$lte": unknown,
  "$startsWith": unknown, "$includes": unknown,
  "$every": unknown, "$some": unknown, "$none": unknown,
  "$near": unknown, "$search": unknown
}
export type Only<K extends keyof FieldOps, V> = { [P in K]: V } & Never<Omit<FieldOps, K>>

// A `$where` is either a set of field conditions or exactly one boolean combinator wrapping more of the
// same. Conditions belong *inside* the array: the engine returns on the first combinator it finds, so a
// sibling key next to `$and`/`$or` would be silently ignored — here it is a type error instead.
export type WhereValue<T> =
  | (T & Never<{ "$and": unknown, "$or": unknown, "$not": unknown }>)
  | ({ "$and": WhereValue<T>[] } & Never<T> & Never<{ "$or": unknown, "$not": unknown }>)
  | ({ "$or":  WhereValue<T>[] } & Never<T> & Never<{ "$and": unknown, "$not": unknown }>)
  | ({ "$not": WhereValue<T> }   & Never<T> & Never<{ "$and": unknown, "$or": unknown }>)

// Operators on one field are ANDed, so they combine freely: `{ $gte: 18, $lt: 65 }` is a half-open
// range and `{ $gte: 18, $ne: 30 }` punches a hole in one. Each group below is all-optional; a field's
// condition type intersects the groups its type supports and marks every remaining operator forbidden,
// so a string operator on a number is still a type error.
export type ValueOps<T> = { "$eq"?: T, "$ne"?: T, "$not"?: T, "$in"?: T[], "$notIn"?: T[] }
export type NumOps<T>   = { "$gt"?: T, "$gte"?: T, "$lt"?: T, "$lte"?: T }
export type StrOps      = { "$includes"?: string, "$startsWith"?: string }

// The numeric and string variants re-admit the type-agnostic operators, so a mixed condition such as
// `{ $gte: 18, $ne: 30 }` still matches a single member of the union the field's type resolves to.
export type CompareValue<T>    = T | (ValueOps<T> & Never<Omit<FieldOps, keyof ValueOps<T>>>)
export type CompareNumValue<T> = ValueOps<T> & NumOps<T> & Never<Omit<FieldOps, keyof ValueOps<T> | keyof NumOps<T>>>
export type CompareStrValue<T> = ValueOps<T> & StrOps    & Never<Omit<FieldOps, keyof ValueOps<T> | keyof StrOps>>

// A numeric field can also be updated in place: `{ balance: { $increment: -800 } }` reads, adds and writes
// inside the update's own transaction, so it is atomic against concurrent writers. The delta is signed, so
// decrementing requires a signed field (`Int`) — a negative delta on an unsigned field is rejected.
// Incrementing a field that is currently null is a no-op.
export type UpdateNumValue = { "$increment": number }

// Filtering into a Json field by dot-path. Keys are JSON paths (e.g. "address.city", "items.0"); a numeric
// segment indexes an array. A bare value is shorthand for `$eq` (a plain object matches the whole subtree).
// Path keys must not start with `$`. A missing leaf or a type mismatch simply doesn't match.
export type JsonType = "string" | "number" | "boolean" | "object" | "array" | "null"
// As on scalar fields, operators on one path are ANDed and may be combined.
export type JsonCondition =
  | JsonValue
  | {
      "$eq"?: JsonValue, "$ne"?: JsonValue, "$not"?: JsonValue,
      "$gt"?: number | string, "$gte"?: number | string,
      "$lt"?: number | string, "$lte"?: number | string,
      "$in"?: JsonValue[], "$notIn"?: JsonValue[],
      "$startsWith"?: string, "$includes"?: string,
      "$contains"?: JsonValue,
      "$exists"?: boolean,
      "$type"?: JsonType
    }
export type JsonPathWhere = { [path: string]: JsonCondition }
export type CompareRefValue<T> = T | Only<"$not", T>
export type CompareRefListValue<T> = Only<"$every", T> | Only<"$some", T> | Only<"$none", T>

// Module-index search (@custom): $near/$search hand a raw payload to the field's index provider.
export type VectorSearch = { vector: number[], k?: number, threshold?: number }      // @custom(vector, …)
export type FullTextSearch = string | { query: string, limit?: number }             // @custom(fulltext, …)
export type CustomSearch = Record<string, any>                                       // other providers
export type CustomSearchValue<P> = Only<"$near", P> | Only<"$search", P>

// Aggregate result: only the requested keys; an empty set yields null (except count)
export type AggregateResult<TModel, T> =
  (T extends { $count: true } ? { count: number } : {}) &
  (T extends { $sum: string } ? { sum: number | null } : {}) &
  (T extends { $avg: string } ? { avg: number | null } : {}) &
  (T extends { $min: infer F } ? { min: (F extends keyof TModel ? TModel[F] : never) | null } : {}) &
  (T extends { $max: infer F } ? { max: (F extends keyof TModel ? TModel[F] : never) | null } : {})

// A lazily-executed operation. `await` runs it as a single request;
// passing it into `db.$transaction([...])` bundles it into one atomic transaction
declare const __op: unique symbol
export type Op<T> = PromiseLike<T> & { readonly [__op]: T }

// A transport-neutral operation descriptor and the pluggable transport that runs it. The HTTP transport
// is selected by passing a URL string; marcidb-embedded provides an in-process FFI transport.
export type MarciOp = { model: string, action: string, query?: any, data?: any, id?: any }
export type MarciTransport = {
  exec(op: MarciOp): Promise<any>
  batch(ops: MarciOp[]): Promise<any[]>
}

// ───────────────────────────── query builder ─────────────────────────────

// The per-model type bag the codegen emits (`UserTypes`) and `Query<T>` below is parametrised by.
export type ModelTypes = {
  name: string
  model: any
  id: Record<string, any>
  /** What an empty select returns: id + every scalar field, as a `{ field: true }` shape. */
  scalars: Record<string, true>
  select: Record<string, any>
  query: Record<string, any>
  where: any
  order: Record<string, any>
  insert: any
  update: any
  aggregate: Record<string, any>
  reindex: boolean
}

// A sub-query placed as a value in a select shape — a builder, `count()` or `aggregate()` of the relation's
// model (`posts: db.post.order("id", "desc").limit(5)`). Branded by model name so a builder of the wrong model
// is a type error; `[__sel]` is the shape it resolves to (see `Resolve`).
declare const __sub: unique symbol
declare const __sel: unique symbol
export type Sub<Name extends string, Shape = any> = { readonly [__sub]: Name, readonly [__sel]: Shape }

// A shape without field keys (only `$`-clauses, or nothing) selects id + every scalar — same rule as the engine.
export type Effective<T extends ModelTypes, S> = [Exclude<keyof S, ServiceKeys>] extends [never] ? T["scalars"] : S
// Sub-queries inside a shape resolve to the shape they carry, recursively; `$`-clauses are left alone.
export type Resolve<S> =
  S extends { readonly [__sel]: infer X } ? Resolve<X>
  : S extends object ? { [K in keyof S]: K extends `$${string}` ? S[K] : Resolve<S[K]> }
  : S
export type Rows<T extends ModelTypes, Sel> = GetResult<T["model"], Resolve<Effective<T, Sel>>>
// The bare key value, accepted wherever an id object is (`after(42)` = `after({ id: 42 })`, `delete(42)`).
export type BareId<I> = I extends { id: infer V } ? V : never
export type IdArg<I> = I | BareId<I>

/**
 * A lazy, immutable query over one model — `db.user` itself is one. Every clause returns a new query, so
 * queries compose (`const active = db.user.where({ active: true })`). `await` runs it as `findMany`; passing
 * it to `$transaction` batches it; placing it in another query's select makes it a sub-select. `Sel` is the
 * projection: id + scalars until `select(...)` sets a shape.
 */
export interface Query<T extends ModelTypes, Sel = T["scalars"]> extends PromiseLike<Rows<T, Sel>[]> {
  readonly [__op]: Rows<T, Sel>[]
  readonly [__sub]: T["name"]
  readonly [__sel]: Effective<T, Sel>
  /** Filter (marcidb `$where`). Repeated calls are ANDed. */
  where(where: T["where"]): Query<T, Sel>
  /** Sort by one field: `order("age", "desc")` or `order({ age: "desc" })`. */
  order(field: keyof T["order"] & string, direction?: "asc" | "desc"): Query<T, Sel>
  order(order: T["order"]): Query<T, Sel>
  limit(n: number): Query<T, Sel>
  skip(n: number): Query<T, Sel>
  /** Keyset cursor: rows strictly after this id in the current order (`$cursor`). */
  after(id: IdArg<T["id"]>): Query<T, Sel>
  /**
   * The projection. Values are `true`, a nested shape, or a sub-query of the relation's model
   * (`posts: db.post.limit(5)`, `posts: db.post.where({ published: true }).count()`). No argument, or no
   * field keys, selects id + every scalar. `$`-clauses are accepted here too (the object form).
   */
  select<S extends T["query"] = T["scalars"]>(shape?: S): Query<T, S>
  /** The first matching row or `null`. */
  first(): Op<Rows<T, Sel> | null>
  /** Row count; inside a select it becomes `{ count }` for the relation. */
  count(): Op<number> & Sub<T["name"], { $count: true }>
  /** @deprecated pass the filter through the chain: `db.user.where(w).count()` (removed in the next minor). */
  count(query: { $where?: T["where"] }): Op<number> & Sub<T["name"], { $count: true }>
  // `NoInfer`: as a select value the contextual type is `Sub<Name, any>`, which would otherwise win the inference of `A`
  aggregate<A extends T["aggregate"]>(query: A): Op<AggregateResult<T["model"], A>> & Sub<T["name"], NoInfer<A>>

  insert(data: T["insert"]): Op<T["id"]>
  update(id: IdArg<T["id"]>, data: T["update"]): Op<void>
  /** Applies `data` to every row the chain's `where` matches (all rows without one); resolves to the number of rows. */
  updateMany(data: T["update"]): Op<number>
  /** @deprecated pass the filter through the chain: `db.user.where(w).updateMany(data)` (removed in the next minor). */
  updateMany(query: { $where?: T["where"] }, data: T["update"]): Op<number>
  delete(id: IdArg<T["id"]>): Op<void>
  /**
   * Deletes every row the chain's `where` matches (cascades apply, as for `delete`); resolves to the number
   * deleted. Refuses to run without a `where` — write `.where({})` to mean "every row".
   */
  deleteMany(): Op<number>

  // ── object form (deprecated): one query object (select shape + $where/$order/$limit/$skip/$cursor)
  /** @deprecated use `.select(query)` — it takes the same object, merged with the chain (removed in the next minor). */
  findMany<Q extends T["query"] = {}>(query?: Q): Op<Rows<T, Q>[]>
  /** @deprecated use `.select(query).first()` (removed in the next minor). */
  findFirst<Q extends T["query"] = {}>(query?: Q): Op<Rows<T, Q> | null>
}

/** `db.<model>`: the root query, plus `reindex()` for models with a `@custom` (vector / full-text) index. */
export type Collection<T extends ModelTypes> = Query<T> & (T["reindex"] extends true ? { reindex(): Op<{ ok: boolean, indexed: number }> } : {})

// ───────────────────────────── builder runtime ─────────────────────────────

/** One field of a model, in slot order: `n` name, `k` key | body | one | many, `m` the relation's target model. */
export type FieldDesc = { n: string; k: "key" | "body" | "one" | "many"; t?: string | null; m?: string };
/** Model name → its field descriptors (structs included, as relation targets). */
export type ModelsMeta = Record<string, readonly FieldDesc[]>;

export type QueryLayerOptions = {
  models: ModelsMeta;
  /** Runs one operation (a query, or a write) and resolves its result. */
  run(op: MarciOp): Promise<any>;
  /** Hook for `insert` payloads (the lecodes SDK generates uuid ids here). */
  prepareInsert?(model: string, data: any): any;
};

const AGGREGATE_KEYS = ["$count", "$sum", "$avg", "$min", "$max"];
const and = (a: any, b: any): any => (a ? (b ? { $and: [a, b] } : a) : b);

type QueryState = { shape?: Record<string, any>; where?: any; order?: any; limit?: number; skip?: number; cursor?: any };

/**
 * The query builder over a set of models and a `run` function. Returns `op(descriptor)` — a lazy operation
 * (`await` runs it, `$transaction` takes its `__op`) — and `collection(model)`, the immutable `db.<model>`
 * root query. Transport-agnostic: the generated client and the embedded/HTTP transports supply `run`.
 */
export function createQueryLayer(options: QueryLayerOptions): { op: (descriptor: MarciOp) => any; collection: (model: string) => any } {
  const { models, run } = options;

  // Lazily-executed operation: `await` runs it through the transport as a single op,
  // while `$transaction` takes only the `__op` descriptor and sends them as one batch.
  const op = (descriptor: MarciOp): any => ({
    __op: descriptor,
    then: (onFulfilled?: any, onRejected?: any) => run(descriptor).then(onFulfilled, onRejected),
    catch: (onRejected?: any) => run(descriptor).catch(onRejected),
    finally: (onFinally?: any) => run(descriptor).finally(onFinally),
  });

  // A field key is anything that isn't a `$`-clause. A shape without field keys selects id + every scalar
  // (the engine's rule too — the client applies it so the binary decoder and older servers see explicit fields).
  const scalarSelect = (model: string): Record<string, true> => {
    const out: Record<string, true> = {};
    for (const f of models[model] ?? []) if (f.k === "key" || f.k === "body") out[f.n] = true;
    return out;
  };
  const keyField = (model: string): string => (models[model] ?? []).find((f) => f.k === "key")?.n ?? "id";
  // `42` → `{ id: 42 }` (the bare value of a single-field key), an object passes through
  const idObject = (model: string, id: any): any => (id !== null && typeof id === "object" ? id : { [keyField(model)]: id });
  // Resolves sub-queries (values carrying `__select`) and fills empty projections, recursively along relations.
  const resolveShape = (model: string, shape: Record<string, any>): Record<string, any> => {
    const out: Record<string, any> = {};
    let fields = 0;
    for (const k in shape) {
      const v = shape[k];
      if (k.charCodeAt(0) === 36 /* $ */) { out[k] = v; continue; }
      if (v !== undefined && v !== false) fields++;
      if (v !== null && typeof v === "object") {
        if ("__select" in v) { out[k] = v.__select; continue; }
        const desc = (models[model] ?? []).find((f) => f.n === k);
        out[k] = desc && desc.m && !AGGREGATE_KEYS.some((a) => a in v) ? resolveShape(desc.m, v) : v;
        continue;
      }
      out[k] = v;
    }
    if (fields === 0) Object.assign(out, scalarSelect(model));
    return out;
  };

  // `db.<model>` — an immutable builder; each clause returns a new one over the same `run`.
  const collection = (model: string): any => {
    const make = (st: QueryState): any => {
      // The wire query: the (object-form) `query` merged over the chain's state.
      const build = (query?: Record<string, any>): Record<string, any> => {
        const q = resolveShape(model, { ...(st.shape ?? {}), ...(query ?? {}) });
        const where = and(st.where, q.$where);
        if (where) q.$where = where; else delete q.$where;
        if (st.order && !q.$order) q.$order = st.order;
        if (st.limit !== undefined && q.$limit === undefined) q.$limit = st.limit;
        if (st.skip !== undefined && q.$skip === undefined) q.$skip = st.skip;
        if (st.cursor !== undefined && q.$cursor === undefined) q.$cursor = st.cursor;
        return q;
      };
      const whereOnly = (query?: Record<string, any>): Record<string, any> => {
        const w = and(st.where, query && query.$where);
        return w ? { $where: w } : {};
      };
      const findMany = (): MarciOp => ({ model, action: "findMany", query: build() });
      return {
        get __op() { return findMany(); },
        get __select() { return build(); },
        then: (onFulfilled?: any, onRejected?: any) => run(findMany()).then(onFulfilled, onRejected),
        catch: (onRejected?: any) => run(findMany()).catch(onRejected),
        finally: (onFinally?: any) => run(findMany()).finally(onFinally),

        where: (where: any) => make({ ...st, where: and(st.where, where) }),
        order: (field: any, direction?: "asc" | "desc") => make({ ...st, order: typeof field === "string" ? { [field]: direction ?? "asc" } : field }),
        limit: (n: number) => make({ ...st, limit: n }),
        skip: (n: number) => make({ ...st, skip: n }),
        after: (id: any) => make({ ...st, cursor: idObject(model, id) }),
        select: (shape?: Record<string, any>) => make({ ...st, shape: shape ?? {} }),
        first: () => op({ model, action: "findFirst", query: build() }),
        count: (query?: Record<string, any>) => {
          const q = whereOnly(query);
          return Object.assign(op({ model, action: "count", query: q }), { __select: { $count: true, ...q } });
        },
        aggregate: (query: Record<string, any>) => {
          const q = { ...(query ?? {}), ...whereOnly(query) };
          if (!q.$where) delete q.$where;
          return Object.assign(op({ model, action: "aggregate", query: q }), { __select: q });
        },

        insert: (data: any) => op({ model, action: "insert", data: options.prepareInsert ? options.prepareInsert(model, data) : data }),
        update: (id: any, data: any) => op({ model, action: "update", id: idObject(model, id), data }),
        // `updateMany(data)`; the deprecated `updateMany(query, data)` form is told apart by arity
        updateMany: (a: any, b?: any) => (b === undefined
          ? op({ model, action: "updateMany", query: whereOnly(), data: a })
          : op({ model, action: "updateMany", query: whereOnly(a), data: b })),
        delete: (id: any) => op({ model, action: "delete", id: idObject(model, id) }),
        deleteMany: () => {
          if (st.where === undefined) throw new Error(`marcidb: ${model}.deleteMany() needs a where — use .where({}) to delete every row`);
          return op({ model, action: "deleteMany", query: whereOnly() });
        },
        reindex: () => op({ model, action: "$reindex" }),

        findMany: (query?: Record<string, any>) => op({ model, action: "findMany", query: build(query) }),
        findFirst: (query?: Record<string, any>) => op({ model, action: "findFirst", query: build(query) }),
      };
    };
    return make({});
  };

  return { op, collection };
}
