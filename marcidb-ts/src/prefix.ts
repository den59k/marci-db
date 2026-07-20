type ServiceKeys = "$where" | "$order" | "$limit" | "$skip" | "$cursor";

// A `Json` field: any JSON value. Stored as a JSONB binary blob server-side; on the wire it is a plain
// decoded JSON value.
export type JsonValue = string | number | boolean | null | JsonValue[] | { [key: string]: JsonValue };

// Aggregate query over related records inside select
type AggregateKeys = { $count: true } | { $sum: string } | { $avg: string } | { $min: string } | { $max: string }

// Distributive over union models (discriminated union for payload enums):
// keys not present in the current union branch are dropped
type GetResult<TModel, TSelect extends Record<string, any>> = TModel extends any ? {
  [K in keyof Omit<TSelect, ServiceKeys> as TSelect[K] extends false | undefined ? never : K extends keyof TModel ? K : never]:
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

type RefUpdate<I> = {
  "$connect"?: I
}

type RefUpdateStruct<I,U> = {
  "$update"?: U,
  "$ensure"?: I,
  "$set"?: I
}

type RefListUpdate<I> = {
  "$connect"?: I | I[],
  "$remove"?: I | I[],
}

type RefListUpdateStruct<I,U> = {
  "$push"?: I | I[],
  "$remove"?: I | I[],
  "$set"?: I[]
}

// Marks a set of keys as forbidden. Needed because TypeScript's excess-property check against a *union*
// permits any key present in some member: a plain union of single-key objects accepts `{ $gte, $lt }`,
// and a plain `T | { $and: T[] }` accepts a field sitting next to `$and`. Both are runtime errors or
// silently-wrong queries, so every branch below spells out the keys it excludes.
type Never<T> = { [K in keyof T]?: never }

// Every field-level operator, so a field's condition can exclude the ones its type doesn't support.
type FieldOps = {
  "$eq": unknown, "$ne": unknown, "$not": unknown, "$in": unknown, "$notIn": unknown,
  "$gt": unknown, "$gte": unknown, "$lt": unknown, "$lte": unknown,
  "$startsWith": unknown, "$includes": unknown,
  "$every": unknown, "$some": unknown, "$none": unknown,
  "$near": unknown, "$search": unknown
}
type Only<K extends keyof FieldOps, V> = { [P in K]: V } & Never<Omit<FieldOps, K>>

// A `$where` is either a set of field conditions or exactly one boolean combinator wrapping more of the
// same. Conditions belong *inside* the array: the engine returns on the first combinator it finds, so a
// sibling key next to `$and`/`$or` would be silently ignored — here it is a type error instead.
type WhereValue<T> =
  | (T & Never<{ "$and": unknown, "$or": unknown, "$not": unknown }>)
  | ({ "$and": WhereValue<T>[] } & Never<T> & Never<{ "$or": unknown, "$not": unknown }>)
  | ({ "$or":  WhereValue<T>[] } & Never<T> & Never<{ "$and": unknown, "$not": unknown }>)
  | ({ "$not": WhereValue<T> }   & Never<T> & Never<{ "$and": unknown, "$or": unknown }>)

// Operators on one field are ANDed, so they combine freely: `{ $gte: 18, $lt: 65 }` is a half-open
// range and `{ $gte: 18, $ne: 30 }` punches a hole in one. Each group below is all-optional; a field's
// condition type intersects the groups its type supports and marks every remaining operator forbidden,
// so a string operator on a number is still a type error.
type ValueOps<T> = { "$eq"?: T, "$ne"?: T, "$not"?: T, "$in"?: T[], "$notIn"?: T[] }
type NumOps<T>   = { "$gt"?: T, "$gte"?: T, "$lt"?: T, "$lte"?: T }
type StrOps      = { "$includes"?: string, "$startsWith"?: string }

// The numeric and string variants re-admit the type-agnostic operators, so a mixed condition such as
// `{ $gte: 18, $ne: 30 }` still matches a single member of the union the field's type resolves to.
type CompareValue<T>    = T | (ValueOps<T> & Never<Omit<FieldOps, keyof ValueOps<T>>>)
type CompareNumValue<T> = ValueOps<T> & NumOps<T> & Never<Omit<FieldOps, keyof ValueOps<T> | keyof NumOps<T>>>
type CompareStrValue<T> = ValueOps<T> & StrOps    & Never<Omit<FieldOps, keyof ValueOps<T> | keyof StrOps>>

// A numeric field can also be updated in place: `{ balance: { $increment: -800 } }` reads, adds and writes
// inside the update's own transaction, so it is atomic against concurrent writers. The delta is signed, so
// decrementing requires a signed field (`Int`) — a negative delta on an unsigned field is rejected.
// Incrementing a field that is currently null is a no-op.
type UpdateNumValue = { "$increment": number }

// Filtering into a Json field by dot-path. Keys are JSON paths (e.g. "address.city", "items.0"); a numeric
// segment indexes an array. A bare value is shorthand for `$eq` (a plain object matches the whole subtree).
// Path keys must not start with `$`. A missing leaf or a type mismatch simply doesn't match.
type JsonType = "string" | "number" | "boolean" | "object" | "array" | "null"
// As on scalar fields, operators on one path are ANDed and may be combined.
type JsonCondition =
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
type JsonPathWhere = { [path: string]: JsonCondition }
type CompareRefValue<T> = T | Only<"$not", T>
type CompareRefListValue<T> = Only<"$every", T> | Only<"$some", T> | Only<"$none", T>

// Module-index search (@custom): $near/$search hand a raw payload to the field's index provider.
type VectorSearch = { vector: number[], k?: number, threshold?: number }      // @custom(vector, …)
type FullTextSearch = string | { query: string, limit?: number }             // @custom(fulltext, …)
type CustomSearch = Record<string, any>                                       // other providers
type CustomSearchValue<P> = Only<"$near", P> | Only<"$search", P>

// Aggregate result: only the requested keys; an empty set yields null (except count)
type AggregateResult<TModel, T> =
  (T extends { $count: true } ? { count: number } : {}) &
  (T extends { $sum: string } ? { sum: number | null } : {}) &
  (T extends { $avg: string } ? { avg: number | null } : {}) &
  (T extends { $min: infer F } ? { min: (F extends keyof TModel ? TModel[F] : never) | null } : {}) &
  (T extends { $max: infer F } ? { max: (F extends keyof TModel ? TModel[F] : never) | null } : {})

// A lazily-executed operation. `await` runs it as a single request;
// passing it into `db.$transaction([...])` bundles it into one atomic transaction
declare const __op: unique symbol
export type Op<T> = PromiseLike<T> & { readonly [__op]: T }

// A reference to a previous operation's result inside $transaction (resolved by the server).
// "0.id" — the id field of operation #0's result; "1.author.id" — a nested path
export declare function ref(path: string): any

// A transport-neutral operation descriptor and the pluggable transport that runs it. The HTTP transport
// is selected by passing a URL string; marcidb-embedded provides an in-process FFI transport.
export type MarciOp = { model: string, action: string, query?: any, data?: any, id?: any }
export type MarciTransport = {
  exec(op: MarciOp): Promise<any>
  batch(ops: MarciOp[]): Promise<any[]>
}

export declare function marcidb(transport: string | MarciTransport): MarciDB;
