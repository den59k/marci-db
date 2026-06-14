type ServiceKeys = "$where" | "$order" | "$limit" | "$skip" | "$cursor";

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

type WhereValue<T> = T | { "$and": T[] } | { "$or": T[] } | { "$not": T }

type CompareValue<T> = T | { "$eq": T } | { "$not": T } | { "$in": T[] } | { "$notIn": T[] }
type CompareNumValue<T> = { "$gt": T } | { "$gte": T } | { "$lt": T } | { "$lte": T }
type CompareStrValue = { "$includes": string,  } | { "$startsWith": string }
type CompareRefValue<T> = T | { "$not": T }
type CompareRefListValue<T> = { "$every": T } | { "$some": T } | { "$none": T }

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

export declare function marcidb(url: string): MarciDB;
