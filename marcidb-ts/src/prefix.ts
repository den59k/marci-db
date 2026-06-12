type ServiceKeys = "$where" | "$order" | "$limit" | "$skip" | "$cursor";

// Дистрибутивен по union-моделям (discriminated union у enum с payload):
// ключи, которых нет в текущей ветке union, отбрасываются
type GetResult<TModel, TSelect extends Record<string, any>> = TModel extends any ? {
  [K in keyof Omit<TSelect, ServiceKeys> as TSelect[K] extends false | undefined ? never : K extends keyof TModel ? K : never]:
    K extends keyof TModel
      ? TSelect[K] extends true
        ? TModel[K]                          // выбрали поле целиком
        : TSelect[K] extends Record<string, any>
          ? TModel[K] extends readonly object[]
            ? GetResult<NonNullable<TModel[K][number]>, TSelect[K]>[]
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

// Результат aggregate: только запрошенные ключи; пустое множество даёт null (кроме count)
type AggregateResult<TModel, T> =
  (T extends { $count: true } ? { count: number } : {}) &
  (T extends { $sum: string } ? { sum: number | null } : {}) &
  (T extends { $avg: string } ? { avg: number | null } : {}) &
  (T extends { $min: infer F } ? { min: (F extends keyof TModel ? TModel[F] : never) | null } : {}) &
  (T extends { $max: infer F } ? { max: (F extends keyof TModel ? TModel[F] : never) | null } : {})

export declare function marcidb(url: string): MarciDB;
