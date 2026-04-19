type ServiceKeys = "$where" | "$order" | "$take" | "$limit";

type GetResult<TModel, TSelect extends Record<string, any>> = {
  [K in keyof Omit<TSelect, ServiceKeys> as TSelect[K] extends false | undefined ? never : K]:
    K extends keyof TModel
      ? TSelect[K] extends true
        ? TModel[K]                          // выбрали поле целиком
        : TSelect[K] extends Record<string, any>
          ? GetResult<NonNullable<TModel[K]>, TSelect[K]> | (null extends TModel[K] ? null : never)
          : TModel[K]                        // fallback
      : never
}

type RefUpdate<I> = {
  "$connect"?: I
}

type RefUpdateStruct<I,U> = {
  "$update"?: U,
  "$ensure"?: I,
  "$set"?: I
}

type RefListUpdate<I> = {
  "$connect": I | I[],
  "$remove": I | I[],
}

type RefListUpdateStruct<I,U> = {
  "$push": I | I[],
  "$remove": I | I[],
  "$set": I[]
}

type WhereValue<T> = T | { "$and": T[] } | { "$or": T[] } | { "$not": T }

type CompareValue<T> = T | { "$eq": T } | { "$not": T } | { "$in": T[] } | { "$notIn": T[] }
type CompareNumValue<T> = { "$gt": T } | { "$gte": T } | { "$lt": T } | { "$lte": T }
type CompareStrValue = { "$includes": string,  } | { "$startsWith": string }
type CompareRefValue<T> = T | { "$not": T }
type CompareRefListValue<T> = { "$every": T } | { "$some": T } | { "$none": T }

export declare function marcidb(url: string): MarciDB;
