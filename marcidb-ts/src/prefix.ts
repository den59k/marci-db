type GetResult<TModel, TSelect extends Record<string, any>> = {
  [K in keyof TSelect as TSelect[K] extends false | undefined ? never : K]:
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

export declare function marci(url: string): MarciDB;
