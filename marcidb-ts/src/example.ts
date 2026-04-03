/*

marcidb.user.findMany({
  
})

*/

type UserModelId = {
  id: number
}

type UserModel = UserModelId & {
  name: string
  info: UserInfoModel | null
}

type UserSelect = {
  name: boolean,
  info: boolean | UserInfoSelect 
}

type UserUpdate = {
  name?: string
  info?: RefUpdateStruct<UserInfoModelInsert,UserInfoModelUpdate> | null
}

type UserInfoModel = {
  id: string
  bio: string
  age: number | null
}

type UserInfoSelect  = {
  bio: boolean
  age: boolean
}

type UserInfoModelInsert = UserModelId & {
  bio: string,
  age?: number | null
}
type UserInfoModelUpdate = {
  bio?: string,
  age?: number | null
}


// Магия: выводим тип результата из select
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


export interface UserActions{
  findMany<T extends UserSelect>(select: T): Promise<GetResult<UserModel, T>[]>
  insert(data: UserInfoModelInsert): Promise<UserModelId>
}

// Функция
// function findMany<T extends UserSelect>(select: T): Promise<GetResult<UserModel, T>[]> {
  
//   return [] as any
// }
