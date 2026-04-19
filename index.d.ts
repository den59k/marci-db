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

// User
type UserModelId = {
  id: string
}
type UserModel = UserModelId & {
  name: string
  info: User_infoModel | null
  posts: PostModel[] | null
}
type UserModelInsert = {
  id: number[] | string
  name: string
  info?: User_infoModelInsert | null
  posts?: PostModelId[]
}
type UserModelUpdate = {
  id?: number[]
  name?: string
  info?: RefUpdateStruct<User_infoModelInsert,User_infoModelUpdate> | null
  posts?: RefListUpdate<PostModelId>[]
}
type UserModelSelect = {
  id?: boolean
  name?: boolean
  info?: User_infoModelSelect | boolean
  posts?: PostModelQuery | boolean
}
type UserModel$Where = {
  id?: CompareValue<number[]>
  name?: CompareValue<string> | CompareStrValue
  info?: CompareRefValue<User_infoModel$Where | null>
  posts?: CompareRefListValue<PostModel$Where>
}
type UserModelQuery = UserModelSelect & {
  $where?: UserModel$Where
}

// Post
type PostModelId = {
  id: number
}
type PostModel = PostModelId & {
  title: string
  author: UserModel | null
}
type PostModelInsert = {
  id?: number
  title: string
  author?: UserModelId | null
}
type PostModelUpdate = {
  id?: number
  title?: string
  author?: RefUpdate<UserModelId> | null
}
type PostModelSelect = {
  id?: boolean
  title?: boolean
  author?: UserModelSelect | boolean
}
type PostModel$Where = {
  id?: CompareValue<number> | CompareNumValue<number>
  title?: CompareValue<string> | CompareStrValue
  author?: CompareRefValue<UserModel$Where | null>
}
type PostModelQuery = PostModelSelect & {
  $where?: PostModel$Where
}

// Project
type ProjectModelId = {
  id: number
}
type ProjectModel = ProjectModelId & {
  name: string
  users: Project_usersModel[] | null
}
type ProjectModelInsert = {
  id?: number
  name: string
  users?: Project_usersModelInsert[]
}
type ProjectModelUpdate = {
  id?: number
  name?: string
  users?: RefListUpdateStruct<Project_usersModelInsert,Project_usersModelUpdate>[]
}
type ProjectModelSelect = {
  id?: boolean
  name?: boolean
  users?: Project_usersModelQuery | boolean
}
type ProjectModel$Where = {
  id?: CompareValue<number> | CompareNumValue<number>
  name?: CompareValue<string> | CompareStrValue
  users?: CompareRefListValue<Project_usersModel$Where>
}
type ProjectModelQuery = ProjectModelSelect & {
  $where?: ProjectModel$Where
}

// User.info
type User_infoModelId = {
}
type User_infoModel = User_infoModelId & {
  bio: string
  age: number
}
type User_infoModelInsert = {
  bio: string
  age: number
}
type User_infoModelUpdate = {
  bio?: string
  age?: number
}
type User_infoModelSelect = {
  bio?: boolean
  age?: boolean
}
type User_infoModel$Where = {
  bio?: CompareValue<string> | CompareStrValue
  age?: CompareValue<number> | CompareNumValue<number>
}
type User_infoModelQuery = User_infoModelSelect & {
  $where?: User_infoModel$Where
}

// Project.users
type Project_usersModelId = {
  user: UserModel
}
type Project_usersModel = Project_usersModelId & {
  role: "creator" | "admin"
  sign: string
}
type Project_usersModelInsert = {
  user: UserModelId
  role: "creator" | "admin"
  sign: string
}
type Project_usersModelUpdate = {
  user?: RefUpdate<UserModelId>
  role?: "creator" | "admin"
  sign?: string
}
type Project_usersModelSelect = {
  user?: UserModelSelect | boolean
  role?: boolean
  sign?: boolean
}
type Project_usersModel$Where = {
  user?: CompareRefValue<UserModel$Where>
  role?: CompareValue<"creator" | "admin">
  sign?: CompareValue<string> | CompareStrValue
}
type Project_usersModelQuery = Project_usersModelSelect & {
  $where?: Project_usersModel$Where
}

export interface MarciDB {
  user: {
    findMany<T extends UserModelQuery>(select: T): Promise<GetResult<UserModel, T>[]>
    findFirst<T extends UserModelQuery>(select: T): Promise<GetResult<UserModel, T> | null>
    insert(data: UserModelInsert): Promise<UserModelId>
    update(id: UserModelId, data: UserModelUpdate): Promise<void>
    delete(id: UserModelId): Promise<void>
  }
  post: {
    findMany<T extends PostModelQuery>(select: T): Promise<GetResult<PostModel, T>[]>
    findFirst<T extends PostModelQuery>(select: T): Promise<GetResult<PostModel, T> | null>
    insert(data: PostModelInsert): Promise<PostModelId>
    update(id: PostModelId, data: PostModelUpdate): Promise<void>
    delete(id: PostModelId): Promise<void>
  }
  project: {
    findMany<T extends ProjectModelQuery>(select: T): Promise<GetResult<ProjectModel, T>[]>
    findFirst<T extends ProjectModelQuery>(select: T): Promise<GetResult<ProjectModel, T> | null>
    insert(data: ProjectModelInsert): Promise<ProjectModelId>
    update(id: ProjectModelId, data: ProjectModelUpdate): Promise<void>
    delete(id: ProjectModelId): Promise<void>
  }
}