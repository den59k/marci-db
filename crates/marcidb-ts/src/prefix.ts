// The generic query layer lives in marcidb-client/runtime (packages/marcidb-client/src/query.ts): the
// query-language types, `Op`, and the builder `Query<T>`. Everything per-model below is emitted by the
// codegen on top of it.
import type {
  JsonValue, GetResult, RefUpdate, RefUpdateStruct, RefListUpdate, RefListUpdateOrdered, RefListUpdateStruct,
  PrimitiveListUpdate, WhereValue, CompareValue, CompareNumValue, CompareStrValue, UpdateNumValue, JsonPathWhere,
  CompareRefValue, CompareRefListValue, VectorSearch, FullTextSearch, CustomSearch, CustomSearchValue,
  Op, MarciOp, MarciTransport, Sub, Query, Collection,
} from "marcidb-client/runtime";
export type { JsonValue, Op, MarciOp, MarciTransport, Sub, Query, Collection, ModelTypes } from "marcidb-client/runtime";

// A reference to a previous operation's result inside $transaction (resolved by the server).
// "0.id" — the id field of operation #0's result; "1.author.id" — a nested path
export declare function ref(path: string): any

export declare function marcidb(transport: string | MarciTransport): MarciDB;
