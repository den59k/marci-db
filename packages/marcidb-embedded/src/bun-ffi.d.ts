// `bun:ffi` is provided by the Bun runtime and is only ever loaded via a dynamic `import("bun:ffi")` guarded
// by a runtime check (so Node never resolves it). Declare it here so `tsc` type-checks the package without
// pulling in `bun-types`; the loader casts the import result to `any`.
declare module "bun:ffi";
