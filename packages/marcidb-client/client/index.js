// Re-export the generated client (default location: node_modules/.marcidb/client). A relative path is
// used deliberately: the bare specifier `.marcidb/client` is an INVALID ESM specifier on Node
// (ERR_INVALID_MODULE_SPECIFIER — it starts with "." but not "./"). A `../` relative path is valid on
// Node and Bun, and resolves the runtime-generated dir without needing it installed as a package.
export * from '../../.marcidb/client/index.js'
