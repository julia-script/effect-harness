/**
 * Platform-neutral JSONL and SQL storage constructors.
 *
 * @since 0.0.0
 */
// @barrel
// effect-review-allow P9-barrel-namespace-only: BunSqliteStore imports bun:sqlite and NodeSqliteStore needs its optional native driver. Import those deliberate platform leaf entrypoints directly; compatibility driver facades are also leaf-only.
/**
 * @since 0.0.0
 */
export * as JsonlStore from './JsonlStore.ts'
/**
 * @since 0.0.0
 */
export * as SqliteStore from './SqliteStore.ts'

/**
 * @since 0.0.0
 */
export * as StrictReceiptJson from './StrictReceiptJson.ts'
