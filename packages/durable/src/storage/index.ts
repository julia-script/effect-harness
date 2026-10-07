/**
 * Platform-neutral storage constructors.
 */
// @barrel
/**
 * Append-only JSONL storage with recovery and compaction.
 *
 * @category re-exports
 */
export * as JsonlStore from './JsonlStore.ts'
/**
 * Domain snapshots stored through Effect persistence services.
 *
 * @category re-exports
 */
export * as SnapshotStore from './SnapshotStore.ts'

/**
 * Descriptor-based JSON validation for durable receipt values.
 *
 * @category re-exports
 */
export * as StrictReceiptJson from './StrictReceiptJson.ts'
