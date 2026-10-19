/**
 * Replaying op batches onto immutable JSON values, as a watcher of a document or a view does.
 */
import type { Schema } from 'effect'
import { cast } from 'effect/Function'
import type { Op } from '../storage/schema/Document.ts'
import { applyTrusted } from './internal/apply.ts'

/**
 * `value` with `ops` applied, sharing what they leave untouched; `value` is not changed. The ops
 * must be ones a watch of `value` delivered, which keep its shape.
 */
export const apply = <T extends Schema.Json>(value: T, ops: ReadonlyArray<Op>): T =>
  // A watch's ops turn a value of its type into the next value of that type.
  cast<Schema.Json, T>(applyTrusted(value, ops))
