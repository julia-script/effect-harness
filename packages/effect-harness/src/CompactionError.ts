import * as Schema from 'effect/Schema'
import type * as AiError from 'effect/ai/AiError'

/** Semantic compaction failures. A native provider failure remains available as cause. */
export class CompactionError extends Schema.TaggedError<CompactionError>()('CompactionError', {
  reason: Schema.Literals([
    'invalid',
    'noModel',
    'invalidSummary',
    'failed',
    'stale',
    'contextOverflow',
  ]),
  message: Schema.String,
  cause: Schema.optionalKey(Schema.Defect()),
}) {}

export type Failure = CompactionError | Schema.SchemaError

/**
 * Provider adapters classify overflow using their own structured error evidence.
 * Native AiError has no ContextOverflow reason; this boundary does not guess from error text.
 */
export const modelFailure = (cause: AiError.AiError, contextOverflow = false): CompactionError =>
  new CompactionError({
    reason: contextOverflow ? 'contextOverflow' : 'failed',
    message: cause.message,
    cause,
  })
