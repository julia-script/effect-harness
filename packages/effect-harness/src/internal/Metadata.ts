/** Persisted format and allocation counters. */
import * as Schema from 'effect/Schema'
import * as Effect from 'effect/Effect'
import * as Errors from '../StorageError.js'

/** One past the largest usable ID or sequence marks an exhausted counter. */
export const Counter = Schema.Finite.check(Schema.makeFilter((n) => Number.isInteger(n))).check(
  Schema.isBetween({ minimum: 1, maximum: Number.MAX_SAFE_INTEGER + 1 }),
)
export const Metadata = Schema.Struct({
  format: Counter,
  minimumReader: Schema.optionalKey(Counter),
  nextId: Counter.check(Schema.isGreaterThanOrEqualTo(2)),
  nextSeq: Counter,
})
export type Metadata = typeof Metadata.Type
export const initialMetadata: Metadata = { format: 1, nextId: 2, nextSeq: 1 }

/** Validate before any record replay, including metadata supplied by another SQL writer. */
export const requireSupported = Effect.fnUntraced(function* (metadata: Metadata) {
  if (metadata.format > 9 || (metadata.minimumReader ?? 1) > 9)
    return yield* Errors.make(
      'unsupportedFormat',
      'metadata',
      'Storage requires an unsupported reader',
    )
  if (metadata.format >= 2 && metadata.minimumReader !== metadata.format)
    return yield* Errors.make(
      'corrupt',
      'metadata',
      'Container format requires its matching reader',
    )
  if (metadata.format === 1 && (metadata.minimumReader ?? 1) !== 1)
    return yield* Errors.make('corrupt', 'metadata', 'Legacy format requires minimum reader 1')
  return metadata
})
