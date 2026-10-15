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
export const initialMetadata: Metadata = { format: 12, minimumReader: 12, nextId: 2, nextSeq: 1 }

/** Validate before any record replay, including metadata supplied by another SQL writer. */
export const requireSupported = Effect.fnUntraced(function* (metadata: Metadata) {
  if (metadata.format !== 12 || metadata.minimumReader !== 12)
    return yield* Errors.make(
      'unsupportedFormat',
      'metadata',
      'Storage requires an unsupported reader',
    )
  return metadata
})
