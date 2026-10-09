/** Persisted format and allocation counters. */
import * as Schema from 'effect/Schema'

/** One past the largest usable ID or sequence marks an exhausted counter. */
export const Counter = Schema.Finite.check(Schema.makeFilter((n) => Number.isInteger(n))).check(
  Schema.isBetween({ minimum: 1, maximum: Number.MAX_SAFE_INTEGER + 1 }),
)
export const Metadata = Schema.Struct({
  format: Schema.Literal(1),
  nextId: Counter.check(Schema.isGreaterThanOrEqualTo(2)),
  nextSeq: Counter,
})
export type Metadata = typeof Metadata.Type
export const initialMetadata: Metadata = { format: 1, nextId: 2, nextSeq: 1 }
