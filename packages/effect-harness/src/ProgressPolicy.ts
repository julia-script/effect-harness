import * as Schema from 'effect/Schema'

/** Each successful write buys a pause from its start, including its encoded snapshot bytes. */
export const Policy = Schema.Struct({
  /** Milliseconds. Zero permits immediate writes when the byte rate permits them. */
  minInterval: Schema.Int.check(
    Schema.isGreaterThanOrEqualTo(0),
    Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER),
  ),
  /** Positive finite UTF-8 bytes per second; the writer measures the complete encoded value. */
  bytesPerSecond: Schema.Finite.check(Schema.isGreaterThan(0)),
})
export type Policy = typeof Policy.Type

export const defaultPolicy: Policy = { minInterval: 100, bytesPerSecond: 100 * 1024 }
