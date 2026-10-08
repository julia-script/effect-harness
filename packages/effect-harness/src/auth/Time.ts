import * as Function from 'effect/Function'
/**
 * Exact finite epoch-millisecond codecs for UTC domain timestamps.
 */
import * as DateTime from 'effect/DateTime'
import * as Schema from 'effect/Schema'
import * as SchemaGetter from 'effect/SchemaGetter'

const origin = DateTime.fromEpochSeconds(0)
/**
 * Converts a platform/protocol finite millisecond value without JavaScript Date truncation.
 *
 * @category constructors
 */
export const fromEpochMillis = (millis: number): DateTime.Utc =>
  DateTime.mapEpochMillis(origin, Function.constant(millis))
/**
 * Domain instant codec retaining the exact finite numeric epoch-millisecond storage representation.
 *
 * @category models
 */
export const EpochMillis = Schema.Finite.pipe(
  Schema.decodeTo(Schema.DateTimeUtc, {
    decode: SchemaGetter.transform(fromEpochMillis),
    encode: SchemaGetter.transform(DateTime.toEpochMillis),
  }),
)
/**
 * Native UTC instant encoded as exact finite epoch milliseconds.
 *
 * @category models
 */
export type EpochMillis = typeof EpochMillis.Type
