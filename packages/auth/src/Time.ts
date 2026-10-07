/**
 * Exact finite epoch-millisecond codecs for UTC domain timestamps.
 *
 * @since 0.0.0
 */
import * as DateTime from 'effect/DateTime'
import * as Schema from 'effect/Schema'
import * as SchemaGetter from 'effect/SchemaGetter'

const origin = DateTime.fromEpochSeconds(0)
/**
 * Converts a platform/protocol finite millisecond value without JavaScript Date truncation.
 *
 * @category constructors
 * @since 0.0.0
 */
export const fromEpochMillis = (millis: number): DateTime.Utc =>
  DateTime.mapEpochMillis(origin, () => millis)
/**
 * Domain instant codec retaining the exact finite numeric epoch-millisecond storage representation.
 *
 * @category models
 * @since 0.0.0
 */
export const EpochMillis = Schema.Finite.pipe(
  Schema.decodeTo(Schema.DateTimeUtc, {
    decode: SchemaGetter.transform(fromEpochMillis),
    encode: SchemaGetter.transform(DateTime.toEpochMillis),
  }),
)
