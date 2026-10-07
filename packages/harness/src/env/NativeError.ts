/**
 * Safe coercion of foreign filesystem error codes.
 *
 * @since 0.0.0
 */
import { constUndefined } from 'effect/Function'
import * as Predicate from 'effect/Predicate'
import * as Schema from 'effect/Schema'
import * as SchemaGetter from 'effect/SchemaGetter'
import * as Option from 'effect/Option'
import * as Result from 'effect/Result'
import * as Serialization from '../Serialization.ts'

/**
 * Native codes are coerced only when present; the rest of a foreign error stays opaque.
 *
 * @category schemas
 * @since 0.0.0
 */
export const PresentCode = Schema.Struct({
  code: Schema.Unknown.pipe(
    Schema.decodeTo(Schema.String, {
      decode: SchemaGetter.String(),
      encode: SchemaGetter.passthroughSubtype<unknown, string>(),
    }),
  ),
})
const decode = Schema.decodeUnknownOption(PresentCode)
/**
 * Getter/coercion defects make a code unavailable while callers retain the original foreign cause.
 *
 * @category combinators
 * @since 0.0.0
 */
export const code = (value: unknown): string | undefined =>
  Result.getOrElse(
    Serialization.attempt(() =>
      Predicate.hasProperty(value, 'code') ? Option.getOrUndefined(decode(value))?.code : undefined,
    ),
    constUndefined,
  )
