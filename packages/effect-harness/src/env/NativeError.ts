/**
 * Safe coercion of foreign filesystem error codes.
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
 * Returns a readable foreign error code or undefined when access or coercion fails.
 *
 * @category combinators
 */
export const codeOrUndefined = (self: unknown): string | undefined =>
  Result.getOrElse(
    Serialization.attempt(() =>
      Predicate.hasProperty(self, 'code') ? Option.getOrUndefined(decode(self))?.code : undefined,
    ),
    constUndefined,
  )

/** Decoded value of the PresentCode schema.
 * @category models
 */
export type PresentCode = typeof PresentCode.Type
