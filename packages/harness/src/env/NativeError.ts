import * as Schema from 'effect/Schema'
import * as SchemaGetter from 'effect/SchemaGetter'
import * as Option from 'effect/Option'
import * as Result from 'effect/Result'
import * as Serialization from '../Serialization.ts'

/** Native codes are coerced only when present; the rest of a foreign error stays opaque. */
export const PresentCode = Schema.Struct({
  code: Schema.Unknown.pipe(
    Schema.decodeTo(Schema.String, {
      decode: SchemaGetter.String(),
      encode: SchemaGetter.passthroughSubtype<unknown, string>(),
    }),
  ),
})
const decode = Schema.decodeUnknownOption(PresentCode)
/** Getter/coercion defects make a code unavailable while callers retain the original foreign cause. */
export const code = (value: unknown): string | undefined =>
  Result.getOrElse(
    Serialization.attempt(() => Option.getOrUndefined(decode(value))?.code),
    () => undefined,
  )
