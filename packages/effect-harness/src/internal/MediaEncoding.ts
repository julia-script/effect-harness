/** Portable base64 encoding uses the standard intrinsic when the host supplies it. */
import * as Base64 from 'effect/encoding/Base64'
import * as Schema from 'effect/Schema'
import * as SchemaGetter from 'effect/SchemaGetter'

export const encode = (bytes: Uint8Array): string =>
  typeof bytes.toBase64 === 'function' ? bytes.toBase64() : Base64.encode(bytes)

export const BytesFromBase64 = Schema.String.pipe(
  Schema.decodeTo(Schema.Uint8Array, {
    decode: SchemaGetter.decodeBase64(),
    encode: SchemaGetter.transform(encode),
  }),
)
