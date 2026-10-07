/**
 * Strict JSON codec boundaries for durable persistence.
 */
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as SchemaGetter from 'effect/SchemaGetter'

/**
 * Bridge a known domain schema to JSON through its own encoder, retaining JSON's omission of undefined object fields.
 *
 * @category schemas
 */
export const json = <S extends Schema.Constraint>(
  schema: S,
): Schema.decodeTo<Schema.fromJsonString<S>, typeof Schema.Json, never, never> =>
  Schema.Json.pipe(
    Schema.decodeTo(Schema.fromJsonString(schema), {
      decode: SchemaGetter.transformEffect((input) =>
        Schema.encodeEffect(Schema.fromJsonString(Schema.Json))(input).pipe(
          Effect.mapError((error) => error.issue),
        ),
      ),
      encode: SchemaGetter.transformEffect((input) =>
        Schema.decodeEffect(Schema.fromJsonString(Schema.Json))(input).pipe(
          Effect.mapError((error) => error.issue),
        ),
      ),
    }),
  )
/**
 * Object-only storage boundary; decoded models need not themselves have a JSON index signature.
 *
 * @category schemas
 */
export const object = <S extends Schema.Constraint>(
  schema: S,
): Schema.decodeTo<Schema.fromJsonString<S>, typeof Schema.JsonObject, never, never> =>
  Schema.JsonObject.pipe(
    Schema.decodeTo(Schema.fromJsonString(schema), {
      decode: SchemaGetter.transformEffect((input) =>
        Schema.encodeEffect(Schema.fromJsonString(Schema.JsonObject))(input).pipe(
          Effect.mapError((error) => error.issue),
        ),
      ),
      encode: SchemaGetter.transformEffect((input) =>
        Schema.decodeEffect(Schema.fromJsonString(Schema.JsonObject))(input).pipe(
          Effect.mapError((error) => error.issue),
        ),
      ),
    }),
  )
