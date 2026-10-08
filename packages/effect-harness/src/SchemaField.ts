/**
 * Compatible optional domain fields with encoded omission semantics.
 */
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as SchemaGetter from 'effect/SchemaGetter'
import * as Predicate from 'effect/Predicate'

/**
 * Creates an optional domain-field codec accepting undefined and omitting it from encoded objects.
 *
 * @category combinators
 */
export const optional = <S extends Schema.Constraint>(
  schema: S,
): Schema.decodeTo<Schema.optional<Schema.toType<S>>, Schema.optional<S>, never, never> =>
  Schema.optional(schema).pipe(
    Schema.decodeTo(Schema.optional(Schema.toType(schema)), {
      decode: SchemaGetter.passthrough(),
      encode: SchemaGetter.transformOptional(Option.filter(Predicate.isNotUndefined)),
    }),
  )
