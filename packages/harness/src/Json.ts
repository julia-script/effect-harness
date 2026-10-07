/**
 * Structural equivalence for validated JSON values.
 *
 * @since 0.0.0
 */
import { dual } from 'effect/Function'
import * as Schema from 'effect/Schema'

const equalsImpl = Schema.toEquivalence(Schema.Json)
/**
 * Compares validated JSON structurally, independent of object key order.
 *
 * @category equivalence
 * @since 0.0.0
 */
export const equals: {
  (that: Schema.Json): (self: Schema.Json) => boolean
  (self: Schema.Json, that: Schema.Json): boolean
} = dual(2, equalsImpl)
