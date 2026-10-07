import * as Schema from 'effect/Schema'

/** Structural equivalence for validated JSON values; object key order does not affect identity. */
export const equal = Schema.toEquivalence(Schema.Json)
