/** Detached version upgrades for reads; persistent acquisition remains a Transaction operation. */
import * as Schema from 'effect/Schema'
import type * as Document from './Document.js'
import * as Record from './Record.js'
import * as Value from './internal/DocumentValue.js'

/** Original storage metadata and a value decoded at the supplied definition's version. */
export type Snapshot<S extends Document.Codec> = Omit<
  Record.StoredDocument,
  'version' | 'value'
> & {
  readonly storedVersion: number
  readonly effectiveVersion: number
  readonly value: S['Type']
}

/** Schema for a detached materialized revision, retaining target codec requirements. */
export const SnapshotSchema = <S extends Document.Codec>(schema: S) =>
  Schema.Struct({
    record: Record.DocumentRecordSchema,
    storedVersion: Schema.Int.check(Schema.isGreaterThanOrEqualTo(1)),
    effectiveVersion: Schema.Int.check(Schema.isGreaterThanOrEqualTo(1)),
    value: schema,
    deltasSinceBase: Schema.Natural,
  })

/**
 * Applies a direct source-version migration to owned encoded JSON and decodes the result.
 * Preserves the original record, storedVersion and replay count; effectiveVersion belongs
 * to the supplied definition. Does not write storage or replace encoded caches.
 * Missing migrations, future versions and policy mismatches conflict. Invalid values and
 * callback failures remain semantic failures; interruption propagates. Callbacks must only
 * transform values, because their external effects cannot be rolled back.
 */
export const materialize = Value.materialize
