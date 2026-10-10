/** Metadata targets for explicit cleanup without document codecs or migration. */
import * as Schema from 'effect/Schema'
import * as Record from './Record.js'

/**
 * Address and incarnation selected from a detached snapshot. Retirement checks the
 * live version through storage metadata, or the actual staged transaction draft.
 */
export const TargetSchema = Schema.Struct({
  address: Record.DocumentAddressSchema,
  id: Record.DocumentId,
})
export type Target = typeof TargetSchema.Type

/**
 * Selects metadata from a raw or decoded snapshot without inspecting its value.
 * Retain the target when retiring content that may later become unreadable.
 * An old target must never identify a recreated incarnation.
 * Transaction.retireDocumentMetadata validates its address and identity.
 */
export const target = (snapshot: Pick<Record.StoredDocument, 'record'>): Target => ({
  address: {
    kind: snapshot.record.kind,
    scope: snapshot.record.scope,
    ...(snapshot.record.key === undefined ? {} : { key: snapshot.record.key }),
  },
  id: snapshot.record.id,
})
