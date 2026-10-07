import * as Schema from 'effect/Schema'

const positiveSafeInteger = Schema.Int.check(
  Schema.isBetween({ minimum: 1, maximum: Number.MAX_SAFE_INTEGER }),
)
/** Canonical committed conversation identity; its numeric encoding is shared with durable Record. */
export const ConversationId = positiveSafeInteger.pipe(
  Schema.brand('@effect-harness/durable/Record/ConversationId'),
)
export type ConversationId = typeof ConversationId.Type
/** Canonical committed entry identity; indexes and retry counters remain ordinary numbers. */
export const EntryId = positiveSafeInteger.pipe(
  Schema.brand('@effect-harness/durable/Record/EntryId'),
)
export type EntryId = typeof EntryId.Type
