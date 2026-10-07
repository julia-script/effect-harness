/**
 * Canonical conversation and entry identity codecs with numeric wire bounds.
 */
import * as Schema from 'effect/Schema'

const positiveSafeInteger = Schema.Int.check(
  Schema.isBetween({ minimum: 1, maximum: Number.MAX_SAFE_INTEGER }),
)
/**
 * Canonical committed conversation identity; its numeric encoding is shared with durable Record.
 *
 * @category schemas
 */
export const ConversationId = positiveSafeInteger.pipe(
  Schema.brand('@effect-harness/durable/Record/ConversationId'),
)
/**
 * Decoded value validated by the `ConversationId` schema.
 *
 * @category models
 */
export type ConversationId = typeof ConversationId.Type
/**
 * Canonical committed entry identity; indexes and retry counters remain ordinary numbers.
 *
 * @category schemas
 */
export const EntryId = positiveSafeInteger.pipe(
  Schema.brand('@effect-harness/durable/Record/EntryId'),
)
/**
 * Decoded value validated by the `EntryId` schema.
 *
 * @category models
 */
export type EntryId = typeof EntryId.Type
