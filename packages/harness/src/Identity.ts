/**
 * Canonical conversation and entry identity codecs with numeric wire bounds.
 *
 * @since 0.0.0
 */
import * as Schema from 'effect/Schema'

const positiveSafeInteger = Schema.Int.check(
  Schema.isBetween({ minimum: 1, maximum: Number.MAX_SAFE_INTEGER }),
)
/**
 * Canonical committed conversation identity; its numeric encoding is shared with durable Record.
 *
 * @category schemas
 * @since 0.0.0
 */
export const ConversationId = positiveSafeInteger.pipe(
  Schema.brand('@effect-harness/durable/Record/ConversationId'),
)
/**
 * Identity conversation id contract.
 *
 * @category models
 * @since 0.0.0
 */
export type ConversationId = typeof ConversationId.Type
/**
 * Canonical committed entry identity; indexes and retry counters remain ordinary numbers.
 *
 * @category schemas
 * @since 0.0.0
 */
export const EntryId = positiveSafeInteger.pipe(
  Schema.brand('@effect-harness/durable/Record/EntryId'),
)
/**
 * Identity entry id contract.
 *
 * @category models
 * @since 0.0.0
 */
export type EntryId = typeof EntryId.Type
