/**
 * Errors a `Tracker` reports.
 */
import { Schema } from 'effect'

/**
 * The draft rejected a write it cannot represent: a value that is not strict JSON, an array hole,
 * an invalid length, a symbol key, or a property definition. The rejected write left the draft
 * unchanged, and the change can no longer be prepared.
 */
export class InvalidWrite extends Schema.TaggedError<
  InvalidWrite,
  { readonly brand: unique symbol }
>('effect-harness/delta/DeltaError/InvalidWrite')('InvalidWrite', {
  message: Schema.String,
}) {}

/** The change was already prepared or aborted, or another change of its tracker was adopted. */
export class ChangeSettled extends Schema.TaggedError<
  ChangeSettled,
  { readonly brand: unique symbol }
>('effect-harness/delta/DeltaError/ChangeSettled')('ChangeSettled', {
  message: Schema.String,
}) {}

/** Why `adopt()` refused a preparation. */
export const AdoptReason = Schema.Literals(['foreign', 'consumed', 'aborted', 'stale', 'notReady'])
export type AdoptReason = typeof AdoptReason.Type

/**
 * `adopt()` refused a preparation: it belongs to another tracker, was already adopted, was
 * aborted, or is stale because the tracker adopted another revision since. The tracker is
 * unchanged.
 */
export class AdoptRejected extends Schema.TaggedError<
  AdoptRejected,
  { readonly brand: unique symbol }
>('effect-harness/delta/DeltaError/AdoptRejected')('AdoptRejected', {
  message: Schema.String,
  reason: AdoptReason,
}) {}
