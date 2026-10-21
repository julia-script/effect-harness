/**
 * How a model request fails. The `reason` decides what the generation does next: compact on
 * `ContextOverflow`, retry on `Retryable`, and end the run otherwise.
 */
import { Schema } from 'effect'
import * as overflow from './internal/overflow.ts'
import { AssistantMessage } from './Message.ts'

/** The request does not fit the model's context window; only compacting helps. */
export class ContextOverflow extends Schema.TaggedError<
  ContextOverflow,
  { readonly brand: unique symbol }
>('effect-harness/model/ModelError/ContextOverflow')('ContextOverflow', {}) {}

/** A transient failure, such as a rate limit, an overloaded provider, or a dropped connection. */
export class Retryable extends Schema.TaggedError<Retryable, { readonly brand: unique symbol }>(
  'effect-harness/model/ModelError/Retryable',
)('Retryable', {
  /** How long the provider asked to wait. */
  retryAfterMs: Schema.optionalKey(Schema.Finite),
}) {}

/** The provider refused the request, for its credentials, its content, or its shape. */
export class Rejected extends Schema.TaggedError<Rejected, { readonly brand: unique symbol }>(
  'effect-harness/model/ModelError/Rejected',
)('Rejected', {}) {}

/** No provider serves the model. */
export class UnknownModel extends Schema.TaggedError<
  UnknownModel,
  { readonly brand: unique symbol }
>('effect-harness/model/ModelError/UnknownModel')('UnknownModel', {
  provider: Schema.String,
  modelId: Schema.String,
}) {}

export const Reason = Schema.Union([ContextOverflow, Retryable, Rejected, UnknownModel])
export type Reason = typeof Reason.Type

export class ModelError extends Schema.TaggedError<ModelError, { readonly brand: unique symbol }>(
  'effect-harness/model/ModelError/ModelError',
)('ModelError', {
  message: Schema.String,
  reason: Reason,
  /**
   * What the provider returned with the failure, with its usage: the generation records it in the
   * transcript, as pi records a response that ended in `error`.
   */
  response: Schema.optionalKey(AssistantMessage),
  cause: Schema.optionalKey(Schema.Defect()),
}) {
  get isRetryable(): boolean {
    return this.reason._tag === 'Retryable'
  }
}

/**
 * Whether a provider's error text reports a context overflow, by the patterns pi-ai matches, so a
 * provider can fail `ContextOverflow` rather than `Rejected`.
 */
export const isOverflowText: (text: string) => boolean = overflow.isOverflowText
