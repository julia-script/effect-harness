/**
 * Errors a `Harness` reports.
 */
import { Schema } from 'effect'
import { ConversationId, SubmissionId } from '../storage/schema/Ids.ts'

/** Input with `whenBusy: 'reject'` reached a busy conversation; nothing was written. */
export class ConversationBusy extends Schema.TaggedError<
  ConversationBusy,
  { readonly brand: unique symbol }
>('effect-harness/protocol/HarnessError/ConversationBusy')('ConversationBusy', {
  message: Schema.String,
  conversationId: ConversationId,
}) {}

/**
 * A submission was refused before anything was written: its input entry's kind is empty or
 * starts with `$`, or its request ID already names a submission of another type.
 */
export class InvalidSubmission extends Schema.TaggedError<
  InvalidSubmission,
  { readonly brand: unique symbol }
>('effect-harness/protocol/HarnessError/InvalidSubmission')('InvalidSubmission', {
  message: Schema.String,
}) {}

/** No submission has this ID. */
export class SubmissionNotFound extends Schema.TaggedError<
  SubmissionNotFound,
  { readonly brand: unique symbol }
>('effect-harness/protocol/HarnessError/SubmissionNotFound')('SubmissionNotFound', {
  message: Schema.String,
  submissionId: SubmissionId,
}) {}

/**
 * A named `init` failed, so its conversation was not created: no initializer has the name, its
 * payload did not decode, or the initializer failed. `cause` is the failure.
 */
export class InitFailed extends Schema.TaggedError<InitFailed, { readonly brand: unique symbol }>(
  'effect-harness/protocol/HarnessError/InitFailed',
)('InitFailed', {
  message: Schema.String,
  init: Schema.String,
  cause: Schema.optionalKey(Schema.Defect()),
}) {}

/**
 * An extension the registry refused, publishing nothing: two tools or sections of one name, a
 * section key that is invalid or reserved, or a task name another extension or a built-in task
 * already has.
 */
export class InvalidExtension extends Schema.TaggedError<
  InvalidExtension,
  { readonly brand: unique symbol }
>('effect-harness/protocol/HarnessError/InvalidExtension')('InvalidExtension', {
  message: Schema.String,
  extension: Schema.String,
}) {}
