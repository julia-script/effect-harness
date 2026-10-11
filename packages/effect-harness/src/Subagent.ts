/** Supervised conversation work owned by a reserved task or borrowed tool invocation. */
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as Agent from './Agent.js'
import type * as Conversation from './Conversation.js'
import * as Record from './Record.js'
import * as Submission from './Submission.js'
import type * as TaskExecution from './TaskExecution.js'
import type * as Transaction from './Transaction.js'
import type * as Execution from './ExecutionError.js'
import type { HarnessError } from './HarnessError.js'
import type { RegistryError } from './RegistryError.js'
import type * as StorageFormat from './StorageFormat.js'

export const OptionsSchema = Schema.Struct({
  key: Schema.optionalKey(Schema.String),
  mode: Schema.optionalKey(Schema.Literals(['foreground', 'background'])),
  agent: Schema.optionalKey(Agent.ChangeSchema),
  fork: Schema.optionalKey(
    Schema.Struct({ conversationId: Record.ConversationId, at: Record.EntryId }),
  ),
  restartPolicy: Schema.optionalKey(Schema.Literals(['resume', 'abandon'])),
})
export type Options = typeof OptionsSchema.Type
export const RequestSchema = Schema.Struct({
  draft: Submission.DraftSchema,
  options: Schema.optionalKey(OptionsSchema),
})
export type Request = typeof RequestSchema.Type
/** Save this data in a checkpoint. A background handle precedes child creation. */
export const HandleSchema = Schema.Struct({
  originTaskId: Record.TaskId,
  originConversationId: Record.ConversationId,
  key: Schema.NonEmptyString,
  taskId: Record.TaskId,
  mode: Schema.Literals(['foreground', 'background']),
})
export type Handle = typeof HandleSchema.Type
export const AttachmentSchema = Schema.Struct({
  conversationId: Record.ConversationId,
  submissionId: Record.SubmissionId,
})
export type Attachment = typeof AttachmentSchema.Type
export const ReceiptSchema = Schema.Struct({
  conversationId: Record.ConversationId,
  submissionId: Record.SubmissionId,
  submission: Submission.SettledSchema,
})
export type Receipt = typeof ReceiptSchema.Type
export const StateSchema = Schema.Struct({
  handle: HandleSchema,
  status: Schema.Literals(['pending', 'running', 'waiting', 'completing', 'terminal']),
  attachment: Schema.optionalKey(AttachmentSchema),
  outcome: Schema.optionalKey(Record.TaskOutcome),
  submission: Schema.optionalKey(Submission.RecordSchema),
})
export type State = typeof StateSchema.Type
export const ResultSchema = Schema.Struct({
  handle: HandleSchema,
  outcome: Record.TaskOutcome,
  attachment: Schema.optionalKey(AttachmentSchema),
  receipt: Schema.optionalKey(ReceiptSchema),
})
export type Result = typeof ResultSchema.Type
export class SubagentError extends Schema.TaggedError<SubagentError>()('SubagentError', {
  reason: Schema.Literals([
    'invalidKey',
    'conflict',
    'denied',
    'pending',
    'notFound',
    'revoked',
    'unsupported',
    'invalid',
  ]),
  message: Schema.String,
}) {}
export type Failure =
  | SubagentError
  | TaskExecution.Failure
  | Execution.Failure
  | HarnessError
  | RegistryError
  | StorageFormat.Failure

export interface Access {
  readonly check: Effect.Effect<void, Failure>
  readonly admit: (tx: Transaction.Transaction, request: Request) => Effect.Effect<Handle, Failure>
  readonly spawn: (request: Request) => Effect.Effect<Handle, Failure>
  readonly read: (handle: Handle) => Effect.Effect<State, Failure>
  readonly wait: (handle: Handle) => Effect.Effect<Result, Failure>
  readonly conversation: (
    id: Record.ConversationId,
  ) => Effect.Effect<Conversation.Conversation, Failure>
  readonly open: (handle: Handle) => Effect.Effect<Conversation.Conversation, Failure>
  readonly abort: (
    handle: Handle,
    options?: { readonly includeBackground?: boolean },
  ) => Effect.Effect<void, Failure>
}
const unavailable = Effect.fail(
  new SubagentError({
    reason: 'unsupported',
    message: 'This invocation has no supervised conversation integration',
  }),
)
/** Unsupported transports and standalone schedulers cannot fabricate native admission. */
export const unsupported: Access = {
  check: unavailable,
  admit: () => unavailable,
  spawn: () => unavailable,
  read: () => unavailable,
  wait: () => unavailable,
  conversation: () => unavailable,
  open: () => unavailable,
  abort: () => unavailable,
}
type Caller = { readonly subagent?: Access }
const access = (caller: Caller) => caller.subagent ?? unsupported
/**
 * Admission saves durable IDs, not a completed model result. Foreground work drains with its
 * owner; background creation is a second transition. Omitted keys require stable replay order.
 * Stored overrides are inherited once; host policy follows its existing preparation boundary.
 */
export const spawn = (caller: Caller, request: Request): Effect.Effect<Handle, Failure> =>
  access(caller).spawn(request)
export const read = (caller: Caller, handle: Handle): Effect.Effect<State, Failure> =>
  access(caller).read(handle)
/** Observer interruption does not cancel admitted work. The invocation ending revokes the wait. */
export const wait = (caller: Caller, handle: Handle): Effect.Effect<Result, Failure> =>
  access(caller).wait(handle)
export const open = (
  caller: Caller,
  handle: Handle,
): Effect.Effect<Conversation.Conversation, Failure> => access(caller).open(handle)
export const abort = (
  caller: Caller,
  handle: Handle,
  options?: { readonly includeBackground?: boolean },
): Effect.Effect<void, Failure> => access(caller).abort(handle, options)
/** Callback services and errors remain explicit; the returned handle retains its caller gate. */
export const withConversation = <A, E, R>(
  caller: Caller,
  handle: Handle,
  use: (conversation: Conversation.Conversation) => Effect.Effect<A, E, R>,
): Effect.Effect<A, E | Failure, R> =>
  access(caller)
    .open(handle)
    .pipe(
      Effect.flatMap(use),
      Effect.tap(() => access(caller).check),
    )
