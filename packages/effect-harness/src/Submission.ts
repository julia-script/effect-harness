/** Client handle for a durable input admission and its committed outcome. */
import type * as Effect from 'effect/Effect'
import type * as Pipeable from 'effect/Pipeable'
import * as Schema from 'effect/Schema'
import * as Prompt from 'effect/ai/Prompt'
import * as DurableRecord from './Record.js'
import { dual } from 'effect/Function'
import type { HarnessError } from './HarnessError.js'
import type { HarnessBackendService } from './HarnessBackend.js'
import { Backend, SubmissionTypeId } from './internal/ClientHandle.js'

export const TypeId = SubmissionTypeId

export const InputSchema = Schema.Union([Schema.String, Schema.Array(Prompt.UserMessagePart)])
export type Input = typeof InputSchema.Type

/** Admission validates and brands the request identity; callers can supply a normal string. */
export const DraftSchema = Schema.Struct({
  type: Schema.tag('input'),
  content: InputSchema,
  requestId: Schema.optionalKey(Schema.NonEmptyString),
})
export type Draft = typeof DraftSchema.Type

export const RecordSchema = Schema.Union([
  DurableRecord.InputQueued,
  DurableRecord.InputPlaced,
  DurableRecord.InputDone,
  DurableRecord.InputUnanswered,
])
export type Record = typeof RecordSchema.Type
export const SettledSchema = Schema.Union([DurableRecord.InputDone, DurableRecord.InputUnanswered])
export type Settled = typeof SettledSchema.Type
export const WithdrawalSchema = Schema.Literals(['aborted', 'already_placed', 'settled'])
export type Withdrawal = typeof WithdrawalSchema.Type

export interface Submission extends Pipeable.Pipeable {
  readonly [TypeId]: typeof TypeId
  readonly [Backend]: HarnessBackendService
  readonly id: DurableRecord.SubmissionId
  readonly conversationId: DurableRecord.ConversationId
}

export const read: {
  (): (self: Submission) => Effect.Effect<Record, HarnessError>
  (self: Submission): Effect.Effect<Record, HarnessError>
} = dual(
  (args) => args.length !== 0,
  (self: Submission) => self[Backend].read(self.id),
)

/** Interrupting wait cancels observation, not the durable submission. */
export const wait: {
  (): (self: Submission) => Effect.Effect<Settled, HarnessError>
  (self: Submission): Effect.Effect<Settled, HarnessError>
} = dual(
  (args) => args.length !== 0,
  (self: Submission) => self[Backend].wait(self.id),
)

export const withdraw: {
  (): (self: Submission) => Effect.Effect<Withdrawal, HarnessError>
  (self: Submission): Effect.Effect<Withdrawal, HarnessError>
} = dual(
  (args) => args.length !== 0,
  (self: Submission) => self[Backend].withdraw(self.id),
)
