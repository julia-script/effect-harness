import { expect, test } from 'tstyche'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import type * as Schema from 'effect/Schema'
import * as CompactionTask from '../src/CompactionTask.js'
import type * as CompactionRequest from '../src/CompactionRequest.js'
import * as Conversation from '../src/Conversation.js'
import type { HarnessBackend } from '../src/HarnessBackend.js'
import type { HarnessError } from '../src/HarnessError.js'
import type { ModelError } from '../src/ModelError.js'
import * as Model from '../src/Model.js'
import * as Record from '../src/Record.js'
import type { Failure } from '../src/SessionError.js'
import type { CompactionError } from '../src/CompactionError.js'
import type { ContextProjectionError } from '../src/ContextProjectionError.js'
import type * as Transaction from '../src/Transaction.js'
import * as Inbox from '../src/internal/Inbox.js'

class Service extends Context.Service<Service, { readonly value: string }>()(
  'compaction/types/Service',
) {}
declare const handle: CompactionTask.Handle
declare const conversation: Conversation.Conversation

declare const capability: Model.CompactionCapability
declare const context: Context.Context<never>
declare const policy: Model.PreparationPolicy & CompactionRequest.Policy
declare const tx: Transaction.Transaction
declare const draft: Inbox.Draft

test('task handles are data and fixed task observers expose their backend requirement', () => {
  expect<CompactionTask.Handle>().type.toBeAssignableTo<{
    readonly id: Record.TaskId
    readonly conversationId: Record.ConversationId
  }>()
  expect(CompactionTask.read(handle)).type.toBe<
    Effect.Effect<Record.Task, HarnessError, HarnessBackend>
  >()
  expect(CompactionTask.wait(handle)).type.toBe<
    Effect.Effect<CompactionTask.Outcome, HarnessError, HarnessBackend>
  >()
  expect(CompactionTask.abort(handle)).type.toBe<
    Effect.Effect<void, HarnessError, HarnessBackend>
  >()
  expect(Conversation.compact(conversation, { instructions: 'Keep exact paths' })).type.toBe<
    Effect.Effect<CompactionTask.Handle, HarnessError>
  >()
  expect<{
    _tag: 'Queued'
    submissionId: Record.SubmissionId
  }>().type.toBeAssignableTo<CompactionTask.Result>()
  expect<{
    _tag: 'Queued'
    entryId: Record.EntryId
  }>().type.not.toBeAssignableTo<CompactionTask.Result>()
  expect<CompactionRequest.Policy['cacheRetention']>().type.toBe<'none'>()
  expect<CompactionRequest.Policy['deferred']>().type.toBe<false>()
})

test('summary adapter construction captures declared callback services honestly', () => {
  const serviceful = Model.makeCompaction({
    binding: { id: 'types/summary', version: 1 },
    configure: () =>
      Effect.gen(function* () {
        yield* Service
        return Context.empty()
      }),
  })
  expect(serviceful).type.toBe<
    Effect.Effect<Model.CompactionCapability, Schema.SchemaError, Service>
  >()
  const independent = Model.makeCompaction({
    binding: { id: 'types/summary', version: 1 },
    configure: (context) => Effect.succeed(context),
  })
  expect(independent).type.toBe<Effect.Effect<Model.CompactionCapability, Schema.SchemaError>>()
  expect(capability.configure(context, policy)).type.toBe<
    Effect.Effect<Context.Context<never>, ModelError>
  >()
})

test('only private fenced or dynamic inbox drafts expose projection and compaction errors', () => {
  expect(
    Inbox.admit(tx, Record.ROOT_CONVERSATION_ID, { mode: 'write', payload: { kind: 'input' } }),
  ).type.toBe<Effect.Effect<Record.Submission, Failure>>()
  expect(Inbox.admit(tx, Record.ROOT_CONVERSATION_ID, draft)).type.toBe<
    Effect.Effect<Record.Submission, Failure | CompactionError | ContextProjectionError>
  >()
})
