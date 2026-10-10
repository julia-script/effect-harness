import { expect, test } from 'tstyche'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import type * as Scope from 'effect/Scope'
import type * as Stream from 'effect/Stream'
import type * as Record from 'effect-harness/Record'
import type * as Registry from 'effect-harness/Registry'
import type * as Session from 'effect-harness/Session'
import * as Task from 'effect-harness/Task'
import type * as TaskExecution from 'effect-harness/TaskExecution'
import type * as Transaction from 'effect-harness/Transaction'
import * as TaskSupervision from 'effect-harness/TaskSupervision'

const child = Task.make({
  kind: 'types.supervised',
  version: 1,
  input: Schema.Natural,
  checkpoint: Schema.Struct({ phase: Schema.Literal('work') }),
  success: Schema.Natural,
  failure: Schema.String,
  initial: () => ({ phase: 'work' as const }),
  phases: { work: () => Effect.void },
})
declare const session: Session.Session
declare const supervisor: TaskSupervision.Service
declare const access: TaskExecution.Access
declare const tx: Transaction.Transaction
declare const taskId: Record.TaskId
declare const conversationId: Record.ConversationId
declare const entryId: Record.EntryId

test('native construction and metadata retain explicit scope and error requirements', () => {
  expect(TaskSupervision.make(session)).type.toBe<
    Effect.Effect<TaskSupervision.Service, TaskSupervision.Failure, Registry.Registry | Scope.Scope>
  >()
  expect(Effect.runPromise(TaskSupervision.make(session))).type.toRaiseError()
  expect(supervisor.graph).type.toBe<
    Effect.Effect<TaskSupervision.Graph, TaskSupervision.Failure>
  >()
  expect(supervisor.watchGraph).type.toBe<
    Stream.Stream<TaskSupervision.GraphFrame, TaskSupervision.Failure>
  >()
  expect(supervisor.abort(taskId, { includeBackground: true })).type.toBe<
    Effect.Effect<void, TaskSupervision.Failure>
  >()
  expect(supervisor.abortConversation(conversationId)).type.toBe<
    Effect.Effect<void, TaskSupervision.Failure>
  >()
  expect(supervisor.outcomes([taskId, taskId])).type.toBe<
    Effect.Effect<ReadonlyArray<Record.TaskOutcome>, TaskSupervision.Failure>
  >()
  expect(supervisor.waitForTask(taskId)).type.toBe<
    Effect.Effect<Record.Task, TaskSupervision.Failure>
  >()
})

test('owned creation preserves typed contracts and requires native IDs', () => {
  expect(access.createChild(tx, child, 1)).type.toBe<
    Effect.Effect<Task.Ref<typeof child>, TaskExecution.Failure>
  >()
  expect(access.createChild(tx, child, '1')).type.toRaiseError()
  expect(access.createBackgroundSupervisor(tx, child, 1, { restartPolicy: 'abandon' })).type.toBe<
    Effect.Effect<Task.Ref<typeof child>, TaskExecution.Failure>
  >()
  expect(access.createConversationTask(tx, conversationId, child, 1)).type.toBe<
    Effect.Effect<Task.Ref<typeof child>, TaskExecution.Failure>
  >()
  expect(access.createConversationTask(tx, taskId, child, 1)).type.toRaiseError()
  expect(
    access.createConversation(tx, {
      fork: { conversationId, at: entryId },
      agent: { model: null },
    }),
  ).type.toBe<Effect.Effect<Record.Conversation, TaskExecution.Failure>>()
  expect(
    access.createConversation(tx, { fork: { conversationId, at: taskId } }),
  ).type.toRaiseError()
  expect(access.outcomes([taskId])).type.toBe<
    Effect.Effect<ReadonlyArray<Record.TaskOutcome>, TaskExecution.Failure>
  >()
})
