import { expect, test } from 'tstyche'
import type * as Effect from 'effect/Effect'
import type * as Record from 'effect-harness/Record'
import type { Failure } from 'effect-harness/SessionError'
import * as TaskPolicy from 'effect-harness/TaskPolicy'
import * as Transaction from 'effect-harness/Transaction'

declare const task: Record.Task
declare const tx: Transaction.Transaction

test('preserves typed persisted policy/intent and transaction failure channels', () => {
  expect(TaskPolicy.restartPolicy(task)).type.toBe<'resume' | 'abandon'>()
  expect(TaskPolicy.abortIntent(task)).type.toBe<TaskPolicy.AbortIntent | undefined>()
  expect(TaskPolicy.recover(task)).type.toBe<Record.Task>()
  expect(TaskPolicy.requestAbort(task, 'user cancelled')).type.toBe<Record.Task>()
  expect(Transaction.requestTaskAbort(tx, task.id, 'user cancelled')).type.toBe<
    Effect.Effect<void, Failure>
  >()
  expect(Transaction.requestTaskAbort(task.id)(tx)).type.toBe<Effect.Effect<void, Failure>>()
  expect<Transaction.TaskCreate['restartPolicy']>().type.toBe<'resume' | 'abandon' | undefined>()
  expect<TaskPolicy.SchedulerFormat>().type.toBe<{
    readonly version: 1
    readonly minimumReader: 1
  }>()
  expect(TaskPolicy.requestAbort(task, 1)).type.toRaiseError()
})
