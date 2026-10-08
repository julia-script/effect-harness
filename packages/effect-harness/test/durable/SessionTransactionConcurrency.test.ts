import { assertFailure, assertSuccess } from '@effect/vitest/utils'
import { rejected } from 'effect-harness/durable/StorageError'
import { assert, describe, it } from '@effect/vitest'

import * as Effect from 'effect/Effect'

import * as Scheduler from 'effect/Scheduler'

import * as Option from 'effect/Option'

import * as Record from 'effect-harness/durable/Record'

import * as Session from 'effect-harness/durable/Session'

import * as Store from 'effect-harness/durable/Store'

describe('SessionTransactionConcurrency', () => {
  it.effect('serializes concurrent task checks and writes under forced scheduler yields', () =>
    Effect.gen(function* () {
      const store = yield* Store.makeMemory
      const session = yield* Session.make.pipe(Effect.provideService(Store.Store, store))
      yield* session.root()
      const id = yield* session.transaction(
        Effect.fnUntraced(function* (tx) {
          return yield* tx.createTask({
            conversationId: Record.ROOT_CONVERSATION_ID,
            kind: 'test',
            version: 1,
            input: {},
            background: false,
            abortRequested: false,
            state: { status: 'running' },
          })
        }),
      )
      yield* session.transaction(
        Effect.fnUntraced(function* (tx) {
          const task = yield* tx.task(id).pipe(Effect.map(Option.getOrUndefined))
          if (task === undefined) return yield* Effect.die('Missing task')
          const result = yield* Effect.all(
            [
              tx
                .write({
                  _tag: 'task' as const,
                  value: { ...task, state: { status: 'terminal' } },
                })
                .pipe(Effect.result),
              tx
                .write({
                  _tag: 'task' as const,
                  value: { ...task, state: { status: 'running' } },
                })
                .pipe(Effect.result),
            ],
            { concurrency: 2 },
          ).pipe(Effect.provideService(Scheduler.MaxOpsBeforeYield, 20))
          assertSuccess(result[0], undefined)
          assertFailure(result[1], rejected('Task is terminal or cannot change conversations'))
        }),
      )
      assert.strictEqual(
        (yield* session.task(id).pipe(Effect.map(Option.getOrUndefined)))?.state.status,
        'terminal',
      )
    }),
  )
})
