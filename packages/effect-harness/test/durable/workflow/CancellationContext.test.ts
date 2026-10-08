import * as Identity from 'effect-harness/durable/Identity'

import { assert, describe, it } from '@effect/vitest'

import * as Effect from 'effect/Effect'

import * as Store from 'effect-harness/durable/Store'

import * as Session from 'effect-harness/durable/Session'

import * as Ownership from 'effect-harness/durable/Ownership'

import * as Cancellation from 'effect-harness/durable/workflow/Cancellation'

describe('CancellationContext', () => {
  it.effect('uses the exact captured invocation Session instead of the ambient Session', () =>
    Effect.gen(function* () {
      const store = yield* Store.makeMemory
      const invocation = yield* Session.make.pipe(Effect.provideService(Store.Store, store))
      const ambient = yield* Session.make.pipe(Effect.provideService(Store.Store, store))
      const root = yield* invocation.root()
      const taskId = yield* invocation.transaction((tx) =>
        tx.createTask({
          conversationId: root.id,
          kind: 'identity',
          version: 1,
          input: null,
          background: false,
          abortRequested: false,
          state: { status: 'running' },
        }),
      )
      const identity = {
        sessionId: Identity.SessionId.make('invocation'),
        conversationId: root.id,
        taskId,
      }
      const current = yield* Cancellation.activity(identity, invocation, Ownership.Current).pipe(
        Effect.provide(Cancellation.layer),
        Effect.provideService(Session.Session, ambient),
      )
      assert.strictEqual(current.session, invocation)
      assert.strictEqual(current.taskId, taskId)
      assert.strictEqual((yield* current.check.pipe(Effect.flip)).reason._tag, 'ClosedError')
      assert.strictEqual(yield* invocation.isClosed, false)
    }),
  )
})
