import * as ReadAdmissionFixture from './storage/ReadAdmissionFixture.ts'

import { assertExitFailure } from '@effect/vitest/utils'

import * as Cause from 'effect/Cause'

import * as SqliteClient from '@effect/sql-sqlite-node/SqliteClient'

import { assert, describe, it } from '@effect/vitest'

import * as Deferred from 'effect/Deferred'

import * as Effect from 'effect/Effect'

import * as Fiber from 'effect/Fiber'

import * as Layer from 'effect/Layer'

import * as Scope from 'effect/Scope'

import * as Stream from 'effect/Stream'

import * as SqlClient from 'effect/sql/SqlClient'

import * as Conversation from 'effect-harness/durable/Conversation'

import * as Inbox from 'effect-harness/durable/Inbox'

import * as Record from 'effect-harness/durable/Record'

import * as Session from 'effect-harness/durable/Session'

import * as View from 'effect-harness/durable/View'

import * as Usage from 'effect-harness/durable/Usage'

const initialize = Effect.gen(function* () {
  const session = yield* Session.Session
  const views = yield* View.View
  const root = yield* session.root(
    Effect.fnUntraced(function* (tx) {
      yield* tx.doc(Conversation.AgentDoc, { owner: Record.ROOT_CONVERSATION_ID })
      yield* tx.doc(Inbox.LiveDoc, { owner: Record.ROOT_CONVERSATION_ID })
      yield* tx.doc(Inbox.InboxDoc, { owner: Record.ROOT_CONVERSATION_ID })
      yield* tx.doc(Conversation.ProviderDoc, {
        owner: Record.ROOT_CONVERSATION_ID,
        seed: 'provider-session',
      })
      yield* tx.doc(Usage.UsageDoc, { owner: Record.ROOT_CONVERSATION_ID })
    }),
  )
  return { session, views, root }
})

const append = (session: Session.Session.Service, id: Record.ConversationId, kind: string) =>
  session.transaction((tx) => tx.appendEntry(id, { kind }))

const collectSql = (watch: View.View.Watch, count: number) =>
  Stream.runCollect(watch.changes.pipe(Stream.take(count))).pipe(Effect.timeout('2 seconds'))

describe('View', () => {
  // Native SQL worker acquisition and transaction notifications progress outside TestClock.
  it.live(
    'owns acquisitions by Scope and permits cancellation while waiting for physical SQL settlement',
    () =>
      Effect.gen(function* () {
        const { session, views, root } = yield* initialize
        const scope = yield* Scope.make()
        const watch = yield* views.watch(root.id).pipe(Scope.provide(scope))
        yield* Scope.close(scope, yield* Effect.exit(Effect.void))
        assert.strictEqual(yield* watch.closed, 'cancelled')
        const sql = yield* SqlClient.SqlClient
        const entered = yield* Deferred.make<void>()
        const release = yield* Deferred.make<void>()
        const transaction = yield* sql
          .withTransaction(
            Effect.gen(function* () {
              yield* append(session, root.id, 'physical')
              yield* Deferred.succeed(entered, undefined)
              yield* Deferred.await(release)
            }),
          )
          .pipe(Effect.forkScoped)
        yield* Deferred.await(entered)
        const startRead = yield* Deferred.make<void>()
        const acquiring = yield* Deferred.await(startRead).pipe(
          Effect.andThen(views.watch(root.id)),
          Effect.forkScoped,
        )
        const admission = yield* ReadAdmissionFixture.ReadAdmission
        const read = yield* admission.track(acquiring.id)
        yield* Deferred.succeed(startRead, undefined)
        yield* read.entered
        const interrupting = yield* Fiber.interrupt(acquiring).pipe(Effect.forkScoped)
        yield* Effect.yieldNow
        yield* Deferred.succeed(release, undefined)
        yield* Fiber.join(transaction)
        yield* Fiber.join(interrupting)
        const cancelled = yield* Fiber.await(acquiring)
        assertExitFailure(cancelled, Cause.interrupt(interrupting.id))
      }).pipe(
        Effect.provide(
          Layer.mergeAll(Session.layer, View.layer).pipe(
            Layer.provideMerge(ReadAdmissionFixture.layer),
            Layer.provideMerge(
              Layer.merge(
                SqliteClient.layer({ filename: ':memory:' }),
                ReadAdmissionFixture.controls,
              ),
            ),
          ),
        ),
      ),
  )

  // Native SQL worker acquisition and transaction notifications progress outside TestClock.
  it.live('publishes neither rolled-back SQL frames nor paused outer transaction writes', () =>
    Effect.gen(function* () {
      const { session, views, root } = yield* initialize
      const watch = yield* views.watch(root.id)
      const state = yield* views.state(root.id)
      const sql = yield* SqlClient.SqlClient
      const entered = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const write = yield* sql
        .withTransaction(
          Effect.gen(function* () {
            yield* append(session, root.id, 'commit')
            yield* Deferred.succeed(entered, undefined)
            yield* Deferred.await(release)
          }),
        )
        .pipe(Effect.forkScoped)
      yield* Deferred.await(entered)
      const admission = yield* ReadAdmissionFixture.ReadAdmission
      const startRead = yield* Deferred.make<void>()
      const acquiring = yield* Deferred.await(startRead).pipe(
        Effect.andThen(views.watch(root.id)),
        Effect.forkScoped,
      )
      const read = yield* admission.track(acquiring.id)
      yield* Deferred.succeed(startRead, undefined)
      yield* read.entered
      assert.strictEqual(state.value.entries.length, 0)
      yield* Deferred.succeed(release, undefined)
      yield* Fiber.join(write)
      yield* (yield* Fiber.join(acquiring)).stop
      assert.deepStrictEqual(
        (yield* collectSql(watch, 1))[0]?.value.entries.map((entry) => entry.kind),
        ['commit'],
      )
      yield* sql
        .withTransaction(
          append(session, root.id, 'rollback').pipe(Effect.andThen(Effect.fail('rollback'))),
        )
        .pipe(Effect.ignore)
      // watch acquisition runs the public shared-mount refresh to completion before these rollback assertions.
      const refreshed = yield* views.watch(root.id)
      assert.deepStrictEqual(
        refreshed.value.entries.map((entry) => entry.kind),
        ['commit'],
      )
      yield* refreshed.stop
      // The public cursor is a positive acknowledgement that the state consumer applied the committed frame.
      yield* Effect.yieldNow.pipe(Effect.repeat({ while: () => state.cursor === 0 }))
      assert.deepStrictEqual(
        state.value.entries.map((entry) => entry.kind),
        ['commit'],
      )
    }).pipe(
      Effect.provide(
        Layer.mergeAll(Session.layer, View.layer).pipe(
          Layer.provideMerge(ReadAdmissionFixture.layer),
          Layer.provideMerge(
            Layer.merge(
              SqliteClient.layer({ filename: ':memory:' }),
              ReadAdmissionFixture.controls,
            ),
          ),
        ),
      ),
    ),
  )
})
