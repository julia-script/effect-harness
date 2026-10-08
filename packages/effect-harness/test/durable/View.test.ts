import * as TestClock from 'effect/testing/TestClock'

import { ResourceScope } from 'effect-harness/durable/testing/Storage'

import { withLayer } from './StorageFixture.ts'

import * as Exit from 'effect/Exit'

import { assert, describe, it } from '@effect/vitest'

import * as Deferred from 'effect/Deferred'

import * as Effect from 'effect/Effect'

import * as Fiber from 'effect/Fiber'

import * as Layer from 'effect/Layer'

import * as Scope from 'effect/Scope'

import * as Schema from 'effect/Schema'

import * as Stream from 'effect/Stream'

import * as Conversation from 'effect-harness/durable/Conversation'

import * as Document from 'effect-harness/durable/Document'

import * as Inbox from 'effect-harness/durable/Inbox'

import * as Record from 'effect-harness/durable/Record'

import * as Session from 'effect-harness/durable/Session'

import * as View from 'effect-harness/durable/View'

import * as Usage from 'effect-harness/durable/Usage'

import * as Store from 'effect-harness/durable/Store'

const layers = Layer.mergeAll(Session.layer, View.layer).pipe(Layer.provideMerge(Store.layerMemory))

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

const collect = (watch: View.View.Watch, count: number) =>
  Effect.gen(function* () {
    const consumer = yield* Stream.runCollect(watch.changes.pipe(Stream.take(count))).pipe(
      Effect.timeout('2 seconds'),
      Effect.forkScoped,
    )
    // The fork admits the consumer before modeled journal polls and mount monitoring advance.
    yield* TestClock.adjust('100 millis')
    return yield* Fiber.join(consumer)
  })

const append = (session: Session.Session.Service, id: Record.ConversationId, kind: string) =>
  session.transaction((tx) => tx.appendEntry(id, { kind }))

const settle = TestClock.adjust('65 millis')

describe('View', () => {
  it.effect('rejects operations whose completed value violates the view schema', () =>
    Effect.gen(function* () {
      const { views, root } = yield* initialize
      const value = (yield* views.watch(root.id)).value
      for (const ops of [
        [['set', ['conversation'], 'invalid']],
        [['delete', ['conversation']]],
        [['set', ['entries'], ['invalid']]],
      ] satisfies Array<Array<View.Op>>) {
        const error = yield* Effect.fromResult(View.apply(value, ops)).pipe(Effect.flip)
        assert.instanceOf(error, View.ViewOperationError)
      }
      const restored = yield* Effect.fromResult(
        View.apply(value, [
          ['set', ['conversation'], 'temporary invalid value'],
          ['set', ['conversation'], value.conversation],
        ]),
      )
      assert.strictEqual(restored.entries, value.entries)
      assert.strictEqual(restored.docs, value.docs)
      assert.deepStrictEqual(restored, value)
    }).pipe(Effect.provide(layers)),
  )

  it.effect(
    'rehydrates a complete snapshot when retained early document frames hide a later journal gap',
    () =>
      Effect.gen(function* () {
        const { session, views, root } = yield* initialize
        const watch = yield* views.watch(root.id)
        const entered = yield* Deferred.make<void>()
        const release = yield* Deferred.make<void>()
        const holding = yield* views
          .observe(
            root.id,
            View.makeProjection({
              initial: () =>
                Deferred.succeed(entered, undefined).pipe(
                  Effect.andThen(Deferred.await(release)),
                  Effect.as(0),
                ),
              project: (change) => Effect.succeed(change.reset ? 1 : undefined),
              reset: () => Effect.succeed(0),
            }),
          )
          .pipe(Effect.forkScoped)
        yield* Deferred.await(entered)
        yield* session.transaction(
          Effect.fnUntraced(function* (tx) {
            const agent = yield* tx.doc(Conversation.AgentDoc, { owner: root.id })
            agent.instructions = 'keeps this early frame'
            yield* tx.appendEntry(root.id, { kind: 'early' })
          }),
        )
        for (let index = 0; index < 180; index++) yield* append(session, root.id, 'later')
        yield* Deferred.succeed(release, undefined)
        yield* Fiber.join(holding)
        const frame = (yield* collect(watch, 1))[0]
        assert.strictEqual(frame?.reset, true)
        assert.strictEqual(frame?.value.entries.length, 181)
      }).pipe(Effect.provide(layers)),
  )

  it.effect('hydrates shared mounts, replays exact frames and preserves unchanged branches', () =>
    Effect.gen(function* () {
      const { session, views, root } = yield* initialize
      const one = yield* views.watch(root.id)
      const two = yield* views.watch(root.id)
      assert.strictEqual(one.value, two.value)
      assert.deepStrictEqual(Object.keys(one.value.docs).sort(), [
        'harness.agent',
        'harness.inbox',
        'harness.live',
        'harness.provider',
        'harness.usage',
      ])
      const initial = one.value
      yield* session.transaction(
        Effect.fnUntraced(function* (tx) {
          const agent = yield* tx.doc(Conversation.AgentDoc, { owner: root.id })
          agent.instructions = 'be concise'
        }),
      )
      yield* append(session, root.id, 'note')
      const frames = yield* collect(one, 2)
      assert.deepStrictEqual(frames[0]?.ops, [
        ['set', ['docs', 'harness.agent', 'instructions'], 'be concise'],
      ])
      assert.strictEqual(frames[0]?.value.entries, initial.entries)
      assert.strictEqual(frames[0]?.value.docs['harness.live'], initial.docs['harness.live'])
      assert.strictEqual(frames[1]?.value.docs, frames[0]?.value.docs)
      let replay = initial
      for (const frame of frames) {
        replay = yield* Effect.fromResult(View.apply(replay, frame.ops))
        assert.deepStrictEqual(replay, frame.value)
      }
      const late = yield* views.watch(root.id)
      assert.strictEqual(late.value, frames[1]?.value)
      yield* one.stop
      yield* append(session, root.id, 'later')
      assert.deepStrictEqual(
        (yield* collect(late, 1))[0]?.value.entries.map((entry) => entry.kind),
        ['note', 'later'],
      )
      assert.strictEqual(yield* one.closed, 'stopped')
    }).pipe(Effect.provide(layers)),
  )

  it.effect('cuts inherited entries and follows only fork commits', () =>
    Effect.gen(function* () {
      const { session, views, root } = yield* initialize
      const a = yield* append(session, root.id, 'a')
      const b = yield* append(session, root.id, 'b')
      const child = yield* session.transaction((tx) =>
        tx.forkConversation(root.id, b.id, {
          ownership: { _tag: 'ownerless' as const },
        }),
      )
      const watch = yield* views.watch(child.id)
      assert.deepStrictEqual(
        watch.value.entries.map((entry) => entry.id),
        [a.id, b.id],
      )
      yield* append(session, root.id, 'parent-only')
      const summary = yield* session.transaction((tx) =>
        tx.appendEntry(child.id, { kind: 'summary', head: b.id }),
      )
      const frames = yield* collect(watch, 1)
      assert.deepStrictEqual(
        frames[0]?.value.entries.map((entry) => entry.kind),
        ['summary', 'b'],
      )
      assert.deepStrictEqual(frames[0]?.ops, [['splice', ['entries'], 0, 1, [summary]]])
    }).pipe(Effect.provide(layers)),
  )

  it.effect('raw heads never restore entries a live mount has cut; a new mount hydrates them', () =>
    Effect.gen(function* () {
      const { session, views, root } = yield* initialize
      const old = yield* append(session, root.id, 'old')
      yield* session.transaction((tx) => tx.appendEntry(root.id, { kind: 'reset', head: 'self' }))
      const watch = yield* views.watch(root.id)
      yield* session.transaction((tx) => tx.appendEntry(root.id, { kind: 'summary', head: old.id }))
      assert.deepStrictEqual(
        (yield* collect(watch, 1))[0]?.value.entries.map((entry) => entry.kind),
        ['summary'],
      )
      yield* watch.stop
      const fresh = yield* views.watch(root.id)
      assert.deepStrictEqual(
        fresh.value.entries.map((entry) => entry.kind),
        ['summary', 'old'],
      )
    }).pipe(Effect.provide(layers)),
  )

  it.effect(
    'retirement and recreation mount new incarnations whole; unrelated documents do not publish',
    () =>
      Effect.gen(function* () {
        const { session, views, root } = yield* initialize
        const watch = yield* views.watch(root.id)
        const other = Document.defineUnsafe({
          kind: 'app.other',
          version: 1,
          scope: 'session',
          schema: Schema.Struct({ n: Schema.Int }),
          initial: () => ({ n: 0 }),
        })
        yield* session.transaction((tx) => tx.doc(other).pipe(Effect.asVoid))
        yield* session.transaction((tx) => tx.retire(Inbox.LiveDoc, { owner: root.id }))
        yield* session.transaction(
          Effect.fnUntraced(function* (tx) {
            const live = yield* tx.doc(Inbox.LiveDoc, { owner: root.id })
            live.tools = []
          }),
        )
        const frames = yield* collect(watch, 2)
        assert.deepStrictEqual(
          frames.map((frame) => frame.ops),
          [
            [['delete', ['docs', 'harness.live']]],
            [['set', ['docs', 'harness.live'], { tools: [] }]],
          ],
        )
      }).pipe(Effect.provide(layers)),
  )

  it.effect('keeps relevant backlog exact across more than101 unrelated commits', () =>
    Effect.gen(function* () {
      const { session, views, root } = yield* initialize
      const other = yield* session.transaction((tx) =>
        tx.createConversation({ ownership: { _tag: 'ownerless' as const } }),
      )
      const watch = yield* views.watch(root.id)
      yield* append(session, root.id, 'exact')
      for (let index = 0; index < 115; index++) yield* append(session, other.id, 'unrelated')
      yield* append(session, root.id, 'exact-again')
      const frames = yield* collect(watch, 2)
      assert.deepStrictEqual(
        frames.map((frame) => frame.reset),
        [false, false],
      )
      assert.deepStrictEqual(
        frames[1]?.value.entries.map((entry) => entry.kind),
        ['exact', 'exact-again'],
      )
    }).pipe(Effect.provide(layers)),
  )

  it.effect('replaces101 pending frames with one latest reset and applies subsequent deltas', () =>
    Effect.gen(function* () {
      const { session, views, root } = yield* initialize
      const watch = yield* views.watch(root.id)
      for (let index = 0; index < 101; index++) yield* append(session, root.id, 'note')
      yield* settle
      const frames = yield* collect(watch, 1)
      assert.strictEqual(frames[0]?.reset, true)
      assert.strictEqual(frames[0]?.value.entries.length, 101)
      assert.ok(frames[0])
      assert.deepStrictEqual(frames[0].ops, [['replace', frames[0].value]])
    }).pipe(Effect.provide(layers)),
  )

  it.effect('excludes an in-flight callback from the100 pending limit and stops immediately', () =>
    Effect.gen(function* () {
      const { session, views, root } = yield* initialize
      const watch = yield* views.watch(root.id)
      const entered = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const received: View.Change[] = []
      const listening = yield* watch
        .listen((change) =>
          Effect.gen(function* () {
            received.push(change)
            if (received.length === 1) {
              yield* Deferred.succeed(entered, undefined)
              yield* Deferred.await(release)
            }
          }),
        )
        .pipe(Effect.forkScoped)
      yield* append(session, root.id, 'inflight')
      yield* TestClock.adjust('65 millis')
      yield* Deferred.await(entered)
      for (let index = 0; index < 100; index++) yield* append(session, root.id, 'pending')
      yield* settle
      yield* Deferred.succeed(release, undefined)
      yield* settle
      assert.strictEqual(received.length, 101)
      assert.ok(received.every((frame) => !frame.reset))
      yield* watch.stop
      assert.strictEqual(yield* watch.closed, 'stopped')
      yield* Fiber.join(listening)
    }).pipe(Effect.provide(layers)),
  )

  it.effect(
    'isolates a failed listener, keeps states live, drops mounts and closes all on Session shutdown',
    () =>
      Effect.gen(function* () {
        const { session, views, root } = yield* initialize
        const bad = yield* views.watch(root.id)
        const stateScope = yield* Effect.acquireRelease(Scope.make(), (scope, exit) =>
          Scope.close(scope, exit),
        )
        const state = yield* views.state(root.id).pipe(Scope.provide(stateScope))
        const listener = yield* bad
          .listen(() => Effect.fail('listener'))
          .pipe(Effect.exit, Effect.forkScoped)
        yield* append(session, root.id, 'one')
        yield* TestClock.adjust('65 millis')
        assert.strictEqual(yield* bad.closed, 'listener_error')
        yield* Fiber.join(listener)
        yield* settle
        assert.strictEqual(state.value.entries.length, 1)
        yield* Scope.close(stateScope, Exit.void)
        const rebuilt = yield* views.watch(root.id)
        assert.notStrictEqual(rebuilt.value, state.value)
        yield* Scope.close(yield* ResourceScope, Exit.void)
        yield* TestClock.adjust('65 millis')
        assert.strictEqual(yield* rebuilt.closed, 'session_closed')
        assert.ok(yield* views.watch(root.id).pipe(Effect.flip))
      }).pipe((effect) =>
        withLayer(
          effect.pipe(Effect.provide(View.layer)),
          Session.layer.pipe(Layer.provideMerge(Store.layerMemory)),
        ),
      ),
  )
})
