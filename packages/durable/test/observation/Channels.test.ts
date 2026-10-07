import { assert, describe, it } from '@effect/vitest'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Fiber from 'effect/Fiber'
import * as Layer from 'effect/Layer'
import * as Scope from 'effect/Scope'
import * as Exit from 'effect/Exit'
import * as Stream from 'effect/Stream'
import * as Conversation from '@effect-harness/durable/Conversation'
import * as Event from '@effect-harness/durable/Event'
import * as Inbox from '@effect-harness/durable/Inbox'
import * as Record from '@effect-harness/durable/Record'
import * as Session from '@effect-harness/durable/Session'
import * as Store from '@effect-harness/durable/Store'
import * as View from '@effect-harness/durable/View'
import * as Memory from '@effect-harness/durable/storage/Memory'
import { rejected } from '@effect-harness/durable/StorageError'
import { ResourceScope, withLayer } from '@effect-harness/durable/testing/Storage'

const layers = Layer.mergeAll(Session.layer, View.layer).pipe(Layer.provideMerge(Memory.layer))
const initialize = Effect.gen(function* () {
  const session = yield* Session.Session
  const views = yield* View.View
  const root = yield* session.root((tx) =>
    Effect.gen(function* () {
      yield* tx.doc(Conversation.AgentDoc, { owner: Record.ROOT_CONVERSATION_ID })
      yield* tx.doc(Inbox.LiveDoc, { owner: Record.ROOT_CONVERSATION_ID })
    }),
  )
  return { session, views, root }
})
const append = (session: Session.Service, id: Record.ConversationId) =>
  session.transaction((tx) => tx.appendEntry(id, { kind: 'channel-fixture' }))
interface Count {
  readonly n: number
  readonly reset: boolean
}
const countProjection = (
  projected: (n: number) => Effect.Effect<void>,
  reset: (tasks: ReadonlyArray<Record.Task>) => Effect.Effect<void> = () => Effect.void,
): View.Projection<Count> => ({
  initial: (value) => Effect.succeed({ n: value.entries.length, reset: false }),
  project: (change) =>
    projected(change.value.entries.length).pipe(
      Effect.as({ n: change.value.entries.length, reset: false }),
    ),
  reset: (value, _seq, tasks) =>
    reset(tasks).pipe(Effect.as({ n: value.entries.length, reset: true })),
})

describe('typed scoped observation channels', () => {
  it.live(
    'a blocked projection cannot block refresh, healthy consumers, new baselines or terminal delivery',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const entered = yield* Deferred.make<void>()
          const release = yield* Deferred.make<void>()
          yield* Effect.gen(function* () {
            const { session, views, root } = yield* initialize
            const slow = yield* views.observe(root.id, {
              initial: () => Effect.succeed(0),
              project: () =>
                Deferred.succeed(entered, undefined).pipe(
                  Effect.andThen(Deferred.await(release)),
                  Effect.as(1),
                ),
              reset: () => Effect.succeed(0),
            })
            const fast = yield* views.watch(root.id)
            yield* append(session, root.id)
            const late = yield* views.watch(root.id).pipe(Effect.timeout('2 seconds'))
            yield* Deferred.await(entered)
            assert.strictEqual(late.value.entries.length, 1)
            assert.strictEqual(fast.value.entries.length, 0)
            const changes = yield* Stream.runCollect(
              fast.changes.pipe(Stream.take(1), Stream.timeout('2 seconds')),
            )
            assert.strictEqual(changes[0]?.value.entries.length, 1)
            yield* Scope.close(yield* ResourceScope, Exit.void)
            assert.strictEqual(
              yield* slow.closed.pipe(Effect.timeout('2 seconds')),
              'session_closed',
            )
            assert.strictEqual(
              yield* late.closed.pipe(Effect.timeout('2 seconds')),
              'session_closed',
            )
          }).pipe(Effect.ensuring(Deferred.succeed(release, undefined)))
        }).pipe((effect) =>
          withLayer(
            effect.pipe(Effect.provide(View.layer)),
            Session.layer.pipe(Layer.provideMerge(Memory.layer)),
          ),
        ),
      ),
  )

  it.live(
    'keeps exactly100 pending values when a consumer pauses after pulling from an existing backlog',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const release = yield* Deferred.make<void>()
          yield* Effect.gen(function* () {
            const { session, views, root } = yield* initialize
            const fifty = yield* Deferred.make<void>()
            const latest = yield* Deferred.make<void>()
            const entered = yield* Deferred.make<void>()
            const reset = yield* Deferred.make<void>()
            const watch = yield* views.observe(
              root.id,
              countProjection((n) => {
                if (n === 50) return Deferred.succeed(fifty, undefined).pipe(Effect.asVoid)
                if (n === 102) return Deferred.succeed(latest, undefined).pipe(Effect.asVoid)
                return Effect.void
              }),
            )
            for (let n = 0; n < 50; n++) yield* append(session, root.id)
            yield* views.watch(root.id)
            yield* Deferred.await(fifty)
            yield* Effect.yieldNow
            assert.strictEqual(watch.value.n, 0)
            const received: Count[] = []
            const listening = yield* watch
              .listen((value) =>
                Effect.gen(function* () {
                  received.push(value)
                  if (received.length === 1) {
                    yield* Deferred.succeed(entered, undefined)
                    yield* Deferred.await(release)
                  }
                  if (value.reset) yield* Deferred.succeed(reset, undefined)
                }),
              )
              .pipe(Effect.forkScoped)
            yield* Deferred.await(entered)
            for (let n = 0; n < 52; n++) yield* append(session, root.id)
            yield* views.watch(root.id)
            yield* Deferred.await(latest)
            yield* Effect.yieldNow
            assert.strictEqual(watch.value.n, 1)
            assert.strictEqual(received.length, 1)
            yield* Deferred.succeed(release, undefined)
            yield* Deferred.await(reset).pipe(Effect.timeout('2 seconds'))
            assert.deepStrictEqual(received, [
              { n: 1, reset: false },
              { n: 102, reset: true },
            ])
            yield* watch.stop
            yield* Fiber.join(listening)
          }).pipe(Effect.ensuring(Deferred.succeed(release, undefined)))
        }).pipe(Effect.provide(layers)),
      ),
  )

  it.live(
    'clears queued and pulled history before semantic end and changes its getter only on consumption',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const release = yield* Deferred.make<void>()
          yield* Effect.gen(function* () {
            const { session, views, root } = yield* initialize
            const ready = yield* Deferred.make<void>()
            const entered = yield* Deferred.make<void>()
            const watch = yield* views.observe(
              root.id,
              countProjection((n) =>
                n === 50 ? Deferred.succeed(ready, undefined).pipe(Effect.asVoid) : Effect.void,
              ),
            )
            for (let n = 0; n < 50; n++) yield* append(session, root.id)
            yield* views.watch(root.id)
            yield* Deferred.await(ready)
            yield* Effect.yieldNow
            assert.strictEqual(watch.value.n, 0)
            const received: Count[] = []
            const listening = yield* watch
              .listen((value) =>
                Effect.gen(function* () {
                  received.push(value)
                  yield* Deferred.succeed(entered, undefined)
                  yield* Deferred.await(release)
                }),
              )
              .pipe(Effect.forkScoped)
            yield* Deferred.await(entered)
            assert.strictEqual(watch.value.n, 1)
            yield* watch.stop
            assert.strictEqual(yield* watch.closed, 'stopped')
            yield* Deferred.succeed(release, undefined)
            yield* Fiber.join(listening).pipe(Effect.timeout('2 seconds'))
            assert.deepStrictEqual(received, [{ n: 1, reset: false }])
            assert.strictEqual(watch.value.n, 1)
            assert.strictEqual(
              (yield* Stream.runCollect(watch.changes).pipe(Effect.result))._tag,
              'Failure',
            )
          }).pipe(Effect.ensuring(Deferred.succeed(release, undefined)))
        }).pipe(Effect.provide(layers)),
      ),
  )

  it.live(
    'reset envelopes retain their own task snapshot while delayed projection sees newer mount state',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const release = yield* Deferred.make<void>()
          yield* Effect.gen(function* () {
            const { session, views, root } = yield* initialize
            const taskId = yield* session.transaction((tx) =>
              tx.createTask({
                conversationId: root.id,
                kind: 'channel-task',
                version: 1,
                input: null,
                background: false,
                abortRequested: false,
                state: { status: 'pending' },
              }),
            )
            const entered = yield* Deferred.make<void>()
            const reset = yield* Deferred.make<ReadonlyArray<Record.Task>>()
            yield* views.observe(
              root.id,
              countProjection(
                (n) =>
                  n === 1
                    ? Deferred.succeed(entered, undefined).pipe(
                        Effect.andThen(Deferred.await(release)),
                      )
                    : Effect.void,
                (tasks) => Deferred.succeed(reset, tasks).pipe(Effect.asVoid),
              ),
            )
            yield* append(session, root.id)
            yield* views.watch(root.id)
            yield* Deferred.await(entered)
            for (let n = 0; n < 100; n++) yield* append(session, root.id)
            yield* session.transaction((tx) =>
              Effect.gen(function* () {
                const task = yield* tx.task(taskId)
                assert.ok(task)
                yield* tx.write({
                  type: 'task',
                  value: {
                    ...task,
                    state: { status: 'terminal', outcome: { status: 'completed' } },
                  },
                })
              }),
            )
            const baseline = yield* views.observe(root.id, {
              initial: (_value, tasks) => Effect.succeed(tasks),
              project: () => Effect.as(Effect.void, undefined),
              reset: (_value, _seq, tasks) => Effect.succeed(tasks),
            })
            assert.strictEqual(
              baseline.value.find((task) => task.id === taskId)?.state.status,
              'terminal',
            )
            yield* Deferred.succeed(release, undefined)
            const tasks = yield* Deferred.await(reset).pipe(Effect.timeout('2 seconds'))
            assert.strictEqual(tasks.find((task) => task.id === taskId)?.state.status, 'pending')
          }).pipe(Effect.ensuring(Deferred.succeed(release, undefined)))
        }).pipe(Effect.provide(layers)),
      ),
  )

  it.live(
    'hydrates from the exact serialized journal baseline and never a newer committed read',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const release = yield* Deferred.make<void>()
          yield* Effect.gen(function* () {
            const session = yield* Session.Session
            const store = yield* Store.Store
            const root = yield* session.root()
            yield* append(session, root.id)
            const captured = yield* Deferred.make<void>()
            let block = false
            let committedReads = 0
            const views = yield* View.make.pipe(
              Effect.provideService(Store.Store, {
                ...store,
                committed: Effect.sync(() => {
                  committedReads++
                }).pipe(Effect.andThen(store.committed)),
                journal: (after) =>
                  store.journal(after).pipe(
                    Effect.tap(() => {
                      if (!block) return Effect.void
                      block = false
                      return Deferred.succeed(captured, undefined).pipe(
                        Effect.andThen(Deferred.await(release)),
                      )
                    }),
                  ),
              }),
            )
            const original = yield* views.watch(root.id)
            yield* original.stop
            block = true
            const acquiring = yield* views
              .watch(root.id)
              .pipe(Effect.forkScoped({ startImmediately: true }))
            yield* Deferred.await(captured)
            yield* append(session, root.id)
            yield* Deferred.succeed(release, undefined)
            const first = yield* Fiber.join(acquiring)
            assert.strictEqual(first.value.entries.length, 1)
            assert.strictEqual(committedReads, 0)
            const late = yield* views.watch(root.id)
            assert.strictEqual(late.value.entries.length, 2)
            const frames = yield* Stream.runCollect(
              first.changes.pipe(Stream.take(1), Stream.timeout('2 seconds')),
            )
            assert.strictEqual(frames[0]?.value.entries.length, 2)
            assert.deepStrictEqual(
              frames[0]?.ops.map((op) => op[0]),
              ['splice'],
            )
          }).pipe(Effect.ensuring(Deferred.succeed(release, undefined)))
        }).pipe(Effect.provide(Session.layer.pipe(Layer.provideMerge(Memory.layer)))),
      ),
  )

  it.live('projection failures end only their own observation and release its mount lease', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { session, views, root } = yield* initialize
        const failure = yield* views.observe(root.id, {
          initial: () => Effect.succeed(0),
          project: () => Effect.fail(rejected('projection fixture')),
          reset: () => Effect.succeed(0),
        })
        const healthy = yield* views.watch(root.id)
        yield* append(session, root.id)
        const refreshed = yield* views.watch(root.id)
        yield* refreshed.stop
        assert.strictEqual(
          yield* failure.closed.pipe(Effect.timeout('2 seconds')),
          'listener_error',
        )
        const frames = yield* Stream.runCollect(
          healthy.changes.pipe(Stream.take(1), Stream.timeout('2 seconds')),
        )
        assert.strictEqual(frames[0]?.value.entries.length, 1)
        yield* healthy.stop
        const fresh = yield* views.watch(root.id)
        assert.notStrictEqual(fresh.value, frames[0]?.value)
        assert.strictEqual(fresh.value.entries.length, 1)
      }).pipe(Effect.provide(layers)),
    ),
  )

  it.live('typed live mounts preserve diagnostic identity and equal-value replacement events', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { session, views, root } = yield* initialize
        yield* session.transaction((tx) =>
          Effect.gen(function* () {
            const live = yield* tx.doc(Inbox.LiveDoc, { owner: root.id })
            live.tools = [
              {
                callId: 'retained',
                name: 'tool',
                status: 'running',
                diagnostics: [{ kind: 'existing' }],
              },
            ]
          }),
        )
        const events = yield* Event.make.pipe(Effect.provideService(View.View, views))
        const watch = yield* events.watch(root.id)
        yield* append(session, root.id)
        yield* session.transaction((tx) =>
          Effect.gen(function* () {
            const live = yield* tx.doc(Inbox.LiveDoc, { owner: root.id })
            assert.ok(live.tools?.[0])
            live.tools[0].output = 'append'
          }),
        )
        yield* session.transaction((tx) =>
          Effect.gen(function* () {
            const live = yield* tx.doc(Inbox.LiveDoc, { owner: root.id })
            assert.ok(live.tools?.[0])
            live.tools[0].diagnostics = [{ kind: 'existing' }]
          }),
        )
        const batches = yield* Stream.runCollect(
          watch.changes.pipe(Stream.take(3), Stream.timeout('2 seconds')),
        )
        assert.deepStrictEqual(
          batches[0]?.map((event) => event.type),
          ['entry_appended'],
        )
        assert.deepStrictEqual(batches[1], [
          {
            type: 'tool_execution_update',
            toolCallId: 'retained',
            toolName: 'tool',
            output: { set: 'append' },
          },
        ])
        assert.deepStrictEqual(batches[2], [
          {
            type: 'tool_execution_update',
            toolCallId: 'retained',
            toolName: 'tool',
            diagnostics: [{ kind: 'existing' }],
          },
        ])
      }).pipe(Effect.provide(layers)),
    ),
  )
})
