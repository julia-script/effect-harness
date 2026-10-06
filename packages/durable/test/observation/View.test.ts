import * as SqliteClient from '@effect/sql-sqlite-node/SqliteClient'
import { assert, describe, it } from '@effect/vitest'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Fiber from 'effect/Fiber'
import * as Layer from 'effect/Layer'
import * as Scope from 'effect/Scope'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as SqlClient from 'effect/sql/SqlClient'
import * as Conversation from '../../src/Conversation.ts'
import * as Document from '../../src/Document.ts'
import * as Inbox from '../../src/Inbox.ts'
import * as Record from '../../src/Record.ts'
import * as Session from '../../src/Session.ts'
import * as View from '../../src/View.ts'
import * as Usage from '../../src/Usage.ts'
import * as Memory from '../../src/storage/Memory.ts'
import * as Sqlite from '../../src/storage/Sqlite.ts'

const layers = Layer.mergeAll(Session.layer, View.layer).pipe(Layer.provideMerge(Memory.layer))
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
const collect = (watch: View.Watch, count: number) =>
  Stream.runCollect(watch.changes.pipe(Stream.take(count))).pipe(Effect.timeout('2 seconds'))
const append = (session: Session.Service, id: Record.ConversationId, kind: string) =>
  session.transaction((tx) => tx.appendEntry(id, { kind }))
const settle = Effect.sleep('65 millis')
describe('committed conversation mounts', () => {
  it.live(
    'rehydrates a complete snapshot when retained early document frames hide a later journal gap',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const { session, views, root } = yield* initialize
          const watch = yield* views.watch(root.id)
          const entered = yield* Deferred.make<void>()
          const release = yield* Deferred.make<void>()
          const holding = yield* views
            .observe(root.id, {
              initial: () =>
                Deferred.succeed(entered, undefined).pipe(
                  Effect.andThen(Deferred.await(release)),
                  Effect.as(0),
                ),
              project: (change) => Effect.succeed(change.reset ? 1 : undefined),
              reset: () => Effect.succeed(0),
            })
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
      ),
  )

  it.live('hydrates shared mounts, replays exact frames and preserves unchanged branches', () =>
    Effect.scoped(
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
          replay = View.apply(replay, frame.ops)
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
    ),
  )

  it.live('cuts inherited entries and follows only fork commits', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { session, views, root } = yield* initialize
        const a = yield* append(session, root.id, 'a')
        const b = yield* append(session, root.id, 'b')
        const child = yield* session.transaction((tx) =>
          tx.forkConversation(root.id, b.id, { ownership: { kind: 'ownerless' } }),
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
    ),
  )

  it.live('raw heads never restore entries a live mount has cut; a new mount hydrates them', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { session, views, root } = yield* initialize
        const old = yield* append(session, root.id, 'old')
        yield* session.transaction((tx) => tx.appendEntry(root.id, { kind: 'reset', head: 'self' }))
        const watch = yield* views.watch(root.id)
        yield* session.transaction((tx) =>
          tx.appendEntry(root.id, { kind: 'summary', head: old.id }),
        )
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
    ),
  )

  it.live(
    'retirement and recreation mount new incarnations whole; unrelated documents do not publish',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const { session, views, root } = yield* initialize
          const watch = yield* views.watch(root.id)
          const other = Document.define({
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
      ),
  )

  it.live('keeps relevant backlog exact across more than101 unrelated commits', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { session, views, root } = yield* initialize
        const other = yield* session.transaction((tx) =>
          tx.createConversation({ ownership: { kind: 'ownerless' } }),
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
    ),
  )

  it.live('replaces101 pending frames with one latest reset and applies subsequent deltas', () =>
    Effect.scoped(
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
    ),
  )

  it.live('excludes an in-flight callback from the100 pending limit and stops immediately', () =>
    Effect.scoped(
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
    ),
  )

  it.live(
    'isolates a failed listener, keeps states live, drops mounts and closes all on Session shutdown',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const { session, views, root } = yield* initialize
          const bad = yield* views.watch(root.id)
          const state = yield* views.state(root.id)
          const listener = yield* bad
            .listen(() => Effect.fail('listener'))
            .pipe(Effect.exit, Effect.forkScoped)
          yield* append(session, root.id, 'one')
          assert.strictEqual(yield* bad.closed, 'listener_error')
          yield* Fiber.join(listener)
          yield* settle
          assert.strictEqual(state.value.entries.length, 1)
          yield* state.dispose
          const rebuilt = yield* views.watch(root.id)
          assert.notStrictEqual(rebuilt.value, state.value)
          yield* session.close
          assert.strictEqual(yield* rebuilt.closed, 'session_closed')
          assert.ok(yield* views.watch(root.id).pipe(Effect.flip))
        }).pipe(Effect.provide(layers)),
      ),
  )

  it.live(
    'owns acquisitions by Scope and permits cancellation while waiting for physical SQL settlement',
    () =>
      Effect.scoped(
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
          const acquiring = yield* views.watch(root.id).pipe(Effect.forkScoped)
          yield* Effect.sleep('30 millis')
          const interrupting = yield* Fiber.interrupt(acquiring).pipe(Effect.forkScoped)
          yield* Effect.yieldNow
          yield* Deferred.succeed(release, undefined)
          yield* Fiber.join(transaction)
          yield* Fiber.join(interrupting)
          assert.strictEqual((yield* Fiber.await(acquiring))._tag, 'Failure')
        }).pipe(
          Effect.provide(
            Layer.mergeAll(Session.layer, View.layer).pipe(
              Layer.provideMerge(Sqlite.layer),
              Layer.provideMerge(SqliteClient.layer({ filename: ':memory:' })),
            ),
          ),
        ),
      ),
  )

  it.live('publishes neither rolled-back SQL frames nor paused outer transaction writes', () =>
    Effect.scoped(
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
        yield* settle
        assert.strictEqual(state.value.entries.length, 0)
        yield* Deferred.succeed(release, undefined)
        yield* Fiber.join(write)
        assert.deepStrictEqual(
          (yield* collect(watch, 1))[0]?.value.entries.map((entry) => entry.kind),
          ['commit'],
        )
        yield* sql
          .withTransaction(
            append(session, root.id, 'rollback').pipe(Effect.andThen(Effect.fail('rollback'))),
          )
          .pipe(Effect.ignore)
        yield* settle
        assert.deepStrictEqual(
          state.value.entries.map((entry) => entry.kind),
          ['commit'],
        )
      }).pipe(
        Effect.provide(
          Layer.mergeAll(Session.layer, View.layer).pipe(
            Layer.provideMerge(Sqlite.layer),
            Layer.provideMerge(SqliteClient.layer({ filename: ':memory:' })),
          ),
        ),
      ),
    ),
  )
})
