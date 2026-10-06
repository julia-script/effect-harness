import * as SqliteClient from '@effect/sql-sqlite-node/SqliteClient'
import * as Context from 'effect/Context'
import * as Option from 'effect/Option'
import * as SqlClient from 'effect/sql/SqlClient'
import * as Sqlite from '../../src/storage/Sqlite.ts'
import * as Store from '../../src/Store.ts'
import { assert, describe, it } from '@effect/vitest'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Fiber from 'effect/Fiber'
import * as Layer from 'effect/Layer'
import * as Ref from 'effect/Ref'
import * as Schema from 'effect/Schema'
import * as Activity from 'effect/workflow/Activity'
import * as Workflow from 'effect/workflow/Workflow'
import * as WorkflowEngine from 'effect/workflow/WorkflowEngine'
import * as Conversation from '../../src/Conversation.ts'
import * as Event from '../../src/Event.ts'
import * as Session from '../../src/Session.ts'
import * as Record from '../../src/Record.ts'
import * as View from '../../src/View.ts'
import * as Memory from '../../src/storage/Memory.ts'
import { ExecutionError } from '../../src/workflow/ExecutionError.ts'

describe('public observer/native compensation lifetime', () => {
  it.live('View, Event and document watches end while Session cleanup is still blocked', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const session = yield* Session.Session
        const root = yield* session.root((tx) =>
          tx.doc(Conversation.AgentDoc, { owner: Record.ROOT_CONVERSATION_ID }).pipe(Effect.asVoid),
        )
        const structural = yield* (yield* View.View).watch(root.id)
        const semantic = yield* (yield* Event.Event).watch(root.id)
        const document = yield* session.watchDoc(Conversation.AgentDoc, { owner: root.id })
        assert.ok(document)
        const started = yield* Deferred.make<void>()
        const release = yield* Deferred.make<void>()
        yield* session.onClose(
          Deferred.succeed(started, undefined).pipe(Effect.andThen(Deferred.await(release))),
        )
        const closing = yield* session.close.pipe(Effect.forkScoped)
        yield* Effect.addFinalizer(() => Deferred.succeed(release, undefined))
        yield* Deferred.await(started)
        assert.strictEqual(
          yield* structural.closed.pipe(Effect.timeout('3 seconds')),
          'session_closed',
        )
        assert.strictEqual(
          yield* semantic.closed.pipe(Effect.timeout('3 seconds')),
          'session_closed',
        )
        assert.strictEqual(
          yield* document.closed.pipe(Effect.timeout('3 seconds')),
          'session_closed',
        )
        assert.isUndefined(closing.pollUnsafe())
        const rejected = yield* Effect.result((yield* View.View).watch(root.id))
        assert.strictEqual(rejected._tag, 'Failure')
        yield* Deferred.succeed(release, undefined)
        yield* Fiber.join(closing)
      }).pipe(
        Effect.provide(
          Event.layer.pipe(
            Layer.provideMerge(View.layer),
            Layer.provideMerge(Session.layer),
            Layer.provide(Memory.layer),
          ),
        ),
      ),
    ),
  )
  it.live(
    'concurrent host domain admission cannot hold the semaphore while waiting for a native SQL lease',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient
          const hostAttempt = yield* Deferred.make<void>()
          const leased = yield* Deferred.make<void>()
          let watching = false
          const withTransaction: SqlClient.SqlClient['withTransaction'] = (effect) =>
            Effect.contextWith((context) =>
              Effect.gen(function* () {
                if (watching && Option.isNone(Context.getOption(context, sql.transactionService)))
                  yield* Deferred.succeed(hostAttempt, undefined)
                return yield* sql.withTransaction(effect)
              }),
            )
          const instrumented = new Proxy(sql, {
            get: (target, key, receiver) =>
              key === 'withTransaction' ? withTransaction : Reflect.get(target, key, receiver),
          })
          const store = yield* Sqlite.make().pipe(
            Effect.provideService(SqlClient.SqlClient, instrumented),
          )
          const session = yield* Session.make().pipe(Effect.provideService(Store.Store, store))
          yield* session.root()
          const native = yield* sql
            .withTransaction(
              Effect.gen(function* () {
                yield* Deferred.succeed(leased, undefined)
                yield* Deferred.await(hostAttempt)
                yield* session.transaction((tx) =>
                  tx.appendEntry(Record.ROOT_CONVERSATION_ID, { kind: 'native' }),
                )
              }),
            )
            .pipe(Effect.forkScoped)
          yield* Deferred.await(leased)
          watching = true
          const host = yield* session
            .transaction((tx) => tx.appendEntry(Record.ROOT_CONVERSATION_ID, { kind: 'host' }))
            .pipe(Effect.forkScoped)
          yield* Fiber.join(native).pipe(Effect.timeout('3 seconds'))
          yield* Fiber.join(host).pipe(Effect.timeout('3 seconds'))
          const state = yield* session.committed
          assert.deepStrictEqual(
            state.entries.map((entry) => entry.entry.kind),
            ['native', 'host'],
          )
        }).pipe(Effect.provide(SqliteClient.layer({ filename: ':memory:' }))),
      ),
  )

  it.live(
    'ordinary native top-level failure runs its compensation once and preserves the cached failure',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const calls = yield* Ref.make<ReadonlyArray<string>>([])
          const task = Workflow.make('test/native-compensation/v1', {
            payload: { key: Schema.String },
            success: Schema.String,
            error: ExecutionError,
            idempotencyKey: ({ key }) => key,
          })
          const executor = task.toLayer(() =>
            Effect.gen(function* () {
              yield* Activity.make({
                name: 'acquire',
                success: Schema.String,
                execute: Effect.succeed('resource'),
              }).pipe(task.withCompensation((value) => Ref.update(calls, (old) => [...old, value])))
              return yield* new ExecutionError({
                reason: 'invalid_state',
                message: 'native failure',
              })
            }),
          )
          yield* Effect.gen(function* () {
            for (let repeat = 0; repeat < 2; repeat++) {
              const result = yield* Effect.result(task.execute({ key: 'same' }))
              assert.strictEqual(result._tag, 'Failure')
            }
            assert.deepStrictEqual(yield* Ref.get(calls), ['resource'])
          }).pipe(Effect.provide(executor.pipe(Layer.provideMerge(WorkflowEngine.layerMemory))))
        }),
      ),
  )
})
