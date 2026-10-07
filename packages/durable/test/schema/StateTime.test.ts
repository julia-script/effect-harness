import { assert, describe, it } from '@effect/vitest'
import * as SqliteClient from '@effect/sql-sqlite-node/SqliteClient'
import * as Time from '@effect-harness/harness/Time'
import * as DateTime from 'effect/DateTime'
import * as Deferred from 'effect/Deferred'
import * as Duration from 'effect/Duration'
import * as Effect from 'effect/Effect'
import * as Fiber from 'effect/Fiber'
import * as HashSet from 'effect/HashSet'
import * as Ref from 'effect/Ref'
import * as Scheduler from 'effect/Scheduler'
import * as Schema from 'effect/Schema'
import * as SchemaGetter from 'effect/SchemaGetter'
import * as SqlClient from 'effect/sql/SqlClient'
import * as Option from 'effect/Option'
import * as Context from 'effect/Context'
import * as Document from '../../src/Document.ts'
import * as Entry from '../../src/Entry.ts'
import * as Event from '../../src/Event.ts'
import * as Inbox from '../../src/Inbox.ts'
import * as Record from '../../src/Record.ts'
import * as Session from '../../src/Session.ts'
import * as Conversation from '../../src/Conversation.ts'
import * as Store from '../../src/Store.ts'
import * as Memory from '../../src/storage/Memory.ts'
import * as Sqlite from '../../src/storage/Sqlite.ts'
import * as TestClock from 'effect/testing/TestClock'
import { remaining } from '../../src/workflow/ModelRetry.ts'

const counter = Document.defineUnsafe({
  kind: 'counter',
  version: 1,
  scope: 'session',
  schema: Schema.Struct({ count: Schema.Finite }),
  initial: () => ({ count: 0 }),
})
const old = (id: number, count: number): Document.Snapshot =>
  Document.makeSnapshot({
    record: {
      id: Record.DocumentId.make(id),
      kind: 'migrated',
      scope: { kind: 'session' },
      createdAt: Record.Seq.make(1),
    },
    version: 1,
    value: { count },
    deltasSinceBase: 0,
  })

describe('owned state, time and read boundaries', () => {
  it.effect('waits only the remaining span of an unchanged fractional cached deadline', () =>
    Effect.gen(function* () {
      const deadline = Time.fromEpochMillis(1000.5)
      assert.strictEqual(Duration.toMillis(yield* remaining(deadline)), 1000.5)
      yield* TestClock.adjust('500 millis')
      assert.strictEqual(Duration.toMillis(yield* remaining(deadline)), 500.5)
      yield* TestClock.adjust('1 second')
      assert.strictEqual(Duration.toMillis(yield* remaining(deadline)), 0)
      assert.strictEqual(DateTime.toEpochMillis(deadline), 1000.5)
    }),
  )
  it.effect(
    'lazy factory values allocate independent stores, sessions and migration identity caches',
    () =>
      Effect.gen(function* () {
        const first = yield* Memory.make
        const second = yield* Memory.make
        assert.notStrictEqual(first, second)
        const one = yield* Session.make.pipe(Effect.provideService(Store.Store, first))
        const two = yield* Session.make.pipe(Effect.provideService(Store.Store, second))
        yield* one.root()
        assert.deepStrictEqual((yield* two.committed).conversations, [])
        assert.notStrictEqual(
          yield* Document.makeMigrationCache,
          yield* Document.makeMigrationCache,
        )
      }),
  )

  it.effect(
    'shares concurrent successful migrations and detaches each return with the exact incarnation key',
    () =>
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>()
        const release = yield* Deferred.make<void>()
        const shape = Schema.Struct({ count: Schema.Finite })
        const codec = shape.pipe(
          Schema.decodeTo(shape, {
            decode: SchemaGetter.passthrough(),
            encode: SchemaGetter.transformEffect((value) =>
              Deferred.succeed(entered, undefined).pipe(
                Effect.andThen(Deferred.await(release)),
                Effect.as(value),
              ),
            ),
          }),
        )
        let calls = 0
        const shared = Document.defineUnsafe({
          kind: 'migrated',
          version: 2,
          scope: 'session',
          schema: codec,
          initial: () => ({ count: 0 }),
          migrate: (value) => {
            calls++
            return { count: Number(value.count) + 1 }
          },
        })
        const cache = yield* Document.makeMigrationCache
        const a = yield* Document.typed(shared, old(2, 10), cache).pipe(Effect.forkScoped)
        yield* Deferred.await(entered)
        const b = yield* Document.typed(shared, old(2, 10), cache).pipe(Effect.forkScoped)
        yield* Effect.yieldNow
        yield* Deferred.succeed(release, undefined)
        const first = yield* Fiber.join(a)
        const second = yield* Fiber.join(b)
        assert.strictEqual(calls, 1)
        assert.notStrictEqual(first.value, second.value)
        Object.defineProperty(first.value, 'count', { value: 999, enumerable: true })
        assert.strictEqual(second.value.count, 11)
        assert.strictEqual((yield* Document.typed(shared, old(2, 10), cache)).value.count, 11)
        assert.strictEqual((yield* Document.typed(shared, old(3, 10), cache)).value.count, 11)
        assert.strictEqual(calls, 2)
        assert.strictEqual((yield* Document.typed(shared, old(2, 20), cache)).value.count, 21)
        assert.strictEqual(calls, 3)
      }),
  )

  it.effect(
    'migration failures expire immediately and equivalent token objects never share identity',
    () =>
      Effect.gen(function* () {
        let calls = 0
        const token = Document.defineUnsafe({
          kind: 'migrated',
          version: 2,
          scope: 'session',
          schema: Schema.Struct({ count: Schema.Finite }),
          initial: () => ({ count: 0 }),
          migrate: (value) => {
            if (++calls === 1) throw new Error('first migration fails')
            return { count: Number(value.count) + 1 }
          },
        })
        const cache = yield* Document.makeMigrationCache
        assert.strictEqual(
          (yield* Document.typed(token, old(2, 1), cache).pipe(Effect.result))._tag,
          'Failure',
        )
        assert.strictEqual((yield* Document.typed(token, old(2, 1), cache)).value.count, 2)
        assert.strictEqual(calls, 2)
        yield* Document.typed({ ...token }, old(2, 1), cache)
        assert.strictEqual(calls, 3)
      }),
  )

  it.effect('detaches decoded codec aliases before returning migrated snapshots', () =>
    Effect.gen(function* () {
      const shared = { count: 11 }
      const shape = Schema.Struct({ count: Schema.Finite })
      const codec = shape.pipe(
        Schema.decodeTo(Schema.declare<{ count: number }>(Schema.is(shape)), {
          decode: SchemaGetter.transform(() => shared),
          encode: SchemaGetter.transform((value) => ({ count: value.count })),
        }),
      )
      const token = Document.defineUnsafe({
        kind: 'migrated',
        version: 2,
        scope: 'session',
        schema: codec,
        initial: () => ({ count: 0 }),
        migrate: (value) => ({ count: Number(value.count) + 1 }),
      })
      const cache = yield* Document.makeMigrationCache
      const first = yield* Document.typed(token, old(2, 10), cache)
      const second = yield* Document.typed(token, old(2, 10), cache)
      assert.notStrictEqual(first.value, shared)
      assert.notStrictEqual(first.value, second.value)
      Object.defineProperty(first.value, 'count', { value: 999, enumerable: true })
      assert.strictEqual(shared.count, 11)
      assert.strictEqual(second.value.count, 11)
      assert.strictEqual((yield* Document.typed(token, old(2, 10), cache)).value.count, 11)
    }),
  )

  it.effect('batches projections, completes each failed Exit and resamples after writes', () =>
    Effect.gen(function* () {
      const original = yield* Memory.make
      const reads = yield* Ref.make(0)
      const sameLease = {}
      const store = Store.Store.of({
        ...original,
        readContext: Effect.succeed(sameLease),
        read: Ref.update(reads, (n) => n + 1).pipe(Effect.andThen(original.read)),
      })
      const session = yield* Session.make.pipe(Effect.provideService(Store.Store, store))
      yield* session.transaction(
        Effect.fnUntraced(function* (tx) {
          yield* tx.doc(counter)
        }),
      )
      const incompatible = { ...counter, definition: { ...counter.definition, version: 2 } }
      const results = yield* Effect.all(
        [
          session.snapshot(counter).pipe(Effect.map(Option.getOrUndefined), Effect.result),
          session.snapshot(incompatible).pipe(Effect.map(Option.getOrUndefined), Effect.result),
        ],
        { concurrency: 2 },
      )
      assert.strictEqual(yield* Ref.get(reads), 1)
      assert.strictEqual(results[0]._tag, 'Success')
      assert.strictEqual(results[1]._tag, 'Failure')
      yield* session.transaction(
        Effect.fnUntraced(function* (tx) {
          ;(yield* tx.doc(counter)).count = 7
        }),
      )
      assert.strictEqual(
        (yield* session.snapshot(counter).pipe(Effect.map(Option.getOrUndefined)))?.value.count,
        7,
      )
      assert.strictEqual(yield* Ref.get(reads), 2)
    }),
  )

  it.effect('restores each entry caller Context for optional document decoding services', () =>
    Effect.gen(function* () {
      class Label extends Context.Service<Label, string>()('test/StateTime/Label') {}
      const encoded = Schema.Struct({ count: Schema.Finite })
      const decoded = Schema.Struct({ count: Schema.Finite, label: Schema.String })
      const codec = encoded.pipe(
        Schema.decodeTo(decoded, {
          decode: SchemaGetter.transformEffect((value) =>
            Effect.serviceOption(Label).pipe(
              Effect.map((label) => ({ ...value, label: Option.getOrElse(label, () => 'absent') })),
            ),
          ),
          encode: SchemaGetter.transform((value) => ({ count: value.count })),
        }),
      )
      const token = Document.defineUnsafe({
        kind: 'context',
        version: 1,
        scope: 'session',
        schema: codec,
        initial: () => ({ count: 0, label: 'initial' }),
      })
      const original = yield* Memory.make
      const lease = {}
      const reads = yield* Ref.make(0)
      const store = Store.Store.of({
        ...original,
        readContext: Effect.succeed(lease),
        read: Ref.update(reads, (count) => count + 1).pipe(Effect.andThen(original.read)),
      })
      const session = yield* Session.make.pipe(Effect.provideService(Store.Store, store))
      yield* session.transaction(
        Effect.fnUntraced(function* (tx) {
          yield* tx.doc(token)
        }),
      )
      const values = yield* Effect.all(
        [
          session
            .snapshot(token)
            .pipe(Effect.map(Option.getOrUndefined), Effect.provideService(Label, 'first')),
          session
            .snapshot(token)
            .pipe(Effect.map(Option.getOrUndefined), Effect.provideService(Label, 'second')),
        ],
        { concurrency: 2 },
      )
      assert.deepStrictEqual(
        values.map((value) => value?.value.label),
        ['first', 'second'],
      )
      assert.strictEqual(yield* Ref.get(reads), 1)
    }),
  )

  it.effect(
    'isolates native SQL transaction previews from simultaneous physical reads without deadlock',
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient
        const scope = yield* Effect.scope
        const original = yield* Sqlite.make
        const reads = yield* Ref.make(0)
        const store = Store.Store.of({
          ...original,
          read: Ref.update(reads, (n) => n + 1).pipe(Effect.andThen(original.read)),
        })
        const session = yield* Session.make.pipe(Effect.provideService(Store.Store, store))
        yield* session.transaction(
          Effect.fnUntraced(function* (tx) {
            yield* tx.doc(counter)
          }),
        )
        const outside =
          yield* Deferred.make<
            Fiber.Fiber<
              Document.Snapshot<{ readonly count: number }> | undefined,
              import('../../src/StorageError.ts').StorageError
            >
          >()
        yield* sql
          .withTransaction(
            Effect.gen(function* () {
              yield* session.transaction(
                Effect.fnUntraced(function* (tx) {
                  ;(yield* tx.doc(counter)).count = 7
                }),
              )
              const local = yield* Effect.all(
                [
                  session.snapshot(counter).pipe(Effect.map(Option.getOrUndefined)),
                  session.snapshot(counter).pipe(Effect.map(Option.getOrUndefined)),
                ],
                { concurrency: 2 },
              ).pipe(Effect.forkIn(scope))
              const physical = yield* session.snapshot(counter).pipe(
                Effect.map(Option.getOrUndefined),
                Effect.updateContext((context: Context.Context<never>) =>
                  Context.omit(sql.transactionService)(context),
                ),
                Effect.forkIn(scope),
              )
              yield* Deferred.succeed(outside, physical)
              const values = yield* Fiber.join(local).pipe(Effect.timeout('1 second'))
              assert.deepStrictEqual(
                values.map((value) => value?.value.count),
                [7, 7],
              )
              return yield* Effect.fail('rollback')
            }),
          )
          .pipe(Effect.flip)
        assert.strictEqual((yield* Fiber.join(yield* Deferred.await(outside)))?.value.count, 0)
        assert.strictEqual(yield* Ref.get(reads), 2)
        const readContext = original.readContext
        if (readContext === undefined) return yield* Effect.die('SQL read context missing')
        const committedContext = yield* readContext
        const lease = yield* sql.withTransaction(readContext)
        const laterLease = yield* sql.withTransaction(readContext)
        assert.notStrictEqual(lease, committedContext)
        assert.notStrictEqual(lease, laterLease)
      }).pipe(Effect.provide(SqliteClient.layer({ filename: ':memory:' }))),
  )

  it.effect('defaults independent tool dispatch to sixteen and rejects invalid limits', () =>
    Effect.gen(function* () {
      const configuration = yield* Conversation.Configuration.pipe(
        Effect.provide(Conversation.layerConfiguration()),
      )
      assert.strictEqual(configuration.toolConcurrency, 16)
      for (const toolConcurrency of [0, -1, 0.5, NaN, Infinity, Number.MAX_SAFE_INTEGER + 1]) {
        const result = yield* Conversation.Configuration.pipe(
          Effect.provide(Conversation.layerConfiguration({ toolConcurrency })),
          Effect.result,
        )
        assert.strictEqual(result._tag, 'Failure')
      }
    }),
  )

  it.effect('serializes concurrent task checks and writes under forced scheduler yields', () =>
    Effect.gen(function* () {
      const store = yield* Memory.make
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
                .write({ type: 'task', value: { ...task, state: { status: 'terminal' } } })
                .pipe(Effect.result),
              tx
                .write({ type: 'task', value: { ...task, state: { status: 'running' } } })
                .pipe(Effect.result),
            ],
            { concurrency: 2 },
          ).pipe(Effect.provideService(Scheduler.MaxOpsBeforeYield, 20))
          assert.strictEqual(result[0]._tag, 'Success')
          assert.strictEqual(result[1]._tag, 'Failure')
        }),
      )
      assert.strictEqual(
        (yield* session.task(id).pipe(Effect.map(Option.getOrUndefined)))?.state.status,
        'terminal',
      )
    }),
  )

  it.effect(
    'keeps fractional Entry/Event time bytes and suppresses unchanged decoded deferred deadlines',
    () =>
      Effect.gen(function* () {
        const data = yield* Schema.decodeEffect(Entry.UserData)({ timestamp: 1000.5 })
        assert.strictEqual(
          DateTime.toEpochMillis(data.timestamp ?? Time.fromEpochMillis(0)),
          1000.5,
        )
        assert.deepStrictEqual(yield* Schema.encodeEffect(Entry.UserData)(data), {
          timestamp: 1000.5,
        })
        assert.strictEqual(
          DateTime.toEpochMillis(
            DateTime.addDuration(data.timestamp ?? Time.fromEpochMillis(0), Duration.millis(0.5)),
          ),
          1001,
        )
        const value = {
          conversation: { id: Record.ROOT_CONVERSATION_ID },
          entries: [],
          docs: { 'harness.live': { generation: { attempt: 1, deferred: { pollAt: 1000.5 } } } },
        }
        const domain = Inbox.domain(value.docs['harness.live'])
        assert.strictEqual(
          DateTime.toEpochMillis(domain.generation?.deferred?.pollAt ?? Time.fromEpochMillis(0)),
          1000.5,
        )
        const snap = yield* Event.snapshot(value)
        const wire = yield* Schema.encodeEffect(Event.BatchJson)([snap])
        assert.include(JSON.stringify(wire), '"pollAt":1000.5')
        const next = {
          ...value,
          docs: { 'harness.live': { generation: { attempt: 2, deferred: { pollAt: 1000.5 } } } },
        }
        const events = yield* Event.translate(
          Record.ROOT_CONVERSATION_ID,
          {
            seq: Record.Seq.make(2),
            before: value,
            value: next,
            ops: [],
            reset: false,
            publication: { seq: Record.Seq.make(2), writes: [], documents: [] },
          },
          yield* Ref.make(HashSet.empty<Record.TaskId>()),
        )
        assert.isFalse(events.some((event) => event.type === 'deferred_poll'))
      }),
  )
})
