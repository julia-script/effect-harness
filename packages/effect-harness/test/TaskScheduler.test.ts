import { assert, describe, it } from '@effect/vitest'
import * as Cause from 'effect/Cause'
import * as Context from 'effect/Context'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as Fiber from 'effect/Fiber'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Scope from 'effect/Scope'
import * as Stream from 'effect/Stream'
import * as Document from 'effect-harness/Document'
import * as Record from 'effect-harness/Record'
import * as Registry from 'effect-harness/Registry'
import * as Session from 'effect-harness/Session'
import * as Storage from 'effect-harness/Storage'
import * as StorageFormat from 'effect-harness/StorageFormat'
import * as Task from 'effect-harness/Task'
import { TaskExecution, type Access } from 'effect-harness/TaskExecution'
import * as TaskScheduler from 'effect-harness/TaskScheduler'
import * as Toolkit from 'effect-harness/Toolkit'
import * as Transaction from 'effect-harness/Transaction'

const root = Record.ROOT_CONVERSATION_ID
const doc = Document.define({
  kind: 'scheduler.doc',
  version: 1,
  schema: Schema.Struct({ n: Schema.Natural }),
  scope: 'task',
  initial: () => ({ n: 0 }),
})
const contract = {
  kind: 'scheduler.counter',
  version: 1,
  input: Schema.Natural,
  checkpoint: Schema.Union([
    Schema.Struct({ phase: Schema.Literal('one'), count: Schema.Natural }),
    Schema.Struct({ phase: Schema.Literal('two'), count: Schema.Natural }),
  ]),
  success: Schema.Natural,
  failure: Schema.String,
}
const install = <R, E>(registry: Registry.Service, registration: Registry.Registration<R, E>) =>
  registry.install(registration)
const registration = <
  I extends Schema.Constraint,
  C extends Task.CheckpointCodec,
  S extends Schema.Constraint,
  F extends Schema.Constraint,
  H extends Task.Phases<I, C>,
  B extends Task.Handler<I, C> | undefined,
  R,
  E,
>(
  task: Task.Task<I, C, S, F, H, B, R, E>,
  enabled = true,
) =>
  Registry.withTask(
    Registry.toolkit({ id: 'scheduler-test', version: 1, enabled }, Toolkit.make()),
    task,
  )
const open = Effect.gen(function* () {
  const session = yield* Session.make()
  yield* Session.commit(session, (tx) => Transaction.ensureRoot(tx))
  const registry = yield* Registry.make
  const scheduler = yield* TaskScheduler.make(session).pipe(
    Effect.provideService(Registry.Registry, registry),
  )
  return { session, registry, scheduler }
})
const counter = (runs: string[], second: Effect.Effect<void> = Effect.void) =>
  Task.make({
    ...contract,
    initial: (count: number) => ({ phase: 'one' as const, count }),
    phases: {
      one: ({ checkpoint }) =>
        Effect.gen(function* () {
          const access = yield* TaskExecution
          runs.push('one')
          yield* access.commit((tx) =>
            Effect.gen(function* () {
              yield* Transaction.appendEntry(tx, root, {
                kind: 'counter.one',
                data: checkpoint.count,
              })
              yield* access.progress(tx, contract, {
                status: 'pending',
                checkpoint: { phase: 'two', count: checkpoint.count + 1 },
              })
            }),
          )
        }),
      two: ({ checkpoint }) =>
        Effect.gen(function* () {
          const access = yield* TaskExecution
          runs.push('two')
          yield* second
          yield* access.commit((tx) =>
            access.progress(tx, contract, {
              status: 'completing',
              outcome: { status: 'completed', result: checkpoint.count + 1 },
            }),
          )
        }),
    },
  })

// Yielding a bounded number of times lets tests distinguish a blocked task from a busy loop.
const settle = Effect.forEach(Array.from({ length: 40 }), () => Effect.yieldNow, { discard: true })

describe('standalone custom task scheduler', () => {
  it.effect(
    'executes two phases, persists domain effects with checkpoints and supports late and concurrent waits',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const { session, registry, scheduler } = yield* open
          const runs: string[] = []
          const definition = counter(runs)
          yield* install(registry, registration(definition))
          const ref = yield* scheduler.start(definition, 5, { conversationId: root })
          const results = yield* Effect.all([scheduler.wait(ref), scheduler.wait(ref)], {
            concurrency: 'unbounded',
          })
          assert.deepStrictEqual(results, [
            { status: 'completed', result: 7 },
            { status: 'completed', result: 7 },
          ])
          assert.deepStrictEqual(runs, ['one', 'two'])
          assert.deepStrictEqual(yield* scheduler.wait(ref), results[0])
          const entries = yield* Session.scanEntries(session, { conversationId: root }).pipe(
            Stream.runCollect,
          )
          assert.strictEqual(entries.filter((entry) => entry.kind === 'counter.one').length, 1)
          assert.strictEqual((yield* scheduler.inspect(ref.id)).record.state.status, 'terminal')
          assert.deepStrictEqual(yield* StorageFormat.inspect, {
            formatVersion: 2,
            minimumReaderVersion: 2,
          })
        }),
      ).pipe(Effect.provide(Storage.layerMemory)),
  )

  it.effect(
    'normalizes typed failure, no progress, defects and mixed defect/interruption distinctly',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const { registry, scheduler } = yield* open
          const behaviors = [
            { name: 'typed', effect: Effect.fail('expected'), status: 'failed' },
            { name: 'empty', effect: Effect.void, status: 'faulted' },
            { name: 'defect', effect: Effect.die('defect'), status: 'faulted' },
            {
              name: 'mixed',
              effect: Effect.failCause(Cause.combine(Cause.die('mixed-defect'), Cause.interrupt())),
              status: 'faulted',
            },
          ] as const
          for (const behavior of behaviors) {
            const definition = Task.make({
              ...contract,
              kind: `scheduler.${behavior.name}`,
              initial: () => ({ phase: 'one' as const, count: 0 }),
              phases: { one: () => behavior.effect, two: () => Effect.void },
            })
            yield* registry.install(
              Registry.withTask(
                Registry.toolkit({ id: behavior.name, version: 1 }, Toolkit.make()),
                definition,
              ),
            )
            const ref = yield* scheduler.start(definition, 0, { conversationId: root })
            const outcome = yield* scheduler.wait(ref)
            assert.strictEqual(outcome.status, behavior.status)
            if (behavior.name === 'typed' && outcome.status === 'failed') {
              assert.strictEqual(outcome.result, 'expected')
              assert.include(outcome.error.message, 'expected')
            }
            if (behavior.name === 'mixed' && outcome.status === 'faulted')
              assert.include(outcome.error.message, 'mixed-defect')
          }
        }),
      ).pipe(Effect.provide(Storage.layerMemory)),
  )

  it.effect(
    'rejects ended and foreign transaction writes, preserves first writer memos and revokes streams',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const { session, registry, scheduler } = yield* open
          const started = yield* Deferred.make<Access>()
          const release = yield* Deferred.make<void>()
          let leaked: Access | undefined
          const definition = Task.make({
            ...contract,
            initial: () => ({ phase: 'one' as const, count: 0 }),
            phases: {
              one: () =>
                Effect.gen(function* () {
                  const access = yield* TaskExecution
                  leaked = access
                  const winners = yield* Effect.forEach(
                    [1, 2],
                    (value) =>
                      access.commit((tx) => access.memoize(tx, '__proto__', Schema.Natural, value)),
                    { concurrency: 'unbounded' },
                  )
                  assert.deepStrictEqual(winners, [1, 1])
                  assert.deepStrictEqual(
                    yield* access.memo('__proto__', Schema.Natural),
                    Option.some(1),
                  )
                  assert.deepStrictEqual(
                    yield* access.memo('toString', Schema.Natural),
                    Option.none(),
                  )
                  yield* access.commit((tx) =>
                    Transaction.ensureDocument(tx, doc, {
                      scope: { _tag: 'task', taskId: access.taskId },
                    }),
                  )
                  yield* Deferred.succeed(started, access)
                  yield* Deferred.await(release)
                  yield* access.commit((tx) =>
                    access.progress(tx, contract, {
                      status: 'completing',
                      outcome: { status: 'completed', result: 1 },
                    }),
                  )
                }),
              two: () => Effect.void,
            },
          })
          yield* install(registry, registration(definition))
          const ref = yield* scheduler.start(definition, 0, { conversationId: root })
          const access = yield* Deferred.await(started)
          const foreign = yield* Session.commit(session, (tx) =>
            access.memoize(tx, 'foreign', Schema.String, 'x').pipe(Effect.flip),
          )
          assert.strictEqual(foreign._tag, 'TaskError')
          const watching = yield* Deferred.make<void>()
          const observer = yield* access
            .watch(doc, { scope: { _tag: 'task', taskId: ref.id } })
            .pipe(
              Stream.tap(() => Deferred.succeed(watching, undefined)),
              Stream.runDrain,
              Effect.exit,
              Effect.forkChild,
            )
          yield* Deferred.await(watching)
          yield* Deferred.succeed(release, undefined)
          yield* scheduler.wait(ref)
          yield* settle
          assert.strictEqual(
            (yield* leaked!.commit(() => Effect.void).pipe(Effect.flip))._tag,
            'TaskError',
          )
          assert.strictEqual(
            (yield* leaked!.memo('anything', Schema.String).pipe(Effect.flip))._tag,
            'TaskError',
          )
          const observed = yield* Fiber.join(observer)
          if (Exit.isFailure(observed)) assert.isTrue(Cause.hasInterruptsOnly(observed.cause))
          const record = yield* Session.task(session, ref.id)
          assert.isTrue(Option.isSome(record))
          if (Option.isSome(record)) assert.isUndefined(record.value.memos)
        }),
      ).pipe(Effect.provide(Storage.layerMemory)),
  )

  it.effect(
    'requires the exact container capability before start or resume, with no task writes',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const actual = yield* Storage.Storage
          const scope = yield* Scope.Scope
          const session = yield* Session.make().pipe(
            Effect.updateContext((_context: Context.Context<never>) =>
              Context.make(Storage.Storage, actual).pipe(Context.add(Scope.Scope, scope)),
            ),
          )
          yield* Session.commit(session, (tx) => Transaction.ensureRoot(tx))
          const registry = yield* Registry.make
          const scheduler = yield* TaskScheduler.make(session).pipe(
            Effect.provideService(Registry.Registry, registry),
          )
          const definition = counter([])
          yield* registry.install(registration(definition))
          const failed = yield* scheduler
            .start(definition, 0, { conversationId: root })
            .pipe(Effect.flip)
          assert.strictEqual(failed._tag, 'StorageFormatError')
          const resume = yield* scheduler.resume.pipe(Effect.flip)
          assert.strictEqual(resume._tag, 'StorageFormatError')
          assert.deepStrictEqual(yield* Session.scanTasks(session).pipe(Stream.runCollect), [])
        }),
      ).pipe(Effect.provide(Storage.layerMemory)),
  )
  it.effect('a refused future format cannot initialize, migrate or invoke custom work', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const actual = yield* Storage.Storage
        let writes = 0
        let callbacks = 0
        const rejected = new StorageFormat.StorageFormatError({
          reason: 'unsupported',
          message: 'Future container needs a newer reader',
        })
        const storage: Storage.Storage['Service'] = {
          ...actual,
          commit: (values) =>
            Effect.sync(() => {
              writes++
            }).pipe(Effect.andThen(actual.commit(values))),
        }
        const capability: StorageFormat.StorageFormat['Service'] = {
          storage,
          inspect: Effect.succeed({ formatVersion: 3, minimumReaderVersion: 3 }),
          requireSupportedFormat: () => Effect.fail(rejected),
        }
        yield* Effect.scoped(
          Effect.gen(function* () {
            const { session, registry, scheduler } = yield* open
            const record = yield* Session.commit(session, (tx) =>
              Transaction.createTask(tx, {
                kind: contract.kind,
                version: 1,
                conversationId: root,
                input: 0,
                background: false,
                abortRequested: false,
                state: { status: 'pending', checkpoint: { phase: 'one', count: 0 } },
              }),
            )
            const definition = Task.make({
              ...contract,
              version: 2,
              initial: () => {
                callbacks++
                return { phase: 'one' as const, count: 0 }
              },
              migrate: () =>
                Effect.sync(() => {
                  callbacks++
                  return { input: 0, checkpoint: { phase: 'one' as const, count: 0 } }
                }),
              phases: {
                one: () =>
                  Effect.sync(() => {
                    callbacks++
                  }),
                two: () => Effect.void,
              },
            })
            yield* registry.install(registration(definition))
            const before = writes
            assert.strictEqual(
              yield* scheduler.start(definition, 0, { conversationId: root }).pipe(Effect.flip),
              rejected,
            )
            assert.strictEqual(yield* scheduler.resume.pipe(Effect.flip), rejected)
            yield* settle
            assert.strictEqual(writes, before)
            assert.strictEqual(callbacks, 0)
            assert.deepStrictEqual(yield* Session.scanTasks(session).pipe(Stream.runCollect), [
              record,
            ])
          }),
        ).pipe(
          Effect.provideService(Storage.Storage, storage),
          Effect.provideService(StorageFormat.StorageFormat, capability),
        )
      }),
    ).pipe(Effect.provide(Storage.layerMemory)),
  )
})
