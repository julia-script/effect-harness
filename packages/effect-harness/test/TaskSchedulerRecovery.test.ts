import * as SqlEventJournal from 'effect/eventlog/SqlEventJournal'
import { assert, describe, it } from '@effect/vitest'
import { NodeServices } from '@effect/platform-node'
import * as SqliteClient from '@effect/sql-sqlite-node/SqliteClient'
import * as Context from 'effect/Context'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Fiber from 'effect/Fiber'
import * as Exit from 'effect/Exit'
import * as Scope from 'effect/Scope'
import * as Stream from 'effect/Stream'
import * as Record from 'effect-harness/Record'
import * as Registry from 'effect-harness/Registry'
import * as Session from 'effect-harness/Session'
import * as Storage from 'effect-harness/Storage'
import { StorageError } from 'effect-harness/StorageError'
import * as StorageFormat from 'effect-harness/StorageFormat'
import * as Task from 'effect-harness/Task'
import { TaskExecution, type Access } from 'effect-harness/TaskExecution'
import * as TaskScheduler from 'effect-harness/TaskScheduler'
import * as Toolkit from 'effect-harness/Toolkit'
import * as Transaction from 'effect-harness/Transaction'

const root = Record.ROOT_CONVERSATION_ID
const contract = {
  kind: 'scheduler.recovery',
  version: 1,
  input: Schema.Natural,
  checkpoint: Schema.Union([
    Schema.Struct({ phase: Schema.Literal('one'), count: Schema.Natural }),
    Schema.Struct({ phase: Schema.Literal('two'), count: Schema.Natural }),
  ]),
  success: Schema.Natural,
  failure: Schema.String,
}
const counter = <R1, R2>(
  one: Effect.Effect<void, never, R1>,
  two: Effect.Effect<void, never, R2>,
) =>
  Task.make({
    ...contract,
    initial: (count: number) => ({ phase: 'one' as const, count }),
    phases: {
      one: ({ checkpoint }) =>
        Effect.gen(function* () {
          const access = yield* TaskExecution
          yield* one
          yield* access.commit((tx) =>
            Effect.gen(function* () {
              yield* Transaction.appendEntry(tx, root, {
                kind: 'recovery.one',
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
          yield* two
          yield* access.commit((tx) =>
            Effect.gen(function* () {
              yield* Transaction.appendEntry(tx, root, {
                kind: 'recovery.two',
                data: checkpoint.count,
              })
              yield* access.progress(tx, contract, {
                status: 'completing',
                outcome: { status: 'completed', result: checkpoint.count + 1 },
              })
            }),
          )
        }),
    },
  })
const install = <
  I extends Schema.Constraint,
  C extends Task.CheckpointCodec,
  S extends Schema.Constraint,
  F extends Schema.Constraint,
  H extends Task.Phases<I, C>,
  B extends Task.Handler<I, C> | undefined,
  R,
  E,
>(
  registry: Registry.Service,
  definition: Task.Task<I, C, S, F, H, B, R, E>,
) =>
  registry.install(
    Registry.withTask(Registry.toolkit({ id: 'recovery', version: 1 }, Toolkit.make()), definition),
  )
const open = Effect.gen(function* () {
  const session = yield* Session.make()
  yield* Session.commit(session, Transaction.ensureRoot)
  const registry = yield* Registry.make
  const scheduler = yield* TaskScheduler.make(session).pipe(
    Effect.provideService(Registry.Registry, registry),
  )
  return { session, registry, scheduler }
})
const settle = Effect.forEach(Array.from({ length: 120 }), () => Effect.yieldNow, { discard: true })

describe('scheduler durable reopen and rejected persistence', () => {
  for (const backend of ['memory', 'sqlite', 'jsonl'] as const)
    it.effect(
      `${backend}: cold Session reopen resumes running checkpoint without repeating the committed phase`,
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const fs = yield* FileSystem.FileSystem
            const directory = yield* fs.makeTempDirectoryScoped()
            const memory = yield* Layer.build(Storage.layerMemory)
            const storage =
              backend === 'sqlite'
                ? Storage.layerJournal().pipe(
                    Layer.provide(SqlEventJournal.layer()),
                    Layer.provide(SqliteClient.layer({ filename: `${directory}/tasks.sqlite` })),
                  )
                : Storage.layerJsonl({ filePath: `${directory}/tasks.jsonl` })
            const use = <A, E>(effect: Effect.Effect<A, E, Storage.Storage | Scope.Scope>) =>
              backend === 'memory'
                ? effect.pipe(Effect.provideContext(memory))
                : effect.pipe(Effect.provide(Layer.fresh(storage)))
            const started = yield* Deferred.make<void>()
            const oldAccess = yield* Deferred.make<Access>()
            let oneRuns = 0
            let finalizers = 0
            const first = counter(
              Effect.sync(() => {
                oneRuns++
              }),
              Effect.gen(function* () {
                yield* Effect.addFinalizer(() =>
                  Effect.sync(() => {
                    finalizers++
                  }),
                )
                yield* Deferred.succeed(oldAccess, yield* TaskExecution)
                yield* Deferred.succeed(started, undefined)
                return yield* Effect.never
              }),
            )
            const id = yield* use(
              Effect.scoped(
                Effect.gen(function* () {
                  const { session, registry, scheduler } = yield* open
                  yield* install(registry, first)
                  const ref = yield* scheduler.start(first, 10, { conversationId: root })
                  yield* Deferred.await(started)
                  const record = yield* Session.task(session, ref.id)
                  assert.isTrue(Option.isSome(record))
                  if (Option.isSome(record))
                    assert.deepStrictEqual(record.value.state, {
                      status: 'running',
                      checkpoint: { phase: 'two', count: 11 },
                    })
                  return ref.id
                }),
              ),
            )
            assert.strictEqual(finalizers, 1)
            assert.strictEqual(
              (yield* (yield* Deferred.await(oldAccess))
                .commit(() => Effect.void)
                .pipe(Effect.flip))._tag,
              'TaskError',
            )
            const second = counter(
              Effect.sync(() => {
                oneRuns++
              }),
              Effect.void,
            )
            yield* use(
              Effect.scoped(
                Effect.gen(function* () {
                  const { session, registry, scheduler } = yield* open
                  yield* install(registry, second)
                  const before = yield* Session.task(session, id)
                  assert.isTrue(Option.isSome(before))
                  if (Option.isSome(before))
                    assert.strictEqual(before.value.state.status, 'running')
                  assert.deepStrictEqual(yield* scheduler.wait({ id, definition: second }), {
                    status: 'completed',
                    result: 12,
                  })
                  assert.strictEqual(oneRuns, 1)
                  const entries = yield* Session.scanEntries(session, {
                    conversationId: root,
                  }).pipe(Stream.runCollect)
                  assert.deepStrictEqual(entries.map((entry) => entry.kind).sort(), [
                    'recovery.one',
                    'recovery.two',
                  ])
                }),
              ),
            )
            yield* use(
              Effect.scoped(
                Effect.gen(function* () {
                  const { registry, scheduler } = yield* open
                  yield* install(registry, second)
                  assert.deepStrictEqual(yield* scheduler.wait({ id, definition: second }), {
                    status: 'completed',
                    result: 12,
                  })
                  assert.strictEqual(oneRuns, 1)
                }),
              ),
            )
          }),
        ).pipe(Effect.provide(NodeServices.layer)),
    )

  for (const failAt of ['reservation', 'phase'] as const)
    it.effect(
      `rejected ${failAt} write releases reservations, rolls back drafts and retries from durable state`,
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const actual = yield* Storage.Storage
            const format = yield* StorageFormat.StorageFormat
            let reject = true
            let phases = 0
            let finalizers = 0
            const io = new StorageError({
              reason: 'io',
              operation: 'scheduler.probe',
              message: 'Injected pre-admission failure',
            })
            const broken: Storage.Storage['Service'] = {
              ...actual,
              commit: (iterable) =>
                Effect.suspend(() => {
                  const writes = [...iterable]
                  const selected =
                    failAt === 'reservation'
                      ? writes.some(
                          (write) =>
                            write._tag === 'task' && write.value.state.status === 'running',
                        )
                      : writes.some((write) => write._tag === 'entry')
                  return reject && selected ? Effect.fail(io) : actual.commit(writes)
                }),
            }
            const services = Context.make(Storage.Storage, broken).pipe(
              Context.add(StorageFormat.StorageFormat, { ...format, storage: broken }),
            )
            yield* Effect.scoped(
              Effect.gen(function* () {
                const { session, registry, scheduler } = yield* open
                const definition = counter(
                  Effect.gen(function* () {
                    phases++
                    yield* Effect.addFinalizer(() =>
                      Effect.sync(() => {
                        finalizers++
                      }),
                    )
                  }),
                  Effect.void,
                )
                yield* install(registry, definition)
                const ref = yield* scheduler.start(definition, 0, { conversationId: root })
                yield* settle
                assert.deepStrictEqual(yield* scheduler.failure, Option.some(io))
                const record = yield* Session.task(session, ref.id)
                assert.isTrue(Option.isSome(record))
                if (Option.isSome(record))
                  assert.deepStrictEqual(record.value.state.checkpoint, { phase: 'one', count: 0 })
                assert.deepStrictEqual(
                  yield* Session.scanEntries(session, { conversationId: root }).pipe(
                    Stream.runCollect,
                  ),
                  [],
                )
                assert.strictEqual(phases, failAt === 'reservation' ? 0 : 1)
                assert.strictEqual(finalizers, phases)
                reject = false
                assert.deepStrictEqual(yield* scheduler.wait(ref), {
                  status: 'completed',
                  result: 2,
                })
                assert.strictEqual(phases, failAt === 'reservation' ? 1 : 2)
              }),
            ).pipe(Effect.provideContext(services))
          }),
        ).pipe(Effect.provide(Storage.layerMemory)),
    )
  it.effect(
    'rejects an already-entered masked callback when its invocation scope ends, rolling back domain drafts',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const session = yield* Session.make()
          yield* Session.commit(session, Transaction.ensureRoot)
          const registry = yield* Registry.make
          const scope = yield* Scope.make()
          yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void))
          const scheduler = yield* Scope.provide(
            TaskScheduler.make(session).pipe(Effect.provideService(Registry.Registry, registry)),
            scope,
          )
          const entered = yield* Deferred.make<void>()
          const release = yield* Deferred.make<void>()
          let finalizers = 0
          const definition = Task.make({
            ...contract,
            initial: () => ({ phase: 'one' as const, count: 0 }),
            phases: {
              one: () =>
                Effect.gen(function* () {
                  const access = yield* TaskExecution
                  yield* Effect.addFinalizer(() =>
                    Effect.sync(() => {
                      finalizers++
                    }),
                  )
                  yield* access.commit((tx) =>
                    Effect.uninterruptible(
                      Effect.gen(function* () {
                        yield* Transaction.appendEntry(tx, root, {
                          kind: 'ended.callback.must.rollback',
                          data: null,
                        })
                        yield* Deferred.succeed(entered, undefined)
                        yield* Deferred.await(release)
                      }),
                    ),
                  )
                }),
              two: () => Effect.void,
            },
          })
          yield* install(registry, definition)
          const ref = yield* scheduler.start(definition, 0, { conversationId: root })
          yield* Deferred.await(entered)
          const closing = yield* Scope.close(scope, Exit.void).pipe(Effect.forkChild)
          yield* settle
          yield* Deferred.succeed(release, undefined)
          yield* Fiber.join(closing)
          assert.strictEqual(finalizers, 1)
          assert.deepStrictEqual(
            yield* Session.scanEntries(session, { conversationId: root }).pipe(Stream.runCollect),
            [],
          )
          const record = yield* Session.task(session, ref.id)
          assert.isTrue(Option.isSome(record))
          if (Option.isSome(record))
            assert.deepStrictEqual(record.value.state, {
              status: 'running',
              checkpoint: { phase: 'one', count: 0 },
            })
          assert.strictEqual((yield* scheduler.resume.pipe(Effect.flip))._tag, 'TaskError')
        }),
      ).pipe(Effect.provide(Storage.layerMemory)),
  )
  it.effect(
    'native JSONL bytes remain unchanged when a phase batch is rejected before persistence',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem
          const directory = yield* fs.makeTempDirectoryScoped()
          const filePath = `${directory}/rejected-phase.jsonl`
          yield* Effect.scoped(
            Effect.gen(function* () {
              const actual = yield* Storage.Storage
              const format = yield* StorageFormat.StorageFormat
              const refused = yield* Deferred.make<void>()
              let reject = true
              let before: Uint8Array | undefined
              const io = new StorageError({
                reason: 'io',
                operation: 'scheduler.nativeJsonlProbe',
                message: 'Reject the phase before admission',
              })
              const storage: Storage.Storage['Service'] = {
                ...actual,
                commit: (iterable) =>
                  Effect.gen(function* () {
                    const writes = [...iterable]
                    if (!reject || !writes.some((write) => write._tag === 'entry'))
                      return yield* actual.commit(writes)
                    before = yield* fs.readFile(filePath).pipe(
                      Effect.mapError(
                        (cause) =>
                          new StorageError({
                            reason: 'io',
                            operation: 'scheduler.nativeJsonlProbe.read',
                            message: 'Cannot read probe bytes',
                            cause,
                          }),
                      ),
                    )
                    yield* Deferred.succeed(refused, undefined)
                    return yield* io
                  }),
              }
              yield* Effect.scoped(
                Effect.gen(function* () {
                  const { session, registry, scheduler } = yield* open
                  const definition = counter(Effect.void, Effect.void)
                  yield* install(registry, definition)
                  const ref = yield* scheduler.start(definition, 0, { conversationId: root })
                  yield* Deferred.await(refused)
                  yield* settle
                  assert.deepStrictEqual(yield* scheduler.failure, Option.some(io))
                  assert.isDefined(before)
                  assert.deepStrictEqual(yield* fs.readFile(filePath), before)
                  assert.deepStrictEqual(
                    yield* Session.scanEntries(session, { conversationId: root }).pipe(
                      Stream.runCollect,
                    ),
                    [],
                  )
                  assert.deepStrictEqual(
                    (yield* scheduler.inspect(ref.id)).record.state.checkpoint,
                    { phase: 'one', count: 0 },
                  )
                  reject = false
                  assert.deepStrictEqual(yield* scheduler.wait(ref), {
                    status: 'completed',
                    result: 2,
                  })
                  assert.notDeepEqual(yield* fs.readFile(filePath), before)
                }),
              ).pipe(
                Effect.provideService(Storage.Storage, storage),
                Effect.provideService(StorageFormat.StorageFormat, { ...format, storage }),
              )
            }),
          ).pipe(Effect.provide(Storage.layerJsonl({ filePath })))
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
  )
})
