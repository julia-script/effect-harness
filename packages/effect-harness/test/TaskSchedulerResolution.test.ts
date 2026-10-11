import { assert, describe, it } from '@effect/vitest'
import * as Context from 'effect/Context'
import * as StorageFormat from 'effect-harness/StorageFormat'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Fiber from 'effect/Fiber'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Scope from 'effect/Scope'
import * as Stream from 'effect/Stream'
import * as Record from 'effect-harness/Record'
import * as Registry from 'effect-harness/Registry'
import * as Session from 'effect-harness/Session'
import * as Storage from 'effect-harness/Storage'
import * as Task from 'effect-harness/Task'
import { TaskError } from 'effect-harness/TaskError'
import { TaskExecution, type Access } from 'effect-harness/TaskExecution'
import * as TaskScheduler from 'effect-harness/TaskScheduler'
import * as Toolkit from 'effect-harness/Toolkit'
import * as Transaction from 'effect-harness/Transaction'
import * as SessionRuntime from '../src/internal/SessionRuntime.js'

const root = Record.ROOT_CONVERSATION_ID
const contract = {
  kind: 'scheduler.resolution',
  version: 2,
  input: Schema.Natural,
  checkpoint: Schema.Struct({ phase: Schema.Literal('run'), count: Schema.Natural }),
  success: Schema.Natural,
  failure: Schema.String,
}
const finish = Effect.gen(function* () {
  const access = yield* TaskExecution
  yield* access.commit((tx) =>
    access.progress(tx, contract, {
      status: 'completing',
      outcome: { status: 'completed', result: 8 },
    }),
  )
})
const options = {
  ...contract,
  initial: () => ({ phase: 'run' as const, count: 0 }),
  phases: { run: () => finish },
}
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
  enabled = true,
) =>
  registry.install(
    Registry.withTask(
      Registry.toolkit({ id: 'resolution', version: 1, enabled }, Toolkit.make()),
      definition,
    ),
  )
const settle = Effect.forEach(Array.from({ length: 80 }), () => Effect.yieldNow, { discard: true })
const open = Effect.gen(function* () {
  const session = yield* Session.make()
  yield* Session.commit(session, Transaction.ensureRoot)
  const registry = yield* Registry.make
  const scheduler = yield* TaskScheduler.make(session).pipe(
    Effect.provideService(Registry.Registry, registry),
  )
  return { session, registry, scheduler }
})
const create = (session: Session.Session, version = 2) =>
  Session.commit(session, (tx) =>
    Transaction.createTask(tx, {
      kind: contract.kind,
      version,
      conversationId: root,
      input: 0,
      background: false,
      abortRequested: false,
      state: { status: 'pending', checkpoint: { phase: 'run', count: 0 } },
    }),
  )
const blocked = (inspection: TaskScheduler.Inspection, reason: string) => {
  assert.strictEqual(inspection.resolution._tag, 'blocked')
  if (inspection.resolution._tag === 'blocked')
    assert.strictEqual(inspection.resolution.error.reason, reason)
}

describe('scheduler resolution and invocation gates', () => {
  it.effect(
    'blocks missing/disabled/newer definitions and wakes after a compatible install without replaying terminal tasks',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const { session, registry, scheduler } = yield* open
          const record = yield* create(session)
          yield* scheduler.resume
          yield* settle
          blocked(yield* scheduler.inspect(record.id), 'missingDefinition')
          const definition = Task.make(options)
          yield* install(registry, definition, false)
          yield* settle
          blocked(yield* scheduler.inspect(record.id), 'disabledDefinition')
          yield* install(registry, Task.make({ ...options, version: 1 }))
          yield* settle
          blocked(yield* scheduler.inspect(record.id), 'newerVersion')
          yield* install(registry, definition)
          assert.deepStrictEqual(yield* scheduler.wait({ id: record.id, definition }), {
            status: 'completed',
            result: 8,
          })
          let invoked = 0
          yield* install(
            registry,
            Task.make({
              ...options,
              phases: {
                run: () =>
                  Effect.sync(() => {
                    invoked++
                  }),
              },
            }),
          )
          yield* scheduler.resume
          yield* settle
          assert.strictEqual(invoked, 0)
        }),
      ).pipe(Effect.provide(Storage.layerMemory)),
  )

  for (const defect of [false, true])
    it.effect(
      `caches ${defect ? 'defective' : 'typed failing'} migration until definition replacement`,
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const { session, registry, scheduler } = yield* open
            const record = yield* create(session, 1)
            let attempts = 0
            const definition = Task.make({
              ...options,
              migrate: () =>
                Effect.sync(() => {
                  attempts++
                }).pipe(
                  Effect.andThen(
                    defect
                      ? Effect.die('bad migration')
                      : Effect.fail(
                          new TaskError({
                            reason: 'migrationFailed',
                            operation: 'test',
                            message: 'bad migration',
                          }),
                        ),
                  ),
                ),
            })
            yield* install(registry, definition)
            yield* scheduler.resume
            yield* settle
            blocked(yield* scheduler.inspect(record.id), 'migrationFailed')
            yield* scheduler.resume
            yield* settle
            assert.strictEqual(attempts, 1)
            const before = yield* Session.task(session, record.id)
            assert.deepStrictEqual(before, Option.some(record))
            const compatible = Task.make({
              ...options,
              migrate: () =>
                Effect.succeed({ input: 0, checkpoint: { phase: 'run' as const, count: 0 } }),
            })
            yield* install(registry, compatible)
            assert.deepStrictEqual(
              yield* scheduler.wait({ id: record.id, definition: compatible }),
              { status: 'completed', result: 8 },
            )
            assert.strictEqual((yield* scheduler.inspect(record.id)).record.version, 2)
          }),
        ).pipe(Effect.provide(Storage.layerMemory)),
    )

  it.effect(
    'commits migration separately and revalidates replacement identity, version and abort before reservation',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const actual = yield* Storage.Storage
          const format = yield* StorageFormat.StorageFormat
          const migrated = yield* Deferred.make<void>()
          const release = yield* Deferred.make<void>()
          let paused = false
          let runCalls = 0
          let abortCalls = 0
          const storedStates: Array<{ version: number; status: string; abort: boolean }> = []
          const storage: Storage.Storage['Service'] = {
            ...actual,
            commit: (iterable) =>
              Effect.gen(function* () {
                const writes = [...iterable]
                const upgraded = writes.find(
                  (write) => write._tag === 'task' && write.value.version === 2,
                )
                const commit = yield* actual.commit(writes)
                for (const write of writes)
                  if (write._tag === 'task')
                    storedStates.push({
                      version: write.value.version,
                      status: write.value.state.status,
                      abort: write.value.abortRequested,
                    })
                if (!paused && upgraded !== undefined) {
                  paused = true
                  yield* Deferred.succeed(migrated, undefined)
                  yield* Deferred.await(release)
                }
                return commit
              }),
          }
          const services = Context.make(Storage.Storage, storage).pipe(
            Context.add(StorageFormat.StorageFormat, { ...format, storage }),
          )
          yield* Effect.scoped(
            Effect.gen(function* () {
              const { session, registry, scheduler } = yield* open
              const record = yield* create(session, 1)
              const definition = Task.make({
                ...options,
                migrate: () =>
                  Effect.succeed({ input: 1, checkpoint: { phase: 'run' as const, count: 2 } }),
                phases: {
                  run: () =>
                    Effect.sync(() => {
                      runCalls++
                    }),
                },
              })
              yield* install(registry, definition)
              yield* scheduler.resume
              yield* Deferred.await(migrated)
              // Session's line is still completing the migration batch. Queue an abort first.
              const abort = yield* Session.commit(session, (tx) =>
                Transaction.requestTaskAbort(tx, record.id),
              ).pipe(Effect.forkChild)
              yield* settle
              const replacement = Task.make({
                ...options,
                version: 3,
                migrate: () =>
                  Effect.succeed({ input: 3, checkpoint: { phase: 'run' as const, count: 4 } }),
                phases: {
                  run: () =>
                    Effect.sync(() => {
                      runCalls++
                    }),
                },
                abort: ({
                  checkpoint,
                }: {
                  readonly input: number
                  readonly checkpoint: typeof contract.checkpoint.Type
                }) =>
                  Effect.gen(function* () {
                    abortCalls++
                    assert.strictEqual(checkpoint.count, 4)
                    const access = yield* TaskExecution
                    yield* access.commit((tx) =>
                      access.progress(
                        tx,
                        { ...contract, version: 3 },
                        {
                          status: 'completing',
                          outcome: { status: 'aborted', reason: 'fresh replacement' },
                        },
                      ),
                    )
                  }),
              })
              yield* install(registry, replacement)
              yield* Deferred.succeed(release, undefined)
              yield* Fiber.join(abort)
              assert.deepStrictEqual(
                yield* scheduler.wait({ id: record.id, definition: replacement }),
                {
                  status: 'aborted',
                  reason: 'fresh replacement',
                },
              )
              assert.strictEqual(runCalls, 0, JSON.stringify(storedStates))
              assert.strictEqual(abortCalls, 1)
              assert.isTrue(
                storedStates.some((state) => state.version === 2 && state.status === 'pending'),
              )
              assert.isFalse(
                storedStates.some((state) => state.version === 2 && state.status === 'running'),
              )
              assert.isTrue(
                storedStates.some(
                  (state) => state.version === 3 && state.status === 'running' && state.abort,
                ),
              )
            }),
          ).pipe(Effect.provideContext(services))
        }),
      ).pipe(Effect.provide(Storage.layerMemory)),
  )

  it.effect(
    'an unrequested interruption preserves a retryable checkpoint and pauses without spinning',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const { registry, scheduler } = yield* open
          let calls = 0
          let interrupt = true
          const definition = Task.make({
            ...options,
            phases: {
              run: () =>
                Effect.suspend(() => {
                  calls++
                  return interrupt ? Effect.interrupt : finish
                }),
            },
          })
          yield* install(registry, definition)
          const ref = yield* scheduler.start(definition, 0, { conversationId: root })
          yield* settle
          assert.strictEqual(calls, 1)
          assert.strictEqual((yield* scheduler.inspect(ref.id)).record.state.status, 'pending')
          assert.isTrue(Option.isSome(yield* scheduler.failure))
          interrupt = false
          assert.deepStrictEqual(yield* scheduler.wait(ref), { status: 'completed', result: 8 })
          assert.strictEqual(calls, 2)
        }),
      ).pipe(Effect.provide(Storage.layerMemory)),
  )

  it.effect(
    'durable abort marks reject run callbacks before cleanup and cleanup gets fresh access and Scope',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const { session, registry, scheduler } = yield* open
          const entered = yield* Deferred.make<Access>()
          const runScope = yield* Deferred.make<Scope.Scope>()
          const abortEntered = yield* Deferred.make<Access>()
          const finishAbort = yield* Deferred.make<void>()
          let released = 0
          let cleanupScope: Scope.Scope | undefined
          const definition = Task.make({
            ...options,
            phases: {
              run: () =>
                Effect.gen(function* () {
                  const access = yield* TaskExecution
                  yield* Effect.addFinalizer(() =>
                    Effect.sync(() => {
                      released++
                    }),
                  )
                  yield* Deferred.succeed(runScope, yield* Scope.Scope)
                  yield* Deferred.succeed(entered, access)
                  return yield* Effect.never
                }),
            },
            abort: () =>
              Effect.gen(function* () {
                const access = yield* TaskExecution
                cleanupScope = yield* Scope.Scope
                yield* Deferred.succeed(abortEntered, access)
                yield* Deferred.await(finishAbort)
                yield* access.commit((tx) =>
                  access.progress(tx, contract, {
                    status: 'completing',
                    outcome: { status: 'aborted', reason: 'test' },
                  }),
                )
              }),
          })
          yield* install(registry, definition)
          const ref = yield* scheduler.start(definition, 0, { conversationId: root })
          const stale = yield* Deferred.await(entered)
          const scope = yield* Deferred.await(runScope)
          yield* Session.commit(session, (tx) => Transaction.requestTaskAbort(tx, ref.id))
          const revoked = yield* stale
            .commit((tx) => Transaction.appendEntry(tx, root, { kind: 'stale', data: null }))
            .pipe(Effect.flip)
          assert.strictEqual(revoked._tag, 'TaskError')
          const cleanup = yield* Deferred.await(abortEntered)
          assert.notStrictEqual(cleanup, stale)
          assert.notStrictEqual(cleanupScope, scope)
          assert.strictEqual(released, 1)
          yield* Deferred.succeed(finishAbort, undefined)
          assert.deepStrictEqual(yield* scheduler.wait(ref), { status: 'aborted', reason: 'test' })
          assert.deepStrictEqual(
            yield* Session.scanEntries(session, { conversationId: root }).pipe(Stream.runCollect),
            [],
          )
        }),
      ).pipe(Effect.provide(Storage.layerMemory)),
  )

  it.effect(
    'observer interruption leaves task live and scoped subscription cleanup removes observers',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const { session, registry, scheduler } = yield* open
          const entered = yield* Deferred.make<void>()
          const release = yield* Deferred.make<void>()
          const definition = Task.make({
            ...options,
            phases: {
              run: () =>
                Deferred.succeed(entered, undefined).pipe(
                  Effect.andThen(Deferred.await(release)),
                  Effect.andThen(finish),
                ),
            },
          })
          yield* install(registry, definition)
          const ref = yield* scheduler.start(definition, 0, { conversationId: root })
          yield* Deferred.await(entered)
          const observer = yield* scheduler.wait(ref).pipe(Effect.forkChild)
          yield* settle
          yield* Fiber.interrupt(observer)
          assert.strictEqual((yield* scheduler.inspect(ref.id)).record.state.status, 'running')
          yield* Deferred.succeed(release, undefined)
          assert.strictEqual((yield* scheduler.wait(ref)).status, 'completed')
          const state = yield* SessionRuntime.get(session)
          assert.strictEqual(state.reconciliations.size, 1)
          assert.strictEqual(state.subscribers.size, 0)
        }),
      ).pipe(Effect.provide(Storage.layerMemory)),
  )
})
