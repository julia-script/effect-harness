import * as TemporaryStorage from './fixtures/Storage.js'
import { assert, describe, it } from '@effect/vitest'
import { NodeServices } from '@effect/platform-node'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import type * as Scope from 'effect/Scope'
import * as Document from '../src/Document.js'
import * as Record from '../src/Record.js'
import * as Registry from '../src/Registry.js'
import * as Session from '../src/Session.js'
import * as Storage from '../src/Storage.js'
import * as Task from '../src/Task.js'
import { TaskExecution } from '../src/TaskExecution.js'
import * as TaskSupervision from '../src/TaskSupervision.js'
import * as Toolkit from '../src/Toolkit.js'
import * as Transaction from '../src/Transaction.js'

const root = Record.ROOT_CONVERSATION_ID
const contract = {
  kind: 'supervision.reopen',
  version: 1,
  input: Schema.Natural,
  checkpoint: Schema.Struct({ phase: Schema.Literal('run') }),
  success: Schema.Natural,
  failure: Schema.String,
}
const document = Document.define({
  kind: 'supervision.reopenDocument',
  version: 1,
  scope: 'task',
  schema: Schema.Struct({ n: Schema.Natural }),
  initial: () => ({ n: 1 }),
})
const settle = Effect.forEach(Array.from({ length: 100 }), () => Effect.yieldNow, { discard: true })
const open = Effect.gen(function* () {
  const session = yield* Session.make()
  yield* Session.commit(session, Transaction.ensureRoot)
  const registry = yield* Registry.make
  const scheduler = yield* TaskSupervision.make(session).pipe(
    Effect.provideService(Registry.Registry, registry),
  )
  return { session, registry, scheduler }
})

describe('native restart policy', () => {
  it.effect(
    'JSONL reopen preserves a two-child waiting checkpoint and ordered receipts without respawning',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem
          const directory = yield* fs.makeTempDirectoryScoped()
          const storage = Storage.layerJsonl({ filePath: `${directory}/join.jsonl` })
          const ready = yield* Deferred.make<void>()
          let spawning = 0
          let joining = 0
          let release = false
          const childContract = { ...contract, kind: 'supervision.reopenChild' }
          const child = Task.make({
            ...childContract,
            initial: () => ({ phase: 'run' as const }),
            phases: {
              run: ({ input }) =>
                Effect.gen(function* () {
                  if (!release) return yield* Effect.never
                  const access = yield* TaskExecution
                  yield* access.commit((tx) =>
                    access.progress(tx, childContract, {
                      status: 'completing',
                      outcome: { status: 'completed', result: input },
                    }),
                  )
                }),
            },
          })
          const parentContract = {
            ...contract,
            kind: 'supervision.reopenJoin',
            checkpoint: Schema.Union([
              Schema.Struct({ phase: Schema.Literal('spawn') }),
              Schema.Struct({ phase: Schema.Literal('join'), ids: Schema.Array(Record.TaskId) }),
            ]),
          }
          const parent = Task.make({
            ...parentContract,
            initial: () => ({ phase: 'spawn' as const }),
            phases: {
              spawn: () =>
                Effect.gen(function* () {
                  spawning++
                  const access = yield* TaskExecution
                  yield* access.commit((tx) =>
                    Effect.gen(function* () {
                      const first = yield* access.createChild(tx, child, 10)
                      const second = yield* access.createChild(tx, child, 20)
                      yield* access.progress(tx, parentContract, {
                        status: 'waiting',
                        checkpoint: { phase: 'join', ids: [second.id, first.id] },
                        on: [first.id, second.id],
                        policy: 'allSettled',
                      })
                    }),
                  )
                  yield* Deferred.succeed(ready, undefined)
                }),
              join: ({ checkpoint }) =>
                Effect.gen(function* () {
                  joining++
                  const access = yield* TaskExecution
                  assert.deepStrictEqual(yield* access.outcomes(checkpoint.ids), [
                    { status: 'completed', result: 20 },
                    { status: 'completed', result: 10 },
                  ])
                  yield* access.commit((tx) =>
                    access.progress(tx, parentContract, {
                      status: 'completing',
                      outcome: { status: 'completed', result: 30 },
                    }),
                  )
                }),
            },
          })
          const registration = Registry.withTask(
            Registry.withTask(Registry.toolkit({ id: 'join', version: 1 }, Toolkit.make()), child),
            parent,
          )
          const ref = yield* Effect.scoped(
            Effect.gen(function* () {
              const { registry, scheduler } = yield* open
              yield* registry.install(registration)
              const ref = yield* scheduler.start(parent, 0, { conversationId: root })
              yield* Deferred.await(ready)
              assert.strictEqual((yield* scheduler.inspect(ref.id)).record.state.status, 'waiting')
              return ref
            }),
          ).pipe(Effect.provide(Layer.fresh(storage)))
          release = true
          yield* Effect.scoped(
            Effect.gen(function* () {
              const { session, registry, scheduler } = yield* open
              yield* registry.install(registration)
              assert.deepStrictEqual(yield* scheduler.wait(ref), {
                status: 'completed',
                result: 30,
              })
              assert.strictEqual(
                (yield* Session.scanTasks(session).pipe(Stream.runCollect)).length,
                3,
              )
              assert.strictEqual(spawning, 1)
              assert.strictEqual(joining, 1)
            }),
          ).pipe(Effect.provide(Layer.fresh(storage)))
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
  )

  for (const backend of ['temporary', 'jsonl'] as const)
    it.effect(
      `${backend}: marks before dispatch, survives two missing-definition reopens, cleans once in fresh scope`,
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const fs = yield* FileSystem.FileSystem
            const directory = yield* fs.makeTempDirectoryScoped()
            const temporary = yield* Layer.build(TemporaryStorage.layer)
            const persistent = Storage.layerJsonl({ filePath: `${directory}/supervision.jsonl` })
            const use = <A, E>(effect: Effect.Effect<A, E, Storage.Storage | Scope.Scope>) =>
              backend === 'temporary'
                ? effect.pipe(Effect.provideContext(temporary))
                : effect.pipe(Effect.provide(Layer.fresh(persistent)))
            const entered = yield* Deferred.make<void>()
            let runs = 0
            let cleanup = 0
            let finalizers = 0
            const first = Task.make({
              ...contract,
              initial: () => ({ phase: 'run' as const }),
              phases: {
                run: () =>
                  Effect.gen(function* () {
                    runs++
                    const access = yield* TaskExecution
                    yield* access.commit((tx) =>
                      Transaction.ensureDocument(tx, document, {
                        scope: { _tag: 'task', taskId: access.taskId },
                      }).pipe(Effect.asVoid),
                    )
                    yield* Deferred.succeed(entered, undefined)
                    return yield* Effect.never
                  }),
              },
            })
            const id = yield* use(
              Effect.scoped(
                Effect.gen(function* () {
                  const { registry, scheduler } = yield* open
                  yield* registry.install(
                    Registry.withTask(
                      Registry.toolkit({ id: 'reopen', version: 1 }, Toolkit.make()),
                      first,
                    ),
                  )
                  const ref = yield* scheduler.start(first, 0, {
                    conversationId: root,
                    restartPolicy: 'abandon',
                  })
                  yield* Deferred.await(entered)
                  yield* scheduler.resume
                  assert.isFalse((yield* scheduler.inspect(ref.id)).record.abortRequested)
                  return ref.id
                }),
              ),
            )
            for (let reopen = 0; reopen < 2; reopen++)
              yield* use(
                Effect.scoped(
                  Effect.gen(function* () {
                    const { session, scheduler } = yield* open
                    yield* scheduler.resume
                    yield* settle
                    const inspection = yield* scheduler.inspect(id)
                    assert.deepStrictEqual(inspection.record.abortIntent, { _tag: 'restart' })
                    assert.strictEqual(inspection.record.state.status, 'pending')
                    assert.strictEqual(inspection.resolution._tag, 'blocked')
                    assert.isTrue(
                      Option.isSome(
                        yield* Session.snapshot(session, document, {
                          scope: { _tag: 'task', taskId: id },
                        }),
                      ),
                    )
                    assert.strictEqual(runs, 1)
                    assert.strictEqual(cleanup, 0)
                  }),
                ),
              )
            const definition = Task.make({
              ...contract,
              initial: () => ({ phase: 'run' as const }),
              phases: {
                run: () =>
                  Effect.sync(() => {
                    runs++
                  }),
              },
              abort: () =>
                Effect.gen(function* () {
                  cleanup++
                  yield* Effect.addFinalizer(() =>
                    Effect.sync(() => {
                      finalizers++
                    }),
                  )
                  const access = yield* TaskExecution
                  yield* access.commit((tx) =>
                    access.progress(tx, contract, {
                      status: 'completing',
                      outcome: { status: 'aborted', reason: 'restart cleanup' },
                    }),
                  )
                }),
            })
            yield* use(
              Effect.scoped(
                Effect.gen(function* () {
                  const { session, registry, scheduler } = yield* open
                  yield* scheduler.resume
                  yield* settle
                  yield* registry.install(
                    Registry.withTask(
                      Registry.toolkit({ id: 'reopen', version: 1 }, Toolkit.make()),
                      definition,
                    ),
                  )
                  assert.strictEqual((yield* scheduler.wait({ id, definition })).status, 'aborted')
                  yield* settle
                  assert.strictEqual(cleanup, 1)
                  assert.strictEqual(finalizers, 1)
                  assert.isTrue(
                    Option.isNone(
                      yield* Session.snapshot(session, document, {
                        scope: { _tag: 'task', taskId: id },
                      }),
                    ),
                  )
                }),
              ),
            )
            yield* use(
              Effect.scoped(
                Effect.gen(function* () {
                  const { registry, scheduler } = yield* open
                  yield* registry.install(
                    Registry.withTask(
                      Registry.toolkit({ id: 'reopen', version: 1 }, Toolkit.make()),
                      definition,
                    ),
                  )
                  assert.strictEqual((yield* scheduler.wait({ id, definition })).status, 'aborted')
                  assert.strictEqual(runs, 1)
                  assert.strictEqual(cleanup, 1)
                }),
              ),
            )
          }),
        ).pipe(Effect.provide(NodeServices.layer)),
    )

  it.effect(
    'explicit abort converts a restart mark, orphans unavailable work and retires documents',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const retained = yield* Layer.build(TemporaryStorage.layer)
          const id = yield* Effect.scoped(
            Effect.gen(function* () {
              const session = yield* Session.make()
              yield* Session.commit(session, Transaction.ensureRoot)
              return yield* Session.commit(session, (tx) =>
                Effect.gen(function* () {
                  const record = yield* Transaction.createTask(tx, {
                    conversationId: root,
                    kind: contract.kind,
                    version: 1,
                    input: 0,
                    background: false,
                    abortRequested: false,
                    restartPolicy: 'abandon',
                    state: { status: 'running', checkpoint: { phase: 'run' } },
                  })
                  yield* Transaction.ensureDocument(tx, document, {
                    scope: { _tag: 'task', taskId: record.id },
                  })
                  return record.id
                }),
              )
            }),
          ).pipe(Effect.provideContext(retained))
          yield* Effect.scoped(
            Effect.gen(function* () {
              const { session, scheduler } = yield* open
              yield* scheduler.resume
              yield* settle
              assert.deepStrictEqual((yield* scheduler.inspect(id)).record.abortIntent, {
                _tag: 'restart',
              })
              yield* scheduler.abort(id)
              const record = yield* scheduler.waitForTask(id)
              assert.deepStrictEqual(record.abortIntent, { _tag: 'request' })
              assert.deepStrictEqual(record.state.outcome, {
                status: 'orphaned',
                reason: 'missingDefinition',
              })
              assert.isTrue(
                Option.isNone(
                  yield* Session.snapshot(session, document, {
                    scope: { _tag: 'task', taskId: id },
                  }),
                ),
              )
            }),
          ).pipe(Effect.provideContext(retained))
        }),
      ),
  )
})
