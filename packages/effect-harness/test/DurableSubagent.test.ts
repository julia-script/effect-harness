import * as SqlEventJournal from 'effect/eventlog/SqlEventJournal'
import { NodeServices } from '@effect/platform-node'
import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Cause from 'effect/Cause'
import * as Context from 'effect/Context'
import * as Deferred from 'effect/Deferred'
import * as Exit from 'effect/Exit'
import * as FileSystem from 'effect/FileSystem'
import * as Fiber from 'effect/Fiber'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Scope from 'effect/Scope'
import * as Stream from 'effect/Stream'
import * as LanguageModel from 'effect/ai/LanguageModel'
import * as Prompt from 'effect/ai/Prompt'
import type * as Response from 'effect/ai/Response'
import * as HarnessRuntime from '../src/HarnessRuntime.js'
import * as Conversation from '../src/Conversation.js'
import * as Model from '../src/Model.js'
import { ProviderAffinity } from '../src/ProviderAffinity.js'
import * as Registry from '../src/Registry.js'
import * as Session from '../src/Session.js'
import * as Storage from '../src/Storage.js'
import * as StorageFormat from '../src/StorageFormat.js'
import { StorageError } from '../src/StorageError.js'
import * as Record from '../src/Record.js'
import * as Subagent from '../src/Subagent.js'
import * as Task from '../src/Task.js'
import * as TaskScheduler from '../src/TaskScheduler.js'
import * as Transaction from '../src/Transaction.js'
import * as AgentState from '../src/internal/AgentState.js'
import { TaskExecution } from '../src/TaskExecution.js'
import * as Toolkit from '../src/Toolkit.js'
import * as Usage from '../src/Usage.js'
import * as ScriptedStream from './fixtures/ScriptedStream.js'

const nativeBun = 'Bun' in globalThis
const services = nativeBun
  ? (await import('@effect/platform-bun')).BunServices.layer
  : NodeServices.layer

describe('native supervised conversations', () => {
  for (const closure of ['healthy', 'unobserved', 'explicit'] as const)
    it.live(
      `${closure}: coordinator closure without an authentic pending proof keeps ordinary behavior`,
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const base = yield* Layer.build(Storage.layerMemory)
            const actual = Context.get(base, Storage.Storage)
            const format = Context.get(base, StorageFormat.StorageFormat)
            const native = new StorageError({
              reason: 'io',
              operation: 'closure.native',
              message: 'Unobserved native write',
            })
            const full = Cause.combine(Cause.fail(native), Cause.die('unobserved defect'))
            let armed = false
            const captured: Storage.Storage['Service'] = {
              ...actual,
              commit: (writes) => (armed ? Effect.failCause(full) : actual.commit(writes)),
            }
            const storage = base.pipe(
              Context.add(Storage.Storage, captured),
              Context.add(StorageFormat.StorageFormat, { ...format, storage: captured }),
            )
            const sessionScope = yield* Scope.make()
            const schedulerScope = yield* Scope.make()
            yield* Effect.addFinalizer(() => Scope.close(sessionScope, Exit.void))
            yield* Effect.addFinalizer(() => Scope.close(schedulerScope, Exit.void))
            const session = yield* Session.make().pipe(
              Effect.provideContext(storage),
              Effect.provideService(Scope.Scope, sessionScope),
            )
            yield* Session.commit(session, (tx) =>
              Transaction.createConversation(tx, { ownership: { _tag: 'ownerless' } }),
            )
            const registry = yield* Registry.make
            const scheduler = yield* TaskScheduler.make(session).pipe(
              Effect.provideService(Registry.Registry, registry),
              Effect.provideService(Scope.Scope, schedulerScope),
            )
            yield* scheduler.resume
            const notifications = yield* Stream.toPull(scheduler.changes)
            if (closure === 'explicit') {
              yield* Scope.close(schedulerScope, Exit.void)
              yield* Scope.close(sessionScope, Exit.void)
              assert.isTrue(Option.isNone(yield* scheduler.failure))
              assert.isTrue(Exit.isFailure(yield* notifications.pipe(Effect.exit)))
              return
            }
            if (closure === 'unobserved') {
              armed = true
              const committed = yield* Session.commit(session, (tx) =>
                Transaction.createConversation(tx, { ownership: { _tag: 'ownerless' } }),
              ).pipe(Effect.exit)
              assert.isTrue(Exit.isFailure(committed))
              if (Exit.isFailure(committed)) assert.strictEqual(committed.cause, full)
            } else yield* Scope.close(sessionScope, Exit.void)
            let found = yield* scheduler.failure
            while (Option.isNone(found)) {
              yield* notifications
              found = yield* scheduler.failure
            }
            if (Option.isSome(found)) {
              if (found.value._tag === 'TaskError') {
                assert.strictEqual(found.value.operation, 'scheduler.session')
                assert.strictEqual(found.value.reason, 'revoked')
              } else if (closure === 'unobserved') assert.strictEqual(found.value, native)
              else {
                assert.strictEqual(found.value._tag, 'SessionError')
                if ('reason' in found.value) assert.strictEqual(found.value.reason, 'closed')
              }
              assert.strictEqual(
                found.value.cause,
                undefined,
                'Unobserved native failures cannot invent a full diagnostic',
              )
            }
          }),
        ).pipe(Effect.provide(services)),
    )
  for (const backend of ['memory', 'sqlite', 'jsonl'] as const)
    for (const projection of ['read', 'guard', 'reused', 'distinct', 'stale'] as const)
      it.live(
        `${backend}/${projection}: native occurrence proof rejects ordinary error reuse`,
        () =>
          Effect.scoped(
            Effect.gen(function* () {
              const fs = yield* FileSystem.FileSystem
              const directory = yield* fs.makeTempDirectoryScoped()
              const sqlite = nativeBun
                ? (yield* Effect.promise(() => import('@effect/sql-sqlite-bun/SqliteClient'))).layer
                : (yield* Effect.promise(() => import('@effect/sql-sqlite-node/SqliteClient')))
                    .layer
              const layer = (() => {
                if (backend === 'memory') return Storage.layerMemory
                if (backend === 'jsonl')
                  return Storage.layerJsonl({ filePath: `${directory}/proof.jsonl` })
                return Storage.layerJournal().pipe(
                  Layer.provide(SqlEventJournal.layer()),
                  Layer.provide(sqlite({ filename: `${directory}/proof.sqlite` })),
                )
              })()
              const base = yield* Layer.build(layer)
              const actual = Context.get(base, Storage.Storage)
              const format = Context.get(base, StorageFormat.StorageFormat)
              const critical = new StorageError({
                reason: 'io',
                operation: 'proof.native',
                message: 'Actual rejected native commit',
              })
              const distinct = new StorageError({
                reason: 'io',
                operation: 'proof.distinct',
                message: 'Independent later failure',
              })
              const full = Cause.combine(
                Cause.fail(critical),
                Cause.combine(Cause.die('proof native defect'), Cause.interrupt(992)),
              )
              let armed = false
              let attempts = 0
              let callbacks = 0
              let phaseCause: Cause.Cause<unknown> | undefined
              let lastReason: Cause.Reason<unknown> | undefined
              let staleCause: Cause.Cause<import('../src/TaskError.js').Failure> | undefined
              const captured: Storage.Storage['Service'] = {
                ...actual,
                commit: (input) => {
                  if (!armed) return actual.commit(input)
                  attempts++
                  armed = false
                  return Effect.failCause(full)
                },
              }
              const storage = base.pipe(
                Context.add(Storage.Storage, captured),
                Context.add(StorageFormat.StorageFormat, { ...format, storage: captured }),
              )
              const ready = yield* Deferred.make<void>()
              const begin = yield* Deferred.make<void>()
              const released = yield* Deferred.make<void>()
              const secondReady = yield* Deferred.make<void>()
              const secondBegin = yield* Deferred.make<void>()
              const secondReleased = yield* Deferred.make<void>()
              const contract = {
                kind: 'subagent.occurrence-proof',
                version: 1,
                input: Schema.Boolean,
                checkpoint: Schema.Struct({ phase: Schema.tag('run') }),
                success: Schema.Void,
                failure: Schema.String,
              } as const
              const program = Task.make({
                ...contract,
                initial: () => ({ phase: 'run' as const }),
                phases: {
                  run: ({ input }) =>
                    Effect.gen(function* () {
                      if (input) {
                        yield* Effect.addFinalizer(() =>
                          Deferred.succeed(secondReleased, undefined),
                        )
                        yield* Deferred.succeed(secondReady, undefined)
                        yield* Deferred.await(secondBegin)
                        if (staleCause === undefined)
                          return yield* Effect.die('Missing actual prior read Cause')
                        return yield* Effect.failCause(staleCause)
                      }
                      yield* Effect.addFinalizer(() => Deferred.succeed(released, undefined))
                      yield* Deferred.succeed(ready, undefined)
                      yield* Deferred.await(begin)
                      const access = yield* TaskExecution
                      armed = true
                      const committed = yield* access
                        .commit((tx) => {
                          callbacks++
                          return access.progress(tx, contract, {
                            status: 'pending',
                            checkpoint: { phase: 'run' },
                          })
                        })
                        .pipe(Effect.exit)
                      assert.strictEqual(committed._tag, 'Failure')
                      if (Exit.isFailure(committed)) assert.strictEqual(committed.cause, full)
                      const read = access.memo('proof', Schema.Void).pipe(Effect.asVoid)
                      const guard = access.subagent!.check
                      const readExits = yield* Effect.all([Effect.exit(read), Effect.exit(read)], {
                        concurrency: 'unbounded',
                      })
                      if (Exit.isFailure(readExits[0])) staleCause = readExits[0].cause
                      const guardExits = yield* Effect.all(
                        [Effect.exit(guard), Effect.exit(guard)],
                        { concurrency: 'unbounded' },
                      )
                      for (const exits of [readExits, guardExits]) {
                        assert.isTrue(Exit.isFailure(exits[0]) && Exit.isFailure(exits[1]))
                        if (Exit.isFailure(exits[0]) && Exit.isFailure(exits[1])) {
                          assert.notStrictEqual(
                            exits[0].cause.reasons[0],
                            exits[1].cause.reasons[0],
                          )
                          assert.isTrue(Cause.isFailReason(exits[0].cause.reasons[0]!))
                          const reason = exits[0].cause.reasons[0]!
                          if (Cause.isFailReason(reason)) assert.strictEqual(reason.error, critical)
                        }
                      }
                      const denied = yield* access
                        .commit(() => {
                          callbacks++
                          return Effect.void
                        })
                        .pipe(Effect.exit)
                      assert.strictEqual(denied._tag, 'Failure')
                      assert.strictEqual(
                        callbacks,
                        1,
                        'Poisoned native commit cannot enter a callback',
                      )
                      assert.strictEqual(
                        attempts,
                        1,
                        'Poison projections cannot publish another batch',
                      )
                      if (projection === 'reused') return yield* critical
                      if (projection === 'distinct' || projection === 'stale')
                        return yield* distinct
                      return yield* (projection === 'read' ? read : guard).pipe(
                        Effect.onExit((exit) =>
                          Effect.sync(() => {
                            if (Exit.isFailure(exit)) lastReason = exit.cause.reasons[0]
                          }),
                        ),
                      )
                    }).pipe(
                      Effect.onExit((exit) =>
                        Effect.sync(() => {
                          if (Exit.isFailure(exit)) phaseCause = exit.cause
                        }),
                      ),
                    ),
                },
              })
              const provider = yield* ScriptedStream.make({
                generateText: () => Effect.die('Occurrence proof must never enter a provider'),
              })
              const runtime = yield* HarnessRuntime.make().pipe(
                Effect.provideContext(storage),
                Effect.provideService(LanguageModel.LanguageModel, provider),
              )
              yield* runtime.registry.install(
                Registry.withTask(
                  Registry.toolkit({ id: contract.kind, version: 1 }, Toolkit.make()),
                  program,
                ),
              )
              const root = yield* runtime.backend.root
              const parent = yield* runtime.scheduler.start(program, false, {
                conversationId: root,
              })
              if (projection === 'stale') {
                yield* runtime.scheduler.start(program, true, { conversationId: root })
                yield* Deferred.await(secondReady)
              }
              yield* Deferred.await(ready)
              const notifications = yield* Stream.toPull(runtime.scheduler.changes)
              const observed = yield* Effect.gen(function* () {
                yield* Deferred.await(released)
                yield* notifications
                return yield* runtime.scheduler.failure
              }).pipe(Effect.forkChild)
              yield* Deferred.succeed(begin, undefined)
              const health = yield* Fiber.join(observed)
              assert.isTrue(Option.isSome(health))
              if (Option.isSome(health)) {
                if (projection === 'reused') assert.strictEqual(health.value, critical)
                else if (projection === 'distinct' || projection === 'stale')
                  assert.strictEqual(health.value, distinct)
                else {
                  assert.strictEqual(health.value.cause, full)
                  assert.isDefined(lastReason)
                  assert.strictEqual(phaseCause?.reasons[0], lastReason)
                }
              }
              if (projection === 'stale') {
                yield* Deferred.succeed(secondBegin, undefined)
                yield* Deferred.await(secondReleased)
                yield* notifications
                const later = yield* runtime.scheduler.failure
                assert.isTrue(Option.isSome(later))
                if (Option.isSome(later))
                  assert.strictEqual(
                    later.value,
                    critical,
                    'Cleared diagnostic cannot accept old native read proof',
                  )
              }
              const projected = yield* runtime.scheduler.wait(parent).pipe(Effect.exit)
              assert.isTrue(Exit.isFailure(projected))
              if (Exit.isFailure(projected)) {
                const reason = projected.cause.reasons[0]!
                if (Cause.isFailReason(reason))
                  assert.strictEqual(
                    reason.error,
                    critical,
                    'Public Session poison stays first-error projected',
                  )
              }
            }),
          ).pipe(Effect.provide(services)),
      )
  for (const backend of ['memory', 'sqlite', 'jsonl'] as const)
    for (const mode of ['foreground', 'background'] as const)
      for (const stage of ['admission', 'settlement'] as const)
        for (const fault of ['reject', 'uncertain'] as const)
          it.live(
            `${backend}/${mode}/${stage}/${fault}: cold native subagent recovery preserves actual commits and never repeats paid work`,
            () =>
              Effect.scoped(
                Effect.gen(function* () {
                  const fs = yield* FileSystem.FileSystem
                  const directory = yield* fs.makeTempDirectoryScoped()
                  const sqlite = nativeBun
                    ? (yield* Effect.promise(() => import('@effect/sql-sqlite-bun/SqliteClient')))
                        .layer
                    : (yield* Effect.promise(() => import('@effect/sql-sqlite-node/SqliteClient')))
                        .layer
                  const layer = (() => {
                    if (backend === 'memory') return Storage.layerMemory
                    if (backend === 'jsonl')
                      return Storage.layerJsonl({ filePath: `${directory}/fault.jsonl` })
                    return Storage.layerJournal().pipe(
                      Layer.provide(SqlEventJournal.layer()),
                      Layer.provide(sqlite({ filename: `${directory}/fault.sqlite` })),
                    )
                  })()
                  const ownerScope = yield* Scope.Scope
                  const firstScope = yield* Scope.make()
                  yield* Effect.addFinalizer(() => Scope.close(firstScope, Exit.void))
                  const base = yield* Layer.build(layer).pipe(
                    Effect.provideService(
                      Scope.Scope,
                      backend === 'memory' ? ownerScope : firstScope,
                    ),
                  )
                  const actual = Context.get(base, Storage.Storage)
                  const format = Context.get(base, StorageFormat.StorageFormat)
                  let intercepted: ReadonlyArray<Record.StorageWrite> | undefined
                  let firstId: Record.TaskId | undefined
                  let requests = 0
                  let receipt: Subagent.Receipt | undefined
                  let admitted: Subagent.Handle | undefined
                  let warmAcknowledged = false
                  let parentCause: Cause.Cause<unknown> | undefined
                  let warm = true
                  const critical = new StorageError({
                    reason: fault === 'uncertain' ? 'uncertain' : 'io',
                    operation: 'subagent.fault',
                    message: `Injected ${stage} ${fault}`,
                  })
                  const full = Cause.combine(
                    Cause.fail(critical),
                    Cause.combine(Cause.die('subagent native fault defect'), Cause.interrupt(991)),
                  )
                  const captured: Storage.Storage['Service'] = {
                    ...actual,
                    commit: (input) => {
                      const writes = [...input]
                      const supervisor = writes.find(
                        (write) =>
                          write._tag === 'task' &&
                          write.value.kind === 'harness.subagent' &&
                          (stage === 'admission'
                            ? write.value.state.status === 'pending'
                            : write.value.state.status === 'completing' ||
                              write.value.state.status === 'terminal'),
                      )
                      if (
                        intercepted !== undefined ||
                        supervisor === undefined ||
                        supervisor._tag !== 'task'
                      )
                        return actual.commit(writes)
                      intercepted = writes
                      firstId = supervisor.value.id
                      const failed = Effect.failCause(full)
                      return fault === 'uncertain'
                        ? actual.commit(writes).pipe(Effect.andThen(failed))
                        : failed
                    },
                  }
                  const storage = base.pipe(
                    Context.add(Storage.Storage, captured),
                    Context.add(StorageFormat.StorageFormat, { ...format, storage: captured }),
                  )
                  const contract = {
                    kind: 'subagent.fault-owner',
                    version: 1,
                    input: Schema.Void,
                    checkpoint: Schema.Struct({ phase: Schema.tag('run') }),
                    success: Schema.Void,
                    failure: Schema.String,
                  } as const
                  const ready = yield* Deferred.make<void>()
                  const begin = yield* Deferred.make<void>()
                  const invocationReleased = yield* Deferred.make<void>()
                  const program = Task.make({
                    ...contract,
                    initial: () => ({ phase: 'run' as const }),
                    phases: {
                      run: () =>
                        Effect.gen(function* () {
                          if (warm) {
                            yield* Effect.addFinalizer(() =>
                              Deferred.succeed(invocationReleased, undefined),
                            )
                            yield* Deferred.succeed(ready, undefined)
                            yield* Deferred.await(begin)
                          }
                          const access = yield* TaskExecution
                          admitted = yield* Subagent.spawn(access, {
                            draft: { type: 'input', content: 'Paid native child' },
                            options: { mode, key: 'fault' },
                          })
                          if (warm) warmAcknowledged = true
                          const result = yield* Subagent.wait(access, admitted)
                          receipt = result.receipt
                          if (receipt === undefined)
                            return yield* Effect.die('Missing actual child receipt')
                          assert.strictEqual(receipt.submission.status, 'done')
                          yield* access.commit((tx) =>
                            access.progress(tx, contract, {
                              status: 'completing',
                              outcome: { status: 'completed', result: undefined },
                            }),
                          )
                        }).pipe(
                          Effect.onExit((exit) =>
                            Effect.sync(() => {
                              if (Exit.isFailure(exit)) parentCause = exit.cause
                            }),
                          ),
                        ),
                    },
                  })
                  const provider = yield* ScriptedStream.make({
                    generateText: () =>
                      Effect.sync(() => {
                        requests++
                        assert.strictEqual(requests, 1, 'Recovery cannot repeat paid provider work')
                        return [
                          { type: 'text' as const, text: 'Paid result' },
                          {
                            type: 'finish' as const,
                            reason: 'stop' as const,
                            usage: {
                              inputTokens: {
                                total: 2,
                                uncached: 2,
                                cacheRead: undefined,
                                cacheWrite: undefined,
                              },
                              outputTokens: { total: 1, text: 1, reasoning: undefined },
                            },
                            response: undefined,
                          },
                        ]
                      }),
                  })
                  const runtime = yield* HarnessRuntime.make().pipe(
                    Effect.provideContext(storage),
                    Effect.provideService(LanguageModel.LanguageModel, provider),
                    Effect.provideService(Scope.Scope, firstScope),
                  )
                  yield* runtime.registry.install(
                    Registry.withTask(
                      Registry.toolkit({ id: contract.kind, version: 1 }, Toolkit.make()),
                      program,
                    ),
                  )
                  const root = yield* runtime.backend.root
                  const parent = yield* runtime.scheduler.start(program, undefined, {
                    conversationId: root,
                  })
                  yield* Deferred.await(ready)
                  // toPull acquires Channel.fromPubSubArray's actual subscription before the fault.
                  const notifications = yield* Stream.toPull(runtime.scheduler.changes)
                  const fastNotifications = yield* Stream.toPull(runtime.scheduler.changes)
                  const fastObserved = yield* Effect.gen(function* () {
                    while (true) {
                      yield* fastNotifications
                      const health = yield* runtime.scheduler.failure
                      if (Option.isSome(health)) return health.value
                    }
                  }).pipe(Effect.forkChild)
                  const observed = yield* Effect.gen(function* () {
                    yield* Deferred.await(invocationReleased)
                    while (true) {
                      yield* notifications
                      const health = yield* runtime.scheduler.failure
                      if (Option.isSome(health)) return health.value
                    }
                  }).pipe(Effect.forkChild)
                  yield* Deferred.succeed(begin, undefined)
                  assert.strictEqual(
                    (yield* runtime.scheduler.wait(parent).pipe(Effect.exit))._tag,
                    'Failure',
                  )
                  const health = yield* Fiber.join(observed).pipe(Effect.timeout('3 seconds'))
                  assert.include(
                    health.message,
                    'subagent native fault defect',
                    `${health._tag}/${'operation' in health ? health.operation : 'schema'}: parent ${parentCause === undefined ? 'no failure' : Cause.pretty(parentCause)}`,
                  )
                  const fastHealth = yield* Fiber.join(fastObserved)
                  assert.strictEqual(fastHealth.cause, full)
                  assert.strictEqual(health.cause, full)
                  assert.isTrue(Cause.isCause(health.cause))
                  if (Cause.isCause(health.cause)) {
                    assert.include(Cause.pretty(health.cause), `Injected ${stage} ${fault}`)
                    assert.include(Cause.pretty(health.cause), 'subagent native fault defect')
                    assert.isTrue(Cause.hasInterrupts(health.cause))
                  }
                  assert.isDefined(intercepted)
                  assert.isDefined(firstId)
                  assert.strictEqual(warmAcknowledged, stage === 'settlement')
                  const batch = intercepted ?? []
                  if (stage === 'admission') {
                    assert.isTrue(
                      batch.some(
                        (write) =>
                          write._tag === 'document.create' &&
                          write.record.kind === 'harness.subagent.slots',
                      ),
                    )
                    assert.strictEqual(
                      batch.some(
                        (write) =>
                          write._tag === 'conversation' && write.value.owner?.taskId === firstId,
                      ),
                      mode === 'foreground',
                    )
                    assert.strictEqual(
                      batch.some((write) => write._tag === 'submission'),
                      mode === 'foreground',
                    )
                    assert.strictEqual(
                      batch.some((write) => write._tag === 'entry'),
                      false,
                    )
                  }
                  const saved = yield* actual.task(firstId!)
                  assert.strictEqual(
                    Option.isSome(saved),
                    stage === 'settlement' || fault === 'uncertain',
                  )
                  if (stage === 'settlement') assert.strictEqual(requests, 1)
                  if (fault === 'uncertain')
                    assert.strictEqual(
                      (yield* runtime.backend.agent(root).pipe(Effect.exit))._tag,
                      'Failure',
                    )
                  yield* Scope.close(firstScope, Exit.void)
                  warm = false
                  const coldScope = yield* Scope.make()
                  yield* Effect.addFinalizer(() => Scope.close(coldScope, Exit.void))
                  const reopenedStorage =
                    backend === 'memory'
                      ? base
                      : yield* Layer.build(layer).pipe(
                          Effect.provideService(Scope.Scope, coldScope),
                        )
                  const reopened = yield* HarnessRuntime.make().pipe(
                    Effect.provideContext(reopenedStorage),
                    Effect.provideService(LanguageModel.LanguageModel, provider),
                    Effect.provideService(Scope.Scope, coldScope),
                  )
                  yield* reopened.registry.install(
                    Registry.withTask(
                      Registry.toolkit({ id: contract.kind, version: 1 }, Toolkit.make()),
                      program,
                    ),
                  )
                  yield* reopened.backend.root
                  assert.strictEqual((yield* reopened.scheduler.wait(parent)).status, 'completed')
                  assert.strictEqual(requests, 1)
                  if (admitted === undefined || receipt === undefined)
                    return yield* Effect.die('Cold child was not observed')
                  if (fault === 'uncertain' || stage === 'settlement')
                    assert.strictEqual(admitted.taskId, firstId)
                  assert.strictEqual(
                    (yield* reopened.backend.wait(receipt.submissionId)).status,
                    'done',
                  )
                  assert.lengthOf(
                    (yield* Session.scanEntries(reopened.session, {
                      conversationId: receipt.conversationId,
                    }).pipe(Stream.runCollect)).filter((entry) => entry.kind === 'assistant'),
                    1,
                  )
                  assert.lengthOf(
                    yield* Session.commit(reopened.session, (tx) =>
                      Usage.receipts(tx, receipt!.conversationId),
                    ),
                    1,
                  )
                  assert.isTrue(Option.isNone(yield* reopened.scheduler.failure))
                }),
              ).pipe(Effect.provide(services)),
          )
  for (const backend of ['memory', 'sqlite', 'jsonl'] as const)
    for (const mode of ['foreground', 'background'] as const)
      it.live(
        `${backend}/${mode}: admission freezes stored selection, overrides, null clears and fork asOf`,
        () =>
          Effect.scoped(
            Effect.gen(function* () {
              const fs = yield* FileSystem.FileSystem
              const directory = yield* fs.makeTempDirectoryScoped()
              const sqlite = nativeBun
                ? (yield* Effect.promise(() => import('@effect/sql-sqlite-bun/SqliteClient'))).layer
                : (yield* Effect.promise(() => import('@effect/sql-sqlite-node/SqliteClient')))
                    .layer
              const storageLayer = (() => {
                if (backend === 'memory') return Storage.layerMemory
                if (backend === 'jsonl')
                  return Storage.layerJsonl({ filePath: `${directory}/selection.jsonl` })
                return Storage.layerJournal().pipe(
                  Layer.provide(SqlEventJournal.layer()),
                  Layer.provide(sqlite({ filename: `${directory}/selection.sqlite` })),
                )
              })()
              const storage = yield* Layer.build(storageLayer)
              const selectedA = {
                ref: { provider: 'selection', modelId: 'a' },
                options: { temperature: 1 },
              }
              const selectedB = {
                ref: { provider: 'selection', modelId: 'b' },
                options: { temperature: 2 },
              }
              const calls: string[] = []
              const affinities: string[] = []
              const models = yield* Effect.forEach(['a', 'b'] as const, (name) =>
                ScriptedStream.make({
                  generateText: () =>
                    Effect.sync(() => {
                      calls.push(name)
                      return [
                        { type: 'text' as const, text: name },
                        {
                          type: 'finish' as const,
                          reason: 'stop' as const,
                          usage: {
                            inputTokens: {
                              total: 2,
                              uncached: 2,
                              cacheRead: undefined,
                              cacheWrite: undefined,
                            },
                            outputTokens: { total: 1, text: 1, reasoning: undefined },
                          },
                          response: undefined,
                        },
                      ]
                    }),
                }).pipe(
                  Effect.map((native) =>
                    Model.make({
                      definition: {
                        ref: { provider: 'selection', modelId: name },
                        capabilities: {
                          tools: true,
                          reasoning: false,
                          images: false,
                          structuredOutput: false,
                        },
                      },
                      languageModel: native,
                      options: Schema.Struct({ temperature: Schema.optionalKey(Schema.Finite) }),
                      configure: (options) =>
                        Effect.gen(function* () {
                          assert.isTrue(
                            options.temperature === undefined ||
                              options.temperature === (name === 'a' ? 1 : 2),
                          )
                          affinities.push((yield* ProviderAffinity).id)
                          return Context.empty()
                        }),
                    }),
                  ),
                ),
              )
              const contract = {
                kind: 'subagent.selection',
                version: 1,
                input: Schema.Void,
                checkpoint: Schema.Struct({ phase: Schema.tag('run') }),
                success: Schema.Void,
                failure: Schema.String,
              } as const
              const program = Task.make({
                ...contract,
                initial: () => ({ phase: 'run' as const }),
                phases: {
                  run: () =>
                    Effect.gen(function* () {
                      const access = yield* TaskExecution
                      yield* Conversation.configure(
                        yield* access.conversation(access.conversationId),
                        { model: selectedA, instructions: 'original selection' },
                      )
                      const at = yield* access.commit((tx) =>
                        Transaction.appendEntry(tx, access.conversationId, {
                          kind: 'historical.selection',
                        }),
                      )
                      if (access.subagent === undefined)
                        return yield* Effect.die('Missing native subagent')
                      const capability = access.subagent
                      const inherited = yield* access.commit((tx) =>
                        Effect.gen(function* () {
                          const handle = yield* capability.admit(tx, {
                            draft: { type: 'input', content: 'inherit' },
                            options: { mode, key: 'inherited' },
                          })
                          yield* Transaction.setDocument(
                            tx,
                            AgentState.document,
                            AgentState.target(access.conversationId),
                            { model: selectedB, instructions: 'changed after admission' },
                          )
                          return handle
                        }),
                      )
                      const variants = [
                        {
                          handle: inherited,
                          model: selectedA,
                          instructions: 'original selection',
                          provider: 'a',
                        },
                        {
                          handle: yield* Subagent.spawn(access, {
                            draft: { type: 'input', content: 'override' },
                            options: {
                              mode,
                              key: 'override',
                              agent: { model: selectedB, instructions: 'explicit override' },
                            },
                          }),
                          model: selectedB,
                          instructions: 'explicit override',
                          provider: 'b',
                        },
                        {
                          handle: yield* Subagent.spawn(access, {
                            draft: { type: 'input', content: 'clear' },
                            options: {
                              mode,
                              key: 'clear',
                              agent: { model: null, instructions: null },
                            },
                          }),
                          model: undefined,
                          instructions: undefined,
                          provider: 'a',
                        },
                        {
                          handle: yield* Subagent.spawn(access, {
                            draft: { type: 'input', content: 'fork' },
                            options: {
                              mode,
                              key: 'historical',
                              fork: { conversationId: access.conversationId, at: at.id },
                            },
                          }),
                          model: selectedA,
                          instructions: 'original selection',
                          provider: 'a',
                        },
                      ]
                      for (const variant of variants) {
                        const result = yield* Subagent.wait(access, variant.handle)
                        assert.strictEqual(result.receipt?.submission.status, 'done')
                        const child = yield* Subagent.open(access, variant.handle)
                        const agent = yield* access.commit((tx) =>
                          Transaction.snapshot(
                            tx,
                            AgentState.document,
                            AgentState.target(child.id),
                          ).pipe(
                            Effect.map((snapshot) =>
                              Option.isSome(snapshot) ? snapshot.value.value : {},
                            ),
                          ),
                        )
                        assert.deepStrictEqual(agent.model, variant.model)
                        assert.strictEqual(agent.instructions, variant.instructions)
                        const entries = yield* Conversation.entries(child).pipe(
                          Stream.filter((entry) => entry.kind === 'assistant'),
                          Stream.take(1),
                          Stream.runCollect,
                        )
                        const assistant = entries
                          .filter((entry) => entry.kind === 'assistant')
                          .at(-1)
                        const messages = yield* Schema.decodeUnknownEffect(
                          Schema.Array(Schema.toCodecJson(Prompt.Message)),
                        )(assistant?.model)
                        assert.strictEqual(
                          messages
                            .flatMap((message) =>
                              message.role === 'assistant'
                                ? message.content
                                    .filter((part) => part.type === 'text')
                                    .map((part) => part.text)
                                : [],
                            )
                            .join(''),
                          variant.provider,
                        )
                      }
                      yield* access.commit((tx) =>
                        access.progress(tx, contract, {
                          status: 'completing',
                          outcome: { status: 'completed', result: undefined },
                        }),
                      )
                    }),
                },
              })
              const registry = yield* Registry.make
              yield* registry.install(
                Registry.withTask(
                  Registry.toolkit({ id: contract.kind, version: 1 }, Toolkit.make()),
                  program,
                ),
              )
              const runtime = yield* HarnessRuntime.make({
                registry,
                models,
                agent: { model: selectedA, instructions: 'original selection' },
              }).pipe(Effect.provideContext(storage))
              const ref = yield* runtime.scheduler.start(program, undefined, {
                conversationId: yield* runtime.backend.root,
              })
              const outcome = yield* runtime.scheduler.wait(ref)
              if (outcome.status !== 'completed') yield* Effect.logError(outcome)
              assert.strictEqual(outcome.status, 'completed')
              assert.deepStrictEqual([...calls].sort(), ['a', 'a', 'a', 'b'])
              assert.lengthOf(affinities, 4)
              assert.strictEqual(new Set(affinities).size, 4)
              assert.isTrue(Option.isNone(yield* runtime.scheduler.failure))
            }),
          ).pipe(Effect.provide(services)),
      )
  for (const backend of ['memory', 'sqlite', 'jsonl'] as const)
    for (const mode of ['foreground', 'background'] as const)
      it.live(`${backend}: ${mode} cold attachment consumes the paid terminal child once`, () =>
        Effect.scoped(
          Effect.gen(function* () {
            const fs = yield* FileSystem.FileSystem
            const directory = yield* fs.makeTempDirectoryScoped()
            const sqlite = nativeBun
              ? (yield* Effect.promise(() => import('@effect/sql-sqlite-bun/SqliteClient'))).layer
              : (yield* Effect.promise(() => import('@effect/sql-sqlite-node/SqliteClient'))).layer
            const layer = (() => {
              if (backend === 'memory') return Storage.layerMemory
              if (backend === 'jsonl')
                return Storage.layerJsonl({ filePath: `${directory}/cold.jsonl` })
              return Storage.layerJournal().pipe(
                Layer.provide(SqlEventJournal.layer()),
                Layer.provide(sqlite({ filename: `${directory}/cold.sqlite` })),
              )
            })()
            const memory = backend === 'memory' ? yield* Layer.build(layer) : undefined
            const ready = yield* Deferred.make<Subagent.Receipt>()
            let warm = true
            let calls = 0
            const contract = {
              kind: `subagent.cold.${mode}`,
              version: 1,
              input: Schema.Void,
              checkpoint: Schema.Union([
                Schema.Struct({ phase: Schema.tag('spawn') }),
                Schema.Struct({ phase: Schema.tag('consume'), handle: Subagent.HandleSchema }),
              ]),
              success: Subagent.ReceiptSchema,
              failure: Schema.String,
            } as const
            const program = Task.make({
              ...contract,
              initial: () => ({ phase: 'spawn' as const }),
              phases: {
                spawn: () =>
                  Effect.gen(function* () {
                    const access = yield* TaskExecution
                    const handle = yield* Subagent.spawn(access, {
                      draft: { type: 'input', content: 'cold child' },
                      options: { mode, key: 'paid' },
                    })
                    yield* access.commit((tx) =>
                      access.progress(tx, contract, {
                        status: 'waiting',
                        on: [handle.taskId],
                        policy: 'allSettled',
                        checkpoint: { phase: 'consume', handle },
                      }),
                    )
                  }),
                consume: ({ checkpoint }) =>
                  Effect.gen(function* () {
                    const access = yield* TaskExecution
                    const result = yield* Subagent.wait(access, checkpoint.handle)
                    if (result.receipt === undefined)
                      return yield* Effect.die('Missing paid child receipt')
                    const receipt = result.receipt
                    if (warm) {
                      yield* Deferred.succeed(ready, receipt)
                      return yield* Effect.never
                    }
                    yield* access.commit((tx) =>
                      access.progress(tx, contract, {
                        status: 'completing',
                        outcome: { status: 'completed', result: receipt },
                      }),
                    )
                  }),
              },
            })
            const model = yield* ScriptedStream.make({
              generateText: () =>
                Effect.sync(() => {
                  assert.isTrue(warm, 'A saved terminal child must not rerun the provider')
                  calls++
                  const finish: Response.FinishPartEncoded = {
                    type: 'finish',
                    reason: 'stop',
                    usage: {
                      inputTokens: {
                        total: 7,
                        uncached: 7,
                        cacheRead: undefined,
                        cacheWrite: undefined,
                      },
                      outputTokens: { total: 4, text: 4, reasoning: undefined },
                    },
                    response: undefined,
                  }
                  return [{ type: 'text' as const, text: 'paid before reopening' }, finish]
                }),
            })
            const open = (scope: Scope.Scope) =>
              Effect.gen(function* () {
                const context = memory ?? (yield* Layer.buildWithScope(layer, scope))
                const registry = yield* Registry.make.pipe(
                  Effect.provideService(Scope.Scope, scope),
                )
                yield* registry.install(
                  Registry.withTask(
                    Registry.toolkit({ id: contract.kind, version: 1 }, Toolkit.make()),
                    program,
                  ),
                )
                return yield* HarnessRuntime.make({ registry }).pipe(
                  Effect.provideContext(context),
                  Effect.provideService(Scope.Scope, scope),
                  Effect.provideService(LanguageModel.LanguageModel, model),
                )
              })
            const firstScope = yield* Scope.make()
            yield* Effect.addFinalizer(() => Scope.close(firstScope, Exit.void))
            const first = yield* open(firstScope)
            const ref = yield* first.scheduler.start(program, undefined, {
              conversationId: yield* first.backend.root,
            })
            const receipt = yield* Deferred.await(ready)
            assert.strictEqual(calls, 1)
            yield* Scope.close(firstScope, Exit.void)
            warm = false
            const secondScope = yield* Scope.make()
            yield* Effect.addFinalizer(() => Scope.close(secondScope, Exit.void))
            const second = yield* open(secondScope)
            const outcome = yield* second.scheduler.wait(ref)
            assert.deepStrictEqual(outcome, { status: 'completed', result: receipt })
            assert.strictEqual(calls, 1)
            assert.lengthOf(
              yield* Session.commit(second.session, (tx) =>
                Usage.receipts(tx, receipt.conversationId),
              ),
              1,
            )
            assert.lengthOf(
              yield* Session.scanEntries(second.session, {
                conversationId: receipt.conversationId,
              }).pipe(
                Stream.filter((entry) => entry.kind === 'assistant'),
                Stream.runCollect,
              ),
              1,
            )
            assert.isTrue(Option.isNone(yield* second.scheduler.failure))
          }),
        ).pipe(Effect.provide(services)),
      )
  for (const backend of ['memory', 'sqlite', 'jsonl'] as const)
    for (const mode of ['foreground', 'background'] as const)
      for (const work of ['input', 'write'] as const)
        it.live(`${backend}: ${mode} admits and observes an actual child ${work}`, () =>
          Effect.scoped(
            Effect.gen(function* () {
              const fs = yield* FileSystem.FileSystem
              const directory = yield* fs.makeTempDirectoryScoped()
              const sqlite = nativeBun
                ? (yield* Effect.promise(() => import('@effect/sql-sqlite-bun/SqliteClient'))).layer
                : (yield* Effect.promise(() => import('@effect/sql-sqlite-node/SqliteClient')))
                    .layer
              const storage = (() => {
                if (backend === 'memory') return Storage.layerMemory
                if (backend === 'jsonl')
                  return Storage.layerJsonl({ filePath: `${directory}/content.jsonl` })
                return Storage.layerJournal().pipe(
                  Layer.provide(SqlEventJournal.layer()),
                  Layer.provide(sqlite({ filename: `${directory}/content.sqlite` })),
                )
              })()
              const contract = {
                kind: `subagent.consumer.${mode}.${work}`,
                version: 1,
                input: Schema.Void,
                checkpoint: Schema.Union([
                  Schema.Struct({ phase: Schema.tag('spawn') }),
                  Schema.Struct({ phase: Schema.tag('observe'), handle: Subagent.HandleSchema }),
                ]),
                success: Subagent.ReceiptSchema,
                failure: Schema.String,
              } as const
              let admitted: Subagent.Handle | undefined
              let retained: Subagent.Access | undefined
              const program = Task.make({
                ...contract,
                initial: () => ({ phase: 'spawn' as const }),
                phases: {
                  spawn: () =>
                    Effect.gen(function* () {
                      const access = yield* TaskExecution
                      retained = access.subagent
                      const request: Subagent.Request = {
                        draft:
                          work === 'write'
                            ? { type: 'write', entry: { kind: 'subagent.control' } }
                            : { type: 'input', content: 'actual supervised model work' },
                        options: { key: 'child', mode },
                      }
                      const handle = yield* Subagent.spawn(access, request)
                      admitted = handle
                      assert.deepStrictEqual(yield* Subagent.spawn(access, request), handle)
                      assert.strictEqual(
                        (yield* Subagent.spawn(access, {
                          ...request,
                          draft: { type: 'write', entry: { kind: 'conflicting-key' } },
                        }).pipe(Effect.flip))._tag,
                        'SubagentError',
                      )
                      assert.strictEqual(
                        (yield* Subagent.read(access, {
                          ...handle,
                          originTaskId: yield* Schema.decodeEffect(Record.TaskId)(900000),
                        }).pipe(Effect.exit))._tag,
                        'Failure',
                      )
                      yield* access.commit((tx) =>
                        access.progress(tx, contract, {
                          status: 'waiting',
                          on: [handle.taskId],
                          policy: 'allSettled',
                          checkpoint: { phase: 'observe', handle },
                        }),
                      )
                    }),
                  observe: ({ checkpoint }) =>
                    Effect.gen(function* () {
                      const access = yield* TaskExecution
                      const result = yield* Subagent.wait(access, checkpoint.handle)
                      assert.strictEqual(result.outcome.status, 'completed')
                      assert.isDefined(result.receipt)
                      if (result.receipt === undefined)
                        return yield* Effect.die('Missing actual child receipt')
                      const receipt = result.receipt
                      yield* access.commit((tx) =>
                        access.progress(tx, contract, {
                          status: 'completing',
                          outcome: { status: 'completed', result: receipt },
                        }),
                      )
                    }),
                },
              })
              const registry = yield* Registry.make
              yield* registry.install(
                Registry.withTask(
                  Registry.toolkit({ id: contract.kind, version: 1 }, Toolkit.make()),
                  program,
                ),
              )
              let calls = 0
              const model = yield* ScriptedStream.make({
                generateText: () =>
                  Effect.sync(() => {
                    calls++
                    const finish: Response.FinishPartEncoded = {
                      type: 'finish',
                      reason: 'stop',
                      usage: {
                        inputTokens: {
                          total: 3,
                          uncached: 3,
                          cacheRead: undefined,
                          cacheWrite: undefined,
                        },
                        outputTokens: { total: 2, text: 2, reasoning: undefined },
                      },
                      response: undefined,
                    }
                    return [{ type: 'text' as const, text: 'actual child result' }, finish]
                  }),
              })
              const context = yield* Layer.build(storage)
              const runtime = yield* HarnessRuntime.make({ registry }).pipe(
                Effect.provideContext(context),
                Effect.provideService(LanguageModel.LanguageModel, model),
              )
              const root = yield* runtime.backend.root
              const parent = yield* runtime.scheduler.start(program, undefined, {
                conversationId: root,
              })
              const outcome = yield* runtime.scheduler.wait(parent)
              assert.strictEqual(outcome.status, 'completed')
              if (outcome.status !== 'completed') return yield* Effect.die(outcome)
              const receipt = outcome.result
              assert.strictEqual(receipt.submission.status, 'done')
              const child = yield* Session.conversation(runtime.session, receipt.conversationId)
              assert.isTrue(Option.isSome(child))
              if (Option.isSome(child))
                assert.strictEqual(child.value.owner?.taskId, admitted?.taskId)
              const entries = yield* Session.scanEntries(runtime.session, {
                conversationId: receipt.conversationId,
                order: 'ascending',
              }).pipe(Stream.runCollect)
              assert.lengthOf(entries, work === 'write' ? 1 : 3)
              if (work === 'input')
                assert.deepStrictEqual(
                  entries.filter((entry) => entry.model !== undefined).map((entry) => entry.kind),
                  ['input', 'assistant'],
                )
              assert.strictEqual(calls, work === 'write' ? 0 : 1)
              assert.lengthOf(
                yield* Session.commit(runtime.session, (tx) =>
                  Usage.receipts(tx, receipt.conversationId),
                ),
                work === 'write' ? 0 : 1,
              )
              assert.lengthOf(
                yield* Session.scanEntries(runtime.session, { conversationId: root }).pipe(
                  Stream.runCollect,
                ),
                0,
              )
              if (retained === undefined || admitted === undefined)
                return yield* Effect.die('Missing retained native access')
              assert.isTrue((yield* retained.read(admitted).pipe(Effect.exit))._tag === 'Failure')
              let lateReads = 0
              const lateRequest: Subagent.Request = {
                get draft(): Subagent.Request['draft'] {
                  lateReads++
                  return { type: 'write', entry: { kind: 'must-not-read' } }
                },
              }
              assert.strictEqual(
                (yield* retained.spawn(lateRequest).pipe(Effect.exit))._tag,
                'Failure',
              )
              assert.strictEqual(lateReads, 0)
              assert.isTrue(Option.isNone(yield* runtime.scheduler.failure))
            }),
          ).pipe(Effect.provide(services)),
        )
  for (const backend of ['memory', 'sqlite', 'jsonl'] as const)
    for (const policy of ['allSettled', 'failFast'] as const)
      it.live(
        `${backend}/${policy}: actual subagent joins preserve direct ownership and ordered duplicate outcomes`,
        () =>
          Effect.scoped(
            Effect.gen(function* () {
              const fs = yield* FileSystem.FileSystem
              const directory = yield* fs.makeTempDirectoryScoped()
              const sqlite = nativeBun
                ? (yield* Effect.promise(() => import('@effect/sql-sqlite-bun/SqliteClient'))).layer
                : (yield* Effect.promise(() => import('@effect/sql-sqlite-node/SqliteClient')))
                    .layer
              const storage = (() => {
                if (backend === 'memory') return Storage.layerMemory
                if (backend === 'jsonl')
                  return Storage.layerJsonl({ filePath: `${directory}/joins.jsonl` })
                return Storage.layerJournal().pipe(
                  Layer.provide(SqlEventJournal.layer()),
                  Layer.provide(sqlite({ filename: `${directory}/joins.sqlite` })),
                )
              })()
              const entered = yield* Deferred.make<void>()
              const releaseFailure = yield* Deferred.make<void>()
              const releaseProvider = yield* Deferred.make<void>()
              let released = 0
              const failureContract = {
                kind: 'subagent.join-failure',
                version: 1,
                input: Schema.Void,
                checkpoint: Schema.Struct({ phase: Schema.tag('run') }),
                success: Schema.Void,
                failure: Schema.String,
              } as const
              const failing = Task.make({
                ...failureContract,
                initial: () => ({ phase: 'run' as const }),
                phases: {
                  run: () =>
                    Deferred.await(releaseFailure).pipe(
                      Effect.andThen(Effect.fail('actual sibling failure')),
                    ),
                },
              })
              const contract = {
                kind: 'subagent.join-consumer',
                version: 1,
                input: Schema.Void,
                checkpoint: Schema.Union([
                  Schema.Struct({ phase: Schema.tag('admit') }),
                  Schema.Struct({
                    phase: Schema.tag('consume'),
                    handle: Subagent.HandleSchema,
                    failed: Record.TaskId,
                  }),
                ]),
                success: Schema.Array(Schema.String),
                failure: Schema.String,
              } as const
              let admitted: Subagent.Handle | undefined
              const program = Task.make({
                ...contract,
                initial: () => ({ phase: 'admit' as const }),
                phases: {
                  admit: () =>
                    Effect.gen(function* () {
                      const access = yield* TaskExecution
                      yield* access.commit((tx) =>
                        Effect.gen(function* () {
                          if (access.subagent === undefined)
                            return yield* Effect.die('No actual native consumer')
                          const handle = yield* access.subagent.admit(tx, {
                            draft: { type: 'input', content: 'held child' },
                            options: { key: 'shared-key' },
                          })
                          admitted = handle
                          const sibling = yield* access.createChild(tx, failing, undefined)
                          yield* access.memoize(tx, 'joining', Schema.String, 'saved before wait')
                          yield* access.progress(tx, contract, {
                            status: 'waiting',
                            on: [handle.taskId, sibling.id],
                            policy,
                            checkpoint: { phase: 'consume', handle, failed: sibling.id },
                          })
                        }),
                      )
                    }),
                  consume: ({ checkpoint }) =>
                    Effect.gen(function* () {
                      const access = yield* TaskExecution
                      assert.deepStrictEqual(
                        yield* access.memo('joining', Schema.String),
                        Option.some('saved before wait'),
                      )
                      const failedId = yield* Schema.decodeEffect(Record.TaskId)(checkpoint.failed)
                      const outcomes = yield* access.outcomes([
                        checkpoint.handle.taskId,
                        failedId,
                        checkpoint.handle.taskId,
                      ])
                      assert.deepStrictEqual(
                        outcomes.map((x) => x.status),
                        [
                          policy === 'failFast' ? 'aborted' : 'completed',
                          'failed',
                          policy === 'failFast' ? 'aborted' : 'completed',
                        ],
                      )
                      const child = yield* Subagent.wait(access, checkpoint.handle)
                      assert.strictEqual(
                        child.outcome.status,
                        policy === 'failFast' ? 'aborted' : 'completed',
                      )
                      yield* access.commit((tx) =>
                        access.progress(tx, contract, {
                          status: 'completing',
                          outcome: { status: 'completed', result: outcomes.map((x) => x.status) },
                        }),
                      )
                    }),
                },
              })
              const registry = yield* Registry.make
              yield* registry.install(
                Registry.withTask(
                  Registry.withTask(
                    Registry.toolkit({ id: 'join-consumers', version: 1 }, Toolkit.make()),
                    failing,
                  ),
                  program,
                ),
              )
              const model = yield* LanguageModel.make({
                generateText: () => Effect.die('Streaming only'),
                streamText: () =>
                  Stream.unwrap(
                    Effect.gen(function* () {
                      yield* Effect.addFinalizer(() =>
                        Effect.sync(() => {
                          released++
                        }),
                      )
                      yield* Deferred.succeed(entered, undefined)
                      yield* Deferred.await(releaseProvider)
                      return Stream.fromIterable([
                        { type: 'text-start' as const, id: 'join' },
                        { type: 'text-delta' as const, id: 'join', delta: 'joined' },
                        { type: 'text-end' as const, id: 'join' },
                        {
                          type: 'finish' as const,
                          reason: 'stop' as const,
                          usage: {
                            inputTokens: {
                              total: 1,
                              uncached: 1,
                              cacheRead: undefined,
                              cacheWrite: undefined,
                            },
                            outputTokens: { total: 1, text: 1, reasoning: undefined },
                          },
                          response: undefined,
                        },
                      ])
                    }),
                  ),
              })
              const context = yield* Layer.build(storage)
              const runtime = yield* HarnessRuntime.make({ registry }).pipe(
                Effect.provideContext(context),
                Effect.provideService(LanguageModel.LanguageModel, model),
              )
              const ref = yield* runtime.scheduler.start(program, undefined, {
                conversationId: yield* runtime.backend.root,
              })
              yield* Deferred.await(entered)
              yield* Deferred.succeed(releaseFailure, undefined)
              if (policy === 'allSettled') yield* Deferred.succeed(releaseProvider, undefined)
              assert.strictEqual((yield* runtime.scheduler.wait(ref)).status, 'completed')
              assert.strictEqual(released, 1)
              if (admitted === undefined) return yield* Effect.die('No saved child')
              const child = yield* Session.task(runtime.session, admitted.taskId)
              assert.isTrue(Option.isSome(child))
              if (Option.isSome(child)) {
                assert.strictEqual(child.value.owner, ref.id)
                assert.strictEqual(child.value.state.status, 'terminal')
              }
              assert.isTrue(Option.isNone(yield* runtime.scheduler.failure))
            }),
          ).pipe(Effect.provide(services)),
      )
  for (const backend of ['memory', 'sqlite', 'jsonl'] as const)
    it.live(
      `${backend}: simultaneous actual parents keep identical keys in separate ownership epochs`,
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const fs = yield* FileSystem.FileSystem
            const directory = yield* fs.makeTempDirectoryScoped()
            const sqlite = nativeBun
              ? (yield* Effect.promise(() => import('@effect/sql-sqlite-bun/SqliteClient'))).layer
              : (yield* Effect.promise(() => import('@effect/sql-sqlite-node/SqliteClient'))).layer
            const storage = (() => {
              if (backend === 'memory') return Storage.layerMemory
              if (backend === 'jsonl')
                return Storage.layerJsonl({ filePath: `${directory}/parents.jsonl` })
              return Storage.layerJournal().pipe(
                Layer.provide(SqlEventJournal.layer()),
                Layer.provide(sqlite({ filename: `${directory}/parents.sqlite` })),
              )
            })()
            const both = yield* Deferred.make<void>()
            let entered = 0
            const contract = {
              kind: 'subagent.simultaneous-parent',
              version: 1,
              input: Schema.Void,
              checkpoint: Schema.Struct({ phase: Schema.tag('run') }),
              success: Subagent.HandleSchema,
              failure: Schema.String,
            } as const
            const program = Task.make({
              ...contract,
              initial: () => ({ phase: 'run' as const }),
              phases: {
                run: () =>
                  Effect.gen(function* () {
                    const access = yield* TaskExecution
                    const handle = yield* Subagent.spawn(access, {
                      draft: { type: 'write', entry: { kind: 'parallel-owned' } },
                      options: { key: 'same' },
                    })
                    entered++
                    if (entered === 2) yield* Deferred.succeed(both, undefined)
                    yield* Deferred.await(both)
                    assert.strictEqual(
                      (yield* Subagent.wait(access, handle)).outcome.status,
                      'completed',
                    )
                    yield* access.commit((tx) =>
                      access.progress(tx, contract, {
                        status: 'completing',
                        outcome: { status: 'completed', result: handle },
                      }),
                    )
                  }),
              },
            })
            const registry = yield* Registry.make
            yield* registry.install(
              Registry.withTask(
                Registry.toolkit({ id: contract.kind, version: 1 }, Toolkit.make()),
                program,
              ),
            )
            const context = yield* Layer.build(storage)
            const model = yield* LanguageModel.make({
              generateText: () => Effect.die('No provider'),
              streamText: () => Stream.die('No provider'),
            })
            const runtime = yield* HarnessRuntime.make({ registry }).pipe(
              Effect.provideContext(context),
              Effect.provideService(LanguageModel.LanguageModel, model),
            )
            const root = yield* runtime.backend.root
            const first = yield* runtime.scheduler.start(program, undefined, {
              conversationId: root,
            })
            const second = yield* runtime.scheduler.start(program, undefined, {
              conversationId: root,
            })
            const left = yield* runtime.scheduler.wait(first)
            const right = yield* runtime.scheduler.wait(second)
            assert.strictEqual(left.status, 'completed')
            assert.strictEqual(right.status, 'completed')
            if (left.status !== 'completed' || right.status !== 'completed')
              return yield* Effect.die('Missing real outcomes')
            assert.notStrictEqual(left.result.taskId, right.result.taskId)
            assert.strictEqual(left.result.key, right.result.key)
            assert.strictEqual(left.result.originTaskId, first.id)
            assert.strictEqual(right.result.originTaskId, second.id)
            for (const [ref, outcome] of [
              [first, left],
              [second, right],
            ] as const) {
              const record = yield* Session.task(runtime.session, outcome.result.taskId)
              assert.isTrue(Option.isSome(record))
              if (Option.isSome(record)) assert.strictEqual(record.value.owner, ref.id)
            }
            assert.isTrue(Option.isNone(yield* runtime.scheduler.failure))
          }),
        ).pipe(Effect.provide(services)),
    )
})
