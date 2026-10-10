import { assert, describe, it } from '@effect/vitest'
import * as Context from 'effect/Context'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as SchemaGetter from 'effect/SchemaGetter'
import * as Scope from 'effect/Scope'
import * as Stream from 'effect/Stream'
import * as Extension from 'effect-harness/Extension'
import * as Document from 'effect-harness/Document'
import * as Record from 'effect-harness/Record'
import * as Registry from 'effect-harness/Registry'
import * as Session from 'effect-harness/Session'
import * as Storage from 'effect-harness/Storage'
import * as Task from 'effect-harness/Task'
import { TaskError } from 'effect-harness/TaskError'
import { TaskExecution } from 'effect-harness/TaskExecution'
import * as TaskScheduler from 'effect-harness/TaskScheduler'
import * as Toolkit from 'effect-harness/Toolkit'
import * as Transaction from 'effect-harness/Transaction'
import * as SessionRuntime from '../src/internal/SessionRuntime.js'

const root = Record.ROOT_CONVERSATION_ID
class Backend extends Context.Service<
  Backend,
  { readonly value: number; readonly live: () => boolean }
>()('scheduler.services/Backend') {}
class Codec extends Context.Service<Codec, number>()('scheduler.services/Codec') {}
const contract = {
  kind: 'scheduler.services',
  version: 1,
  input: Schema.Natural,
  checkpoint: Schema.Union([
    Schema.Struct({ phase: Schema.Literal('one') }),
    Schema.Struct({ phase: Schema.Literal('two') }),
  ]),
  success: Schema.Natural,
  failure: Schema.String,
}
const target = Document.define({
  kind: 'scheduler.hold',
  version: 1,
  scope: 'task',
  schema: Schema.Struct({ n: Schema.Natural }),
  initial: () => ({ n: 0 }),
})
const open = Effect.gen(function* () {
  const session = yield* Session.make()
  yield* Session.commit(session, Transaction.ensureRoot)
  const registry = yield* Registry.make
  const scheduler = yield* TaskScheduler.make(session).pipe(
    Effect.provideService(Registry.Registry, registry),
  )
  return { session, registry, scheduler }
})
const finish = (result: number) =>
  TaskExecution.pipe(
    Effect.flatMap((access) =>
      access.commit((tx) =>
        access.progress(tx, contract, {
          status: 'completing',
          outcome: { status: 'completed', result },
        }),
      ),
    ),
  )
const settle = Effect.forEach(Array.from({ length: 80 }), () => Effect.yieldNow, { discard: true })

describe('scheduler captured services and supplied boundaries', () => {
  it.effect(
    'pins old registration resources through a phase and uses only the replacement services for the next phase',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const { registry, scheduler } = yield* open
          const entered = yield* Deferred.make<void>()
          const release = yield* Deferred.make<void>()
          const released: number[] = []
          const seen: number[] = []
          const resource = (value: number) =>
            Layer.effect(
              Backend,
              Effect.acquireRelease(
                Effect.sync(() => ({ value, live: () => !released.includes(value) })),
                () =>
                  Effect.sync(() => {
                    released.push(value)
                  }),
              ),
            )
          const definition = Task.make({
            ...contract,
            initial: () => ({ phase: 'one' as const }),
            phases: {
              one: () =>
                Effect.gen(function* () {
                  const backend = yield* Backend
                  seen.push(backend.value)
                  yield* Deferred.succeed(entered, undefined)
                  yield* Deferred.await(release)
                  assert.isTrue(backend.live())
                  const access = yield* TaskExecution
                  yield* access.commit((tx) =>
                    access.progress(tx, contract, {
                      status: 'pending',
                      checkpoint: { phase: 'two' },
                    }),
                  )
                }),
              two: () =>
                Effect.gen(function* () {
                  const backend = yield* Backend
                  seen.push(backend.value)
                  assert.isTrue(backend.live())
                  yield* finish(backend.value)
                }),
            },
          })
          const registration = (value: number) =>
            Registry.withTask(
              Registry.toolkit({ id: 'backend', version: 1 }, Toolkit.make()),
              definition.pipe(Task.provide(resource(value))),
            )
          yield* registry.install(registration(7))
          const ref = yield* scheduler
            .start(definition, 0, { conversationId: root })
            .pipe(Effect.provideService(Backend, { value: 99, live: () => true }))
          yield* Deferred.await(entered)
          yield* registry.install(registration(8))
          assert.deepStrictEqual(released, [])
          yield* Deferred.succeed(release, undefined)
          assert.deepStrictEqual(yield* scheduler.wait(ref), { status: 'completed', result: 8 })
          yield* settle
          assert.deepStrictEqual(seen, [7, 8])
          assert.deepStrictEqual(released, [7])
          yield* registry.uninstall('backend')
          assert.deepStrictEqual(released, [7, 8])
        }),
      ).pipe(Effect.provide(Storage.layerMemory)),
  )

  it.effect(
    'uses captured codec services for typed object creation and phase writes, retaining public outcome decoder requirements',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const { registry, scheduler } = yield* open
          const number = Schema.String.pipe(
            Schema.decodeTo(Schema.Natural, {
              decode: SchemaGetter.transformEffect((value: string) =>
                Codec.pipe(Effect.map((salt) => Number(value) - salt)),
              ),
              encode: SchemaGetter.transformEffect((value: number) =>
                Codec.pipe(Effect.map((salt) => String(value + salt))),
              ),
            }),
          )
          const coded = { ...contract, input: Schema.Struct({ count: number }), success: number }
          const definition = Task.make({
            ...coded,
            initial: () => ({ phase: 'one' as const }),
            phases: {
              one: ({ input }) =>
                TaskExecution.pipe(
                  Effect.flatMap((access) =>
                    access.commit((tx) =>
                      access.progress(tx, coded, {
                        status: 'completing',
                        outcome: { status: 'completed', result: input.count + 1 },
                      }),
                    ),
                  ),
                ),
              two: () => Effect.void,
            },
          })
          yield* registry.install(
            Registry.withTask(
              Registry.toolkit({ id: 'codec', version: 1 }, Toolkit.make()),
              definition.pipe(Task.provide(Layer.succeed(Codec, 10))),
            ),
          )
          const ref = yield* scheduler.start(definition, { count: 3 }, { conversationId: root })
          assert.deepStrictEqual(
            yield* scheduler.wait(ref).pipe(Effect.provideService(Codec, 10)),
            { status: 'completed', result: 4 },
          )
          const stored = (yield* scheduler.inspect(ref.id)).record
          assert.deepStrictEqual(stored.input, { count: '13' })
          assert.deepStrictEqual(stored.state.outcome, { status: 'completed', result: '14' })
        }),
      ).pipe(Effect.provide(Storage.layerMemory)),
  )

  it.effect('encodes a typed domain failure through its captured serviceful codec', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { registry, scheduler } = yield* open
        const number = Schema.String.pipe(
          Schema.decodeTo(Schema.Natural, {
            decode: SchemaGetter.transformEffect((value: string) =>
              Codec.pipe(Effect.map((salt) => Number(value) - salt)),
            ),
            encode: SchemaGetter.transformEffect((value: number) =>
              Codec.pipe(Effect.map((salt) => String(value + salt))),
            ),
          }),
        )
        const definition = Task.make({
          ...contract,
          failure: number,
          initial: () => ({ phase: 'one' as const }),
          phases: { one: () => Effect.fail(4), two: () => Effect.void },
        })
        yield* registry.install(
          Registry.withTask(
            Registry.toolkit({ id: 'failure-codec', version: 1 }, Toolkit.make()),
            definition.pipe(Task.provide(Layer.succeed(Codec, 10))),
          ),
        )
        const ref = yield* scheduler.start(definition, 0, { conversationId: root })
        const outcome = yield* scheduler.wait(ref).pipe(Effect.provideService(Codec, 10))
        assert.strictEqual(outcome.status, 'failed')
        if (outcome.status === 'failed') assert.strictEqual(outcome.result, 4)
        const stored = yield* Task.decode(
          Record.TaskOutcome,
          (yield* scheduler.inspect(ref.id)).record.state.outcome,
        )
        assert.strictEqual(stored.status, 'failed')
        if (stored.status === 'failed') assert.strictEqual(stored.result, '14')
      }),
    ).pipe(Effect.provide(Storage.layerMemory)),
  )

  it.effect(
    'provides the actual leased extension declarations through invocation registry inspection',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const { registry, scheduler } = yield* open
          const entered = yield* Deferred.make<void>()
          const release = yield* Deferred.make<void>()
          const extension = Extension.make({ name: 'leased-extension' })
          yield* registry.install(Registry.extension({ id: 'extension', version: 1 }, extension))
          const definition = Task.make({
            ...contract,
            initial: () => ({ phase: 'one' as const }),
            phases: {
              one: () =>
                Effect.gen(function* () {
                  const access = yield* TaskExecution
                  yield* Deferred.succeed(entered, undefined)
                  yield* Deferred.await(release)
                  assert.deepStrictEqual(
                    access.registry.extensions.map((item) => item.name),
                    ['leased-extension'],
                  )
                  yield* finish(1)
                }),
              two: () => Effect.void,
            },
          })
          yield* registry.install(
            Registry.withTask(
              Registry.toolkit({ id: 'extension-task', version: 1 }, Toolkit.make()),
              definition,
            ),
          )
          const ref = yield* scheduler.start(definition, 0, { conversationId: root })
          yield* Deferred.await(entered)
          yield* registry.uninstall('extension')
          yield* Deferred.succeed(release, undefined)
          assert.strictEqual((yield* scheduler.wait(ref)).status, 'completed')
        }),
      ).pipe(Effect.provide(Storage.layerMemory)),
  )

  it.effect(
    'holds immutable outcomes and documents until ordinary owned records settle, then retires atomically',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const { session, registry, scheduler } = yield* open
          const child = yield* Deferred.make<Record.Task>()
          const definition = Task.make({
            ...contract,
            initial: () => ({ phase: 'one' as const }),
            phases: {
              one: () =>
                Effect.gen(function* () {
                  const access = yield* TaskExecution
                  yield* access.commit((tx) =>
                    Effect.gen(function* () {
                      yield* Transaction.ensureDocument(tx, target, {
                        scope: { _tag: 'task', taskId: access.taskId },
                      })
                      yield* access.memoize(tx, 'memo', Schema.String, 'held')
                      const owned = yield* Transaction.createTask(tx, {
                        kind: 'held.child',
                        version: 1,
                        conversationId: root,
                        owner: access.taskId,
                        background: false,
                        input: null,
                        abortRequested: false,
                        state: { status: 'pending', checkpoint: { phase: 'child' } },
                      })
                      yield* Deferred.succeed(child, owned)
                      yield* access.progress(tx, contract, {
                        status: 'completing',
                        outcome: { status: 'completed', result: 5 },
                      })
                    }),
                  )
                }),
              two: () => Effect.void,
            },
          })
          yield* registry.install(
            Registry.withTask(
              Registry.toolkit({ id: 'hold', version: 1 }, Toolkit.make()),
              definition,
            ),
          )
          const ref = yield* scheduler.start(definition, 0, { conversationId: root })
          const owned = yield* Deferred.await(child)
          yield* settle
          const parent = (yield* scheduler.inspect(ref.id)).record
          assert.strictEqual(parent.state.status, 'completing')
          assert.isUndefined(parent.memos)
          assert.isTrue(
            Option.isSome(
              yield* Session.snapshot(session, target, { scope: { _tag: 'task', taskId: ref.id } }),
            ),
          )
          const mutation = yield* Session.commit(session, (tx) =>
            Transaction.putTask(tx, {
              ...parent,
              state: { status: 'terminal', outcome: { status: 'completed', result: 6 } },
            }),
          ).pipe(Effect.flip)
          assert.strictEqual(mutation._tag, 'SessionError')
          yield* Session.commit(session, (tx) =>
            Transaction.putTask(tx, {
              ...owned,
              state: { status: 'terminal', outcome: { status: 'completed', result: null } },
            }),
          )
          assert.deepStrictEqual(yield* scheduler.wait(ref), { status: 'completed', result: 5 })
          assert.isTrue(
            Option.isNone(
              yield* Session.snapshot(session, target, { scope: { _tag: 'task', taskId: ref.id } }),
            ),
          )
        }),
      ).pipe(Effect.provide(Storage.layerMemory)),
  )

  it.effect(
    'coalesces more than the Session observer limit while slow observers remain independent',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const { session, registry, scheduler } = yield* open
          const parked = yield* Deferred.make<void>()
          const observing = yield* Deferred.make<void>()
          yield* scheduler.changes.pipe(
            Stream.runForEach(() =>
              Deferred.succeed(observing, undefined).pipe(Effect.andThen(Deferred.await(parked))),
            ),
            Effect.forkScoped,
          )
          yield* settle
          yield* scheduler.resume
          yield* Deferred.await(observing)
          const definition = Task.make({
            ...contract,
            initial: () => ({ phase: 'one' as const }),
            phases: { one: () => finish(1), two: () => Effect.void },
          })
          yield* registry.install(
            Registry.withTask(
              Registry.toolkit({ id: 'coalesced', version: 1 }, Toolkit.make()),
              definition,
            ),
          )
          const refs = yield* Session.commit(session, (tx) =>
            Effect.forEach(Array.from({ length: 96 }), () =>
              Transaction.createTask(tx, {
                kind: contract.kind,
                version: 1,
                conversationId: root,
                input: 0,
                background: false,
                abortRequested: false,
                state: { status: 'pending', checkpoint: { phase: 'one' } },
              }),
            ),
          )
          const outcomes = yield* Effect.forEach(
            refs,
            (ref) => scheduler.wait({ id: ref.id, definition }),
            { concurrency: 'unbounded' },
          )
          assert.isTrue(outcomes.every((outcome) => outcome.status === 'completed'))
          assert.isTrue(Option.isNone(yield* scheduler.failure))
          assert.strictEqual((yield* SessionRuntime.get(session)).reconciliations.size, 1)
        }),
      ).pipe(Effect.provide(Storage.layerMemory)),
  )

  it.effect(
    'missing host operations fail explicitly; a supplied invocation host uses captured services and Scope',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const session = yield* Session.make()
          yield* Session.commit(session, Transaction.ensureRoot)
          const registry = yield* Registry.make
          const scheduler = yield* TaskScheduler.make(session, {
            host: {
              open: () =>
                Backend.pipe(
                  Effect.map((backend) => ({
                    agent: Effect.succeed({ instructions: `backend:${backend.value}` }),
                  })),
                ),
            },
          }).pipe(
            Effect.provideService(Registry.Registry, registry),
            Effect.provideService(Backend, { value: 12, live: () => true }),
          )
          const definition = Task.make({
            ...contract,
            initial: () => ({ phase: 'one' as const }),
            phases: {
              one: () =>
                Effect.gen(function* () {
                  const access = yield* TaskExecution
                  assert.strictEqual((yield* access.agent).instructions, 'backend:12')
                  for (const operation of [
                    access.abort(access.taskId),
                    access.abortOwned(),
                    access.conversation(root),
                  ]) {
                    const missing = yield* operation.pipe(Effect.flip)
                    assert.isTrue(missing instanceof TaskError)
                    assert.include(missing.message, 'supplied host capability')
                  }
                  yield* finish(12)
                }),
              two: () => Effect.void,
            },
          })
          yield* registry.install(
            Registry.withTask(
              Registry.toolkit({ id: 'host', version: 1 }, Toolkit.make()),
              definition,
            ),
          )
          const ref = yield* scheduler.start(definition, 0, { conversationId: root })
          assert.strictEqual((yield* scheduler.wait(ref)).status, 'completed')
        }),
      ).pipe(Effect.provide(Storage.layerMemory)),
  )
  it.effect(
    'leaves abort cascades including background children blocked without an ownership supervisor',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const { session, registry, scheduler } = yield* open
          let calls = 0
          const definition = Task.make({
            ...contract,
            initial: () => ({ phase: 'one' as const }),
            phases: {
              one: () =>
                Effect.sync(() => {
                  calls++
                }),
              two: () => Effect.void,
            },
            abort: () =>
              Effect.sync(() => {
                calls++
              }),
          })
          yield* registry.install(
            Registry.withTask(
              Registry.toolkit({ id: 'owned', version: 1 }, Toolkit.make()),
              definition,
            ),
          )
          const records = yield* Session.commit(session, (tx) =>
            Effect.gen(function* () {
              const parent = yield* Transaction.createTask(tx, {
                kind: contract.kind,
                version: 1,
                conversationId: root,
                input: 0,
                background: false,
                abortRequested: false,
                state: { status: 'running', checkpoint: { phase: 'one' } },
              })
              const conversation = yield* Transaction.createConversation(tx, {
                ownership: { _tag: 'task', taskId: parent.id },
              })
              const child = yield* Transaction.createTask(tx, {
                kind: contract.kind,
                version: 1,
                conversationId: conversation.id,
                input: 0,
                background: true,
                abortRequested: false,
                state: { status: 'pending', checkpoint: { phase: 'one' } },
              })
              return [parent, child]
            }),
          )
          yield* Session.commit(session, (tx) => Transaction.requestTaskAbort(tx, records[0]!.id))
          yield* scheduler.resume
          yield* settle
          assert.strictEqual(calls, 0)
          for (const record of records) {
            const inspection = yield* scheduler.inspect(record.id)
            assert.strictEqual(inspection.resolution._tag, 'blocked')
            if (inspection.resolution._tag === 'blocked')
              assert.strictEqual(inspection.resolution.error.reason, 'incompatible')
            assert.strictEqual(inspection.record.state.status, 'pending')
          }
          assert.isTrue(Option.isNone(yield* scheduler.failure))
          assert.strictEqual(
            (yield* scheduler
              .start(definition, 0, { conversationId: root, restartPolicy: 'abandon' })
              .pipe(Effect.flip))._tag,
            'TaskError',
          )
          assert.strictEqual((yield* Session.scanTasks(session).pipe(Stream.runCollect)).length, 2)
        }),
      ).pipe(Effect.provide(Storage.layerMemory)),
  )

  it.effect(
    'preserves Session nested-transaction rejection inside a captured supplied ownership boundary',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const session = yield* Session.make()
          yield* Session.commit(session, Transaction.ensureRoot)
          const registry = yield* Registry.make
          const scheduler = yield* TaskScheduler.make(session, {
            host: {
              open: () => Effect.succeed({}),
              reconcile: () => Session.commit(session, () => Effect.succeed(true)),
            },
          }).pipe(Effect.provideService(Registry.Registry, registry))
          const definition = Task.make({
            ...contract,
            initial: () => ({ phase: 'one' as const }),
            phases: { one: () => finish(1), two: () => Effect.void },
          })
          yield* registry.install(
            Registry.withTask(
              Registry.toolkit({ id: 'nested-host', version: 1 }, Toolkit.make()),
              definition,
            ),
          )
          const record = yield* Session.commit(session, (tx) =>
            Transaction.createTask(tx, {
              kind: contract.kind,
              version: 1,
              conversationId: root,
              input: 0,
              background: false,
              abortRequested: false,
              state: {
                status: 'waiting',
                checkpoint: { phase: 'one' },
                on: [],
                policy: 'allSettled',
              },
            }),
          )
          yield* scheduler.resume
          yield* settle
          const failure = yield* scheduler.failure
          assert.isTrue(Option.isSome(failure))
          if (Option.isSome(failure)) {
            assert.strictEqual(failure.value._tag, 'SessionError')
            if (failure.value._tag === 'SessionError')
              assert.strictEqual(failure.value.reason, 'conflict')
          }
          assert.strictEqual((yield* scheduler.inspect(record.id)).record.state.status, 'waiting')
        }),
      ).pipe(Effect.provide(Storage.layerMemory)),
  )

  it.effect('rolls back domain drafts when the typed terminal codec refuses a result', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { session, registry, scheduler } = yield* open
        const definition = Task.make({
          ...contract,
          initial: () => ({ phase: 'one' as const }),
          phases: {
            one: () =>
              TaskExecution.pipe(
                Effect.flatMap((access) =>
                  access.commit((tx) =>
                    Effect.gen(function* () {
                      yield* Transaction.appendEntry(tx, root, {
                        kind: 'codec.must.rollback',
                        data: null,
                      })
                      yield* access.progress(tx, contract, {
                        status: 'completing',
                        outcome: { status: 'completed', result: -1 },
                      })
                    }),
                  ),
                ),
              ),
            two: () => Effect.void,
          },
        })
        yield* registry.install(
          Registry.withTask(
            Registry.toolkit({ id: 'codec-rollback', version: 1 }, Toolkit.make()),
            definition,
          ),
        )
        const ref = yield* scheduler.start(definition, 0, { conversationId: root })
        assert.strictEqual((yield* scheduler.wait(ref)).status, 'faulted')
        assert.deepStrictEqual(
          yield* Session.scanEntries(session, { conversationId: root }).pipe(Stream.runCollect),
          [],
        )
      }),
    ).pipe(Effect.provide(Storage.layerMemory)),
  )
  it.effect(
    'rejects progress through a different codec under the same kind/version without domain writes',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const { session, registry, scheduler } = yield* open
          const definition = Task.make({
            ...contract,
            initial: () => ({ phase: 'one' as const }),
            phases: {
              one: () =>
                TaskExecution.pipe(
                  Effect.flatMap((access) =>
                    access.commit((tx) =>
                      Effect.gen(function* () {
                        yield* Transaction.appendEntry(tx, root, {
                          kind: 'wrong.contract.must.rollback',
                          data: null,
                        })
                        yield* access.progress(
                          tx,
                          { ...contract, success: Schema.String },
                          {
                            status: 'completing',
                            outcome: { status: 'completed', result: 'incompatible codec' },
                          },
                        )
                      }),
                    ),
                  ),
                ),
              two: () => Effect.void,
            },
          })
          yield* registry.install(
            Registry.withTask(
              Registry.toolkit({ id: 'wrong-codec', version: 1 }, Toolkit.make()),
              definition,
            ),
          )
          const ref = yield* scheduler.start(definition, 0, { conversationId: root })
          assert.strictEqual((yield* scheduler.wait(ref)).status, 'faulted')
          assert.deepStrictEqual(
            yield* Session.scanEntries(session, { conversationId: root }).pipe(Stream.runCollect),
            [],
          )
        }),
      ).pipe(Effect.provide(Storage.layerMemory)),
  )
  it.effect(
    'normalizes failures inside the fresh invocation Scope and releases codec finalizers there',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const { registry, scheduler } = yield* open
          let phaseScope: Scope.Scope | undefined
          let codecScope: Scope.Scope | undefined
          let finalizers = 0
          const failure = Schema.Natural.pipe(
            Schema.decodeTo(Schema.Natural, {
              decode: SchemaGetter.transform((value: number) => value),
              encode: SchemaGetter.transformEffect((value: number) =>
                Effect.gen(function* () {
                  codecScope = yield* Scope.Scope
                  yield* Effect.addFinalizer(() =>
                    Effect.sync(() => {
                      finalizers++
                    }),
                  )
                  return value
                }),
              ),
            }),
          )
          const definition = Task.make({
            ...contract,
            failure,
            initial: () => ({ phase: 'one' as const }),
            phases: {
              one: () =>
                Effect.gen(function* () {
                  phaseScope = yield* Scope.Scope
                  return yield* Effect.fail(4)
                }),
              two: () => Effect.void,
            },
          })
          yield* registry.install(
            Registry.withTask(
              Registry.toolkit({ id: 'failure-scope', version: 1 }, Toolkit.make()),
              definition,
            ),
          )
          const ref = yield* scheduler.start(definition, 0, { conversationId: root })
          assert.strictEqual((yield* scheduler.wait(ref)).status, 'failed')
          yield* settle
          assert.strictEqual(codecScope, phaseScope)
          assert.strictEqual(finalizers, 1)
        }),
      ).pipe(Effect.provide(Storage.layerMemory)),
  )
})
