import * as SqlEventJournal from 'effect/eventlog/SqlEventJournal'
import { NodeServices } from '@effect/platform-node'
import { assert, describe, it } from '@effect/vitest'
import * as Context from 'effect/Context'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Scope from 'effect/Scope'
import * as Stream from 'effect/Stream'
import * as LanguageModel from 'effect/ai/LanguageModel'
import * as AgentState from '../src/internal/AgentState.js'
import * as HarnessRuntime from '../src/HarnessRuntime.js'
import * as Hook from '../src/Hook.js'
import * as Registry from '../src/Registry.js'
import * as Session from '../src/Session.js'
import * as Storage from '../src/Storage.js'
import * as Task from '../src/Task.js'
import * as TaskScheduler from '../src/TaskScheduler.js'
import { TaskExecution } from '../src/TaskExecution.js'
import * as Tool from '../src/Tool.js'
import { ToolExecution } from '../src/ToolExecution.js'
import * as Invocation from '../src/ToolInvocation.js'
import * as ToolWrapper from '../src/ToolWrapper.js'
import * as ToolProgress from '../src/ToolProgress.js'
import * as Round from '../src/internal/ToolRound.js'
import * as SchemaGetter from 'effect/SchemaGetter'
import * as Fiber from 'effect/Fiber'
import * as Nested from '../src/internal/NestedInvocation.js'
import * as Toolkit from '../src/Toolkit.js'
import * as Transaction from '../src/Transaction.js'
import * as Usage from '../src/Usage.js'
import * as ContextProjection from '../src/ContextProjection.js'
import * as Settings from '../src/Settings.js'

const nativeBun = 'Bun' in globalThis
const services = nativeBun
  ? (await import('@effect/platform-bun')).BunServices.layer
  : NodeServices.layer
const contract = {
  kind: 'nested.recovery',
  version: 1,
  input: Schema.Void,
  checkpoint: Schema.Union([
    Schema.Struct({ phase: Schema.tag('admit') }),
    Schema.Struct({ phase: Schema.tag('consume'), handle: Invocation.HandleSchema }),
  ]),
  success: Schema.String,
  failure: Schema.String,
} as const

describe('native nested recovery and ownership', () => {
  it.live(
    'the factory receives fresh reserved Access and operation Scope over captured constructor Scope',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          class FactoryHost extends Context.Service<FactoryHost, string>()('nested/factory-host') {}
          const constructorScope = yield* Scope.make()
          yield* Effect.addFinalizer(() => Scope.close(constructorScope, Exit.void))
          const session = yield* Session.make()
          const registry = yield* Registry.make
          let opened = 0
          let closed = 0
          let hostScope: Scope.Scope | undefined
          let factoryScope: Scope.Scope | undefined
          const scheduler = yield* TaskScheduler.make(session, {
            host: {
              open: () =>
                Effect.gen(function* () {
                  assert.strictEqual(yield* FactoryHost, 'captured host')
                  hostScope = yield* Scope.Scope
                  assert.notStrictEqual(hostScope, constructorScope)
                  return {}
                }),
            },
            invocation: {
              open: (access, _lease, mode) =>
                Effect.gen(function* () {
                  opened++
                  assert.strictEqual(mode, 'run')
                  assert.strictEqual(yield* FactoryHost, 'captured host')
                  factoryScope = yield* Scope.Scope
                  assert.strictEqual(factoryScope, hostScope)
                  assert.notStrictEqual(factoryScope, constructorScope)
                  assert.isFalse(Nested.registerGate(access, () => Effect.void))
                  const record = yield* access.commit((tx) => Transaction.task(tx, access.taskId))
                  assert.isTrue(Option.isSome(record))
                  if (Option.isSome(record))
                    assert.strictEqual(record.value.state.status, 'running')
                  yield* Effect.acquireRelease(Effect.void, () =>
                    Effect.sync(() => {
                      closed++
                    }),
                  )
                  return Invocation.unsupported
                }),
            },
          }).pipe(
            Effect.provideService(Registry.Registry, registry),
            Effect.provideService(FactoryHost, 'captured host'),
            Effect.provideService(Scope.Scope, constructorScope),
          )
          const factoryContract = {
            ...contract,
            kind: 'nested.factory-scope',
            checkpoint: Schema.Struct({ phase: Schema.tag('run') }),
          }
          const program = Task.make({
            ...factoryContract,
            initial: () => ({ phase: 'run' as const }),
            phases: {
              run: () =>
                Effect.gen(function* () {
                  const access = yield* TaskExecution
                  assert.strictEqual(yield* Scope.Scope, factoryScope)
                  const unsupported = yield* access.invocation
                    .invokeTool('absent', {})
                    .pipe(Effect.flip)
                  assert.deepInclude(unsupported, { _tag: 'ExecutionError', reason: 'unsupported' })
                  yield* access.commit((tx) =>
                    access.progress(tx, factoryContract, {
                      status: 'completing',
                      outcome: { status: 'completed', result: 'done' },
                    }),
                  )
                }),
            },
          })
          yield* registry.install(
            Registry.withTask(
              Registry.toolkit({ id: 'factory-program', version: 1 }, Toolkit.make()),
              program,
            ),
          )
          const root = yield* Session.commit(session, (tx) => Transaction.ensureRoot(tx))
          const ref = yield* scheduler.start(program, undefined, { conversationId: root.id })
          assert.deepStrictEqual(yield* scheduler.wait(ref), {
            status: 'completed',
            result: 'done',
          })
          assert.strictEqual(opened, 1)
          assert.strictEqual(closed, 1)
          assert.isTrue(Option.isNone(yield* scheduler.failure))
        }),
      ).pipe(Effect.provide(Storage.layerMemory)),
  )
  it.live(
    'neutral snapshot detaches aliases, cycles, own properties, byte views, Date and options before asynchronous owner selection',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const selecting = yield* Deferred.make<void>()
          const release = yield* Deferred.make<void>()
          const date = new Date('2024-01-02T00:00:00.000Z')
          const backing = new Uint8Array([10, 20, 30, 40])
          const shared = { value: 'original' }
          const payload: Record<string, unknown> = Object.create(null)
          payload.a = shared
          payload.b = shared
          payload.self = payload
          payload.date = date
          payload.bytes = backing.subarray(1, 3)
          payload.other = backing.subarray(2, 4)
          Object.defineProperty(payload, '__proto__', { value: 'safe', enumerable: true })
          Object.defineProperty(payload, 'constructor', {
            value: 'safe constructor',
            enumerable: true,
          })
          const options = {
            key: 'original',
            progress: false,
            binding: { id: 'native-runtime-tools', version: 1 },
          }
          let encoded = 0
          const data = Schema.String.pipe(
            Schema.decodeTo(Schema.Unknown, {
              decode: SchemaGetter.transform((value) => value),
              encode: SchemaGetter.transform((value) => {
                encoded++
                assert.isObject(value)
                const record = value as Record<string, unknown>
                assert.isNull(Object.getPrototypeOf(record))
                assert.strictEqual(record.a, record.b)
                assert.strictEqual(record.self, record)
                assert.deepStrictEqual(record.a, { value: 'original' })
                assert.strictEqual(record.__proto__, 'safe')
                assert.deepEqual(record['constructor'], 'safe constructor')
                assert.instanceOf(record.date, Date)
                assert.strictEqual((record.date as Date).toISOString(), '2024-01-02T00:00:00.000Z')
                const bytes = record.bytes as Uint8Array
                const other = record.other as Uint8Array
                assert.deepStrictEqual([...bytes], [20, 30])
                assert.strictEqual(bytes.buffer, other.buffer)
                assert.notStrictEqual(bytes.buffer, backing.buffer)
                return 'copied'
              }),
            }),
          )
          const leaf = Tool.make('snapshot', {
            parameters: Schema.Struct({ payload: data }),
            success: Schema.String,
          })
          const tk = Toolkit.make(leaf)
          const model = yield* LanguageModel.make({
            generateText: () => Effect.die('No model'),
            streamText: () => Stream.die('No model'),
          })
          const runtime = yield* HarnessRuntime.make({ tools: tk }).pipe(
            Effect.provideService(LanguageModel.LanguageModel, model),
            Effect.provide(
              tk.toLayer({ snapshot: ({ payload }) => Effect.succeed(String(payload)) }),
            ),
          )
          const simple = {
            ...contract,
            kind: 'nested.snapshot',
            checkpoint: Schema.Struct({ phase: Schema.tag('run') }),
          }
          const program = Task.make({
            ...simple,
            initial: () => ({ phase: 'run' as const }),
            phases: {
              run: () =>
                Effect.gen(function* () {
                  const access = yield* TaskExecution
                  const actual = access.invocation
                  // Hold selection before delegating to the real native withTool; admissions are never mocked.
                  const held: Invocation.Access = {
                    ...actual,
                    withTool: (declaration, use, third) =>
                      Deferred.succeed(selecting, undefined).pipe(
                        Effect.andThen(Deferred.await(release)),
                        Effect.andThen(actual.withTool(declaration, use, third)),
                      ),
                  }
                  const receipt = yield* Invocation.invoke(
                    { invocation: held },
                    leaf,
                    { payload },
                    options,
                  )
                  assert.deepInclude(receipt, {
                    status: 'succeeded',
                    key: 'original',
                    value: 'copied',
                  })
                  yield* access.commit((tx) =>
                    access.progress(tx, simple, {
                      status: 'completing',
                      outcome: { status: 'completed', result: 'done' },
                    }),
                  )
                }),
            },
          })
          yield* runtime.registry.install(
            Registry.withTask(
              Registry.toolkit({ id: 'program', version: 1 }, Toolkit.make()),
              program,
            ),
          )
          const root = yield* runtime.backend.root
          const ref = yield* runtime.scheduler.start(program, undefined, { conversationId: root })
          yield* Deferred.await(selecting)
          assert.strictEqual(encoded, 0)
          shared.value = 'mutated'
          date.setTime(0)
          backing.fill(99)
          options.key = 'mutated'
          options.binding.id = 'missing'
          options.progress = true
          yield* Deferred.succeed(release, undefined)
          assert.deepStrictEqual(yield* runtime.scheduler.wait(ref), {
            status: 'completed',
            result: 'done',
          })
          assert.strictEqual(encoded, 1)
          assert.strictEqual(payload.self, payload)
          assert.strictEqual(shared.value, 'mutated')
        }),
      ).pipe(Effect.provide(Storage.layerMemory)),
  )

  it.live(
    'neutral snapshot rejects accessors, opaque instances, limits, shared/detached bytes and throwing Proxy inspection honestly',
    () =>
      Effect.gen(function* () {
        let getters = 0
        let constructors = 0
        class Private {
          #value = 1
          constructor() {
            constructors++
          }
          read() {
            return this.#value
          }
        }
        const opaque = new Private()
        const accessor = Object.defineProperty({}, 'value', {
          get() {
            getters++
            return 'unsafe'
          },
          enumerable: true,
        })
        const cyclic: Record<string, unknown> = {}
        cyclic.self = cyclic
        const oversized = { values: Array.from({ length: 4097 }, () => ({})) }
        const properties = Object.fromEntries(
          Array.from({ length: 65537 }, (_, index) => [String(index), index]),
        )
        const detached = new Uint8Array(4)
        structuredClone(detached.buffer, { transfer: [detached.buffer] })
        const throwing = new Proxy(
          {},
          {
            getPrototypeOf() {
              throw new Error('inspection failure')
            },
          },
        )
        const declaration = Tool.make('snapshot.reject', {
          parameters: Schema.Struct({ payload: Schema.Unknown }),
          success: Schema.String,
        })
        for (const payload of [
          accessor,
          opaque,
          oversized,
          properties,
          new Uint8Array(8 * 1024 * 1024 + 1),
          new Uint8Array(new SharedArrayBuffer(4)),
          detached,
          throwing,
          new Date(NaN),
        ]) {
          const failure = yield* Invocation.invoke(
            { invocation: Invocation.unsupported },
            declaration,
            { payload },
          ).pipe(Effect.flip)
          assert.deepInclude(failure, { _tag: 'InvocationError', reason: 'unsupportedArguments' })
        }
        assert.strictEqual(getters, 0)
        assert.strictEqual(constructors, 1)
        assert.strictEqual(opaque.read(), 1)
        const supported = yield* Invocation.invoke(
          { invocation: Invocation.unsupported },
          declaration,
          { payload: cyclic },
        ).pipe(Effect.flip)
        assert.deepInclude(supported, { _tag: 'ExecutionError', reason: 'unsupported' })
      }),
  )

  it.live(
    'parent completion marks every child before joining cleanup and abort mode refuses new admission',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const firstEntered = yield* Deferred.make<void>()
          const secondEntered = yield* Deferred.make<void>()
          const secondStopped = yield* Deferred.make<void>()
          const firstStopped = yield* Deferred.make<void>()
          const leaf = Tool.makeResult('cleanup', {
            parameters: Schema.Struct({ first: Schema.Boolean }),
            success: Schema.String,
            resultValidation: 'strict',
          })
          const tk = Toolkit.make(leaf)
          const model = yield* LanguageModel.make({
            generateText: () => Effect.die('No model'),
            streamText: () => Stream.die('No model'),
          })
          const runtime = yield* HarnessRuntime.make({ tools: tk }).pipe(
            Effect.provideService(LanguageModel.LanguageModel, model),
            Effect.provide(
              tk.toLayer({
                cleanup: ({ first }) =>
                  Effect.gen(function* () {
                    const access = yield* ToolExecution
                    yield* access.details({ first })
                    yield* Effect.addFinalizer(() =>
                      first
                        ? Deferred.await(secondStopped).pipe(
                            Effect.andThen(Deferred.succeed(firstStopped, undefined)),
                          )
                        : Deferred.succeed(secondStopped, undefined),
                    )
                    yield* Deferred.succeed(first ? firstEntered : secondEntered, undefined)
                    return yield* Effect.never
                  }),
              }),
            ),
          )
          const simple = {
            ...contract,
            kind: 'nested.cleanup',
            checkpoint: Schema.Struct({ phase: Schema.tag('run') }),
          }
          let first: Invocation.Handle | undefined
          let second: Invocation.Handle | undefined
          const program = Task.make({
            ...simple,
            initial: () => ({ phase: 'run' as const }),
            phases: {
              run: () =>
                Effect.gen(function* () {
                  const access = yield* TaskExecution
                  yield* access.commit((tx) =>
                    Effect.gen(function* () {
                      const a = yield* access.invocation.admit(tx, {
                        name: leaf.name,
                        arguments: { first: true },
                        options: { key: 'first', progress: false },
                      })
                      const b = yield* access.invocation.admit(tx, {
                        name: leaf.name,
                        arguments: { first: false },
                        options: { key: 'second', progress: false },
                      })
                      if (a._tag !== 'admitted' || b._tag !== 'admitted')
                        return yield* Effect.die('Expected children')
                      first = a.handle
                      second = b.handle
                    }),
                  )
                  yield* Deferred.await(firstEntered)
                  yield* Deferred.await(secondEntered)
                  yield* access.commit((tx) =>
                    access.progress(tx, simple, {
                      status: 'completing',
                      outcome: { status: 'completed', result: 'done' },
                    }),
                  )
                }),
            },
          })
          yield* runtime.registry.install(
            Registry.withTask(
              Registry.toolkit({ id: 'program', version: 1 }, Toolkit.make()),
              program,
            ),
          )
          const root = yield* runtime.backend.root
          const ref = yield* runtime.scheduler.start(program, undefined, { conversationId: root })
          assert.deepStrictEqual(yield* runtime.scheduler.wait(ref), {
            status: 'completed',
            result: 'done',
          })
          assert.isTrue(yield* Deferred.isDone(firstStopped))
          assert.isTrue(yield* Deferred.isDone(secondStopped))
          const records = yield* Session.scanTasks(runtime.session).pipe(Stream.runCollect)
          for (const handle of [first!, second!]) {
            const record = records.find((item) => item.id === handle.taskId)!
            assert.strictEqual(record.state.status, 'terminal')
            // The parent's task-scoped result members retire with it; conversation progress and usage remain.
            assert.isTrue(
              Option.isNone(
                yield* Session.snapshot(
                  runtime.session,
                  Nested.results,
                  Nested.target(ref.id, handle.taskId),
                ),
              ),
            )
            const progress = Option.getOrThrow(
              yield* Session.snapshot(
                runtime.session,
                ToolProgress.document,
                Round.target(root, Round.progressKey(handle.taskId)),
              ),
            ).value
            assert.strictEqual(progress.status, 'cancelled')
          }
          const receipts = yield* Session.commit(runtime.session, (tx) => Usage.receipts(tx, root))
          assert.strictEqual(
            receipts.filter(
              (receipt) =>
                receipt.identity._tag === 'nestedCall' && receipt.outcome._tag === 'interrupted',
            ).length,
            2,
          )
          let abortChecked = false
          const aborting = Task.make({
            ...simple,
            kind: 'nested.abort-guard',
            initial: () => ({ phase: 'run' as const }),
            phases: { run: () => Effect.never },
            abort: () =>
              Effect.gen(function* () {
                const access = yield* TaskExecution
                const rejected = yield* access
                  .commit((tx) =>
                    access.invocation.admit(tx, {
                      name: leaf.name,
                      arguments: { first: false },
                      options: { key: 'abort' },
                    }),
                  )
                  .pipe(Effect.flip)
                assert.deepInclude(rejected, { reason: 'revoked' })
                assert.isTrue(
                  Option.isNone(yield* access.snapshot(Nested.calls, Nested.target(access.taskId))),
                )
                abortChecked = true
              }),
          })
          yield* runtime.registry.install(
            Registry.withTask(
              Registry.toolkit({ id: 'abort', version: 1 }, Toolkit.make()),
              aborting,
            ),
          )
          const aborted = yield* runtime.scheduler.start(aborting, undefined, {
            conversationId: root,
          })
          yield* runtime.scheduler.abort(aborted.id)
          assert.isTrue(abortChecked)
        }),
      ).pipe(Effect.provide(Storage.layerMemory)),
  )

  it.live(
    'parallel parents and reverse same-name children keep structural keys, progress and accounting separate after observer interruption',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const release = yield* Deferred.make<void>()
          const entered = yield* Deferred.make<void>()
          const bothSecond = yield* Deferred.make<void>()
          let firstEntered = 0
          let secondFinished = 0
          const toolkitLeaf = Tool.makeResult('same', {
            parameters: Schema.Struct({ a: Schema.String, b: Schema.String }),
            success: Schema.String,
            resultValidation: 'strict',
          })
          const tk = Toolkit.make(toolkitLeaf)
          let runs = 0
          const progressIds: string[] = []
          const model = yield* LanguageModel.make({
            generateText: () => Effect.die('No model'),
            streamText: () => Stream.die('No model'),
          })
          const settings = yield* Settings.make({
            compaction: {
              enabled: false,
              reserveTokens: 20,
              keepRecentTokens: 10,
              backgroundTokens: 0,
            },
          })
          const runtime = yield* HarnessRuntime.make({
            tools: tk,
            hooks: [
              Hook.make({
                event: 'beforeCompact',
                execute: () =>
                  Effect.succeed({
                    _tag: 'summary',
                    summary: 'Native nested usage stays authoritative.',
                  } as const),
              }),
            ],
          }).pipe(
            Effect.provideService(LanguageModel.LanguageModel, model),
            Effect.provideService(Settings.Settings, settings),
            Effect.provide(
              tk.toLayer({
                same: ({ a }) =>
                  Effect.gen(function* () {
                    runs++
                    const access = yield* ToolExecution
                    progressIds.push(access.callId)
                    yield* access.output(`same:${a}`)
                    yield* access.details({ a })
                    if (a === 'first') {
                      if (++firstEntered === 2) yield* Deferred.succeed(entered, undefined)
                      yield* Deferred.await(release)
                    }
                    return {
                      content: [],
                      structuredOutput: a,
                      spend: { amount: 2, currency: 'USD', source: 'parallel' },
                    }
                  }),
              }),
            ),
          )
          const simple = {
            ...contract,
            kind: 'nested.parallel',
            checkpoint: Schema.Struct({ phase: Schema.tag('run') }),
          }
          const program = Task.make({
            ...simple,
            initial: () => ({ phase: 'run' as const }),
            phases: {
              run: () =>
                Effect.gen(function* () {
                  const access = yield* TaskExecution
                  const first = yield* access.commit((tx) =>
                    access.invocation.admit(tx, {
                      name: toolkitLeaf.name,
                      arguments: { a: 'first', b: 'equal' },
                      options: { key: 'constructor', progress: false },
                    }),
                  )
                  const same = yield* access.commit((tx) =>
                    access.invocation.admit(tx, {
                      name: toolkitLeaf.name,
                      arguments: { b: 'equal', a: 'first' },
                      options: { key: 'constructor', progress: false },
                    }),
                  )
                  assert.deepStrictEqual(first, same)
                  const second = yield* access.commit((tx) =>
                    access.invocation.admit(tx, {
                      name: toolkitLeaf.name,
                      arguments: { a: 'second', b: 'equal' },
                      options: { key: 'second', progress: false },
                    }),
                  )
                  if (first._tag !== 'admitted' || second._tag !== 'admitted')
                    return yield* Effect.die('Expected children')
                  const observer = yield* access.invocation
                    .wait(first.handle)
                    .pipe(Effect.forkScoped)
                  yield* Deferred.await(entered)
                  yield* Fiber.interrupt(observer)
                  const child = Option.getOrThrow(
                    yield* access.commit((tx) => Transaction.task(tx, first.handle.taskId)),
                  )
                  assert.strictEqual(child.state.status, 'running')
                  const later = yield* Invocation.decode(access, toolkitLeaf, second.handle)
                  assert.deepInclude(later, { status: 'succeeded', value: 'second' })
                  if (++secondFinished === 2) yield* Deferred.succeed(bothSecond, undefined)
                  yield* Deferred.await(bothSecond)
                  yield* Deferred.succeed(release, undefined)
                  assert.deepInclude(yield* Invocation.decode(access, toolkitLeaf, first.handle), {
                    status: 'succeeded',
                    value: 'first',
                  })
                  yield* access.commit((tx) =>
                    access.progress(tx, simple, {
                      status: 'completing',
                      outcome: { status: 'completed', result: 'done' },
                    }),
                  )
                }),
            },
          })
          yield* runtime.registry.install(
            Registry.withTask(
              Registry.toolkit({ id: 'program', version: 1 }, Toolkit.make()),
              program,
            ),
          )
          const root = yield* runtime.backend.root
          const other = yield* runtime.backend.create()
          const parents = yield* Effect.all([
            runtime.scheduler.start(program, undefined, { conversationId: root }),
            runtime.scheduler.start(program, undefined, { conversationId: other }),
          ])
          const outcomes = yield* Effect.forEach(
            parents,
            (parent) => runtime.scheduler.wait(parent),
            { concurrency: 'unbounded' },
          )
          for (const outcome of outcomes)
            assert.deepStrictEqual(outcome, { status: 'completed', result: 'done' })
          assert.strictEqual(runs, 4)
          assert.strictEqual(new Set(progressIds).size, 4)
          for (const conversation of [root, other]) {
            const receipts = yield* Session.commit(runtime.session, (tx) =>
              Usage.receipts(tx, conversation),
            )
            assert.lengthOf(receipts, 2)
            assert.deepStrictEqual((yield* Session.usage(runtime.session, conversation)).costs, [
              { currency: 'USD', model: 0, tool: 4 },
            ])
            yield* Session.commit(runtime.session, (tx) =>
              Transaction.appendEntry(tx, conversation, {
                kind: 'input',
              }),
            )
            const fork = yield* runtime.backend.fork({ conversationId: conversation })
            assert.lengthOf(
              yield* Session.commit(runtime.session, (tx) => Usage.receipts(tx, fork)),
              0,
            )
          }
          assert.deepStrictEqual((yield* Session.usage(runtime.session)).costs, [
            { currency: 'USD', model: 0, tool: 8 },
          ])
          const baseline = yield* Session.usage(runtime.session)
          yield* Session.commit(runtime.session, (tx) =>
            Transaction.appendEntry(tx, root, { kind: 'nested.reset', head: 'self' }),
          )
          assert.lengthOf((yield* ContextProjection.read(runtime.session, root)).activeEntries, 1)
          assert.deepStrictEqual(yield* Session.usage(runtime.session), baseline)
          yield* Session.commit(runtime.session, (tx) =>
            Effect.gen(function* () {
              for (const text of ['old input ', 'recent input '])
                yield* Transaction.appendEntry(tx, root, {
                  kind: 'input',
                  model: [{ role: 'user', content: text.repeat(100) }],
                })
            }),
          )
          const compaction = yield* runtime.backend.compact!(root)
          const compacted = yield* runtime.backend.waitCompaction!(compaction)
          assert.strictEqual(compacted.status, 'completed')
          if (compacted.status === 'completed') {
            assert.strictEqual(compacted.result._tag, 'Queued')
            if (compacted.result._tag === 'Queued')
              assert.strictEqual(
                (yield* runtime.backend.wait(compacted.result.submissionId)).status,
                'done',
              )
          }
          yield* ContextProjection.read(runtime.session, root)
          assert.deepStrictEqual(yield* Session.usage(runtime.session), baseline)
        }),
      ).pipe(Effect.provide(Storage.layerMemory)),
  )

  for (const consumerEnd of ['release', 'cancel'] as const)
    it.live(
      `original, outer and asynchronous consumer codecs retain independent services through uninstall and ${consumerEnd}`,
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            class Label extends Context.Service<Label, string>()('nested/ownership/Label') {}
            const decoderEntered = yield* Deferred.make<void>()
            const decoderRelease = yield* Deferred.make<void>()
            const calls: string[] = []
            const closed: string[] = []
            const text = Schema.String.pipe(
              Schema.decodeTo(Schema.String, {
                decode: SchemaGetter.transformEffect((value) =>
                  Effect.gen(function* () {
                    const label = yield* Label
                    calls.push(`decode:${label}`)
                    if (label === 'CONSUMER' && value.includes('ORIGINAL')) {
                      yield* Deferred.succeed(decoderEntered, undefined)
                      yield* Deferred.await(decoderRelease)
                    }
                    return `${label}:${value}`
                  }),
                ),
                encode: SchemaGetter.transformEffect((value) =>
                  Effect.map(Label, (label) => {
                    calls.push(`encode:${label}`)
                    return `${label}:${value}`
                  }),
                ),
              }),
            )
            const original = Tool.make('wrapped', {
              parameters: Schema.Struct({ value: text }),
              success: text,
            })
            const outer = Tool.make('wrapped', {
              parameters: Schema.Struct({ value: text }),
              success: text,
            })
            const toolkit = Toolkit.make(original)
            const labelLayer = (label: string) =>
              Layer.effect(
                Label,
                Effect.acquireRelease(Effect.succeed(label), () =>
                  Effect.sync(() => {
                    closed.push(label)
                  }),
                ),
              )
            const registry = yield* Registry.make
            yield* registry.install(
              Registry.provide(
                Registry.toolkit({ id: 'original', version: 1 }, toolkit),
                toolkit
                  .toLayer(
                    Effect.map(Label, (label) => ({
                      wrapped: ({ value }) =>
                        Effect.sync(() => {
                          assert.strictEqual(label, 'ORIGINAL')
                          return value
                        }),
                    })),
                  )
                  .pipe(Layer.provideMerge(labelLayer('ORIGINAL'))),
              ),
            )
            yield* registry.install(
              Registry.toolWrapper(
                { id: 'wrapper', version: 1 },
                ToolWrapper.bind(outer, (item) => Effect.succeed(item)).pipe(
                  ToolWrapper.provide(labelLayer('OUTER')),
                ),
              ),
            )
            const model = yield* LanguageModel.make({
              generateText: () => Effect.die('No model'),
              streamText: () => Stream.die('No model'),
            })
            const runtime = yield* HarnessRuntime.make({ registry }).pipe(
              Effect.provideService(LanguageModel.LanguageModel, model),
            )
            const simple = {
              ...contract,
              kind: 'nested.codec-owner',
              checkpoint: Schema.Struct({ phase: Schema.tag('run') }),
            }
            const program = Task.make({
              ...simple,
              initial: () => ({ phase: 'run' as const }),
              phases: {
                run: () =>
                  Effect.gen(function* () {
                    const access = yield* TaskExecution
                    const result = yield* Invocation.invoke(
                      access,
                      outer,
                      { value: 'input' },
                      { key: 'wrapped', binding: { id: 'original', version: 1 } },
                    ).pipe(Effect.provideService(Label, 'CONSUMER'))
                    assert.deepInclude(result, {
                      status: 'succeeded',
                      valueKind: 'decoded',
                      value: 'CONSUMER:ORIGINAL:ORIGINAL:CONSUMER:input',
                    })
                    yield* access.commit((tx) =>
                      access.progress(tx, simple, {
                        status: 'completing',
                        outcome: { status: 'completed', result: 'done' },
                      }),
                    )
                  }),
              },
            })
            yield* registry.install(
              Registry.withTask(
                Registry.toolkit({ id: 'program', version: 1 }, Toolkit.make()),
                program,
              ),
            )
            const root = yield* runtime.backend.root
            const ref = yield* runtime.scheduler.start(program, undefined, { conversationId: root })
            yield* Deferred.await(decoderEntered)
            yield* registry.uninstall('original')
            yield* registry.uninstall('wrapper')
            assert.lengthOf(closed, 0)
            if (consumerEnd === 'cancel') {
              yield* runtime.scheduler.abort(ref.id)
              assert.strictEqual((yield* runtime.scheduler.wait(ref)).status, 'aborted')
            } else {
              yield* Deferred.succeed(decoderRelease, undefined)
              assert.deepStrictEqual(yield* runtime.scheduler.wait(ref), {
                status: 'completed',
                result: 'done',
              })
            }
            for (let n = 0; n < 20 && closed.length < 2; n++) yield* Effect.yieldNow
            assert.sameMembers(closed, ['ORIGINAL', 'OUTER'])
            assert.include(calls, 'decode:ORIGINAL')
            assert.include(calls, 'decode:OUTER')
            assert.include(calls, 'decode:CONSUMER')
            assert.include(calls, 'encode:CONSUMER')
            yield* registry.install(
              Registry.provide(
                Registry.toolkit({ id: 'original', version: 2 }, toolkit),
                toolkit
                  .toLayer(
                    Effect.map(Label, (label) => ({
                      wrapped: ({ value }) =>
                        Effect.sync(() => {
                          assert.strictEqual(label, 'NEW')
                          return value
                        }),
                    })),
                  )
                  .pipe(Layer.provideMerge(labelLayer('NEW'))),
              ),
            )
            const nextContract = { ...simple, kind: 'nested.codec-next-owner' }
            const nextProgram = Task.make({
              ...nextContract,
              initial: () => ({ phase: 'run' as const }),
              phases: {
                run: () =>
                  Effect.gen(function* () {
                    const access = yield* TaskExecution
                    const result = yield* Invocation.invoke(
                      access,
                      original,
                      { value: 'next' },
                      { key: 'wrapped', binding: { id: 'original', version: 2 } },
                    ).pipe(Effect.provideService(Label, 'CONSUMER'))
                    assert.deepInclude(result, {
                      status: 'succeeded',
                      valueKind: 'decoded',
                      value: 'CONSUMER:NEW:NEW:CONSUMER:next',
                    })
                    yield* access.commit((tx) =>
                      access.progress(tx, nextContract, {
                        status: 'completing',
                        outcome: { status: 'completed', result: 'done' },
                      }),
                    )
                  }),
              },
            })
            yield* registry.install(
              Registry.withTask(
                Registry.toolkit({ id: 'next-program', version: 1 }, Toolkit.make()),
                nextProgram,
              ),
            )
            const nextRoot = yield* runtime.backend.create()
            const nextRef = yield* runtime.scheduler.start(nextProgram, undefined, {
              conversationId: nextRoot,
            })
            assert.deepStrictEqual(yield* runtime.scheduler.wait(nextRef), {
              status: 'completed',
              result: 'done',
            })
            yield* registry.uninstall('original')
            for (let n = 0; n < 20 && closed.length < 3; n++) yield* Effect.yieldNow
            assert.sameMembers(closed, ['ORIGINAL', 'OUTER', 'NEW'])
            assert.include(calls, 'decode:NEW')
          }),
        ).pipe(Effect.provide(Storage.layerMemory)),
    )

  it.live(
    'borrow-local cleanup marks only its children, retains sibling and next-handler admission and revokes stale access',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const enteredA = yield* Deferred.make<void>()
          const enteredB = yield* Deferred.make<void>()
          const releaseB = yield* Deferred.make<void>()
          let childA: import('../src/Record.js').TaskId | undefined
          let childB: import('../src/Record.js').TaskId | undefined
          const leaf = Tool.makeResult('borrow.leaf', {
            parameters: Schema.Struct({ value: Schema.String }),
            success: Schema.String,
            resultValidation: 'strict',
          })
          const toolkit = Toolkit.make(leaf)
          const model = yield* LanguageModel.make({
            generateText: () => Effect.die('No model'),
            streamText: () => Stream.die('No model'),
          })
          const runtime = yield* HarnessRuntime.make({ tools: toolkit }).pipe(
            Effect.provideService(LanguageModel.LanguageModel, model),
            Effect.provide(
              toolkit.toLayer({
                'borrow.leaf': ({ value }) =>
                  Effect.gen(function* () {
                    const access = yield* ToolExecution
                    yield* access.details({ value })
                    if (value === 'A') {
                      childA = access.taskId
                      yield* Deferred.succeed(enteredA, undefined)
                      return yield* Effect.never
                    }
                    if (value === 'B') {
                      childB = access.taskId
                      yield* Deferred.succeed(enteredB, undefined)
                      yield* Deferred.await(releaseB)
                    }
                    return { content: [], structuredOutput: value }
                  }),
              }),
            ),
          )
          const simple = {
            ...contract,
            kind: 'nested.borrow',
            checkpoint: Schema.Struct({ phase: Schema.tag('run') }),
          }
          const program = Task.make({
            ...simple,
            initial: () => ({ phase: 'run' as const }),
            phases: {
              run: () =>
                Effect.gen(function* () {
                  const access = yield* TaskExecution
                  const a = Nested.borrow(access.invocation, Effect.void)
                  const b = Nested.borrow(access.invocation, Effect.void)
                  const first = yield* a
                    .invokeTool(leaf.name, { value: 'A' }, { key: 'A', progress: false })
                    .pipe(Effect.forkScoped)
                  const second = yield* b
                    .invokeTool(leaf.name, { value: 'B' }, { key: 'B', progress: false })
                    .pipe(Effect.forkScoped)
                  yield* Deferred.await(enteredA)
                  yield* Deferred.await(enteredB)
                  yield* Nested.close(a, true)
                  assert.strictEqual((yield* access.outcomes([childA!]))[0]!.status, 'aborted')
                  const secondChild = yield* access.commit((tx) =>
                    Transaction.task(tx, childB!).pipe(Effect.map(Option.getOrThrow)),
                  )
                  assert.strictEqual(secondChild.state.status, 'running')
                  assert.deepInclude(yield* Fiber.join(first).pipe(Effect.flip), {
                    _tag: 'InvocationError',
                    reason: 'revoked',
                  })
                  yield* Deferred.succeed(releaseB, undefined)
                  assert.strictEqual((yield* Fiber.join(second)).status, 'succeeded')
                  assert.strictEqual(
                    (yield* Nested.borrow(access.invocation, Effect.void).invokeTool(
                      leaf.name,
                      { value: 'C' },
                      { key: 'C', progress: false },
                    )).status,
                    'succeeded',
                  )
                  const stale = yield* access
                    .commit((tx) =>
                      a.admit(tx, { name: 'missing', arguments: {}, options: { key: 'stale' } }),
                    )
                    .pipe(Effect.flip)
                  assert.deepInclude(stale, { _tag: 'InvocationError', reason: 'revoked' })
                  yield* access.commit((tx) =>
                    access.progress(tx, simple, {
                      status: 'completing',
                      outcome: { status: 'completed', result: 'done' },
                    }),
                  )
                }),
            },
          })
          yield* runtime.registry.install(
            Registry.withTask(
              Registry.toolkit({ id: 'program', version: 1 }, Toolkit.make()),
              program,
            ),
          )
          const root = yield* runtime.backend.root
          const ref = yield* runtime.scheduler.start(program, undefined, { conversationId: root })
          assert.deepStrictEqual(yield* runtime.scheduler.wait(ref), {
            status: 'completed',
            result: 'done',
          })
        }),
      ).pipe(Effect.provide(Storage.layerMemory)),
  )
  for (const backend of ['memory', 'sqlite', 'jsonl'] as const)
    for (const recovery of [
      'safe',
      'unsafe',
      'observed-revoked',
      'observed-missing',
      'channel-mismatch',
      'current-unsafe',
    ] as const)
      it.live(`${backend}: cold ${recovery} retains identity, saved policy and observed cost`, () =>
        Effect.scoped(
          Effect.gen(function* () {
            const fs = yield* FileSystem.FileSystem
            const directory = yield* fs.makeTempDirectoryScoped()
            const sqlite = nativeBun
              ? (yield* Effect.promise(() => import('@effect/sql-sqlite-bun/SqliteClient'))).layer
              : (yield* Effect.promise(() => import('@effect/sql-sqlite-node/SqliteClient'))).layer
            const memory = yield* Layer.build(Storage.layerMemory)
            const openStorage = (scope: Scope.Scope) =>
              backend === 'memory'
                ? Effect.succeed(memory)
                : Layer.build(
                    backend === 'sqlite'
                      ? Storage.layerJournal().pipe(
                          Layer.provide(SqlEventJournal.layer()),
                          Layer.provide(sqlite({ filename: `${directory}/cold.sqlite` })),
                        )
                      : Storage.layerJsonl({ filePath: `${directory}/cold.jsonl` }),
                  ).pipe(Effect.provideService(Scope.Scope, scope))
            const entered = yield* Deferred.make<void>()
            const observed = yield* Deferred.make<void>()
            let runs = 0
            let beforeHooks = 0
            let afterHooks = 0
            const leaf = Tool.makeResult('leaf', {
              success: Schema.String,
              resultValidation: 'strict',
              replay: recovery === 'unsafe' ? 'unsafe' : 'safe',
            })
            let current = leaf
            const model = yield* LanguageModel.make({
              generateText: () => Effect.die('No model in a generic program'),
              streamText: () => Stream.die('No model in a generic program'),
            })
            const program = Task.make({
              ...contract,
              initial: () => ({ phase: 'admit' as const }),
              phases: {
                admit: () =>
                  Effect.gen(function* () {
                    const access = yield* TaskExecution
                    yield* access.commit((tx) =>
                      Effect.gen(function* () {
                        const admission = yield* access.invocation.admit(tx, {
                          name: leaf.name,
                          arguments: {},
                          options: { key: 'child', progress: false },
                        })
                        if (admission._tag !== 'admitted')
                          return yield* Effect.die('unexpected denial')
                        yield* access.memoize(
                          tx,
                          'constructor',
                          Invocation.HandleSchema,
                          admission.handle,
                        )
                        yield* access.progress(tx, contract, {
                          status: 'waiting',
                          on: [admission.handle.taskId],
                          policy: 'allSettled',
                          checkpoint: { phase: 'consume', handle: admission.handle },
                        })
                      }),
                    )
                  }),
                consume: ({ checkpoint }) =>
                  Effect.gen(function* () {
                    const access = yield* TaskExecution
                    assert.deepStrictEqual(
                      Option.getOrThrow(yield* access.memo('constructor', Invocation.HandleSchema)),
                      checkpoint.handle,
                    )
                    const wire = yield* access.invocation.wait(checkpoint.handle)
                    const receipt = yield* Invocation.decode(access, current, checkpoint.handle)
                    let expected = 'denied'
                    if (recovery === 'safe') expected = 'succeeded'
                    if (recovery === 'unsafe') expected = 'interrupted'
                    assert.strictEqual(wire.status, expected)
                    assert.strictEqual(receipt.status, expected)
                    if (expected !== 'succeeded')
                      assert.strictEqual(receipt.valueKind, 'unavailable')
                    yield* access.commit((tx) =>
                      access.progress(tx, contract, {
                        status: 'completing',
                        outcome: { status: 'completed', result: wire.status },
                      }),
                    )
                  }),
              },
            })
            const firstScope = yield* Scope.make()
            yield* Effect.addFinalizer(() => Scope.close(firstScope, Exit.void))
            const firstContext = yield* openStorage(firstScope)
            const firstRegistry = yield* Registry.make.pipe(
              Effect.provideService(Scope.Scope, firstScope),
            )
            yield* firstRegistry.install(
              Registry.withTask(
                Registry.toolkit({ id: 'program', version: 1 }, Toolkit.make()),
                program,
              ),
            )
            const tk = Toolkit.make(leaf)
            const first = yield* HarnessRuntime.make({
              registry: firstRegistry,
              tools: tk,
              hooks: [
                Hook.make({
                  event: 'beforeTool',
                  execute: () =>
                    Effect.sync(() => {
                      beforeHooks++
                    }),
                }),
                Hook.make({
                  event: 'afterTool',
                  execute: () =>
                    Effect.gen(function* () {
                      afterHooks++
                      yield* Deferred.succeed(observed, undefined)
                      return yield* Effect.never
                    }),
                }),
              ],
            }).pipe(
              Effect.provideContext(firstContext),
              Effect.provideService(Scope.Scope, firstScope),
              Effect.provideService(LanguageModel.LanguageModel, model),
              Effect.provide(
                tk.toLayer({
                  leaf: () =>
                    Effect.gen(function* () {
                      runs++
                      yield* Deferred.succeed(entered, undefined)
                      if (
                        recovery === 'safe' ||
                        recovery === 'unsafe' ||
                        recovery === 'channel-mismatch' ||
                        recovery === 'current-unsafe'
                      )
                        return yield* Effect.never
                      return {
                        content: [],
                        structuredOutput: 'PRIVATE',
                        spend: { amount: 7, currency: 'USD', source: 'observed' },
                        control: { terminate: true },
                      }
                    }),
                }),
              ),
            )
            const root = yield* first.backend.root
            const ref = yield* first.scheduler.start(program, undefined, { conversationId: root })
            yield* Deferred.await(entered)
            if (recovery.startsWith('observed')) yield* Deferred.await(observed)
            const before = yield* Session.scanTasks(first.session).pipe(Stream.runCollect)
            const child = before.find((item) => item.kind === 'harness.tool.nested')!
            assert.strictEqual(child.owner, ref.id)
            if (recovery === 'observed-revoked')
              yield* Session.commit(first.session, (tx) =>
                Transaction.setDocument(tx, AgentState.document, AgentState.target(root), {
                  tools: [],
                }),
              )
            yield* Scope.close(firstScope, Exit.void)
            if (recovery === 'channel-mismatch')
              current = leaf
                .annotate(Tool.ResultChannels, false)
                .annotate(Tool.StructuredOutput, false)
            if (recovery === 'current-unsafe')
              current = leaf.annotate(Tool.Policy, {
                replay: 'unsafe',
                executionMode: 'sequential',
              })
            const secondScope = yield* Scope.make()
            yield* Effect.addFinalizer(() => Scope.close(secondScope, Exit.void))
            const secondContext = yield* openStorage(secondScope)
            const secondRegistry = yield* Registry.make.pipe(
              Effect.provideService(Scope.Scope, secondScope),
            )
            yield* secondRegistry.install(
              Registry.withTask(
                Registry.toolkit({ id: 'program', version: 1 }, Toolkit.make()),
                program,
              ),
            )
            const nextTk = Toolkit.make(current)
            const second = yield* HarnessRuntime.make({
              registry: secondRegistry,
              ...(recovery === 'observed-missing' ? {} : { tools: nextTk }),
            }).pipe(
              Effect.provideContext(secondContext),
              Effect.provideService(Scope.Scope, secondScope),
              Effect.provideService(LanguageModel.LanguageModel, model),
              Effect.provide(
                nextTk.toLayer({
                  leaf: () =>
                    Effect.sync(() => {
                      runs++
                      return { content: [], structuredOutput: 'RECOVERED' }
                    }),
                }),
              ),
            )
            yield* second.backend.root
            let expected = 'denied'
            if (recovery === 'safe') expected = 'succeeded'
            if (recovery === 'unsafe') expected = 'interrupted'
            assert.deepStrictEqual(yield* second.scheduler.wait(ref), {
              status: 'completed',
              result: expected,
            })
            assert.strictEqual(runs, recovery === 'safe' ? 2 : 1)
            assert.strictEqual(beforeHooks, 1)
            assert.strictEqual(afterHooks, recovery.startsWith('observed') ? 1 : 0)
            const after = yield* Session.scanTasks(second.session).pipe(Stream.runCollect)
            assert.strictEqual(
              after.filter((item) => item.kind === 'harness.tool.nested').length,
              1,
            )
            assert.strictEqual(
              after.find((item) => item.kind === 'harness.tool.nested')!.id,
              child.id,
            )
            const receipts = yield* Session.commit(second.session, (tx) => Usage.receipts(tx, root))
            assert.lengthOf(receipts, recovery === 'safe' ? 2 : 1)
            assert.strictEqual(
              receipts.filter(
                (receipt) => receipt.usage?._tag === 'tool' && receipt.usage.cost?.amount === 7,
              ).length,
              recovery.startsWith('observed') ? 1 : 0,
            )
            assert.isTrue(Option.isNone(yield* second.scheduler.failure))
          }),
        ).pipe(Effect.provide(services)),
      )
})
