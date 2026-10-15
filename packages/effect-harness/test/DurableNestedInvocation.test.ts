import { NodeServices } from '@effect/platform-node'
import { assert, describe, it } from '@effect/vitest'
import * as Context from 'effect/Context'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as Scope from 'effect/Scope'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Result from 'effect/Result'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as LanguageModel from 'effect/ai/LanguageModel'
import * as Prompt from 'effect/ai/Prompt'
import * as HarnessRuntime from '../src/HarnessRuntime.js'
import * as Registry from '../src/Registry.js'
import type * as Record from '../src/Record.js'
import * as Session from '../src/Session.js'
import * as Storage from '../src/Storage.js'
import * as StorageFormat from '../src/StorageFormat.js'
import * as Task from '../src/Task.js'
import { TaskExecution } from '../src/TaskExecution.js'
import * as Tool from '../src/Tool.js'
import { ToolExecution } from '../src/ToolExecution.js'
import * as ToolInvocation from '../src/ToolInvocation.js'
import * as ToolProgress from '../src/ToolProgress.js'
import * as ToolResult from '../src/ToolResult.js'
import * as Toolkit from '../src/Toolkit.js'
import * as Usage from '../src/Usage.js'
import * as Nested from '../src/internal/NestedInvocation.js'
import * as Round from '../src/internal/ToolRound.js'
import { StorageError } from '../src/StorageError.js'

const nativeBun = 'Bun' in globalThis
const nativeServices = nativeBun
  ? (await import('@effect/platform-bun')).BunServices.layer
  : NodeServices.layer
const contract = {
  kind: 'nested.program',
  version: 1,
  input: Schema.Void,
  checkpoint: Schema.Struct({ phase: Schema.Literal('execute') }),
  success: Schema.Json,
  failure: Schema.String,
} as const

describe('native nested invocation', () => {
  for (const backend of ['memory', 'sqlite', 'jsonl'] as const)
    it.live(
      `${backend}: cold native fallback preserves exact persisted channels without replay`,
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const fs = yield* FileSystem.FileSystem
            const directory = yield* fs.makeTempDirectoryScoped()
            const sqlite = nativeBun
              ? (yield* Effect.promise(() => import('@effect/sql-sqlite-bun/SqliteClient'))).layer
              : (yield* Effect.promise(() => import('@effect/sql-sqlite-node/SqliteClient'))).layer
            const layer = (() => {
              if (backend === 'sqlite')
                return Storage.layerSql.pipe(
                  Layer.provide(sqlite({ filename: `${directory}/content.sqlite` })),
                )
              if (backend === 'jsonl')
                return Storage.layerJsonl({ filePath: `${directory}/content.jsonl` })
              return Storage.layerMemory
            })()
            const memory =
              backend === 'memory' ? yield* Layer.build(Storage.layerMemory) : undefined
            const open = (scope: Scope.Scope) =>
              memory === undefined ? Layer.buildWithScope(layer, scope) : Effect.succeed(memory)
            const parts: Record<string, ToolResult.Content> = {
              empty: [],
              text: [Prompt.textPart({ text: 'single' })],
              string: [Prompt.filePart({ mediaType: 'image/png', data: 'encoded' })],
              url: [
                Prompt.filePart({
                  mediaType: 'image/png',
                  data: new URL('https://example.test/cold.png'),
                }),
              ],
              bytes: [Prompt.filePart({ mediaType: 'image/png', data: new Uint8Array([1, 2, 3]) })],
              mixed: [
                Prompt.textPart({ text: 'first' }),
                Prompt.filePart({
                  mediaType: 'application/octet-stream',
                  data: new Uint8Array([4, 5]),
                }),
              ],
            }
            const expected = parts
            const nativeCodec = Schema.toCodecJson(ToolResult.ContentSchema)
            for (const mode of Object.keys(parts)) {
              const encoded = yield* Schema.encodeEffect(nativeCodec)(parts[mode]!)
              assert.deepStrictEqual(
                yield* Schema.decodeEffect(nativeCodec)(encoded),
                expected[mode],
              )
            }
            const leaf = Tool.makeResult('cold-shape', {
              parameters: Schema.Struct({ mode: Schema.String }),
              replay: 'safe',
            })
            const toolkit = Toolkit.make(leaf)
            const reached = yield* Deferred.make<void>()
            let warm = true
            let runs = 0
            const handles = Schema.Array(
              Schema.Struct({ mode: Schema.String, handle: ToolInvocation.HandleSchema }),
            )
            const coldContract = {
              ...contract,
              kind: 'nested.cold-content',
              checkpoint: Schema.Union([
                Schema.Struct({ phase: Schema.tag('execute') }),
                Schema.Struct({ phase: Schema.tag('consume'), handles }),
              ]),
            }
            const program = Task.make({
              ...coldContract,
              initial: () => ({ phase: 'execute' as const }),
              phases: {
                execute: () =>
                  Effect.gen(function* () {
                    const access = yield* TaskExecution
                    const saved = yield* Effect.forEach(Object.keys(parts), (mode) =>
                      access
                        .commit((tx) =>
                          access.invocation.admit(tx, {
                            name: leaf.name,
                            arguments: { mode },
                            options: { key: mode },
                          }),
                        )
                        .pipe(
                          Effect.flatMap((admitted) =>
                            admitted._tag === 'admitted'
                              ? Effect.succeed({ mode, handle: admitted.handle })
                              : Effect.die('Native content admission denied'),
                          ),
                        ),
                    )
                    yield* access.commit((tx) =>
                      access.progress(tx, coldContract, {
                        status: 'waiting',
                        on: saved.map((slot) => slot.handle.taskId),
                        policy: 'allSettled',
                        checkpoint: { phase: 'consume', handles: saved },
                      }),
                    )
                  }),
                consume: ({ checkpoint: { handles: saved } }) =>
                  Effect.gen(function* () {
                    const access = yield* TaskExecution
                    const values = yield* Effect.forEach(saved, (slot) =>
                      ToolInvocation.decode(access, leaf, slot.handle).pipe(
                        Effect.map((receipt) => ({ slot, receipt })),
                      ),
                    )
                    for (const { slot, receipt } of values) {
                      assert.strictEqual(receipt.status, 'succeeded')
                      assert.strictEqual(receipt.valueKind, 'content')
                      if (receipt.valueKind === 'content')
                        assert.deepStrictEqual(
                          receipt.value,
                          ToolResult.contentValue(expected[slot.mode]!),
                        )
                      assert.notProperty(receipt, 'control')
                    }
                    if (warm) {
                      yield* Deferred.succeed(reached, undefined)
                      return yield* Effect.never
                    }
                    yield* access.commit((tx) =>
                      access.progress(tx, coldContract, {
                        status: 'completing',
                        outcome: { status: 'completed', result: 'cold' },
                      }),
                    )
                  }),
              },
            })
            const model = yield* LanguageModel.make({
              generateText: () => Effect.die('No model'),
              streamText: () => Stream.die('No model'),
            })
            const setup = (scope: Scope.Scope) =>
              Effect.gen(function* () {
                const context = yield* open(scope)
                const registry = yield* Registry.make.pipe(
                  Effect.provideService(Scope.Scope, scope),
                )
                yield* registry.install(
                  Registry.withTask(
                    Registry.toolkit({ id: 'content-program', version: 1 }, Toolkit.make()),
                    program,
                  ),
                )
                return yield* HarnessRuntime.make({ registry, tools: toolkit }).pipe(
                  Effect.provideContext(context),
                  Effect.provideService(Scope.Scope, scope),
                  Effect.provideService(LanguageModel.LanguageModel, model),
                  Effect.provide(
                    toolkit.toLayer({
                      'cold-shape': ({ mode }) =>
                        Effect.sync(() => {
                          if (!warm) throw new Error('Committed content must not replay')
                          runs++
                          return {
                            content: parts[mode]!,
                            details: { mode },
                            control: { terminate: true as const },
                          }
                        }),
                    }),
                  ),
                )
              })
            const firstScope = yield* Scope.make()
            yield* Effect.addFinalizer(() => Scope.close(firstScope, Exit.void))
            const first = yield* setup(firstScope)
            const root = yield* first.backend.root
            const ref = yield* first.scheduler.start(program, undefined, { conversationId: root })
            yield* Deferred.await(reached)
            assert.strictEqual(runs, 6)
            yield* Scope.close(firstScope, Exit.void)
            warm = false
            const secondScope = yield* Scope.make()
            yield* Effect.addFinalizer(() => Scope.close(secondScope, Exit.void))
            const second = yield* setup(secondScope)
            assert.deepStrictEqual(yield* second.scheduler.wait(ref), {
              status: 'completed',
              result: 'cold',
            })
            assert.strictEqual(runs, 6)
            assert.lengthOf(
              yield* Session.scanEntries(second.session, { conversationId: root }).pipe(
                Stream.runCollect,
              ),
              0,
            )
            assert.lengthOf(
              yield* Session.commit(second.session, (tx) => Usage.receipts(tx, root)),
              6,
            )
            assert.isTrue(Option.isNone(yield* second.scheduler.failure))
          }),
        ).pipe(Effect.provide(nativeServices)),
    )
  for (const backend of ['memory', 'sqlite', 'jsonl'] as const)
    for (const mode of ['missing', 'foreign-storage', 'lower', 'mismatched-pair'] as const)
      it.live(`${backend}: ${mode} protection refuses native nested admission`, () =>
        Effect.scoped(
          Effect.gen(function* () {
            const fs = yield* FileSystem.FileSystem
            const directory = yield* fs.makeTempDirectoryScoped()
            const sqlite = nativeBun
              ? (yield* Effect.promise(() => import('@effect/sql-sqlite-bun/SqliteClient'))).layer
              : (yield* Effect.promise(() => import('@effect/sql-sqlite-node/SqliteClient'))).layer
            const layer = (() => {
              if (backend === 'sqlite')
                return Storage.layerSql.pipe(
                  Layer.provide(sqlite({ filename: `${directory}/guard.sqlite` })),
                )
              if (backend === 'jsonl')
                return Storage.layerJsonl({ filePath: `${directory}/guard.jsonl` })
              return Storage.layerMemory
            })()
            const actualContext = yield* Layer.build(layer)
            const actual = Context.get(actualContext, Storage.Storage)
            const actualFormat = Context.get(actualContext, StorageFormat.StorageFormat)
            const originalMarker = yield* actualFormat.inspect
            if (mode === 'missing') {
              const rejected = yield* StorageFormat.requireSupportedFormat({
                formatVersion: 10,
                minimumReaderVersion: 10,
              }).pipe(Effect.provideContext(Context.make(Storage.Storage, actual)), Effect.result)
              assert.isTrue(Result.isFailure(rejected))
              if (Result.isFailure(rejected) && rejected.failure._tag === 'StorageFormatError')
                assert.strictEqual(rejected.failure.reason, 'unavailable')
              assert.deepStrictEqual(yield* actualFormat.inspect, originalMarker)
              assert.lengthOf(yield* actual.scanTasks().pipe(Stream.runCollect), 0)
              return
            }
            const marker = {
              formatVersion: 9,
              minimumReaderVersion: mode === 'mismatched-pair' ? 10 : 9,
            }
            let invalidStorage = false
            const foreignStorage = { ...actual }
            const context = Context.make(Storage.Storage, actual).pipe(
              Context.add(StorageFormat.StorageFormat, {
                get storage() {
                  return invalidStorage ? foreignStorage : actual
                },
                inspect: Effect.succeed(marker),
                requireSupportedFormat: () => Effect.succeed(marker),
              }),
            )
            let runs = 0
            const leaf = Tool.make('guarded', { success: Schema.String })
            const toolkit = Toolkit.make(leaf)
            const model = yield* LanguageModel.make({
              generateText: () => Effect.die('No model'),
              streamText: () => Stream.die('No model'),
            })
            const runtime = yield* HarnessRuntime.make({ tools: toolkit }).pipe(
              Effect.provideContext(context),
              Effect.provideService(LanguageModel.LanguageModel, model),
              Effect.provide(
                toolkit.toLayer({
                  guarded: () =>
                    Effect.sync(() => {
                      runs++
                      return 'wrong'
                    }),
                }),
              ),
              Effect.result,
            )
            assert.isTrue(Result.isSuccess(runtime))
            if (Result.isFailure(runtime)) return yield* runtime.failure
            const active = runtime.success
            const program = Task.make({
              ...contract,
              initial: () => ({ phase: 'execute' as const }),
              phases: {
                execute: () =>
                  Effect.gen(function* () {
                    const access = yield* TaskExecution
                    invalidStorage = mode === 'foreign-storage'
                    const rejected = yield* ToolInvocation.invoke(
                      access,
                      leaf,
                      {},
                      { key: 'guarded' },
                    ).pipe(Effect.result)
                    assert.isTrue(Result.isFailure(rejected))
                    if (Result.isFailure(rejected))
                      assert.strictEqual(rejected.failure._tag, 'StorageFormatError')
                    if (
                      Result.isFailure(rejected) &&
                      rejected.failure._tag === 'StorageFormatError'
                    ) {
                      assert.strictEqual(rejected.failure._tag, 'StorageFormatError')
                      assert.strictEqual(
                        rejected.failure.reason,
                        mode === 'foreign-storage' ? 'unavailable' : 'unsupported',
                      )
                    }
                    assert.isTrue(
                      Option.isNone(
                        yield* access.snapshot(Nested.calls, Nested.target(access.taskId)),
                      ),
                    )
                    assert.lengthOf(
                      yield* access.commit((tx) => Usage.receipts(tx, access.conversationId)),
                      0,
                    )
                    yield* access.commit((tx) =>
                      access.progress(tx, contract, {
                        status: 'completing',
                        outcome: { status: 'completed', result: 'refused' },
                      }),
                    )
                  }),
              },
            })
            yield* active.registry.install(
              Registry.withTask(
                Registry.toolkit({ id: 'guard-program', version: 1 }, Toolkit.make()),
                program,
              ),
            )
            const root = yield* active.backend.root
            const ref = yield* active.scheduler.start(program, undefined, { conversationId: root })
            assert.deepStrictEqual(yield* active.scheduler.wait(ref), {
              status: 'completed',
              result: 'refused',
            })
            assert.strictEqual(runs, 0)
            assert.deepStrictEqual(yield* actualFormat.inspect, originalMarker)
            assert.lengthOf(
              (yield* Session.scanTasks(active.session, { conversationId: root }).pipe(
                Stream.runCollect,
              )).filter((task) => task.owner !== undefined),
              0,
            )
            const health = yield* active.scheduler.failure
            if (mode === 'foreign-storage') {
              assert.isTrue(Option.isSome(health))
              if (Option.isSome(health))
                assert.include(health.value.message, 'matching StorageFormat capability')
            } else assert.isTrue(Option.isNone(health))
          }),
        ).pipe(Effect.provide(nativeServices)),
      )
  for (const backend of ['memory', 'sqlite', 'jsonl'] as const)
    it.live(
      `${backend}: typed task calls, duplicate admission and final-only atomic progress`,
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const fs = yield* FileSystem.FileSystem
            const directory = yield* fs.makeTempDirectoryScoped()
            const sqlite = nativeBun
              ? (yield* Effect.promise(() => import('@effect/sql-sqlite-bun/SqliteClient'))).layer
              : (yield* Effect.promise(() => import('@effect/sql-sqlite-node/SqliteClient'))).layer
            const layer = (() => {
              if (backend === 'sqlite')
                return Storage.layerSql.pipe(
                  Layer.provide(sqlite({ filename: `${directory}/nested.sqlite` })),
                )
              if (backend === 'jsonl')
                return Storage.layerJsonl({ filePath: `${directory}/nested.jsonl` })
              return Storage.layerMemory
            })()
            const context = yield* Layer.build(layer)
            const actual = Context.get(context, Storage.Storage)
            const format = Context.get(context, StorageFormat.StorageFormat)
            const batches: ReadonlyArray<Record.StorageWrite>[] = []
            let probing = false
            let forbiddenReads = 0
            let forbiddenFormats = 0
            const read = Effect.sync(() => {
              if (probing) forbiddenReads++
            })
            const captured: Storage.Storage['Service'] = {
              ...actual,
              task: (id) => read.pipe(Effect.andThen(actual.task(id))),
              findDocument: (address, at) =>
                read.pipe(Effect.andThen(actual.findDocument(address, at))),
              document: (id, at) => read.pipe(Effect.andThen(actual.document(id, at))),
              scanDocuments: (query) =>
                Stream.unwrap(read.pipe(Effect.as(actual.scanDocuments(query)))),
              commit: (writes) => {
                const owned = [...writes]
                return actual.commit(owned).pipe(
                  Effect.tap(() =>
                    Effect.sync(() => {
                      batches.push(owned)
                    }),
                  ),
                )
              },
            }
            const storage = context.pipe(
              Context.add(Storage.Storage, captured),
              Context.add(StorageFormat.StorageFormat, {
                ...format,
                storage: captured,
                inspect: Effect.sync(() => {
                  if (probing) forbiddenFormats++
                }).pipe(Effect.andThen(format.inspect)),
                requireSupportedFormat: (requested) =>
                  Effect.sync(() => {
                    if (probing) forbiddenFormats++
                  }).pipe(Effect.andThen(format.requireSupportedFormat(requested))),
              }),
            )
            const child = Tool.makeResult('nested.child', {
              parameters: Schema.Struct({ text: Schema.String }),
              success: Schema.String,
              resultValidation: 'strict',
              callers: ['tools'],
              replay: 'safe',
              outputRetention: { retain: 'tail', maxBytes: 512, maxLines: 3 },
            })
            const toolkit = Toolkit.make(child)
            const entered = yield* Deferred.make<void>()
            const release = yield* Deferred.make<void>()
            const parentReady = yield* Deferred.make<void>()
            const probesDone = yield* Deferred.make<void>()
            let runs = 0
            let late: ToolExecution['Service'] | undefined
            let handle: ToolInvocation.Handle | undefined
            let parentAccess: TaskExecution['Service'] | undefined
            const model = yield* LanguageModel.make({
              generateText: () => Effect.die('A generic task must not request a model'),
              streamText: () => Stream.die('A generic task must not request a model'),
            })
            const runtime = yield* HarnessRuntime.make({ tools: toolkit }).pipe(
              Effect.provideContext(storage),
              Effect.provideService(LanguageModel.LanguageModel, model),
              Effect.provide(
                toolkit.toLayer({
                  'nested.child': ({ text }) =>
                    Effect.gen(function* () {
                      runs++
                      const access = yield* ToolExecution
                      late = access
                      yield* access.output('prefix\n😀 final\n')
                      yield* access.details({ accepted: true })
                      yield* access.diagnostic({ severity: 'info', message: 'locally accepted' })
                      yield* Deferred.succeed(entered, undefined)
                      yield* Deferred.await(release)
                      return {
                        content: [Prompt.textPart({ text: 'model-hidden' })],
                        structuredOutput: text.toUpperCase(),
                        details: { ui: 'visible' },
                        spend: { amount: 2, currency: 'USD', source: 'test' },
                        control: { terminate: true },
                      }
                    }),
                }),
              ),
            )
            const program = Task.make({
              ...contract,
              initial: () => ({ phase: 'execute' as const }),
              phases: {
                execute: () =>
                  Effect.gen(function* () {
                    const access = yield* TaskExecution
                    parentAccess = access
                    const request = {
                      name: child.name,
                      arguments: { text: 'hello' },
                      options: { key: 'toString', progress: false },
                    }
                    const admissions = yield* Effect.all(
                      [
                        access.commit((tx) => access.invocation.admit(tx, request)),
                        access.commit((tx) => access.invocation.admit(tx, request)),
                      ],
                      { concurrency: 'unbounded' },
                    )
                    assert.strictEqual(admissions[0]!._tag, 'admitted')
                    assert.deepStrictEqual(admissions[0], admissions[1])
                    if (admissions[0]!._tag !== 'admitted')
                      return yield* Effect.die('expected admitted child')
                    handle = admissions[0]!.handle
                    yield* Deferred.succeed(parentReady, undefined)
                    yield* Deferred.await(probesDone)
                    yield* Deferred.await(entered)
                    assert.isTrue(
                      Option.isNone(
                        yield* access.snapshot(
                          ToolProgress.document,
                          Round.target(access.conversationId, Round.progressKey(handle.taskId)),
                        ),
                      ),
                    )
                    yield* Deferred.succeed(release, undefined)
                    const receipt = yield* ToolInvocation.decode(access, child, handle)
                    assert.strictEqual(receipt.status, 'succeeded')
                    if (receipt.status === 'succeeded' && receipt.valueKind === 'decoded')
                      assert.strictEqual(receipt.value, 'HELLO')
                    else return yield* Effect.die('expected concrete decoded channel')
                    assert.deepStrictEqual(receipt.details, { ui: 'visible' })
                    assert.notProperty(receipt, 'control')
                    const repeated = yield* ToolInvocation.invoke(
                      access,
                      child,
                      { text: 'hello' },
                      { key: 'toString', progress: false },
                    )
                    assert.deepStrictEqual(repeated, receipt)
                    const conflict = yield* access
                      .commit((tx) =>
                        access.invocation.admit(tx, {
                          ...request,
                          arguments: { text: 'different' },
                        }),
                      )
                      .pipe(Effect.flip)
                    assert.strictEqual(conflict._tag, 'InvocationError')
                    yield* access.commit((tx) =>
                      access.progress(tx, contract, {
                        status: 'completing',
                        outcome: { status: 'completed', result: 'HELLO' },
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
            const parent = yield* runtime.scheduler.start(program, undefined, {
              conversationId: root,
            })
            yield* Deferred.await(parentReady)
            const before = batches.length
            probing = true
            for (const request of [
              { name: child.name, arguments: { text: 'hello' }, options: { key: 'fresh' } },
              {
                name: child.name,
                arguments: { text: 'hello' },
                options: { key: 'toString', progress: false },
              },
              { name: 'missing', arguments: {}, options: { key: 'denied' } },
            ]) {
              const rejected = yield* Session.commit(runtime.session, (tx) =>
                parentAccess!.invocation.admit(tx, request).pipe(Effect.flip),
              )
              assert.strictEqual(rejected._tag, 'TaskError')
            }
            assert.strictEqual(batches.length, before)
            probing = false
            assert.strictEqual(forbiddenReads, 0)
            assert.strictEqual(forbiddenFormats, 0)
            yield* Deferred.succeed(probesDone, undefined)
            assert.deepStrictEqual(yield* runtime.scheduler.wait(parent), {
              status: 'completed',
              result: 'HELLO',
            })
            assert.strictEqual(runs, 1)
            assert.isDefined(handle)
            assert.strictEqual(
              (yield* StorageFormat.inspect.pipe(Effect.provideContext(storage)))
                .minimumReaderVersion,
              12,
            )
            const receipts = yield* Session.commit(runtime.session, (tx) =>
              Usage.receipts(tx, root),
            )
            assert.strictEqual(
              receipts.filter((item) => item.identity._tag === 'nestedCall').length,
              1,
            )
            const progress = Option.getOrThrow(
              yield* Session.snapshot(
                runtime.session,
                ToolProgress.document,
                Round.target(root, Round.progressKey(handle!.taskId)),
              ),
            ).value
            assert.strictEqual(progress.status, 'completed')
            assert.deepStrictEqual(progress.details, { accepted: true })
            assert.include(progress.output.text, '😀 final')
            const documents = (batch: ReadonlyArray<Record.StorageWrite>, kind: string) =>
              batch.filter(
                (write) => write._tag === 'document.create' && write.record.kind === kind,
              )
            const admission = batches.find((batch) =>
              batch.some(
                (write) =>
                  write._tag === 'task' &&
                  write.value.id === handle!.taskId &&
                  write.value.state.status === 'pending',
              ),
            )!
            assert.isDefined(admission)
            assert.isAtLeast(documents(admission, Nested.calls.definition.kind).length, 1)
            assert.isAtLeast(documents(admission, Nested.results.definition.kind).length, 1)
            assert.lengthOf(documents(admission, ToolProgress.document.definition.kind), 0)
            const final = batches.find(
              (batch) =>
                documents(batch, ToolProgress.document.definition.kind).length > 0 &&
                batch.some(
                  (write) =>
                    write._tag === 'task' &&
                    write.value.id === handle!.taskId &&
                    ['completing', 'terminal'].includes(write.value.state.status),
                ),
            )!
            assert.isDefined(final)
            assert.isTrue(final.some((write) => write._tag === 'document.change'))
            assert.isAtLeast(
              final.filter(
                (write) => write._tag === 'document.change' || write._tag === 'document.create',
              ).length,
              3,
            )
            const entries = yield* Session.scanEntries(runtime.session, {
              conversationId: root,
            }).pipe(Stream.runCollect)
            assert.isFalse(entries.some((entry) => entry.kind.startsWith('tool.')))
            assert.strictEqual(
              (yield* late!.details({ late: true }).pipe(Effect.flip))._tag,
              'ExecutionError',
            )
          }),
        ).pipe(Effect.provide(nativeServices)),
    )

  for (const backend of ['memory', 'sqlite', 'jsonl'] as const)
    for (const fault of ['reject', 'uncertain'] as const)
      it.live(`${backend}: ${fault} final batch never acknowledges a durable invocation`, () =>
        Effect.scoped(
          Effect.gen(function* () {
            const fs = yield* FileSystem.FileSystem
            const directory = yield* fs.makeTempDirectoryScoped()
            const sqlite = nativeBun
              ? (yield* Effect.promise(() => import('@effect/sql-sqlite-bun/SqliteClient'))).layer
              : (yield* Effect.promise(() => import('@effect/sql-sqlite-node/SqliteClient'))).layer
            const layer = (() => {
              if (backend === 'sqlite')
                return Storage.layerSql.pipe(
                  Layer.provide(sqlite({ filename: `${directory}/fault.sqlite` })),
                )
              if (backend === 'jsonl')
                return Storage.layerJsonl({ filePath: `${directory}/fault.jsonl` })
              return Storage.layerMemory
            })()
            const ownerScope = yield* Scope.Scope
            const firstScope = yield* Scope.make()
            yield* Effect.addFinalizer(() => Scope.close(firstScope, Exit.void))
            const context = yield* Layer.build(layer).pipe(
              Effect.provideService(Scope.Scope, backend === 'memory' ? ownerScope : firstScope),
            )
            const actual = Context.get(context, Storage.Storage)
            const format = Context.get(context, StorageFormat.StorageFormat)
            let intercepted: ReadonlyArray<Record.StorageWrite> | undefined
            const captured: Storage.Storage['Service'] = {
              ...actual,
              commit: (input) => {
                const writes = [...input]
                const final = writes.some(
                  (write) =>
                    write._tag === 'document.create' &&
                    write.record.kind === ToolProgress.document.definition.kind,
                )
                if (!final) return actual.commit(writes)
                intercepted = writes
                const failed = Effect.fail(
                  new StorageError({
                    reason: fault === 'uncertain' ? 'uncertain' : 'io',
                    operation: 'test.final',
                    message: `Injected ${fault} final batch`,
                  }),
                )
                return fault === 'uncertain'
                  ? actual.commit(writes).pipe(Effect.andThen(failed))
                  : failed
              },
            }
            const storage = context.pipe(
              Context.add(Storage.Storage, captured),
              Context.add(StorageFormat.StorageFormat, { ...format, storage: captured }),
            )
            const leaf = Tool.makeResult('fault.leaf', {
              success: Schema.String,
              resultValidation: 'strict',
              replay: 'safe',
            })
            const toolkit = Toolkit.make(leaf)
            let runs = 0
            let locallyAccepted = false
            let durableReceipt = false
            let childId: Record.TaskId | undefined
            const model = yield* LanguageModel.make({
              generateText: () => Effect.die('No model'),
              streamText: () => Stream.die('No model'),
            })
            const runtime = yield* HarnessRuntime.make({ tools: toolkit }).pipe(
              Effect.provideContext(storage),
              Effect.provideService(Scope.Scope, firstScope),
              Effect.provideService(LanguageModel.LanguageModel, model),
              Effect.provide(
                toolkit.toLayer({
                  'fault.leaf': () =>
                    Effect.gen(function* () {
                      runs++
                      const access = yield* ToolExecution
                      childId = access.taskId
                      yield* access.details({ local: 'accepted' })
                      locallyAccepted = true
                      return {
                        content: [],
                        structuredOutput: 'result',
                        spend: { amount: 4, currency: 'USD', source: 'fault' },
                      }
                    }),
                }),
              ),
            )
            const program = Task.make({
              ...contract,
              initial: () => ({ phase: 'execute' as const }),
              phases: {
                execute: () =>
                  Effect.gen(function* () {
                    const access = yield* TaskExecution
                    yield* ToolInvocation.invoke(
                      access,
                      leaf,
                      {},
                      { key: 'fault', progress: false },
                    )
                    durableReceipt = true
                    yield* access.commit((tx) =>
                      access.progress(tx, contract, {
                        status: 'completing',
                        outcome: { status: 'completed', result: 'reconciled' },
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
            yield* runtime.scheduler.wait(ref).pipe(Effect.flip)
            assert.isTrue(locallyAccepted)
            assert.isFalse(durableReceipt)
            assert.isDefined(intercepted)
            const batch = intercepted!
            assert.isTrue(
              batch.some(
                (write) =>
                  write._tag === 'task' &&
                  write.value.id === childId &&
                  ['completing', 'terminal'].includes(write.value.state.status),
              ),
            )
            assert.isAtLeast(
              batch.filter(
                (write) => write._tag === 'document.create' || write._tag === 'document.change',
              ).length,
              3,
            )
            const persisted = yield* actual.findDocument({
              kind: ToolProgress.document.definition.kind,
              scope: { _tag: 'conversation', conversationId: root },
              key: Round.progressKey(childId!),
            })
            assert.strictEqual(Option.isSome(persisted), fault === 'uncertain')
            if (Option.isSome(persisted)) {
              const doc = Option.getOrThrow(yield* actual.document(persisted.value.id))
              assert.strictEqual(doc.value.status, 'completed')
              assert.deepStrictEqual(doc.value.details, { local: 'accepted' })
            }
            assert.isTrue(Option.isSome(yield* runtime.scheduler.failure))
            yield* Scope.close(firstScope, Exit.void)
            const reopenedScope = yield* Scope.make()
            yield* Effect.addFinalizer(() => Scope.close(reopenedScope, Exit.void))
            const reopenedContext =
              backend === 'memory'
                ? context
                : yield* Layer.build(layer).pipe(Effect.provideService(Scope.Scope, reopenedScope))
            const registry = yield* Registry.make.pipe(
              Effect.provideService(Scope.Scope, reopenedScope),
            )
            yield* registry.install(
              Registry.withTask(
                Registry.toolkit({ id: 'program', version: 1 }, Toolkit.make()),
                program,
              ),
            )
            const reopened = yield* HarnessRuntime.make({ registry, tools: toolkit }).pipe(
              Effect.provideContext(reopenedContext),
              Effect.provideService(Scope.Scope, reopenedScope),
              Effect.provideService(LanguageModel.LanguageModel, model),
              Effect.provide(
                toolkit.toLayer({
                  'fault.leaf': () =>
                    Effect.die('Observed result must not replay handler after final write failure'),
                }),
              ),
            )
            yield* reopened.backend.root
            assert.deepStrictEqual(yield* reopened.scheduler.wait(ref), {
              status: 'completed',
              result: 'reconciled',
            })
            assert.isTrue(durableReceipt)
            assert.strictEqual(runs, 1)
            const receipts = yield* Session.commit(reopened.session, (tx) =>
              Usage.receipts(tx, root),
            )
            assert.lengthOf(receipts, 1)
            const receipt = receipts[0]!
            assert.strictEqual(receipt.identity._tag, 'nestedCall')
            if (receipt.identity._tag === 'nestedCall')
              assert.strictEqual(receipt.identity.taskId, childId)
            assert.strictEqual(receipt.usage?._tag, 'tool')
            if (receipt.usage?._tag === 'tool') assert.strictEqual(receipt.usage.cost?.amount, 4)
            assert.isTrue(Option.isNone(yield* reopened.scheduler.failure))
          }),
        ).pipe(Effect.provide(nativeServices)),
      )
})
