import * as TemporaryStorage from './fixtures/Storage.js'
import { assert, it } from '@effect/vitest'
import { NodeServices } from '@effect/platform-node'

import * as Clock from 'effect/Clock'
import * as Context from 'effect/Context'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as Fiber from 'effect/Fiber'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as SchemaGetter from 'effect/SchemaGetter'
import * as Scope from 'effect/Scope'
import * as Stream from 'effect/Stream'
import * as TestClock from 'effect/testing/TestClock'
import * as LanguageModel from 'effect/ai/LanguageModel'
import * as HarnessRuntime from 'effect-harness/HarnessRuntime'
import * as Model from 'effect-harness/Model'
import * as Record from 'effect-harness/Record'
import * as Session from 'effect-harness/Session'
import * as Settings from 'effect-harness/Settings'
import * as Storage from 'effect-harness/Storage'
import * as Usage from 'effect-harness/Usage'

const root = Record.ROOT_CONVERSATION_ID
const definition: Model.Definition = {
  ref: { provider: 'custom', modelId: 'pinned-cancel' },
  capabilities: { tools: false, reasoning: false, images: false, structuredOutput: false },
}
const native = LanguageModel.make({
  generateText: () => Effect.die('no native generation'),
  streamText: () => Stream.die('no native streaming'),
})
const pending = Effect.gen(function* () {
  return {
    _tag: 'Deferred' as const,
    handle: 'pinned',
    nextPollAt: (yield* Clock.currentTimeMillis) + 600000,
  }
})
const waitSaved = Effect.fnUntraced(function* (runtime: HarnessRuntime.HarnessRuntimeService) {
  for (;;) {
    const tasks = yield* Session.scanTasks(runtime.session).pipe(Stream.runCollect)
    const checkpoint = Schema.decodeUnknownOption(Schema.JsonObject)(tasks[0]?.state.checkpoint)
    if (Option.isSome(checkpoint) && checkpoint.value['phase'] === 'deferred') return
    yield* Effect.yieldNow
  }
})
const submit = Effect.fnUntraced(function* (runtime: HarnessRuntime.HarnessRuntimeService) {
  yield* runtime.backend.root
  const job = yield* runtime.backend.submit({
    conversationId: root,
    draft: { type: 'input', content: 'cancel' },
  })
  yield* waitSaved(runtime)
  return job
})
const diagnostic = Effect.fnUntraced(function* (runtime: HarnessRuntime.HarnessRuntimeService) {
  const task = (yield* Session.scanTasks(runtime.session).pipe(Stream.runCollect))[0]
  assert.isTrue(task?.abortRequested)
  if (task?.state.status !== 'terminal') return assert.fail('missing terminal task')
  const outcome = yield* Schema.decodeUnknownEffect(Record.TaskOutcome)(task.state.outcome)
  if (outcome.status !== 'aborted') return assert.fail('missing aborted outcome')
  return yield* Schema.decodeUnknownEffect(
    Schema.Struct({
      cancellation: Schema.Struct({
        status: Schema.String,
        message: Schema.optionalKey(Schema.String),
      }),
    }),
  )(outcome.result)
})

for (const backend of ['temporary', 'jsonl'] as const)
  for (const variant of [
    'active',
    'same',
    'version',
    'definition',
    'configuration-id',
    'configuration-version',
    'missing-model',
    'missing-deferred',
  ] as const)
    it.live(
      `${backend} ${variant} abort checks saved bindings before any replacement effects and settles once`,
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const fs = yield* FileSystem.FileSystem
            const directory = yield* fs.makeTempDirectoryScoped()

            const disk = Storage.layerJsonl({ filePath: `${directory}/cancel.jsonl` })
            const store = backend === 'temporary' ? TemporaryStorage.layer : disk
            const temporary =
              backend === 'temporary' ? yield* Layer.build(Layer.fresh(store)) : undefined
            const openStore = () =>
              temporary === undefined ? Layer.build(Layer.fresh(store)) : Effect.succeed(temporary)
            let creates = 0,
              restores = 0,
              cancels = 0,
              decodes = 0
            const handle = Schema.String.pipe(
              Schema.decodeTo(Schema.String, {
                decode: SchemaGetter.transformEffect((value) =>
                  Effect.sync(() => {
                    decodes++
                    return value
                  }),
                ),
                encode: SchemaGetter.transform((value) => value),
              }),
            )
            const adapter = yield* Model.makeDeferred<typeof handle, never>({
              handle,
              start: () =>
                Effect.sync(() => {
                  creates++
                }).pipe(Effect.andThen(pending)),
              poll: () => Effect.die('not due'),
              cancel: () =>
                Effect.sync(() => {
                  cancels++
                }),
            })
            const configuration: Model.Configuration = {
              binding: { id: 'pinned-config', version: 1 },
              encode: () => Effect.succeed(null),
              restore: () =>
                Effect.sync(() => {
                  restores++
                  return Context.empty()
                }),
            }
            const input = {
              definition,
              version: 1,
              options: Schema.JsonObject,
              languageModel: yield* native,
              deferred: adapter,
              configuration,
              configure: () => Effect.succeed(Context.empty()),
            }
            const firstOwner = yield* Scope.make()
            const policy = yield* Settings.make({ stream: { deferred: true } })
            const first = yield* HarnessRuntime.make({ models: [Model.make(input)] }).pipe(
              Effect.provideContext(yield* openStore()),
              Effect.provideService(Scope.Scope, firstOwner),
              Effect.provideService(Settings.Settings, policy),
            )
            const job = yield* submit(first)
            let runtime = first
            let owner = firstOwner
            if (variant !== 'active') {
              yield* Scope.close(firstOwner, Exit.void)
              assert.strictEqual(cancels, 0)
              const replacement = Model.make({
                ...input,
                version: variant === 'version' ? 2 : 1,
                definition:
                  variant === 'definition'
                    ? { ...definition, capabilities: { ...definition.capabilities, images: true } }
                    : definition,
                deferred: variant === 'missing-deferred' ? undefined : adapter,
                configuration: {
                  ...configuration,
                  binding: {
                    id: variant === 'configuration-id' ? 'different' : 'pinned-config',
                    version: variant === 'configuration-version' ? 2 : 1,
                  },
                },
              })
              owner = yield* Scope.make()
              runtime = yield* HarnessRuntime.make({
                models:
                  variant === 'missing-model'
                    ? [
                        Model.make({
                          ...input,
                          definition: {
                            ...definition,
                            ref: { ...definition.ref, modelId: 'different-model' },
                          },
                        }),
                      ]
                    : [replacement],
              }).pipe(
                Effect.provideContext(yield* openStore()),
                Effect.provideService(Scope.Scope, owner),
              )
            }
            const before = { restores, decodes }
            yield* runtime.backend.abort(root)
            assert.strictEqual((yield* runtime.backend.wait(job.id)).status, 'unanswered')
            const report = yield* diagnostic(runtime)
            const compatible = variant === 'same' || variant === 'active'
            assert.strictEqual(report.cancellation.status, compatible ? 'completed' : 'failed')
            if (!compatible) assert.isNotEmpty(report.cancellation.message)
            assert.strictEqual(restores - before.restores, compatible ? 1 : 0)
            assert.strictEqual(decodes - before.decodes, compatible ? 1 : 0)
            assert.strictEqual(cancels, compatible ? 1 : 0)
            assert.strictEqual(creates, 1)
            const receipts = yield* Session.commit(runtime.session, (tx) =>
              Usage.receipts(tx, root),
            )
            assert.strictEqual(receipts.length, 1)
            assert.strictEqual(receipts[0]?.outcome._tag, 'interrupted')
            yield* runtime.backend.abort(root)
            assert.deepEqual(yield* diagnostic(runtime), report)
            assert.deepEqual(
              yield* Session.commit(runtime.session, (tx) => Usage.receipts(tx, root)),
              receipts,
            )
            yield* Scope.close(owner, Exit.void)
          }),
        ).pipe(Effect.provide(NodeServices.layer)),
    )

class Config extends Context.Service<Config, string>()('DeferredCancellationRepair/Config') {}
class Client extends Context.Service<Client, { readonly alive: () => boolean }>()(
  'DeferredCancellationRepair/Client',
) {}

for (const narrow of [false, true])
  for (const uninterruptible of [false, true])
    it.effect(
      `saved config and fresh codec/callback scope win over ${narrow ? 'restricted' : 'full'} capture; uninterruptible=${uninterruptible}`,
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            let alive = true,
              decodedAcquisitions = 0,
              decodedFinalizers = 0,
              callbackFinalizers = 0,
              restoreAcquisitions = 0,
              restoreFinalizers = 0,
              configurationDecodes = 0,
              configurationFinalizers = 0
            const modelOwner = yield* Scope.make()
            yield* Effect.addFinalizer(() => Scope.close(modelOwner, Exit.void))
            const client = yield* Effect.acquireRelease(
              Effect.succeed({ alive: () => alive }),
              () =>
                Effect.sync(() => {
                  alive = false
                }),
            ).pipe(Effect.provideService(Scope.Scope, modelOwner))
            const entered = yield* Deferred.make<void>()
            const release = yield* Deferred.make<void>()
            const seen: string[] = []
            const handle = Schema.String.pipe(
              Schema.decodeTo(Schema.String, {
                decode: SchemaGetter.transformEffect((value) =>
                  Effect.gen(function* () {
                    decodedAcquisitions++
                    assert.strictEqual(yield* Config, 'saved-request')
                    assert.isTrue((yield* Client).alive())
                    yield* Effect.acquireRelease(Effect.void, () =>
                      Effect.sync(() => {
                        decodedFinalizers++
                      }),
                    )
                    return value
                  }),
                ),
                encode: SchemaGetter.transform((value) => value),
              }),
            )
            const captured = Context.make(Config, 'construction').pipe(Context.add(Client, client))
            const adapter = yield* Model.makeDeferred<typeof handle, Config | Client | Scope.Scope>(
              {
                handle,
                ...(narrow
                  ? {
                      capture: Scope.Scope.pipe(
                        Effect.map((scope) => Context.add(captured, Scope.Scope, scope)),
                      ),
                    }
                  : {}),
                start: () => pending,
                poll: () => Effect.die('not due'),
                cancel: () =>
                  Effect.gen(function* () {
                    seen.push(yield* Config)
                    assert.strictEqual(yield* Client, client)
                    yield* Effect.acquireRelease(Effect.void, () =>
                      Effect.sync(() => {
                        callbackFinalizers++
                      }),
                    )
                    yield* Deferred.succeed(entered, undefined)
                    if (uninterruptible) yield* Deferred.await(release).pipe(Effect.uninterruptible)
                    assert.isTrue((yield* Client).alive())
                  }),
              },
            ).pipe(Effect.provideContext(captured), Effect.provideService(Scope.Scope, modelOwner))
            const configurationSchema = Schema.String.pipe(
              Schema.decodeTo(Schema.String, {
                decode: SchemaGetter.transformEffect((value) =>
                  Effect.gen(function* () {
                    configurationDecodes++
                    yield* Effect.acquireRelease(Effect.void, () =>
                      Effect.sync(() => {
                        configurationFinalizers++
                      }),
                    )
                    return value
                  }),
                ),
                encode: SchemaGetter.transform((value) => value),
              }),
            )
            const configuration = yield* Model.makeConfiguration({
              id: 'saved',
              version: 1,
              schema: configurationSchema,
              tags: [Config],
              capture: (ctx) => Effect.succeed(Context.getUnsafe(ctx, Config)),
              restore: (value) =>
                Effect.gen(function* () {
                  restoreAcquisitions++
                  yield* Effect.acquireRelease(Effect.void, () =>
                    Effect.sync(() => {
                      restoreFinalizers++
                    }),
                  )
                  return Context.make(Config, value)
                }),
            }).pipe(Effect.provideService(Scope.Scope, modelOwner))
            const runtimeOwner = yield* Scope.make()
            const runtime = yield* HarnessRuntime.make({
              models: [
                Model.make({
                  definition,
                  options: Schema.JsonObject,
                  languageModel: yield* native,
                  deferred: adapter,
                  configuration,
                  configure: () => Effect.succeed(Context.make(Config, 'saved-request')),
                }),
              ],
            }).pipe(
              Effect.provideService(Scope.Scope, runtimeOwner),
              Effect.provideService(
                Settings.Settings,
                yield* Settings.make({ stream: { deferred: true } }),
              ),
            )
            const job = yield* submit(runtime)
            const beforeRestoreAcquisitions = restoreAcquisitions
            const beforeConfigurationDecodes = configurationDecodes
            const abort = yield* Effect.forkChild(
              runtime.backend
                .abort(root)
                .pipe(
                  Effect.provideService(Config, 'ambient'),
                  Effect.provideService(Client, { alive: () => false }),
                ),
            )
            yield* Deferred.await(entered)
            if (uninterruptible) yield* TestClock.adjust(10000)
            yield* Fiber.join(abort)
            const report = yield* diagnostic(runtime)
            const status = (yield* runtime.backend.wait(job.id)).status
            const closing = yield* Effect.forkChild(Scope.close(runtimeOwner, Exit.void))
            yield* Effect.yieldNow
            const early = {
              closing: closing.pollUnsafe(),
              callbackFinalizers,
              decodedFinalizers,
              restoreFinalizers,
              configurationFinalizers,
            }
            if (uninterruptible) yield* Deferred.succeed(release, undefined)
            yield* Fiber.join(closing)
            assert.deepEqual(seen, ['saved-request'])
            assert.strictEqual(
              report.cancellation.status,
              uninterruptible ? 'timeout' : 'completed',
            )
            assert.strictEqual(status, 'unanswered')
            if (uninterruptible) {
              assert.isUndefined(early.closing)
              assert.strictEqual(early.callbackFinalizers, 0)
              assert.strictEqual(early.decodedFinalizers, decodedAcquisitions - 1)
              assert.strictEqual(early.restoreFinalizers, beforeRestoreAcquisitions)
              assert.strictEqual(early.configurationFinalizers, beforeConfigurationDecodes)
            }
            assert.strictEqual(callbackFinalizers, 1)
            assert.strictEqual(decodedFinalizers, decodedAcquisitions)
            assert.strictEqual(restoreFinalizers, restoreAcquisitions)
            assert.strictEqual(configurationFinalizers, configurationDecodes)
            assert.isTrue(alive)
            yield* Scope.close(modelOwner, Exit.void)
            assert.isFalse(alive)
          }),
        ).pipe(Effect.provide(TemporaryStorage.layer)),
    )

it.effect(
  'direct cancel keeps construction defaults without an explicit saved configuration and gives fresh scope priority',
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const owner = yield* Scope.make()
        const operation = yield* Scope.make()
        let finalized = 0
        const seen: string[] = []
        const adapter = yield* Model.makeDeferred<Schema.String, Config | Scope.Scope>({
          handle: Schema.String,
          start: () => pending,
          poll: () => Effect.die('unused'),
          cancel: () =>
            Effect.gen(function* () {
              seen.push(yield* Config)
              yield* Effect.acquireRelease(Effect.void, () =>
                Effect.sync(() => {
                  finalized++
                }),
              )
            }),
        }).pipe(
          Effect.provideService(Config, 'construction'),
          Effect.provideService(Scope.Scope, owner),
        )
        yield* adapter
          .cancel('pinned')
          .pipe(
            Effect.provideService(Config, 'ambient'),
            Effect.provideService(Scope.Scope, operation),
          )
        yield* adapter
          .cancel('pinned', Context.make(Config, 'saved'))
          .pipe(
            Effect.provideService(Config, 'ambient'),
            Effect.provideService(Scope.Scope, operation),
          )
        assert.deepEqual(seen, ['construction', 'saved'])
        assert.strictEqual(finalized, 0)
        yield* Scope.close(operation, Exit.void)
        assert.strictEqual(finalized, 2)
        yield* Scope.close(owner, Exit.void)
        assert.strictEqual(finalized, 2)
      }),
    ),
)

for (const uninterruptible of [false, true])
  it.effect(
    `configuration restore callback and decoder acquisitions belong to current scope; uninterruptible=${uninterruptible}`,
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const construction = yield* Scope.make()
          const operation = yield* Scope.make()
          let callbackFinalized = 0,
            decoderFinalized = 0
          const entered = yield* Deferred.make<void>()
          const release = yield* Deferred.make<void>()
          const codec = Schema.String.pipe(
            Schema.decodeTo(Schema.String, {
              decode: SchemaGetter.transformEffect((value) =>
                Effect.gen(function* () {
                  assert.strictEqual(yield* Config, 'construction')
                  yield* Effect.acquireRelease(Effect.void, () =>
                    Effect.sync(() => {
                      decoderFinalized++
                    }),
                  )
                  return value
                }),
              ),
              encode: SchemaGetter.transform((value) => value),
            }),
          )
          const configuration = yield* Model.makeConfiguration({
            id: 'scoped-restore',
            version: 1,
            schema: codec,
            tags: [Config],
            capture: (ctx) => Effect.succeed(Context.getUnsafe(ctx, Config)),
            restore: (value) =>
              Effect.gen(function* () {
                assert.strictEqual(yield* Config, 'construction')
                yield* Effect.acquireRelease(Effect.void, () =>
                  Effect.sync(() => {
                    callbackFinalized++
                  }),
                )
                yield* Deferred.succeed(entered, undefined)
                if (uninterruptible) yield* Deferred.await(release).pipe(Effect.uninterruptible)
                return Context.make(Config, value)
              }),
          }).pipe(
            Effect.provideService(Config, 'construction'),
            Effect.provideService(Scope.Scope, construction),
          )
          const restoring = yield* Effect.forkChild(
            configuration
              .restore('saved')
              .pipe(
                Effect.provideService(Config, 'ambient'),
                Effect.provideService(Scope.Scope, operation),
              ),
          )
          yield* Deferred.await(entered)
          if (uninterruptible) {
            yield* Effect.yieldNow
            assert.strictEqual(callbackFinalized, 0)
            assert.strictEqual(decoderFinalized, 0)
            yield* Deferred.succeed(release, undefined)
            yield* Fiber.join(restoring)
            yield* Scope.close(operation, Exit.void)
          } else {
            assert.strictEqual(Context.getUnsafe(yield* Fiber.join(restoring), Config), 'saved')
            yield* Scope.close(operation, Exit.void)
          }
          assert.strictEqual(callbackFinalized, 1)
          assert.strictEqual(decoderFinalized, 1)
          yield* Scope.close(construction, Exit.void)
          assert.strictEqual(callbackFinalized, 1)
          assert.strictEqual(decoderFinalized, 1)
        }),
      ),
  )
