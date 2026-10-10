import { assert, describe, it } from '@effect/vitest'
import { NodeServices } from '@effect/platform-node'
import { SqliteClient } from '@effect/sql-sqlite-node'
import * as Cause from 'effect/Cause'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Scope from 'effect/Scope'
import * as Stream from 'effect/Stream'
import * as AiError from 'effect/ai/AiError'
import * as LanguageModel from 'effect/ai/LanguageModel'
import type * as Response from 'effect/ai/Response'
import * as Extension from 'effect-harness/Extension'
import * as HarnessRuntime from 'effect-harness/HarnessRuntime'
import * as Hook from 'effect-harness/Hook'
import * as Model from 'effect-harness/Model'
import { ModelError, ModelUnsupportedError } from 'effect-harness/ModelError'
import * as Prepared from 'effect-harness/PreparedRequest'
import * as PromptSection from 'effect-harness/PromptSection'
import * as Record from 'effect-harness/Record'
import * as Storage from 'effect-harness/Storage'
import * as StorageFormat from 'effect-harness/StorageFormat'

const root = Record.ROOT_CONVERSATION_ID
class Configuration extends Context.Service<Configuration, { readonly value: string }>()(
  'test/restore/Configuration',
) {}
const answer: Response.PartEncoded[] = [
  { type: 'text', text: 'restored' },
  {
    type: 'finish',
    reason: 'stop',
    usage: {
      inputTokens: { total: 1, uncached: 1, cacheRead: undefined, cacheWrite: undefined },
      outputTokens: { total: 1, text: 1, reasoning: undefined },
    },
    response: undefined,
  },
]
const fixture = Effect.fnUntraced(function* (
  backend: 'memory' | 'sqlite' | 'jsonl',
  mode:
    | 'failure'
    | 'caughtProvider'
    | 'mixed'
    | 'multiple'
    | 'defect'
    | 'interrupt'
    | 'mixedInterrupt' = 'failure',
) {
  const directory = yield* (yield* FileSystem.FileSystem).makeTempDirectoryScoped()
  const layer =
    backend === 'sqlite'
      ? Storage.layerSql.pipe(
          Layer.provide(SqliteClient.layer({ filename: `${directory}/restore.db` })),
        )
      : Storage.layerJsonl({ filePath: `${directory}/restore.jsonl` })
  const store = yield* Layer.build(backend === 'memory' ? Storage.layerMemory : Layer.fresh(layer))
  const storage = Context.get(store, Storage.Storage)
  const open = () =>
    backend === 'memory' ? Effect.succeed(store) : Layer.build(Layer.fresh(layer))
  const counters = { configure: 0, render: 0, hook: 0, restore: 0, provider: 0 }
  const failure = new ModelError({
    reason: new ModelUnsupportedError({ message: 'restore offline' }),
  })
  let recovered = false
  let accepted: Prepared.Snapshot | undefined
  let received: Prepared.Snapshot | undefined
  const tasks = storage
    .scanTasks({ conversationId: root, kind: 'harness.run' })
    .pipe(Stream.runCollect)
  const entries = storage.scanEntries({ conversationId: root }).pipe(Stream.runCollect)
  const configuration = yield* Model.makeConfiguration({
    id: 'test/restore/configuration',
    version: 1,
    schema: Schema.Struct({ value: Schema.String }),
    tags: [Configuration],
    capture: (context) =>
      Effect.succeed(Option.getOrThrow(Context.getOption(context, Configuration))),
    restore: (value) =>
      Effect.gen(function* () {
        counters.restore++
        const task = (yield* tasks).find((task) => task.state.checkpoint !== undefined)!
        accepted ??= yield* Schema.decodeUnknownEffect(
          Schema.Struct({ preparation: Schema.toCodecJson(Prepared.SnapshotSchema) }),
        )(task.state.checkpoint).pipe(Effect.map((wire) => wire.preparation))
        assert.strictEqual(
          (yield* entries).filter((entry) => entry.kind === 'context.change').length,
          1,
        )
        if (recovered) return Context.make(Configuration, value)
        if (mode === 'caughtProvider') {
          yield* Effect.fail(failure).pipe(Effect.ignore)
          return Context.make(Configuration, value)
        }
        if (mode === 'mixed')
          return yield* Effect.failCause(
            Cause.combine(Cause.fail(failure), Cause.die('restore defect')),
          )
        if (mode === 'multiple')
          return yield* Effect.failCause(
            Cause.combine(Cause.fail(failure), Cause.fail('second error')),
          )
        if (mode === 'defect') return yield* Effect.die(failure)
        if (mode === 'interrupt') return yield* Effect.interrupt
        if (mode === 'mixedInterrupt')
          return yield* Effect.failCause(Cause.combine(Cause.fail(failure), Cause.interrupt(1)))
        return yield* failure
      }),
  })
  const native = yield* LanguageModel.make({
    generateText: () =>
      Effect.gen(function* () {
        counters.provider++
        if (mode === 'caughtProvider')
          return yield* new AiError.AiError({
            module: 'test',
            method: 'generateText',
            reason: new AiError.UnknownError({ metadata: { prior: failure } }),
          })
        received = Option.getOrThrow(yield* Effect.serviceOption(Prepared.PreparedRequest)).current
        assert.strictEqual(
          Option.getOrThrow(yield* Effect.serviceOption(Configuration)).value,
          'accepted',
        )
        return answer
      }),
    streamText: () => Stream.empty,
  })
  const model = Model.make({
    definition: {
      ref: { provider: 'offline', modelId: 'restore' },
      capabilities: { tools: false, images: false, reasoning: false, structuredOutput: false },
    },
    options: Schema.Struct({}),
    languageModel: native,
    configuration,
    configure: () =>
      Effect.sync(() => {
        counters.configure++
        return Context.make(Configuration, { value: 'accepted' })
      }),
  })
  const extension = Extension.make({
    name: 'restore',
    sections: [
      PromptSection.make({
        key: 'saved',
        render: () =>
          Effect.sync(() => {
            counters.render++
            return 'accepted section'
          }),
      }),
    ],
    hooks: [
      Hook.make({
        event: 'beforeRequest',
        execute: ({ prompt }) =>
          Effect.sync(() => {
            counters.hook++
            return prompt
          }),
      }),
    ],
  })
  const make = (context: Context.Context<Storage.Storage | StorageFormat.StorageFormat>) =>
    HarnessRuntime.make({ models: [model], extensions: [extension] }).pipe(
      Effect.provideContext(context),
    )
  const scope = yield* Scope.make()
  yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void))
  const first = yield* make(store).pipe(Effect.provideService(Scope.Scope, scope))
  yield* first.backend.root
  const job = yield* first.backend.submit({
    conversationId: root,
    draft: { type: 'input', content: 'input' },
  })
  return {
    first,
    job,
    scope,
    store,
    open,
    make,
    tasks,
    entries,
    submission: storage.submission(job.id),
    counters,
    accepted: () => accepted!,
    received: () => received!,
    recover: () => {
      recovered = true
    },
  }
})

for (const backend of ['memory', 'sqlite', 'jsonl'] as const) {
  describe(`${backend} accepted restore recovery`, () => {
    it.live(
      'retains the exact accepted checkpoint, stops retries, and explicitly recovers cold',
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const f = yield* fixture(backend)
            const first = yield* f.first.backend.wait(f.job.id).pipe(Effect.exit)
            assert.isTrue(
              Exit.isFailure(first),
              'restore failure must be reported, not settled unanswered',
            )
            if (Exit.isFailure(first)) {
              assert.include(Cause.pretty(first.cause), 'restore offline')
            }
            const task = (yield* f.tasks)[0]!
            assert.strictEqual(task.state.status, 'running')
            const wire = yield* Schema.decodeUnknownEffect(
              Schema.Struct({ preparation: Schema.toCodecJson(Prepared.SnapshotSchema) }),
            )(task.state.checkpoint)
            assert.deepStrictEqual(wire.preparation, f.accepted())
            assert.notStrictEqual(Option.getOrThrow(yield* f.submission).status, 'unanswered')
            yield* Effect.sleep('50 millis')
            assert.deepStrictEqual(f.counters, {
              configure: 1,
              render: 1,
              hook: 1,
              restore: 1,
              provider: 0,
            })
            assert.strictEqual(
              (yield* StorageFormat.inspect.pipe(Effect.provideContext(f.store))).formatVersion,
              5,
            )
            yield* Scope.close(f.scope, Exit.void)
            assert.isTrue(Exit.isFailure(yield* f.first.backend.wait(f.job.id).pipe(Effect.exit)))
            f.recover()
            const coldStore = yield* f.open()
            const cold = yield* f.make(coldStore)
            assert.strictEqual((yield* cold.backend.wait(f.job.id)).status, 'done')
            assert.deepStrictEqual(f.received(), f.accepted())
            assert.deepStrictEqual(f.counters, {
              configure: 1,
              render: 1,
              hook: 1,
              restore: 2,
              provider: 1,
            })
            const completed = Option.getOrThrow(
              yield* Context.get(coldStore, Storage.Storage).task(task.id),
            )
            assert.strictEqual(completed.state.status, 'terminal')
            assert.isUndefined(completed.state.checkpoint)
            assert.strictEqual(
              (yield* f.entries).filter((entry) => entry.kind === 'context.change').length,
              1,
            )
          }),
        ).pipe(Effect.provide(NodeServices.layer)),
    )
    it.live(
      'aborts retained work in a fresh scope without restoring or invoking the provider',
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const f = yield* fixture(backend)
            assert.isTrue(Exit.isFailure(yield* f.first.backend.wait(f.job.id).pipe(Effect.exit)))
            yield* Scope.close(f.scope, Exit.void)
            const coldStore = yield* f.open()
            const cold = yield* f.make(coldStore)
            yield* cold.backend.abort(root)
            assert.strictEqual((yield* cold.backend.wait(f.job.id)).status, 'unanswered')
            assert.deepStrictEqual(f.counters, {
              configure: 1,
              render: 1,
              hook: 1,
              restore: 1,
              provider: 0,
            })
            const task = (yield* f.tasks)[0]!
            assert.strictEqual(
              Option.getOrThrow(yield* Context.get(coldStore, Storage.Storage).task(task.id)).state
                .status,
              'terminal',
            )
          }),
        ).pipe(Effect.provide(NodeServices.layer)),
    )
    it.live('permanent failure retries only once per explicit cold recovery', () =>
      Effect.scoped(
        Effect.gen(function* () {
          const f = yield* fixture(backend)
          assert.isTrue(Exit.isFailure(yield* f.first.backend.wait(f.job.id).pipe(Effect.exit)))
          yield* Scope.close(f.scope, Exit.void)
          const failedScope = yield* Scope.make()
          const failed = yield* f
            .make(yield* f.open())
            .pipe(Effect.provideService(Scope.Scope, failedScope))
          assert.isTrue(Exit.isFailure(yield* failed.backend.wait(f.job.id).pipe(Effect.exit)))
          yield* Effect.sleep('50 millis')
          assert.strictEqual(f.counters.restore, 2)
          assert.strictEqual(f.counters.provider, 0)
          yield* Scope.close(failedScope, Exit.void)
          f.recover()
          const recovered = yield* f.make(yield* f.open())
          assert.strictEqual((yield* recovered.backend.wait(f.job.id)).status, 'done')
          assert.deepStrictEqual(f.received(), f.accepted())
          assert.deepStrictEqual(f.counters, {
            configure: 1,
            render: 1,
            hook: 1,
            restore: 3,
            provider: 1,
          })
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
    )
    for (const mode of ['caughtProvider', 'mixed', 'multiple', 'defect'] as const) {
      it.live(`${mode} cannot borrow restore failure retention`, () =>
        Effect.scoped(
          Effect.gen(function* () {
            const f = yield* fixture(backend, mode)
            assert.strictEqual((yield* f.first.backend.wait(f.job.id)).status, 'unanswered')
            assert.strictEqual((yield* f.tasks)[0]!.state.status, 'terminal')
            assert.isUndefined((yield* f.tasks)[0]!.state.checkpoint)
            assert.strictEqual(f.counters.restore, 1)
            assert.strictEqual(f.counters.provider, mode === 'caughtProvider' ? 1 : 0)
          }),
        ).pipe(Effect.provide(NodeServices.layer)),
      )
    }
    for (const mode of ['interrupt', 'mixedInterrupt'] as const) {
      it.live(`${mode} preserves interruption and explicit checkpoint recovery`, () =>
        Effect.scoped(
          Effect.gen(function* () {
            const f = yield* fixture(backend, mode)
            assert.isTrue(Exit.isFailure(yield* f.first.backend.wait(f.job.id).pipe(Effect.exit)))
            const pending = (yield* f.tasks)[0]!
            assert.notStrictEqual(pending.state.status, 'terminal')
            assert.isDefined(pending.state.checkpoint)
            yield* Effect.sleep('50 millis')
            assert.strictEqual(f.counters.restore, 1)
            assert.strictEqual(f.counters.provider, 0)
            yield* Scope.close(f.scope, Exit.void)
            f.recover()
            const cold = yield* f.make(yield* f.open())
            assert.strictEqual((yield* cold.backend.wait(f.job.id)).status, 'done')
            assert.deepStrictEqual(f.received(), f.accepted())
            assert.deepStrictEqual(f.counters, {
              configure: 1,
              render: 1,
              hook: 1,
              restore: 2,
              provider: 1,
            })
          }),
        ).pipe(Effect.provide(NodeServices.layer)),
      )
    }
  })
}
