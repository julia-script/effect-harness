import { assert, describe, it } from '@effect/vitest'
import { NodeServices } from '@effect/platform-node'
import { SqliteClient } from '@effect/sql-sqlite-node'
import * as Context from 'effect/Context'
import * as Cause from 'effect/Cause'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Scope from 'effect/Scope'
import * as Stream from 'effect/Stream'
import * as LanguageModel from 'effect/ai/LanguageModel'
import * as Model from 'effect-harness/Model'
import { ModelError, ModelUnsupportedError } from 'effect-harness/ModelError'
import * as CompactionStatus from 'effect-harness/CompactionStatus'
import * as Extension from 'effect-harness/Extension'
import * as HarnessRuntime from 'effect-harness/HarnessRuntime'
import * as Hook from 'effect-harness/Hook'
import * as Record from 'effect-harness/Record'
import * as RunControl from 'effect-harness/RunControl'
import * as Session from 'effect-harness/Session'
import * as Settings from 'effect-harness/Settings'
import * as Storage from 'effect-harness/Storage'
import { StorageError } from 'effect-harness/StorageError'
import * as StorageFormat from 'effect-harness/StorageFormat'
import * as Transaction from 'effect-harness/Transaction'
import * as Usage from 'effect-harness/Usage'

const root = Record.ROOT_CONVERSATION_ID
const fixture = Effect.gen(function* () {
  const directory = yield* (yield* FileSystem.FileSystem).makeTempDirectoryScoped()
  const memory = yield* Layer.build(Storage.layerMemory)
  const sql = Storage.layerSql.pipe(
    Layer.provide(SqliteClient.layer({ filename: `${directory}/origin.sqlite` })),
  )
  const jsonl = Storage.layerJsonl({ filePath: `${directory}/origin.jsonl` })
  return {
    open: (backend: 'memory' | 'sqlite' | 'jsonl') =>
      backend === 'memory'
        ? Effect.succeed(memory)
        : Layer.build(Layer.fresh(backend === 'sqlite' ? sql : jsonl)),
  }
})
const make = Effect.fnUntraced(function* (
  storage: Context.Context<Storage.Storage | StorageFormat.StorageFormat>,
  execute: Hook.Hook<'beforeCompact'>['execute'],
  nativeCompaction?: Model.CompactionCapability,
) {
  const settings = yield* Settings.make({
    compaction: { enabled: false, reserveTokens: 20, keepRecentTokens: 10, backgroundTokens: 0 },
  })
  const native = yield* LanguageModel.make({
    generateText: () => Effect.die('Origin fixtures must not invoke a provider'),
    streamText: () => Stream.die('Origin fixtures must not generate'),
  })
  return yield* HarnessRuntime.make({
    ...(nativeCompaction === undefined ? {} : { nativeCompaction }),
    extensions: [
      Extension.make({
        name: 'origin-regression',
        hooks: [Hook.make({ event: 'beforeCompact', execute })],
      }),
    ],
  }).pipe(
    Effect.provideContext(storage),
    Effect.provideService(Settings.Settings, settings),
    Effect.provideService(LanguageModel.LanguageModel, native),
  )
})
const seed = (runtime: HarnessRuntime.HarnessRuntimeService, count = 2) =>
  Session.commit(runtime.session, (tx) =>
    Effect.gen(function* () {
      const entries: Record.Entry[] = []
      for (let index = 0; index < count; index++)
        entries.push(
          yield* Transaction.appendEntry(tx, root, {
            kind: 'input',
            model: [{ role: 'user', content: `entry ${index} `.repeat(100) }],
          }),
        )
      return entries
    }),
  )
const origin = (runtime: HarnessRuntime.HarnessRuntimeService) =>
  Session.snapshot(runtime.session, RunControl.contextProtocol, RunControl.target(root))
const history = (runtime: HarnessRuntime.HarnessRuntimeService) =>
  Session.scanEntries(runtime.session, { conversationId: root, order: 'ascending' }).pipe(
    Stream.runCollect,
  )

for (const backend of ['memory', 'sqlite', 'jsonl'] as const) {
  describe(`${backend} compaction context origin`, () => {
    for (const activated of [false, true]) {
      for (const mutation of [
        'suffix',
        'submissionSuffix',
        'selectedReplace',
        'retainedReplace',
        'omit',
        'head',
      ] as const)
        it.live(`${activated ? 'saved' : 'unsaved'} origin preserves ${mutation} fence`, () =>
          Effect.scoped(
            Effect.gen(function* () {
              const store = yield* fixture
              const storage = yield* store.open(backend)
              const both = yield* Deferred.make<void>()
              const release = yield* Deferred.make<void>()
              let hooks = 0
              const runtime = yield* make(storage, () =>
                Effect.gen(function* () {
                  hooks++
                  if (hooks === 2) yield* Deferred.succeed(both, undefined)
                  yield* Deferred.await(release)
                  return { _tag: 'summary', summary: 'Keep the original conversation.' }
                }),
              )
              yield* runtime.backend.root
              const seeded = yield* seed(runtime)
              assert.isTrue(Option.isNone(yield* origin(runtime)))
              if (activated) {
                const protocol = {
                  version: 1 as const,
                  origin: { conversationId: root, through: seeded[1]!.id },
                  legacyMode: 'preserveManaged' as const,
                }
                yield* Session.commit(runtime.session, (tx) =>
                  Effect.gen(function* () {
                    yield* Transaction.ensureDocument(
                      tx,
                      RunControl.contextProtocol,
                      RunControl.target(root),
                    )
                    yield* Transaction.setDocument(
                      tx,
                      RunControl.contextProtocol,
                      RunControl.target(root),
                      protocol,
                    )
                  }),
                )
              }
              const handles = yield* Effect.all(
                [runtime.backend.compact!(root), runtime.backend.compact!(root)],
                { concurrency: 2 },
              )
              yield* Deferred.await(both).pipe(Effect.timeout('5 seconds'))
              const captured = Option.getOrThrow(yield* origin(runtime)).value
              assert.strictEqual(captured.origin.through, seeded[1]!.id)
              if (mutation === 'submissionSuffix') {
                const submission = yield* runtime.backend.submit({
                  conversationId: root,
                  draft: {
                    type: 'write',
                    entry: {
                      kind: 'custom',
                      model: [{ role: 'user', content: 'Ordinary Inbox suffix' }],
                    },
                  },
                })
                assert.strictEqual((yield* runtime.backend.wait(submission.id)).status, 'done')
              } else
                yield* Session.commit(runtime.session, (tx) => {
                  if (mutation === 'suffix')
                    return Transaction.appendEntry(tx, root, {
                      kind: 'custom',
                      model: [{ role: 'user', content: 'Ordinary safe suffix' }],
                    })
                  if (mutation === 'head')
                    return Transaction.appendEntry(tx, root, {
                      kind: 'custom',
                      head: seeded[0]!.id,
                    })
                  return Transaction.appendEntry(tx, root, {
                    kind: 'edit',
                    edits:
                      mutation === 'omit'
                        ? [{ _tag: 'omit', target: seeded[0]!.id }]
                        : [
                            {
                              _tag: 'replace',
                              target: seeded[mutation === 'selectedReplace' ? 0 : 1]!.id,
                              messages: [{ role: 'user', content: 'Edited original' }],
                            },
                          ],
                  })
                })
              yield* Deferred.succeed(release, undefined)
              const outcomes = yield* Effect.all(
                handles.map((handle) => runtime.backend.waitCompaction!(handle)),
                { concurrency: 2 },
              )
              for (const outcome of outcomes)
                if (outcome.status === 'completed' && outcome.result._tag === 'Queued')
                  yield* runtime.backend.wait(outcome.result.submissionId)
              const entries = yield* history(runtime)
              assert.strictEqual(
                entries.filter((entry) => entry.kind === 'compaction').length,
                mutation === 'suffix' || mutation === 'submissionSuffix' ? 1 : 0,
              )
              assert.strictEqual(hooks, 2)
              if (mutation !== 'suffix' && mutation !== 'submissionSuffix')
                assert.isTrue(outcomes.every((outcome) => outcome.status === 'failed'))
              assert.deepEqual(Option.getOrThrow(yield* origin(runtime)).value, captured)
              assert.deepEqual(
                Option.getOrThrow(
                  yield* Session.snapshot(
                    runtime.session,
                    CompactionStatus.document,
                    CompactionStatus.target(root),
                  ),
                ).value.items,
                [],
              )
              assert.deepEqual(
                yield* Session.commit(runtime.session, (tx) => Usage.receipts(tx, root)),
                [],
              )
            }),
          ).pipe(Effect.provide(NodeServices.layer)),
        )
    }
    for (const mode of ['noop', 'decline', 'invalid', 'noCapability', 'configuration'] as const)
      it.live(`${mode} publishes no compaction or reader upgrade`, () =>
        Effect.scoped(
          Effect.gen(function* () {
            const storage = yield* (yield* fixture).open(backend)
            let hooks = 0
            const capability =
              mode === 'configuration'
                ? yield* Model.makeCompaction({
                    binding: { id: 'origin/configuration', version: 1 },
                    configure: () =>
                      Effect.fail(
                        new ModelError({
                          reason: new ModelUnsupportedError({
                            message: 'Origin configuration refused',
                          }),
                        }),
                      ),
                  })
                : undefined
            const runtime = yield* make(
              storage,
              () => {
                hooks++
                if (mode === 'decline') return Effect.succeed({ _tag: 'decline' })
                if (mode === 'invalid') return Effect.succeed({ _tag: 'summary', summary: ' ' })
                return Effect.void
              },
              capability,
            )
            yield* runtime.backend.root
            const seeded = yield* seed(runtime, mode === 'noop' ? 1 : 2)
            const before = yield* StorageFormat.inspect.pipe(Effect.provideContext(storage))
            const handle = yield* runtime.backend.compact!(root)
            const result = yield* runtime.backend.waitCompaction!(handle)
            assert.strictEqual(
              result.status,
              mode === 'invalid' || mode === 'noCapability' || mode === 'configuration'
                ? 'failed'
                : 'completed',
            )
            assert.strictEqual(hooks, mode === 'noop' ? 0 : 1)
            const saved = yield* origin(runtime)
            if (mode === 'noop') assert.isTrue(Option.isNone(saved))
            else
              assert.strictEqual(Option.getOrThrow(saved).value.origin.through, seeded.at(-1)!.id)
            assert.deepEqual(yield* history(runtime), seeded)
            assert.deepEqual(
              yield* StorageFormat.inspect.pipe(Effect.provideContext(storage)),
              before,
            )
            assert.deepEqual(
              yield* Session.commit(runtime.session, (tx) => Usage.receipts(tx, root)),
              [],
            )
          }),
        ).pipe(Effect.provide(NodeServices.layer)),
      )
    it.live('cold reopen reuses the committed origin before a later ordinary suffix', () =>
      Effect.scoped(
        Effect.gen(function* () {
          const store = yield* fixture
          const storage = yield* store.open(backend)
          const entered = yield* Deferred.make<void>()
          const firstScope = yield* Scope.make()
          yield* Effect.addFinalizer(() => Scope.close(firstScope, Exit.void))
          const first = yield* make(storage, () =>
            Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)),
          ).pipe(Effect.provideService(Scope.Scope, firstScope))
          yield* first.backend.root
          const seeded = yield* seed(first)
          const handle = yield* first.backend.compact!(root)
          yield* Deferred.await(entered).pipe(Effect.timeout('5 seconds'))
          const saved = Option.getOrThrow(yield* origin(first)).value
          assert.strictEqual(saved.origin.through, seeded[1]!.id)
          yield* Scope.close(firstScope, Exit.void)
          const second = yield* make(yield* store.open(backend), () =>
            Effect.succeed({ _tag: 'summary', summary: 'Recovered summary' }),
          )
          assert.deepEqual(Option.getOrThrow(yield* origin(second)).value, saved)
          yield* Session.commit(second.session, (tx) =>
            Transaction.appendEntry(tx, root, {
              kind: 'custom',
              model: [{ role: 'user', content: 'Cold safe suffix' }],
            }),
          )
          const result = yield* second.backend.waitCompaction!(handle)
          assert.strictEqual(result.status, 'completed')
          if (result.status === 'completed' && result.result._tag === 'Queued')
            yield* second.backend.wait(result.result.submissionId)
          assert.deepEqual(Option.getOrThrow(yield* origin(second)).value, saved)
          assert.strictEqual(
            (yield* history(second)).filter((entry) => entry.kind === 'compaction').length,
            1,
          )
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
    )
    for (const mode of ['rejected', 'uncertain'] as const)
      it.live(`${mode} origin capture stays authoritative before hook or provider work`, () =>
        Effect.scoped(
          Effect.gen(function* () {
            const store = yield* fixture
            const storage = yield* store.open(backend)
            const native = Context.get(storage, Storage.Storage)
            const firstScope = yield* Scope.make()
            yield* Effect.addFinalizer(() => Scope.close(firstScope, Exit.void))
            let armed = true
            let captures = 0
            let hooks = 0
            const observed: typeof Storage.Storage.Service = {
              ...native,
              commit: (input) =>
                Effect.gen(function* () {
                  const writes = Array.from(input)
                  if (
                    armed &&
                    writes.some(
                      (write) =>
                        write._tag === 'document.create' &&
                        write.record.kind === 'harness.contextProtocol',
                    )
                  ) {
                    armed = false
                    captures++
                    if (mode === 'uncertain') yield* native.commit(writes)
                    return yield* new StorageError({
                      reason: mode === 'rejected' ? 'conflict' : 'uncertain',
                      operation: 'origin.capture',
                      message: `Origin capture ${mode}`,
                    })
                  }
                  return yield* native.commit(writes)
                }),
            }
            const format = Context.get(storage, StorageFormat.StorageFormat)
            const wrapped = storage.pipe(
              Context.add(Storage.Storage, observed),
              Context.add(StorageFormat.StorageFormat, { ...format, storage: observed }),
            )
            const first = yield* make(wrapped, () => {
              hooks++
              return Effect.succeed({ _tag: 'summary', summary: 'A valid supplied summary' })
            }).pipe(Effect.provideService(Scope.Scope, firstScope))
            yield* first.backend.root
            const seeded = yield* seed(first)
            const before = yield* StorageFormat.inspect.pipe(Effect.provideContext(storage))
            const handle = yield* first.backend.compact!(root)
            const failed = yield* first.backend.waitCompaction!(handle).pipe(Effect.exit)
            assert.isTrue(Exit.isFailure(failed))
            if (Exit.isFailure(failed))
              assert.include(Cause.pretty(failed.cause), `Origin capture ${mode}`)
            assert.strictEqual(captures, 1)
            assert.strictEqual(hooks, 0)
            yield* Scope.close(firstScope, Exit.void)
            const reopened = yield* store.open(backend)
            const inspectScope = yield* Scope.make()
            yield* Effect.addFinalizer(() => Scope.close(inspectScope, Exit.void))
            const session = yield* Session.make().pipe(
              Effect.provideContext(reopened),
              Effect.provideService(Scope.Scope, inspectScope),
            )
            const saved = yield* Session.snapshot(
              session,
              RunControl.contextProtocol,
              RunControl.target(root),
            )
            if (mode === 'rejected') assert.isTrue(Option.isNone(saved))
            else assert.strictEqual(Option.getOrThrow(saved).value.origin.through, seeded[1]!.id)
            assert.deepEqual(
              yield* Session.scanEntries(session, {
                conversationId: root,
                order: 'ascending',
              }).pipe(Stream.runCollect),
              seeded,
            )
            assert.deepEqual(
              yield* StorageFormat.inspect.pipe(Effect.provideContext(reopened)),
              before,
            )
            yield* Session.commit(session, (tx) =>
              Transaction.appendEntry(tx, root, {
                kind: 'custom',
                model: [{ role: 'user', content: 'Suffix after capture acknowledgement' }],
              }),
            )
            yield* Scope.close(inspectScope, Exit.void)
            const second = yield* make(reopened, () => {
              hooks++
              return Effect.succeed({ _tag: 'summary', summary: 'Recovered supplied summary' })
            })
            const result = yield* second.backend.waitCompaction!(handle)
            assert.strictEqual(result.status, 'completed')
            if (result.status === 'completed' && result.result._tag === 'Queued')
              yield* second.backend.wait(result.result.submissionId)
            assert.strictEqual(hooks, 1)
            assert.strictEqual(
              (yield* history(second)).filter((entry) => entry.kind === 'compaction').length,
              1,
            )
            if (mode === 'uncertain')
              assert.deepEqual(
                Option.getOrThrow(yield* origin(second)).value,
                Option.getOrThrow(saved).value,
              )
          }),
        ).pipe(Effect.provide(NodeServices.layer)),
      )
  })
}
