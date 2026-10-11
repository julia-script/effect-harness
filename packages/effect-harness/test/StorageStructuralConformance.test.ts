import { assert, describe, it } from '@effect/vitest'
import { NodeServices } from '@effect/platform-node'
import * as SqliteClient from '@effect/sql-sqlite-node/SqliteClient'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Path from 'effect/Path'
import * as Result from 'effect/Result'
import * as Stream from 'effect/Stream'
import * as Identity from 'effect-harness/Identity'
import * as Storage from 'effect-harness/Storage'
import * as Testing from 'effect-harness/Testing'

const names = [
  'storage: prototype-sensitive JSON keys',
  'storage: lossless indexed string identities',
  'storage: out-of-ID-order entry streams',
  'storage: multigeneration fork cutoffs',
  'storage: waiting and completing task records',
  'storage: passive write submission records',
  'storage: long document replacement tails',
  'storage: safe-integer allocator exhaustion',
]
const memory = Effect.succeed({
  open: Layer.build(Storage.layerMemory).pipe(Effect.map((c) => Context.get(c, Storage.Storage))),
})
const persistent = (backend: 'sqlite' | 'jsonl', directories: Array<string>) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem
    const path = yield* Path.Path
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: 'storage-structural-' })
    directories.push(directory)
    const filename = path.join(directory, 'storage')
    const layer =
      backend === 'sqlite'
        ? Storage.layerSql.pipe(Layer.provide(SqliteClient.layer({ filename })))
        : Storage.layerJsonl({ filePath: filename }).pipe(
            Layer.provide(Layer.succeed(FileSystem.FileSystem, fs)),
          )
    return { open: Layer.build(layer).pipe(Effect.map((c) => Context.get(c, Storage.Storage))) }
  })
for (const backend of ['memory', 'sqlite', 'jsonl'] as const)
  describe(`public structural storage: ${backend}`, () => {
    const directories: Array<string> = []
    const make = backend === 'memory' ? memory : persistent(backend, directories)
    const cases = Testing.storageConformance({
      make,
      capabilities: { history: true, reopen: backend !== 'memory' },
    }).filter((c) => names.includes(c.name))
    it.effect('exposes every structural case exactly once', () =>
      Effect.sync(() => {
        assert.deepEqual(cases.map((c) => c.name).toSorted(), names.toSorted())
      }),
    )
    for (const mode of ['reuse-current', 'reuse-current-row', 'fresh-copy'] as const)
      it.live(`long-tail current reads: ${mode}`, () =>
        Effect.gen(function* () {
          let nativeRevision: unknown
          let returnedRevision: unknown
          let currentReads = 0
          const selected = Testing.storageConformance({
            make: make.pipe(
              Effect.map((fixture) => ({
                open: fixture.open.pipe(
                  Effect.map((storage) => {
                    let reused: Storage.StoredDocument | undefined
                    return {
                      ...storage,
                      document: (id, at) =>
                        storage.document(id, at).pipe(
                          Effect.map((value) => {
                            if (at !== undefined || Option.isNone(value)) return value
                            currentReads++
                            nativeRevision = value.value.value.revision
                            reused ??= value.value
                            const returned =
                              mode === 'fresh-copy' ? structuredClone(value.value) : reused
                            if (mode === 'reuse-current-row' && currentReads === 2) {
                              Reflect.set(returned.value, 'revision', 124)
                              const rows = returned.value.rows
                              assert.isTrue(Array.isArray(rows))
                              if (Array.isArray(rows)) {
                                const row: unknown = rows[0]
                                assert.isTrue(typeof row === 'object' && row !== null)
                                if (typeof row === 'object' && row !== null)
                                  Reflect.set(row, 'value', -9999)
                              }
                            }
                            returnedRevision = returned.value.revision
                            return Option.some(returned)
                          }),
                        ),
                    }
                  }),
                ),
              })),
            ),
            capabilities: { history: true },
          }).find((c) => c.name === names[6])
          assert.isDefined(selected)
          const result = yield* Effect.result(selected!.run)
          assert.strictEqual(currentReads, 2)
          assert.strictEqual(nativeRevision, 124)
          if (mode === 'fresh-copy') {
            assert.isTrue(Result.isSuccess(result))
            assert.strictEqual(returnedRevision, 124)
          } else {
            assert.strictEqual(returnedRevision, mode === 'reuse-current' ? -1 : 124)
            assert.isTrue(Result.isFailure(result), 'must reject the mutated reused read')
            if (Result.isFailure(result)) {
              assert.instanceOf(result.failure, Testing.StorageConformanceError)
              if (result.failure instanceof Testing.StorageConformanceError) {
                assert.strictEqual(result.failure.caseName, names[6])
                assert.strictEqual(
                  result.failure.assertion,
                  'long tail returned values remain detached',
                )
              }
            }
          }
          const fs = yield* FileSystem.FileSystem
          for (const directory of directories) assert.isFalse(yield* fs.exists(directory))
        }).pipe(Effect.provide(NodeServices.layer)),
      )
    for (const mode of [
      'input',
      'state',
      'memos',
      'checkpoint',
      'on',
      'memo-nested',
      'fresh-copy',
    ] as const)
      it.live(`waiting submitted aliases: ${mode}`, () =>
        Effect.gen(function* () {
          let nativePolicy: unknown
          let nativeMemo: unknown
          const selected = Testing.storageConformance({
            make: make.pipe(
              Effect.map((fixture) => ({
                open: fixture.open.pipe(
                  Effect.map((storage) => {
                    let captured: Storage.Task | undefined
                    return {
                      ...storage,
                      commit: (writes) => {
                        const batch = [...writes]
                        for (const write of batch)
                          if (write._tag === 'task' && write.value.id === 3) captured = write.value
                        return storage.commit(batch)
                      },
                      task: (id) =>
                        storage.task(id).pipe(
                          Effect.map((value) => {
                            if (
                              id !== 3 ||
                              Option.isNone(value) ||
                              value.value.state.status !== 'waiting'
                            )
                              return value
                            assert.isDefined(captured)
                            nativePolicy = value.value.state.policy
                            nativeMemo = value.value.memos?.saved
                            if (mode === 'fresh-copy')
                              return Option.some(structuredClone(captured!))
                            if (mode === 'input') return Option.some(captured!)
                            if (mode === 'checkpoint' || mode === 'on')
                              return Option.some({
                                ...value.value,
                                state: { ...value.value.state, [mode]: captured!.state[mode] },
                              })
                            if (mode === 'memo-nested')
                              return Option.some({
                                ...value.value,
                                memos: {
                                  ...value.value.memos,
                                  ['__proto__']: captured!.memos!.__proto__!,
                                },
                              })
                            return Option.some({ ...value.value, [mode]: captured![mode] })
                          }),
                        ),
                    }
                  }),
                ),
              })),
            ),
            capabilities: { history: true },
          }).find((c) => c.name === names[4])!
          const result = yield* Effect.result(selected.run)
          assert.strictEqual(nativePolicy, 'allSettled')
          assert.strictEqual(nativeMemo, 1)
          if (mode === 'fresh-copy') assert.isTrue(Result.isSuccess(result))
          else {
            assert.isTrue(Result.isFailure(result), 'submitted aliases must be rejected')
            if (Result.isFailure(result)) {
              assert.instanceOf(result.failure, Testing.StorageConformanceError)
              if (result.failure instanceof Testing.StorageConformanceError)
                assert.strictEqual(
                  result.failure.assertion,
                  'waiting reads detach wait state and memo values',
                )
            }
          }
          const fs = yield* FileSystem.FileSystem
          for (const directory of directories) assert.isFalse(yield* fs.exists(directory))
        }).pipe(Effect.provide(NodeServices.layer)),
      )
    for (const c of cases)
      it.live(c.name, () =>
        c.run.pipe(
          Effect.andThen(
            Effect.gen(function* () {
              const fs = yield* FileSystem.FileSystem
              for (const directory of directories)
                assert.isFalse(
                  yield* fs.exists(directory),
                  'factory scope removes temporary storage',
                )
            }),
          ),
          Effect.provide(NodeServices.layer),
        ),
      )
  })

const broken = (
  name: string,
  assertion: string,
  wrap: (s: Testing.StorageService) => Testing.StorageService,
) =>
  Effect.gen(function* () {
    const cases = Testing.storageConformance({
      make: memory.pipe(Effect.map((f) => ({ open: f.open.pipe(Effect.map(wrap)) }))),
      capabilities: { history: true },
    })
    const selected = cases.find((c) => c.name === name)
    assert.isDefined(selected)
    const failure = yield* selected!.run.pipe(Effect.flip)
    assert.instanceOf(failure, Testing.StorageConformanceError)
    if (failure instanceof Testing.StorageConformanceError) {
      assert.strictEqual(failure.caseName, name)
      assert.strictEqual(failure.assertion, assertion)
    }
  })
it.effect('rejects an adapter dropping prototype-sensitive own keys', () =>
  broken(names[0]!, 'prototype key remains an own property', (s) => ({
    ...s,
    entry: (id, options) =>
      s.entry(id, options).pipe(
        Effect.map(
          Option.map((stored) => {
            if (typeof stored.entry.data === 'object' && stored.entry.data !== null)
              Reflect.deleteProperty(stored.entry.data, '__proto__')
            return stored
          }),
        ),
      ),
  })),
)
it.effect('rejects an adapter collapsing surrogate task indexes', () =>
  broken(names[1]!, 'task kind lookup keeps distinct surrogate identities', (s) => ({
    ...s,
    scanTasks: (query) =>
      s.scanTasks(query === undefined ? undefined : { ...query, kind: '\ud800' }),
  })),
)
it.effect('rejects an adapter collapsing surrogate request lookup', () =>
  broken(names[1]!, 'request lookup keeps distinct surrogate identities', (s) => ({
    ...s,
    submissionByRequest: (conversationId) =>
      s.submissionByRequest(conversationId, Identity.RequestId.make('\ud800')),
  })),
)
it.effect('rejects an adapter truncating a stream at one page', () =>
  broken(names[2]!, 'shuffled entries stream completely in ascending ID order', (s) => ({
    ...s,
    scanEntries: (q) => s.scanEntries(q).pipe(Stream.take(64)),
  })),
)
it.effect('rejects an adapter reversing requested ID order', () =>
  broken(names[2]!, 'shuffled entries stream completely in ascending ID order', (s) => ({
    ...s,
    scanEntries: (q) =>
      s.scanEntries({ ...q, order: q.order === 'ascending' ? 'descending' : 'ascending' }),
  })),
)
it.effect('rejects an adapter losing deeper ancestors', () =>
  broken(names[3]!, 'every ancestor cutoff excludes siblings and later parent writes', (s) => ({
    ...s,
    scanEntries: (q) =>
      s
        .scanEntries(q)
        .pipe(Stream.filter((e) => e.conversationId !== Storage.ROOT_CONVERSATION_ID)),
  })),
)
it.effect('rejects stale completing task status indexes', () => {
  let stale: Storage.Task | undefined
  return broken(names[4]!, 'task replacement removes stale status index', (s) => ({
    ...s,
    scanTasks: (q) => {
      if (q?.status !== 'completing') return s.scanTasks(q)
      return Stream.unwrap(
        Stream.runCollect(s.scanTasks(q)).pipe(
          Effect.map((records) => {
            stale ??= records[0]
            return Stream.fromIterable(
              records.length === 0 && stale !== undefined ? [stale] : records,
            )
          }),
        ),
      )
    },
  }))
})
it.effect('rejects passive receipts losing their exact tagged record', () =>
  broken(names[5]!, 'passive done has entry without input answer', (s) => ({
    ...s,
    submission: () => Effect.succeedNone,
  })),
)
it.effect('rejects a document adapter returning current content for every historical cut', () =>
  broken(names[6]!, 'long tail preserves exact historical cuts around replacement', (s) => ({
    ...s,
    document: (id) => s.document(id),
  })),
)
it.effect(
  'disabled history omits long-tail checks and core cases never read historical points',
  () =>
    Effect.gen(function* () {
      let historicalReads = 0
      const cases = Testing.storageConformance({
        make: memory.pipe(
          Effect.map((fixture) => ({
            open: fixture.open.pipe(
              Effect.map((storage) => ({
                ...storage,
                document: (id, at) => {
                  if (at !== undefined && at !== 'current') historicalReads++
                  return storage.document(id, at)
                },
              })),
            ),
          })),
        ),
        capabilities: { history: false },
      }).filter((c) => names.includes(c.name))
      assert.isFalse(cases.some((c) => c.name === names[6]))
      for (const c of cases) yield* c.run
      assert.strictEqual(historicalReads, 0)
    }),
)

for (const mode of ['input', 'nested-read', 'fresh-copy'] as const) {
  it.effect(`completing outcome aliases: ${mode}`, () => {
    let captured: Storage.Task | undefined
    let reused: Storage.Task | undefined
    const wrap = (s: Testing.StorageService): Testing.StorageService => ({
      ...s,
      commit: (writes) => {
        const batch = [...writes]
        for (const write of batch)
          if (write._tag === 'task' && write.value.id === 4) captured = write.value
        return s.commit(batch)
      },
      task: (id) =>
        s.task(id).pipe(
          Effect.map(
            Option.map((value) => {
              if (id !== 4 || value.state.status !== 'completing') return value
              if (mode === 'input') return captured!
              if (mode === 'fresh-copy') return structuredClone(value)
              reused ??= value
              return { ...value, state: reused.state }
            }),
          ),
        ),
    })
    if (mode === 'fresh-copy') {
      const c = Testing.storageConformance({
        make: memory.pipe(Effect.map((f) => ({ open: f.open.pipe(Effect.map(wrap)) }))),
      }).find((c) => c.name === names[4])!
      return c.run
    }
    return broken(names[4]!, 'completing reads detach nested outcome values', wrap)
  })
  it.effect(`passive detail aliases: ${mode}`, () => {
    let captured: Storage.Submission | undefined
    let reused: Storage.Submission | undefined
    const wrap = (s: Testing.StorageService): Testing.StorageService => ({
      ...s,
      commit: (writes) => {
        const batch = [...writes]
        for (const write of batch)
          if (write._tag === 'submission' && write.value.status === 'unanswered')
            captured = write.value
        return s.commit(batch)
      },
      submission: (id) =>
        s.submission(id).pipe(
          Effect.map(
            Option.map((value) => {
              if (value.status !== 'unanswered') return value
              if (mode === 'input') return captured!
              if (mode === 'fresh-copy') return structuredClone(value)
              reused ??= value
              return reused
            }),
          ),
        ),
    })
    if (mode === 'fresh-copy') {
      const c = Testing.storageConformance({
        make: memory.pipe(Effect.map((f) => ({ open: f.open.pipe(Effect.map(wrap)) }))),
      }).find((c) => c.name === names[5])!
      return c.run
    }
    return broken(names[5]!, 'passive reads detach nested detail values', wrap)
  })
}
it.effect('rejects reused mutable historical rows after a matching cut', () => {
  const saved = new Map<number, Storage.StoredDocument>()
  return broken(names[6]!, 'long historical returned values remain detached', (s) => ({
    ...s,
    document: (id, at) =>
      s.document(id, at).pipe(
        Effect.map((value) => {
          if (typeof at !== 'number' || Option.isNone(value)) return value
          const prior = saved.get(at)
          if (prior !== undefined) return Option.some(prior)
          saved.set(at, value.value)
          return value
        }),
      ),
  }))
})
it.effect('rejects submitted nested passive detail changed after native commit', () => {
  let captured: Storage.Submission | undefined
  return broken(names[5]!, 'passive unanswered has detail without placement', (s) => ({
    ...s,
    commit: (writes) => {
      const batch = [...writes]
      for (const write of batch)
        if (write._tag === 'submission' && write.value.status === 'unanswered')
          captured = write.value
      return s.commit(batch).pipe(
        Effect.tap(() =>
          Effect.sync(() => {
            if (
              captured?.status === 'unanswered' &&
              typeof captured.detail === 'object' &&
              captured.detail !== null
            )
              Reflect.set(captured.detail, 'retryable', true)
          }),
        ),
      )
    },
    submission: (id) =>
      s
        .submission(id)
        .pipe(
          Effect.map(Option.map((value) => (value.status === 'unanswered' ? captured! : value))),
        ),
    submissionByRequest: (id, request) =>
      s
        .submissionByRequest(id, request)
        .pipe(
          Effect.map(Option.map((value) => (value.status === 'unanswered' ? captured! : value))),
        ),
    scanSubmissions: (query) =>
      s
        .scanSubmissions(query)
        .pipe(Stream.map((value) => (value.status === 'unanswered' ? captured! : value))),
  }))
})

it.effect('rejects submitted completing nested outcome changed during commit', () => {
  let captured: Storage.Task | undefined
  return broken(
    names[4]!,
    'completing record retains outcome without checkpoint or memos',
    (s) => ({
      ...s,
      commit: (writes) => {
        const batch = [...writes]
        for (const write of batch) {
          if (write._tag !== 'task' || write.value.state.status !== 'completing') continue
          captured = write.value
          const outcome = captured.state.outcome
          if (typeof outcome === 'object' && outcome !== null && 'error' in outcome) {
            const error = outcome.error
            if (typeof error === 'object' && error !== null)
              Reflect.set(error, 'message', 'commit mutation')
          }
        }
        return s.commit(batch)
      },
      task: (id) =>
        s
          .task(id)
          .pipe(
            Effect.map(
              Option.map((value) => (value.state.status === 'completing' ? captured! : value)),
            ),
          ),
    }),
  )
})
it.effect('rejects submitted passive done placement changed during commit', () =>
  broken(names[5]!, 'passive done has entry without input answer', (s) => ({
    ...s,
    commit: (writes) => {
      const batch = [...writes]
      for (const write of batch)
        if (write._tag === 'submission' && write.value.status === 'done')
          Reflect.set(write.value, 'entry', Storage.EntryId.make(99))
      return s.commit(batch)
    },
  })),
)
it.effect('rejects submitted initial historical JSON changed during commit', () =>
  broken(names[6]!, 'long tail preserves exact historical cuts around replacement', (s) => ({
    ...s,
    commit: (writes) => {
      const batch = [...writes]
      for (const write of batch) {
        if (write._tag !== 'document.create' || write.content._tag !== 'base') continue
        const rows = write.content.value.rows
        if (Array.isArray(rows)) {
          const row: unknown = rows[0]
          if (typeof row === 'object' && row !== null) Reflect.set(row, 'stable', 'commit mutation')
        }
      }
      return s.commit(batch)
    },
  })),
)

for (const mode of ['success', 'wrong-error'] as const)
  it.effect(`rejects exhausted allocator: ${mode}`, () =>
    broken(
      'storage: safe-integer allocator exhaustion',
      'exhausted allocator rejects repeated mint attempts',
      (s) => ({
        ...s,
        mintId:
          mode === 'success'
            ? () => Effect.succeed(2 as never)
            : () =>
                Effect.fail(
                  new Storage.StorageError({
                    reason: 'conflict',
                    operation: 'mintId',
                    message: 'wrong exhaustion error',
                  }),
                ),
      }),
    ),
  )

it.live('reopen checks reject a cold surrogate index collapse', () =>
  Effect.gen(function* () {
    let opens = 0
    const cases = Testing.storageConformance({
      make: persistent('jsonl', []).pipe(
        Effect.map((fixture) => ({
          open: fixture.open.pipe(
            Effect.map((storage) => {
              opens++
              return opens === 1
                ? storage
                : {
                    ...storage,
                    scanTasks: (query) => storage.scanTasks({ ...query, kind: '\ud800' }),
                  }
            }),
          ),
        })),
      ),
      capabilities: { reopen: true },
    })
    const result = yield* cases.find((c) => c.name === names[1])!.run.pipe(Effect.flip)
    assert.instanceOf(result, Testing.StorageConformanceError)
    if (result instanceof Testing.StorageConformanceError)
      assert.strictEqual(result.assertion, 'reopen keeps surrogate task index')
    assert.strictEqual(opens, 2)
  }).pipe(Effect.provide(NodeServices.layer)),
)
