import * as SqlEventJournal from 'effect/eventlog/SqlEventJournal'
import { assert, describe, it } from '@effect/vitest'
import { NodeServices } from '@effect/platform-node'
import * as SqliteClient from '@effect/sql-sqlite-node/SqliteClient'
import * as Context from 'effect/Context'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Fiber from 'effect/Fiber'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Path from 'effect/Path'
import * as Ref from 'effect/Ref'
import * as Sequence from 'effect-harness/Sequence'
import * as Storage from 'effect-harness/Storage'
import * as Testing from 'effect-harness/Testing'

const memory = Effect.succeed({
  open: Layer.build(Storage.layerMemory).pipe(
    Effect.map((context) => Context.get(context, Storage.Storage)),
  ),
})

// A new temporary store per factory evaluation; fresh backend connections per open.
const persistent = (backend: 'sqlite' | 'jsonl', directories: Array<string>) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem
    const path = yield* Path.Path
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: 'storage-conformance-' })
    directories.push(directory)
    const filename = path.join(directory, 'storage')
    const layer =
      backend === 'sqlite'
        ? Storage.layerJournal().pipe(
            Layer.provide(SqlEventJournal.layer()),
            Layer.provide(SqliteClient.layer({ filename })),
          )
        : Storage.layerJsonl({ filePath: filename }).pipe(
            Layer.provide(Layer.succeed(FileSystem.FileSystem, fs)),
          )
    return {
      open: Layer.build(layer).pipe(Effect.map((context) => Context.get(context, Storage.Storage))),
    }
  })

for (const backend of ['memory', 'sqlite', 'jsonl'] as const)
  describe(`public conformance: ${backend}`, () => {
    const directories: Array<string> = []
    const make = backend === 'memory' ? memory : persistent(backend, directories)
    for (const test of Testing.storageConformance({
      make,
      capabilities: { history: true, reopen: backend !== 'memory' },
    }))
      it.live(test.name, () =>
        test.run.pipe(
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

const brokenFixture = (breakStorage: (storage: Testing.StorageService) => Testing.StorageService) =>
  memory.pipe(Effect.map((fixture) => ({ open: fixture.open.pipe(Effect.map(breakStorage)) })))
const caseNamed = <E, R>(
  cases: ReadonlyArray<Testing.StorageConformanceCase<E, R>>,
  name: string,
) => {
  const test = cases.find((test) => test.name.includes(name))
  assert.isDefined(test)
  return test!
}
const expectAssertion = Effect.fn('testing.expectAssertion')(function* <E, R>(
  test: Testing.StorageConformanceCase<E, R>,
  assertion: string,
) {
  const error = yield* test.run.pipe(Effect.flip)
  assert.instanceOf(error, Testing.StorageConformanceError)
  if (error instanceof Testing.StorageConformanceError) {
    assert.strictEqual(error.caseName, test.name)
    assert.strictEqual(error.assertion, assertion)
  }
})

it.effect('reports a duplicate-ID allocator with a meaningful assertion', () =>
  expectAssertion(
    caseNamed(
      Testing.storageConformance({
        make: brokenFixture((storage) => ({
          ...storage,
          mintId: <I extends Storage.StorageId>() => Effect.succeed(Storage.EntryId.make(2) as I),
        })),
      }),
      'identity allocation',
    ),
    'fresh IDs reserve root and are unique',
  ),
)

it.effect('detects a non-atomic adapter that persists each write before later rejection', () =>
  expectAssertion(
    caseNamed(
      Testing.storageConformance({
        make: brokenFixture((storage) => ({
          ...storage,
          commit: Effect.fn('broken.commit')(function* (writes: Iterable<Storage.StorageWrite>) {
            let seq = 0
            for (const write of writes) seq = yield* storage.commit([write])
            return Sequence.make(seq)
          }),
        })),
      }),
      'mixed batch rollback',
    ),
    'rollback preserves task',
  ),
)

it.effect('detects an adapter that collapses historical document reads to current', () =>
  expectAssertion(
    caseNamed(
      Testing.storageConformance({
        make: brokenFixture((storage) => ({ ...storage, document: (id) => storage.document(id) })),
        capabilities: { history: true },
      }),
      'historical document',
    ),
    'history keeps exact old schema and content',
  ),
)

it.effect('detects loss of backing state during reopen', () =>
  expectAssertion(
    caseNamed(
      Testing.storageConformance({ make: memory, capabilities: { reopen: true } }),
      'reopened state',
    ),
    'reopen restores conversation',
  ),
)

it.effect('detects incorrect owner-edge filtering', () =>
  expectAssertion(
    caseNamed(
      Testing.storageConformance({
        make: brokenFixture((storage) => ({
          ...storage,
          scanConversations: () => storage.scanConversations(),
        })),
      }),
      'ownership records',
    ),
    'owner edge filters',
  ),
)

it.effect('detects an ambient hook that fails to enter a transaction', () =>
  expectAssertion(
    caseNamed(
      Testing.storageConformance({
        make: memory.pipe(
          Effect.map((fixture) => ({
            ...fixture,
            withAmbientTransaction: <A, E, R>(effect: Effect.Effect<A, E, R>) => effect,
          })),
        ),
        capabilities: { sqlAmbient: true },
      }),
      'SQL ambient',
    ),
    'ambient initialization rejects',
  ),
)

it.effect('detects an uncertainty fixture that leaves a healthy service accessible', () =>
  expectAssertion(
    caseNamed(
      Testing.storageConformance({
        make: memory.pipe(
          Effect.map((fixture) => ({
            ...fixture,
            uncertainCommit: () =>
              Effect.fail(
                new Storage.StorageError({
                  reason: 'uncertain',
                  operation: 'commit',
                  message: 'injected broken guarantee',
                }),
              ),
            recoverTransaction: Effect.void,
          })),
        ),
        capabilities: { sqlUncertain: true },
      }),
      'SQL uncertain',
    ),
    'uncertain service rejects commit',
  ),
)

for (const capability of ['sqlAmbient', 'sqlUncertain'] as const)
  it.effect(`enabled ${capability} fails explicitly without fixture hooks`, () => {
    const cases = Testing.storageConformance({ make: memory, capabilities: { [capability]: true } })
    return expectAssertion(
      caseNamed(cases, capability === 'sqlAmbient' ? 'SQL ambient' : 'SQL uncertain'),
      capability === 'sqlAmbient'
        ? 'sqlAmbient requires withAmbientTransaction'
        : 'sqlUncertain requires uncertainCommit and recoverTransaction',
    )
  })

it.effect('omits disabled capabilities and runs core checks without history hooks', () =>
  Effect.gen(function* () {
    let historicalReads = 0
    const make = brokenFixture((storage) => ({
      ...storage,
      document: (id, at) => {
        if (at !== undefined) historicalReads++
        return storage.document(id, at)
      },
    }))
    const cases = Testing.storageConformance({
      make,
      capabilities: { history: false, reopen: false, sqlAmbient: false, sqlUncertain: false },
    })
    assert.strictEqual(cases.length, 15)
    for (const test of cases) yield* test.run
    assert.strictEqual(historicalReads, 0)
  }),
)

it.effect('evaluates factories once per run and cleans both scopes after assertion failure', () =>
  Effect.gen(function* () {
    const events = yield* Ref.make<ReadonlyArray<string>>([])
    const make = Effect.acquireRelease(
      Ref.update(events, (events) => [...events, 'factory']).pipe(
        Effect.as({
          open: Effect.gen(function* () {
            yield* Effect.addFinalizer(() =>
              Ref.update(events, (events) => [...events, 'service cleanup']),
            )
            const storage = yield* memory.pipe(Effect.flatMap((fixture) => fixture.open))
            return {
              ...storage,
              mintId: <I extends Storage.StorageId>() =>
                Effect.succeed(Storage.EntryId.make(2) as I),
            }
          }),
        }),
      ),
      () => Ref.update(events, (events) => [...events, 'store cleanup']),
    )
    const test = caseNamed(Testing.storageConformance({ make }), 'identity allocation')
    for (let i = 0; i < 2; i++)
      yield* expectAssertion(test, 'fresh IDs reserve root and are unique')
    assert.deepEqual(yield* Ref.get(events), [
      'factory',
      'service cleanup',
      'store cleanup',
      'factory',
      'service cleanup',
      'store cleanup',
    ])
  }),
)

it.effect('cleans factory resources if a case is interrupted while opening', () =>
  Effect.gen(function* () {
    const started = yield* Deferred.make<void>()
    const cleaned = yield* Ref.make(false)
    const make = Effect.acquireRelease(
      Effect.succeed({
        open: Deferred.succeed(started, undefined).pipe(Effect.andThen(Effect.never)),
      }),
      () => Ref.set(cleaned, true),
    )
    const fiber = yield* Effect.forkChild(
      caseNamed(Testing.storageConformance({ make }), 'identity allocation').run,
    )
    yield* Deferred.await(started)
    yield* Fiber.interrupt(fiber)
    assert.isTrue(yield* Ref.get(cleaned))
  }),
)

it.effect('preserves typed factory failures', () =>
  Effect.gen(function* () {
    const test = caseNamed(
      Testing.storageConformance({ make: Effect.fail('fixture unavailable' as const) }),
      'identity allocation',
    )
    assert.strictEqual(yield* test.run.pipe(Effect.flip), 'fixture unavailable')
  }),
)

it.live('cleans the temporary backing store after successful execution', () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem
    const directories = yield* Ref.make<ReadonlyArray<string>>([])
    const make = Effect.gen(function* () {
      const directory = yield* fs.makeTempDirectoryScoped({
        prefix: 'storage-conformance-cleanup-',
      })
      yield* Ref.update(directories, (directories) => [...directories, directory])
      return {
        open: Layer.build(Storage.layerJsonl({ filePath: `${directory}/store` })).pipe(
          Effect.map((context) => Context.get(context, Storage.Storage)),
        ),
      }
    })
    yield* caseNamed(Testing.storageConformance({ make }), 'identity allocation').run
    for (const directory of yield* Ref.get(directories)) assert.isFalse(yield* fs.exists(directory))
  }).pipe(Effect.provide(NodeServices.layer)),
)
