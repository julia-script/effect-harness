import { assert, describe, it } from '@effect/vitest'
import { NodeServices } from '@effect/platform-node'
import { SqliteClient } from '@effect/sql-sqlite-node'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as SqlClient from 'effect/sql/SqlClient'
import * as Storage from 'effect-harness/Storage'
import * as Testing from 'effect-harness/Testing'
import * as Jsonl from '../src/internal/Jsonl.js'
import * as Sql from '../src/internal/Sql.js'
import * as Store from '../src/internal/Store.js'

const root: Storage.StorageWrite = {
  _tag: 'conversation',
  value: { id: Storage.ROOT_CONVERSATION_ID },
}
const document: Storage.StorageWrite = {
  _tag: 'document.create',
  record: { id: Storage.DocumentId.make(2), kind: 'counter', scope: { _tag: 'session' } },
  content: { _tag: 'base', version: 1, value: { count: 0 } },
}
const replace = (count: number): Storage.StorageWrite => ({
  _tag: 'document.change',
  id: Storage.DocumentId.make(2),
  content: { _tag: 'base', version: 1, value: { count } },
})
const fixture = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem
  const directory = yield* fs.makeTempDirectoryScoped()
  return { fs, directory, layer: SqliteClient.layer({ filename: `${directory}/store.sqlite` }) }
})
const open = (storeId = 'default') => Sql.make({ storeId }).pipe(Effect.flatMap(Store.make))
const conformance = Effect.gen(function* () {
  const { layer } = yield* fixture
  return {
    open: Layer.build(Layer.fresh(Storage.layerSql().pipe(Layer.provide(layer)))).pipe(
      Effect.map((context) => Context.get(context, Storage.Storage)),
    ),
  }
})

describe('SQL shared-store conformance', () => {
  for (const test of Testing.storageConformance({
    make: conformance,
    capabilities: { history: true, reopen: true },
  }))
    it.live(test.name, () => test.run.pipe(Effect.provide(NodeServices.layer)))
})

it.live('SQLite stores the same opaque frames and retained payloads as JSONL', () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { fs, directory, layer } = yield* fixture
      yield* Effect.scoped(
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient
          const sqlite = yield* open()
          const filePath = `${directory}/main.jsonl`
          const jsonl = yield* Jsonl.make({ filePath }).pipe(Effect.flatMap(Store.make))
          for (const store of [sqlite, jsonl]) {
            yield* store.commit([root, document])
            yield* store.commit([replace(1)])
          }
          const commits = yield* sql<{
            frame: string
          }>`SELECT frame FROM effect_harness_commits_v1 ORDER BY position`
          assert.deepEqual(
            commits.map((row) => row.frame),
            (yield* fs.readFileString(filePath)).trim().split('\n'),
          )
          const payloads = yield* sql<{
            line: string
          }>`SELECT line FROM effect_harness_payloads_v1 ORDER BY position`
          assert.deepEqual(
            payloads.map((row) => row.line),
            (yield* fs.readFileString(`${filePath}.parts/doc-2.jsonl`)).trim().split('\n'),
          )
          assert.lengthOf(payloads, 1)
        }).pipe(Effect.provide(layer)),
      )
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
)

it.live('store IDs isolate counters, payload reclamation, and reopen state', () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { layer } = yield* fixture
      yield* Effect.scoped(
        Effect.gen(function* () {
          const first = yield* open('__proto__/A')
          const second = yield* open('__proto__/a')
          yield* first.commit([root, document])
          yield* second.commit([root, document])
          yield* first.commit([replace(7)])
          assert.deepEqual(
            Option.getOrThrow(yield* second.document(Storage.DocumentId.make(2))).value,
            { count: 0 },
          )
        }).pipe(Effect.provide(layer)),
      )
      yield* Effect.scoped(
        Effect.gen(function* () {
          const first = yield* open('__proto__/A')
          const second = yield* open('__proto__/a')
          assert.deepEqual(
            Option.getOrThrow(yield* first.document(Storage.DocumentId.make(2))).value,
            { count: 7 },
          )
          assert.deepEqual(
            Option.getOrThrow(yield* second.document(Storage.DocumentId.make(2))).value,
            { count: 0 },
          )
        }).pipe(Effect.provide(Layer.fresh(layer))),
      )
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
)

it.live('ambient SQL transactions cannot publish or reserve harness state', () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { layer } = yield* fixture
      yield* Effect.scoped(
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient
          assert.strictEqual(
            (yield* sql.withTransaction(open()).pipe(Effect.flip)).reason,
            'invalid',
          )
          const storage = yield* open()
          const operations: ReadonlyArray<Effect.Effect<number, Storage.StorageError>> = [
            storage.mintId(),
            storage.commit([root]),
            storage.commit([]),
          ]
          for (const operation of operations) {
            const error = yield* sql.withTransaction(operation).pipe(Effect.flip)
            assert.strictEqual(error.reason, 'invalid')
          }
          assert.strictEqual(yield* storage.mintId(), 2)
          assert.isTrue(Option.isNone(yield* storage.conversation(Storage.ROOT_CONVERSATION_ID)))
          yield* storage.commit([root])
        }).pipe(Effect.provide(layer)),
      )
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
)

for (const failure of ['statement', 'commit'] as const)
  it.live(`a real SQL ${failure} failure seals storage and rolls back the entire batch`, () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { layer } = yield* fixture
        yield* Effect.scoped(
          Effect.gen(function* () {
            const sql = yield* SqlClient.SqlClient
            yield* sql`PRAGMA foreign_keys = ON`
            yield* sql`CREATE TABLE parent (id INTEGER PRIMARY KEY)`
            yield* sql`CREATE TABLE child (parent INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED)`
            yield* Effect.scoped(
              Effect.gen(function* () {
                const storage = yield* open()
                yield* storage.commit([root, document])
                if (failure === 'statement')
                  yield* sql`CREATE TRIGGER reject_append BEFORE INSERT ON effect_harness_commits_v1
              BEGIN SELECT RAISE(ABORT, 'injected statement failure'); END`
                else
                  yield* sql`CREATE TRIGGER reject_append AFTER INSERT ON effect_harness_commits_v1
              BEGIN INSERT INTO child VALUES (999); END`
                assert.strictEqual(
                  (yield* storage.commit([replace(1)]).pipe(Effect.flip)).reason,
                  'uncertain',
                )
                assert.strictEqual(
                  (yield* storage.document(Storage.DocumentId.make(2)).pipe(Effect.flip)).reason,
                  'uncertain',
                )
                assert.strictEqual((yield* storage.mintId().pipe(Effect.flip)).reason, 'uncertain')
              }),
            )
            yield* sql`DROP TRIGGER reject_append`
            const frames = yield* sql`SELECT frame FROM effect_harness_commits_v1`
            const payloads = yield* sql`SELECT line FROM effect_harness_payloads_v1`
            assert.lengthOf(frames, 1)
            assert.lengthOf(payloads, 1)
          }).pipe(Effect.provide(layer)),
        )
        yield* Effect.scoped(
          Effect.gen(function* () {
            const storage = yield* open()
            assert.deepEqual(
              Option.getOrThrow(yield* storage.document(Storage.DocumentId.make(2))).value,
              { count: 0 },
            )
            yield* storage.commit([replace(2)])
          }).pipe(Effect.provide(Layer.fresh(layer))),
        )
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  )

it.live('stale writers cannot append or reclaim a newer writer’s payloads', () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { layer } = yield* fixture
      yield* Effect.scoped(
        Effect.gen(function* () {
          const first = yield* Sql.make()
          const second = yield* Sql.make()
          const firstStore = yield* Store.make(first)
          const staleStore = yield* Store.make(second)
          yield* firstStore.commit([root, document])
          assert.strictEqual(
            (yield* staleStore.commit([root]).pipe(Effect.flip)).reason,
            'uncertain',
          )
          assert.strictEqual(
            (yield* second.replace({ key: 'doc-2', lines: [] }).pipe(Effect.flip)).reason,
            'io',
          )
          const sql = yield* SqlClient.SqlClient
          assert.lengthOf(yield* sql`SELECT line FROM effect_harness_payloads_v1`, 1)
        }).pipe(Effect.provide(layer)),
      )
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
)

it.live('failed reclamation rolls back its delete and is retried on reopening', () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { layer } = yield* fixture
      yield* Effect.scoped(
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient
          yield* Effect.scoped(
            Effect.gen(function* () {
              const storage = yield* open()
              yield* storage.commit([root, document])
              // Append uses position 2 before the second frame exists. Only the later
              // replacement fails, after deleting the stream inside its transaction.
              yield* sql`CREATE TRIGGER reject_reclaim BEFORE INSERT ON effect_harness_payloads_v1
          WHEN NEW.position = 1 AND (SELECT COUNT(*) FROM effect_harness_commits_v1) = 2
          BEGIN SELECT RAISE(ABORT, 'injected reclamation failure'); END`
              yield* storage.commit([replace(9)])
              assert.deepEqual(
                Option.getOrThrow(yield* storage.document(Storage.DocumentId.make(2))).value,
                { count: 9 },
              )
              assert.lengthOf(yield* sql`SELECT line FROM effect_harness_payloads_v1`, 2)
            }),
          )
          yield* sql`DROP TRIGGER reject_reclaim`
        }).pipe(Effect.provide(layer)),
      )
      yield* Effect.scoped(
        Effect.gen(function* () {
          const storage = yield* open()
          const sql = yield* SqlClient.SqlClient
          assert.deepEqual(
            Option.getOrThrow(yield* storage.document(Storage.DocumentId.make(2))).value,
            { count: 9 },
          )
          assert.lengthOf(yield* sql`SELECT line FROM effect_harness_payloads_v1`, 1)
        }).pipe(Effect.provide(Layer.fresh(layer))),
      )
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
)
