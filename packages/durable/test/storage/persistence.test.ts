import * as Scope from 'effect/Scope'
import * as Exit from 'effect/Exit'
import { NodeFileSystem } from '@effect/platform-node'
import * as SqliteClient from '@effect/sql-sqlite-node/SqliteClient'
import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Path from 'effect/Path'
import * as Schema from 'effect/Schema'
import * as SqlClient from 'effect/sql/SqlClient'
import * as Document from '../../src/Document.ts'
import * as Record from '../../src/Record.ts'
import * as Session from '../../src/Session.ts'
import { Store } from '../../src/Store.ts'
import { StorageError } from '../../src/StorageError.ts'
import * as Jsonl from '../../src/storage/Jsonl.ts'
import * as Sqlite from '../../src/storage/Sqlite.ts'
const token = Document.defineUnsafe({
  kind: 'counter',
  version: 1,
  scope: 'session',
  schema: Schema.Struct({ count: Schema.Finite.pipe(Schema.mutableKey) }),
  initial: () => ({ count: 0 }),
})
const firstSeq = Schema.decodeSync(Record.Seq)(1)
const env = Layer.merge(NodeFileSystem.layer, Path.layer)
const setup = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem
  const path = yield* Path.Path
  const directory = yield* fs.makeTempDirectoryScoped()
  return { fs, directory, file: path.join(directory, 'commits.jsonl') }
})
const fail = <A, E, R>(effect: Effect.Effect<A, E, R>) => effect.pipe(Effect.flip, Effect.orDie)
const commit = (store: Store['Service']) =>
  Effect.gen(function* () {
    const session = yield* Session.make().pipe(Effect.provideService(Store, store))
    yield* session.root()
    return yield* session.transaction(
      Effect.fnUntraced(function* (tx) {
        const d = yield* tx.doc(token)
        d.count = 3
        return { count: d.count }
      }),
      { key: 'once', fingerprint: 'input' },
    )
  })
describe('JSONL recovery', () => {
  it.effect('reopens full state and durable receipt after compaction', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { directory } = yield* setup
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* Jsonl.make({ directory, fsync: true })
            return yield* commit(store)
          }),
        )
        yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* Jsonl.make({ directory })
            const session = yield* Session.make().pipe(Effect.provideService(Store, store))
            assert.strictEqual((yield* session.snapshot(token))?.value.count, 3)
            assert.deepStrictEqual(
              yield* session.transaction(() => Effect.die('receipt callback ran'), {
                key: 'once',
                fingerprint: 'input',
              }),
              result,
            )
          }),
        )
      }).pipe(Effect.provide(env)),
    ),
  )
  it.effect('reopens explicit void receipt results without collapsing them to null', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { directory } = yield* setup
        yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* Jsonl.make({ directory })
            const session = yield* Session.make().pipe(Effect.provideService(Store, store))
            assert.strictEqual(
              yield* session.transaction((tx) => tx.ensureRoot.pipe(Effect.asVoid), {
                key: 'void',
              }),
              undefined,
            )
          }),
        )
        yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* Jsonl.make({ directory })
            const session = yield* Session.make().pipe(Effect.provideService(Store, store))
            assert.strictEqual(
              yield* session.transaction(() => Effect.die('receipt callback ran'), { key: 'void' }),
              undefined,
            )
            assert.strictEqual((yield* store.read).receipts[0]?.resultIsVoid, true)
          }),
        )
      }).pipe(Effect.provide(env)),
    ),
  )
  it.effect('truncates torn utf8 tails but rejects malformed newline-terminated commits', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { fs, directory, file } = yield* setup
        yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* Jsonl.make({ directory })
            yield* commit(store)
          }),
        )
        const valid = yield* fs.readFile(file)
        yield* fs.writeFile(file, new Uint8Array([0xe2, 0x82]), { flag: 'a' })
        yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* Jsonl.make({ directory })
            assert.strictEqual((yield* store.read).receipts.length, 1)
          }),
        )
        assert.deepStrictEqual(yield* fs.readFile(file), valid)
        yield* fs.writeFileString(file, '{"garbage":true}\n', { flag: 'a' })
        const error = yield* fail(Effect.scoped(Jsonl.make({ directory })))
        assert.strictEqual(error.reason._tag, 'Corrupt')
      }).pipe(Effect.provide(env)),
    ),
  )
  it.effect(
    'poisons after an append with uncertain settlement and recovers durable effects on reopen',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const { fs, directory } = yield* setup
          let inject = true
          const faulty = FileSystem.FileSystem.of({
            ...fs,
            writeFileString: (file, data, options) => {
              if (inject && options?.flag === 'a') {
                inject = false
                return fs
                  .writeFileString(file, data, options)
                  .pipe(Effect.andThen(fs.readFile('/missing-jsonl-injected-fault')), Effect.asVoid)
              }
              return fs.writeFileString(file, data, options)
            },
          })
          yield* Effect.scoped(
            Effect.gen(function* () {
              const store = yield* Jsonl.make({ directory }).pipe(
                Effect.provideService(FileSystem.FileSystem, faulty),
              )
              const error = yield* fail(
                store.commit([
                  { type: 'conversation', value: { id: Record.ROOT_CONVERSATION_ID } },
                ]),
              )
              assert.strictEqual(error.certainty, 'uncertain')
              assert.strictEqual((yield* fail(store.read)).reason._tag, 'Poisoned')
              assert.strictEqual((yield* fail(store.commit([]))).reason._tag, 'Poisoned')
            }),
          )
          yield* Effect.scoped(
            Effect.gen(function* () {
              const store = yield* Jsonl.make({ directory })
              assert.deepStrictEqual((yield* store.read).conversations, [
                { id: Record.ROOT_CONVERSATION_ID },
              ])
            }),
          )
        }).pipe(Effect.provide(env)),
      ),
  )
  it.effect('best-effort reclaim errors do not reverse an admitted append', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { fs, directory } = yield* setup
        const faulty = FileSystem.FileSystem.of({
          ...fs,
          rename: () => fs.readFile('/missing-reclaim-injected-fault').pipe(Effect.asVoid),
        })
        yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* Jsonl.make({ directory }).pipe(
              Effect.provideService(FileSystem.FileSystem, faulty),
            )
            yield* commit(store)
            assert.strictEqual((yield* store.read).receipts.length, 1)
          }),
        )
        yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* Jsonl.make({ directory })
            assert.strictEqual((yield* store.read).receipts.length, 1)
          }),
        )
      }).pipe(Effect.provide(env)),
    ),
  )
})
describe('SQLite schema and reopen', () => {
  const client = SqliteClient.layer({ filename: ':memory:' })
  it.effect('reopens state, seq and stable receipts without Session cache', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const first = yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* Sqlite.make()
            return yield* commit(store)
          }),
        )
        yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* Sqlite.make()
            const session = yield* Session.make().pipe(Effect.provideService(Store, store))
            assert.strictEqual((yield* session.snapshot(token))?.value.count, 3)
            assert.deepStrictEqual(
              yield* session.transaction(() => Effect.die('receipt callback ran'), {
                key: 'once',
                fingerprint: 'input',
              }),
              first,
            )
          }),
        )
      }).pipe(Effect.provide(client)),
    ),
  )
  it.effect('upgrades schema v1 in place and preserves preexisting JSON receipts', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient
        yield* Sqlite.migrate(Sqlite.MIGRATIONS.slice(0, 1))
        const state: Record.State = {
          ...Record.emptyState(),
          nextSeq: 2,
          receipts: [{ key: 'old', fingerprint: 'input', result: { count: 1 }, seq: firstSeq }],
        }
        yield* sql`INSERT INTO durable_state VALUES(1,1,2,${JSON.stringify(state)})`
        yield* sql`INSERT INTO durable_receipt VALUES(${JSON.stringify('old')},${JSON.stringify('input')},${JSON.stringify({ count: 1 })},1)`
        const resourceScope = yield* Effect.acquireRelease(Scope.make(), (scope, exit) =>
          Scope.close(scope, exit),
        )
        const store = yield* Sqlite.make().pipe(Scope.provide(resourceScope))
        const session = yield* Session.make().pipe(
          Effect.provideService(Store, store),
          Scope.provide(resourceScope),
        )
        assert.strictEqual(
          (yield* sql<{ version: number }>`SELECT version FROM durable_schema`)[0]?.version,
          Sqlite.CURRENT_SCHEMA_VERSION,
        )
        assert.deepStrictEqual(
          yield* session.transaction(() => Effect.die('old receipt reran'), {
            key: 'old',
            fingerprint: 'input',
          }),
          { count: 1 },
        )
        assert.strictEqual(
          yield* session.transaction(() => Effect.void, { key: 'void' }),
          undefined,
        )
        yield* Scope.close(resourceScope, Exit.void)
        const reopened = yield* Sqlite.make()
        const session2 = yield* Session.make().pipe(Effect.provideService(Store, reopened))
        assert.strictEqual(
          yield* session2.transaction(() => Effect.die('void replay ran'), { key: 'void' }),
          undefined,
        )
      }).pipe(Effect.provide(client)),
    ),
  )
  it.effect(
    'rejects missing metadata and future schemas instead of silently creating fresh state',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient
          yield* Effect.scoped(Sqlite.make())
          yield* sql`DELETE FROM durable_state`
          assert.strictEqual((yield* fail(Effect.scoped(Sqlite.make()))).reason._tag, 'Corrupt')
          yield* sql`UPDATE durable_schema SET version=999`
          assert.strictEqual((yield* fail(Effect.scoped(Sqlite.make()))).reason._tag, 'Corrupt')
        }).pipe(Effect.provide(client)),
      ),
  )
  it.effect('rolls schema upgrades back atomically when any statement fails', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient
        const error = yield* fail(
          Sqlite.migrate([
            {
              version: 1,
              statements: ['CREATE TABLE first(id INTEGER PRIMARY KEY)', 'INVALID SQL'],
            },
          ]),
        )
        assert.strictEqual(error.certainty, 'rejected')
        assert.deepStrictEqual(
          yield* sql`SELECT name FROM sqlite_master WHERE type='table' AND name IN ('first','durable_schema')`,
          [],
        )
        yield* Sqlite.migrate()
        assert.strictEqual(
          (yield* sql<{ version: number }>`SELECT version FROM durable_schema`)[0]?.version,
          Sqlite.CURRENT_SCHEMA_VERSION,
        )
      }).pipe(Effect.provide(client)),
    ),
  )
  it.effect(
    'constraints reject duplicate receipt keys and invalid JSON, keeping all writes rolled back',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient
          const store = yield* Sqlite.make()
          yield* commit(store)
          const before = yield* store.read
          yield* fail(
            sql.withTransaction(
              Effect.gen(function* () {
                yield* sql`UPDATE durable_state SET next_seq=90`
                yield* sql`INSERT INTO durable_journal(seq,frame) VALUES(90,'invalid json')`
              }),
            ),
          )
          assert.deepStrictEqual(yield* store.read, before)
          const receipt = (yield* sql<{ key: string }>`SELECT key FROM durable_receipt`)[0]
          assert.ok(receipt)
          yield* fail(
            sql`INSERT INTO durable_receipt(key,fingerprint,result,seq) VALUES(${receipt.key},'', '{}',1)`,
          )
        }).pipe(Effect.provide(client)),
      ),
  )
  it.effect('missing schema version row is corruption', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient
        yield* Sqlite.migrate()
        yield* sql`DELETE FROM durable_schema`
        const error = yield* fail(Sqlite.migrate())
        assert.ok(error instanceof StorageError)
        assert.strictEqual(error.reason._tag, 'Corrupt')
      }).pipe(Effect.provide(client)),
    ),
  )
})
