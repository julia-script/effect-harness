import { assert, describe, it } from '@effect/vitest'
import { NodeServices } from '@effect/platform-node'
import * as SqliteClient from '@effect/sql-sqlite-node/SqliteClient'
import * as Context from 'effect/Context'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Fiber from 'effect/Fiber'
import * as Exit from 'effect/Exit'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Reactivity from 'effect/reactivity/Reactivity'
import * as SqlClient from 'effect/sql/SqlClient'
import * as SqlError from 'effect/sql/SqlError'
import * as Statement from 'effect/sql/Statement'
import * as Storage from 'effect-harness/Storage'
import * as Format from 'effect-harness/StorageFormat'
import * as Metadata from '../src/internal/Metadata.js'
import * as Memory from '../src/internal/Memory.js'
import * as Kernel from '../src/internal/Kernel.js'
import * as Record from 'effect-harness/Record'
import { program as storageFormatExample } from '../../../apps/example/src/tour/StorageFormat.js'

const legacy = { formatVersion: 1, minimumReaderVersion: 1 }
const current = { formatVersion: 2, minimumReaderVersion: 2 }
const OldReaderMetadata = Schema.Struct({
  format: Schema.Literal(1),
  nextId: Metadata.Counter.check(Schema.isGreaterThanOrEqualTo(2)),
  nextSeq: Metadata.Counter,
})
const fixture = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem
  const directory = yield* fs.makeTempDirectoryScoped({ prefix: 'storage-format-' })
  return { fs, filename: `${directory}/store` }
})
const backendLayer = (backend: 'memory' | 'sqlite' | 'jsonl', filename: string) => {
  if (backend === 'memory') return Storage.layerMemory
  if (backend === 'sqlite')
    return Storage.layerSql.pipe(Layer.provide(SqliteClient.layer({ filename })))
  return Storage.layerJsonl({ filePath: filename })
}

it.effect('runs the public example through temporary source mapping', () =>
  storageFormatExample.pipe(
    Effect.map((result) =>
      assert.deepEqual(result, {
        before: legacy,
        activated: current,
        sequence: 1,
      }),
    ),
  ),
)

for (const backend of ['memory', 'sqlite', 'jsonl'] as const) {
  describe(`format guard: ${backend}`, () => {
    it.live('opens legacy unchanged and serializes upgrades with IDs and domain commits', () =>
      Effect.scoped(
        Effect.gen(function* () {
          const { filename } = yield* fixture
          yield* Effect.gen(function* () {
            const storage = yield* Storage.Storage
            assert.deepEqual(yield* Format.inspect, legacy)
            yield* storage.commit([
              { _tag: 'conversation', value: { id: Storage.ROOT_CONVERSATION_ID } },
              {
                _tag: 'task',
                value: {
                  id: Storage.TaskId.make(2),
                  conversationId: Storage.ROOT_CONVERSATION_ID,
                  kind: 'format-proof',
                  version: 1,
                  input: null,
                  background: false,
                  abortRequested: false,
                  state: { status: 'pending', checkpoint: { phase: 'idle' } },
                },
              },
            ])
            assert.deepEqual(yield* Format.inspect, legacy)
            const results = yield* Effect.all(
              [
                Effect.all(
                  Array.from({ length: 12 }, () => storage.mintId<Storage.EntryId>()),
                  { concurrency: 'unbounded' },
                ),
                Effect.all(
                  Array.from({ length: 12 }, (_, i) =>
                    storage.commit([
                      {
                        _tag: 'task',
                        value: {
                          id: Storage.TaskId.make(2),
                          conversationId: Storage.ROOT_CONVERSATION_ID,
                          kind: 'format-proof',
                          version: 1,
                          input: null,
                          background: false,
                          abortRequested: false,
                          state: { status: 'pending', checkpoint: { phase: 'idle', count: i } },
                        },
                      },
                    ]),
                  ),
                  { concurrency: 'unbounded' },
                ),
                Effect.all(
                  Array.from({ length: 12 }, (_, i) =>
                    Format.requireSupportedFormat(i % 2 === 0 ? current : legacy),
                  ),
                  { concurrency: 'unbounded' },
                ),
              ],
              { concurrency: 'unbounded' },
            )
            assert.deepEqual(
              [...results[0]].sort((a, b) => a - b),
              Array.from({ length: 12 }, (_, i) => i + 3),
            )
            assert.deepEqual(
              [...results[1]].sort((a, b) => a - b),
              Array.from({ length: 12 }, (_, i) => i + 2),
            )
            assert.deepEqual(yield* Format.inspect, current)
            assert.deepEqual(yield* Format.requireSupportedFormat(legacy), current)
            assert.strictEqual(yield* storage.mintId<Storage.EntryId>(), 15)
            assert.strictEqual(yield* storage.commit([]), 14)
            assert.isTrue(Option.isSome(yield* storage.conversation(Storage.ROOT_CONVERSATION_ID)))
            for (const requested of [
              { formatVersion: 3, minimumReaderVersion: 2 },
              { formatVersion: 2, minimumReaderVersion: 3 },
              { formatVersion: 2, minimumReaderVersion: 1 },
              { formatVersion: 1, minimumReaderVersion: 2 },
            ]) {
              const error = yield* Format.requireSupportedFormat(requested).pipe(Effect.flip)
              assert.instanceOf(error, Format.StorageFormatError)
            }
            assert.deepEqual(yield* Format.inspect, current)
          }).pipe(Effect.provide(backendLayer(backend, filename)))
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
    )
    it.live('revokes an escaped format capability with its owning Storage scope', () =>
      Effect.scoped(
        Effect.gen(function* () {
          const { filename } = yield* fixture
          const capability = yield* Format.StorageFormat.pipe(
            Effect.provide(backendLayer(backend, filename)),
          )
          const failure = yield* capability.requireSupportedFormat(current).pipe(Effect.flip)
          assert.instanceOf(failure, Storage.StorageError)
          if (failure instanceof Storage.StorageError) assert.strictEqual(failure.reason, 'closed')
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
    )
  })
}

it.effect(
  'requires an explicit matching custom capability without changing the Storage structural contract',
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const context = yield* Layer.build(Storage.layerMemory)
        const storage = Context.get(context, Storage.Storage)
        const native = Context.get(context, Format.StorageFormat)
        const custom: Storage.Storage['Service'] = { ...storage }
        const missing = yield* Format.requireSupportedFormat(current).pipe(
          Effect.provideService(Storage.Storage, custom),
          Effect.flip,
        )
        assert.instanceOf(missing, Format.StorageFormatError)
        const mismatched = yield* Format.requireSupportedFormat(current).pipe(
          Effect.provideService(Storage.Storage, custom),
          Effect.provideService(Format.StorageFormat, native),
          Effect.flip,
        )
        assert.instanceOf(mismatched, Format.StorageFormatError)
        assert.deepEqual(yield* native.inspect, legacy)
        let calls = 0
        const explicit: Format.StorageFormat['Service'] = {
          storage: custom,
          inspect: native.inspect,
          requireSupportedFormat: (requested) =>
            Effect.sync(() => {
              calls++
            }).pipe(Effect.andThen(native.requireSupportedFormat(requested))),
        }
        assert.deepEqual(
          yield* Format.requireSupportedFormat(current).pipe(
            Effect.provideService(Storage.Storage, custom),
            Effect.provideService(Format.StorageFormat, explicit),
          ),
          current,
        )
        assert.strictEqual(calls, 1)
      }),
    ),
)

it.effect(
  'memory reopening retained backend state preserves the marker and counters; independent layers stay isolated',
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const access = yield* Memory.makeAccess
        const first = yield* Effect.scoped(
          Kernel.make(access).pipe(
            Effect.tap((storage) =>
              storage.storageFormat
                .requireSupportedFormat(current)
                .pipe(Effect.andThen(storage.mintId())),
            ),
          ),
        )
        const second = yield* Kernel.make(access)
        assert.deepEqual(yield* second.storageFormat.inspect, current)
        assert.strictEqual(yield* second.mintId(), 3)
        assert.strictEqual((yield* first.commit([]).pipe(Effect.flip)).reason, 'closed')
        assert.deepEqual(yield* Format.inspect.pipe(Effect.provide(Storage.layerMemory)), legacy)
        yield* access.save([], { format: 9, minimumReader: 9, nextId: 4, nextSeq: 1 })
        assert.strictEqual(
          (yield* Kernel.make(access).pipe(Effect.flip)).reason,
          'unsupportedFormat',
        )
      }),
    ),
)

it.effect('document metadata reads stored version without materializing invalid old content', () =>
  Effect.scoped(
    Effect.gen(function* () {
      const access = yield* Memory.makeAccess
      const id = Record.DocumentId.make(2)
      const record: Record.Document = {
        id,
        kind: 'old-schema',
        scope: { _tag: 'conversation', conversationId: Record.ROOT_CONVERSATION_ID },
        history: 'rewindable',
        fork: 'asOf',
        createdAt: Record.Seq.make(1),
      }
      yield* access.save(
        [
          {
            _tag: 'document',
            value: {
              record,
              revisions: [
                { seq: Record.Seq.make(1), content: { _tag: 'base', version: 1, value: {} } },
                {
                  seq: Record.Seq.make(2),
                  content: { _tag: 'delta', version: 2, ops: [['set', ['missing', 'value'], 3]] },
                },
              ],
            },
          },
        ],
        { format: 1, nextId: 3, nextSeq: 3 },
      )
      const storage = yield* Kernel.make(access)
      assert.strictEqual((yield* storage.document(id).pipe(Effect.flip)).reason, 'corrupt')
      const metadata = yield* storage.documentMetadata(id)
      assert.isTrue(Option.isSome(metadata))
      if (Option.isSome(metadata)) {
        assert.strictEqual(metadata.value.version, 2)
        Reflect.set(metadata.value.record, 'kind', 'mutated')
      }
      const again = yield* storage.documentMetadata(id)
      if (Option.isSome(again)) assert.strictEqual(again.value.record.kind, 'old-schema')
      assert.isTrue(Option.isNone(yield* storage.documentMetadata(Record.DocumentId.make(4))))
    }),
  ),
)

for (const backend of ['sqlite', 'jsonl'] as const)
  it.live(
    `${backend} cold reopen preserves v2 and rejects the exact previous format=1 reader schema`,
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const { fs, filename } = yield* fixture
          yield* Effect.gen(function* () {
            const storage = yield* Storage.Storage
            yield* storage.commit([
              { _tag: 'conversation', value: { id: Storage.ROOT_CONVERSATION_ID } },
            ])
            assert.strictEqual(yield* storage.mintId(), 2)
            yield* Format.requireSupportedFormat(current)
          }).pipe(Effect.provide(backendLayer(backend, filename)))
          if (backend === 'jsonl') {
            const frames = (yield* fs.readFileString(filename)).trim().split('\n')
            const marker = yield* Schema.decodeUnknownEffect(
              Schema.fromJsonString(Schema.Struct({ metadata: Metadata.Metadata })),
            )(frames.at(-1))
            yield* Schema.decodeUnknownEffect(OldReaderMetadata)(marker.metadata).pipe(Effect.flip)
          } else {
            yield* Effect.gen(function* () {
              const sql = yield* SqlClient.SqlClient
              const marker =
                (yield* sql`SELECT format, next_id AS nextId, next_seq AS nextSeq FROM effect_harness_metadata`)[0]
              yield* Schema.decodeUnknownEffect(OldReaderMetadata)(marker).pipe(Effect.flip)
            }).pipe(Effect.provide(SqliteClient.layer({ filename })))
          }
          yield* Effect.gen(function* () {
            const storage = yield* Storage.Storage
            assert.deepEqual(yield* Format.inspect, current)
            assert.strictEqual(yield* storage.mintId(), 3)
            assert.strictEqual(yield* storage.commit([]), 2)
            assert.isTrue(Option.isSome(yield* storage.conversation(Storage.ROOT_CONVERSATION_ID)))
          }).pipe(Effect.provide(backendLayer(backend, filename)))
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
  )

for (const marker of [
  { format: 7, minimumReader: 2 },
  { format: 2, minimumReader: 7 },
  { format: 9, minimumReader: 2 },
  { format: 2, minimumReader: 9 },
])
  it.live(
    `rejects JSONL mismatched or future marker ${JSON.stringify(marker)} before decoding domain rows, preserving bytes`,
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const { fs, filename } = yield* fixture
          const bytes =
            JSON.stringify({
              metadata: { ...marker, nextId: 2, nextSeq: 1 },
              rows: [{ _tag: 'future.opcode' }],
            }) + '\n'
          yield* fs.writeFileString(filename, bytes)
          const error = yield* Format.inspect.pipe(
            Effect.provide(Storage.layerJsonl({ filePath: filename })),
            Effect.flip,
          )
          assert.instanceOf(error, Storage.StorageError)
          if (error instanceof Storage.StorageError)
            assert.strictEqual(
              error.reason,
              marker.format > 8 || marker.minimumReader > 8 ? 'unsupportedFormat' : 'corrupt',
            )
          assert.strictEqual(yield* fs.readFileString(filename), bytes)
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
  )

it.live(
  'rejects SQL mismatched and future format and minimum-reader at open and on guard without changing metadata',
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { filename } = yield* fixture
        yield* Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient
          const context = yield* Layer.build(Storage.layerSql)
          const format = Context.get(context, Format.StorageFormat)
          for (const [version, reader] of [
            [7, 2],
            [2, 7],
            [9, 2],
            [2, 9],
          ] as const) {
            yield* sql`UPDATE effect_harness_metadata SET format=${version},minimum_reader=${reader}`
            const before = yield* sql`SELECT * FROM effect_harness_metadata`
            const reason = version > 8 || reader > 8 ? 'unsupportedFormat' : 'corrupt'
            const failed = yield* format.requireSupportedFormat(current).pipe(Effect.flip)
            assert.instanceOf(failed, Storage.StorageError)
            if (failed instanceof Storage.StorageError) assert.strictEqual(failed.reason, reason)
            assert.deepEqual(yield* sql`SELECT * FROM effect_harness_metadata`, before)
            const reopen = yield* Layer.build(Storage.layerSql).pipe(Effect.flip)
            assert.strictEqual(reopen.reason, reason)
          }
        }).pipe(Effect.provide(SqliteClient.layer({ filename })))
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
)

for (const failure of ['partial', 'synced-but-reported-failed'] as const)
  it.live(`JSONL ${failure} upgrade revokes the writer; cold reopen reconciles actual bytes`, () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { fs, filename } = yield* fixture
        yield* Effect.gen(function* () {
          const storage = yield* Storage.Storage
          yield* storage.commit([
            { _tag: 'conversation', value: { id: Storage.ROOT_CONVERSATION_ID } },
          ])
        }).pipe(Effect.provide(Storage.layerJsonl({ filePath: filename })))
        const original = yield* fs.readFileString(filename)
        const fault = yield* fs.readFile(`${filename}.missing`).pipe(Effect.flip)
        const injected: FileSystem.FileSystem = {
          ...fs,
          open: (path, options) =>
            fs.open(path, options).pipe(
              Effect.map((file) => ({
                ...file,
                writeAll: (bytes) =>
                  failure === 'partial'
                    ? file.writeAll(bytes.subarray(0, 12)).pipe(Effect.andThen(Effect.fail(fault)))
                    : file.writeAll(bytes),
                sync:
                  failure === 'synced-but-reported-failed'
                    ? file.sync.pipe(Effect.andThen(Effect.fail(fault)))
                    : file.sync,
              })),
            ),
        }
        yield* Effect.gen(function* () {
          const storage = yield* Storage.Storage
          const format = yield* Format.StorageFormat
          const failed = yield* format.requireSupportedFormat(current).pipe(Effect.flip)
          assert.instanceOf(failed, Storage.StorageError)
          if (failed instanceof Storage.StorageError) assert.strictEqual(failed.reason, 'uncertain')
          const length = (yield* fs.readFile(filename)).length
          yield* format.inspect.pipe(Effect.flip)
          yield* format.requireSupportedFormat(legacy).pipe(Effect.flip)
          assert.strictEqual((yield* storage.mintId().pipe(Effect.flip)).reason, 'uncertain')
          assert.strictEqual((yield* fs.readFile(filename)).length, length)
        }).pipe(
          Effect.provide(Storage.layerJsonl({ filePath: filename })),
          Effect.provideService(FileSystem.FileSystem, injected),
        )
        yield* Effect.gen(function* () {
          const storage = yield* Storage.Storage
          assert.deepEqual(yield* Format.inspect, failure === 'partial' ? legacy : current)
          if (failure === 'partial')
            assert.strictEqual(yield* fs.readFileString(filename), original)
          assert.isTrue(Option.isSome(yield* storage.conversation(Storage.ROOT_CONVERSATION_ID)))
          assert.strictEqual(yield* storage.mintId(), 2)
          assert.strictEqual(yield* storage.commit([]), 2)
        }).pipe(Effect.provide(Storage.layerJsonl({ filePath: filename })))
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  )

it.live('interruption during JSONL sync cannot expose a partially activated in-memory marker', () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { fs, filename } = yield* fixture
      const syncing = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const injected: FileSystem.FileSystem = {
        ...fs,
        open: (path, options) =>
          fs.open(path, options).pipe(
            Effect.map((file) => ({
              ...file,
              writeAll: (bytes) => file.writeAll(bytes),
              sync: Deferred.succeed(syncing, undefined).pipe(
                Effect.andThen(Deferred.await(release)),
                Effect.andThen(file.sync),
              ),
            })),
          ),
      }
      yield* Effect.gen(function* () {
        const format = yield* Format.StorageFormat
        const fiber = yield* Effect.forkChild(format.requireSupportedFormat(current))
        yield* Deferred.await(syncing)
        const interrupt = yield* Effect.forkChild(Fiber.interrupt(fiber), {
          startImmediately: true,
        })
        assert.deepEqual(yield* format.inspect, legacy)
        yield* Deferred.succeed(release, undefined)
        yield* Fiber.join(interrupt)
        assert.isTrue(Exit.hasInterrupts(yield* Fiber.await(fiber)))
        assert.deepEqual(yield* format.inspect, current)
      }).pipe(
        Effect.provide(Storage.layerJsonl({ filePath: filename })),
        Effect.provideService(FileSystem.FileSystem, injected),
      )
      assert.deepEqual(
        yield* Format.inspect.pipe(Effect.provide(Storage.layerJsonl({ filePath: filename }))),
        current,
      )
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
)

for (const mode of ['statement', 'commit', 'committed-but-reported-failed', 'rollback'] as const)
  it.live(
    `SQL ${mode} failure never claims downgrade or permits activation; reopen reconciles the marker`,
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const { filename } = yield* fixture
          yield* Effect.gen(function* () {
            const native = yield* SqlClient.SqlClient
            const sql =
              mode === 'rollback' || mode === 'committed-but-reported-failed'
                ? yield* SqlClient.make({
                    acquirer: Effect.succeed(yield* native.reserve),
                    compiler: Statement.makeCompilerSqlite(),
                    spanAttributes: [],
                    ...(mode === 'rollback'
                      ? { rollback: 'ROLLBACK BROKEN' }
                      : {
                          commit: (connection: import('effect/sql/SqlConnection').Connection) =>
                            connection.executeUnprepared('COMMIT', [], undefined).pipe(
                              Effect.andThen(
                                Effect.fail(
                                  new SqlError.SqlError({
                                    reason: new SqlError.ConnectionError({
                                      cause: 'lost commit acknowledgment',
                                    }),
                                  }),
                                ),
                              ),
                            ),
                        }),
                  }).pipe(Effect.provide(Reactivity.layer))
                : native
            yield* Effect.scoped(
              Effect.gen(function* () {
                const context = yield* Layer.build(Storage.layerSql).pipe(
                  Effect.provideService(SqlClient.SqlClient, sql),
                )
                const storage = Context.get(context, Storage.Storage)
                const format = Context.get(context, Format.StorageFormat)
                // The injected commit client would fail even the initial domain commit.
                if (mode !== 'committed-but-reported-failed')
                  yield* storage.commit([
                    { _tag: 'conversation', value: { id: Storage.ROOT_CONVERSATION_ID } },
                  ])
                if (mode === 'commit') {
                  yield* sql`PRAGMA foreign_keys=ON`
                  yield* sql`CREATE TABLE parent(id INTEGER PRIMARY KEY)`
                  yield* sql`CREATE TABLE child(id INTEGER, FOREIGN KEY(id) REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED)`
                  yield* sql`CREATE TRIGGER fail_format AFTER UPDATE OF format ON effect_harness_metadata WHEN NEW.format=2 BEGIN INSERT INTO child(id) VALUES(999); END`
                } else if (mode === 'statement' || mode === 'rollback')
                  yield* sql`CREATE TRIGGER fail_format BEFORE UPDATE OF format ON effect_harness_metadata WHEN NEW.format=2 BEGIN SELECT RAISE(ABORT,'format write fails'); END`
                const failure = yield* format.requireSupportedFormat(current).pipe(Effect.flip)
                assert.instanceOf(failure, Storage.StorageError)
                if (failure instanceof Storage.StorageError)
                  assert.strictEqual(failure.reason, 'uncertain')
                const poison = yield* format.inspect.pipe(Effect.flip)
                assert.instanceOf(poison, Storage.StorageError)
                assert.strictEqual(
                  (yield* storage.commit([]).pipe(Effect.flip)).reason,
                  'uncertain',
                )
              }),
            )
            if (mode === 'rollback') yield* sql`ROLLBACK`
            if (mode !== 'committed-but-reported-failed') yield* sql`DROP TRIGGER fail_format`
          }).pipe(Effect.provide(SqliteClient.layer({ filename })))
          yield* Effect.gen(function* () {
            const storage = yield* Storage.Storage
            assert.deepEqual(
              yield* Format.inspect,
              mode === 'committed-but-reported-failed' ? current : legacy,
            )
            assert.strictEqual(yield* storage.mintId(), 2)
            assert.strictEqual(
              yield* storage.commit([]),
              mode === 'committed-but-reported-failed' ? 1 : 2,
            )
          }).pipe(Effect.provide(backendLayer('sqlite', filename)))
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
  )

it.live('JSONL reopen rejects downgrade bytes appended by an already-open foreign writer', () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { fs, filename } = yield* fixture
      const foreign = yield* fs.open(filename, { flag: 'a+' })
      yield* Format.requireSupportedFormat(current).pipe(
        Effect.provide(Storage.layerJsonl({ filePath: filename })),
      )
      // The original writer protocol appends cached format-1 metadata, without checking v2.
      yield* foreign.writeAll(
        new TextEncoder().encode(
          JSON.stringify({ metadata: { format: 1, nextId: 2, nextSeq: 1 }, rows: [] }) + '\n',
        ),
      )
      yield* foreign.sync
      const before = yield* fs.readFile(filename)
      const failure = yield* Format.inspect.pipe(
        Effect.provide(Storage.layerJsonl({ filePath: filename })),
        Effect.flip,
      )
      assert.instanceOf(failure, Storage.StorageError)
      if (failure instanceof Storage.StorageError) assert.strictEqual(failure.reason, 'corrupt')
      assert.deepEqual(yield* fs.readFile(filename), before)
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
)

it.live(
  'SQL leaves already-open foreign writers usable; their original counter writes preserve the v2 marker',
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { filename } = yield* fixture
        yield* Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient
          const context = yield* Layer.build(Storage.layerSql)
          yield* Context.get(context, Format.StorageFormat).requireSupportedFormat(current)
          // The exact metadata update emitted by the previous format-1 SQL writer.
          yield* sql`UPDATE effect_harness_metadata SET next_id=3,next_seq=2 WHERE singleton=1`
          assert.deepEqual(yield* Context.get(context, Format.StorageFormat).inspect, current)
          assert.strictEqual(yield* Context.get(context, Storage.Storage).mintId(), 3)
        }).pipe(Effect.provide(SqliteClient.layer({ filename })))
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
)

it.live(
  'two SQL instances serialize format requests with ordinary writes on the same metadata row',
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { filename } = yield* fixture
        yield* Effect.gen(function* () {
          const first = yield* Layer.build(Storage.layerSql)
          const second = yield* Layer.build(Storage.layerSql)
          const storage = Context.get(first, Storage.Storage)
          const other = Context.get(second, Storage.Storage)
          yield* Effect.all(
            [
              Context.get(first, Format.StorageFormat).requireSupportedFormat(current),
              Context.get(second, Format.StorageFormat).requireSupportedFormat(current),
              storage.mintId(),
              other.mintId(),
              storage.commit([]),
              other.commit([]),
            ],
            { concurrency: 'unbounded' },
          )
          assert.deepEqual(yield* Context.get(second, Format.StorageFormat).inspect, current)
          assert.strictEqual(yield* storage.mintId(), 4)
          assert.strictEqual(yield* other.commit([]), 3)
        }).pipe(Effect.provide(SqliteClient.layer({ filename })))
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
)
