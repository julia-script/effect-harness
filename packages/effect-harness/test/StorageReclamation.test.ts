import * as PackedBody from './fixtures/PackedBody.js'
import { assert, it } from '@effect/vitest'
import { NodeServices } from '@effect/platform-node'
import * as SqliteClient from '@effect/sql-sqlite-node/SqliteClient'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as SqlClient from 'effect/sql/SqlClient'
import * as Storage from 'effect-harness/Storage'
import * as Record from 'effect-harness/Record'
import * as Kernel from '../src/internal/Kernel.js'
import * as Memory from '../src/internal/Memory.js'

const image = 'x'.repeat(1024 * 1024)
const root = Record.ROOT_CONVERSATION_ID
const historyId = Record.DocumentId.make(2)
const currentId = Record.DocumentId.make(3)
const taskId = Record.TaskId.make(4)
const checkpoint = (step: number): Record.Task => ({
  id: taskId,
  conversationId: root,
  kind: 'synthetic.image',
  version: 1,
  input: null,
  background: false,
  abortRequested: false,
  state: { status: 'pending', checkpoint: { phase: 'image', image, step } },
})
const seed = (storage: Storage.Storage['Service']) =>
  storage.commit([
    { _tag: 'conversation', value: { id: root } },
    {
      _tag: 'document.create',
      record: {
        id: historyId,
        kind: 'history',
        scope: { _tag: 'conversation', conversationId: root },
        history: 'rewindable',
        fork: 'asOf',
      },
      content: { _tag: 'base', version: 1, value: { image, count: 0 } },
    },
    {
      _tag: 'document.create',
      record: {
        id: currentId,
        kind: 'current',
        scope: { _tag: 'task', taskId },
      },
      content: { _tag: 'base', version: 1, value: { image, count: 0 } },
    },
    { _tag: 'task', value: checkpoint(0) },
  ])
const replace = (storage: Storage.Storage['Service'], step: number) =>
  storage.commit([
    {
      _tag: 'document.change',
      id: currentId,
      content: {
        _tag: 'base',
        version: 1,
        value: { image, count: step },
      },
    },
    { _tag: 'task', value: checkpoint(step) },
  ])
const fixture = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem
  const directory = yield* fs.makeTempDirectoryScoped({ prefix: 'storage-reclamation-' })
  return { fs, filename: `${directory}/store` }
})
const retainedJsonl = (fs: FileSystem.FileSystem, filename: string) =>
  Effect.gen(function* () {
    let bytes = (yield* fs.readFile(filename)).byteLength
    for (const name of yield* fs.readDirectory(`${filename}.parts`))
      bytes += Number((yield* fs.stat(`${filename}.parts/${name}`)).size)
    return bytes
  })

it.effect(
  'standalone retirement releases a current-only megabyte while preserving rewindable history in memory',
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const access = yield* Memory.makeAccess
        const storage = yield* Kernel.make(access)
        const first = yield* seed(storage)
        yield* storage.commit([{ _tag: 'document.retire', id: currentId }])
        const retired = Option.getOrThrow(yield* access.get(currentId))
        assert.isBelow(JSON.stringify(retired).length, 1024)
        assert.isTrue(Option.isNone(yield* storage.document(currentId)))
        yield* storage.commit([{ _tag: 'document.retire', id: historyId }])
        assert.strictEqual(
          Option.getOrThrow(yield* storage.document(historyId, first)).value.image,
          image,
        )
      }),
    ),
)

for (const backend of ['jsonl', 'sqlite'] as const)
  it.live(
    `${backend} writes each historical revision once and reclaims replaced image payloads during normal commits`,
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const { fs, filename } = yield* fixture
          let written = 0
          const measuredFs: FileSystem.FileSystem = {
            ...fs,
            open: (path, options) =>
              fs.open(path, options).pipe(
                Effect.map((file) => ({
                  ...file,
                  sync: file.sync,
                  truncate: file.truncate,
                  writeAll: (bytes) =>
                    file.writeAll(bytes).pipe(
                      Effect.tap(() =>
                        Effect.sync(() => {
                          written += bytes.byteLength
                        }),
                      ),
                    ),
                })),
              ),
          }
          const layer =
            backend === 'jsonl'
              ? Storage.layerJsonl({ filePath: filename }).pipe(
                  Layer.provideMerge(SqliteClient.layer({ filename: ':memory:' })),
                )
              : Storage.layerSql.pipe(Layer.provideMerge(SqliteClient.layer({ filename })))
          const inspect = Effect.gen(function* () {
            if (backend === 'jsonl')
              return { written, retained: yield* retainedJsonl(fs, filename) }
            const sql = yield* SqlClient.SqlClient
            const writes = yield* sql`SELECT COALESCE(SUM(bytes), 0) AS bytes FROM measured_writes`
            const retained = yield* sql`SELECT
          (SELECT COALESCE(SUM(LENGTH(payload)),0) FROM effect_harness_records) +
          (SELECT COALESCE(SUM(LENGTH(content)),0) FROM effect_harness_revisions) AS bytes`
            return { written: Number(writes[0]?.bytes), retained: Number(retained[0]?.bytes) }
          })
          yield* Effect.gen(function* () {
            const storage = yield* Storage.Storage
            if (backend === 'sqlite') {
              const sql = yield* SqlClient.SqlClient
              yield* sql`CREATE TABLE measured_writes(bytes INTEGER NOT NULL)`
              for (const [table, column] of [
                ['effect_harness_records', 'payload'],
                ['effect_harness_revisions', 'content'],
              ] as const)
                for (const action of ['INSERT', 'UPDATE'] as const)
                  yield* sql`CREATE TRIGGER ${sql(`${table}_${action}`)} AFTER ${sql.literal(action)} ON ${sql(table)}
                BEGIN INSERT INTO measured_writes VALUES(LENGTH(NEW.${sql(column)})); END`
            }
            const first = yield* seed(storage)
            const initial = yield* inspect
            for (let count = 1; count <= 24; count++)
              yield* storage.commit([
                {
                  _tag: 'document.change',
                  id: historyId,
                  content: {
                    _tag: 'delta',
                    version: 1,
                    ops: [['set', ['count'], count]],
                  },
                },
              ])
            const deltas = yield* inspect
            // A megabyte base must not be serialized again for a few-byte counter patch.
            assert.isBelow(deltas.written - initial.written, 32 * 1024)
            assert.isBelow(deltas.retained, 3 * image.length + 32 * 1024)
            for (let step = 1; step <= 8; step++) {
              yield* replace(storage, step)
              assert.isBelow((yield* inspect).retained, 3 * image.length + 64 * 1024)
            }
            yield* storage.commit([{ _tag: 'document.retire', id: currentId }])
            yield* storage.commit([
              {
                _tag: 'task',
                value: {
                  ...checkpoint(8),
                  state: { status: 'terminal', outcome: { status: 'completed', result: null } },
                },
              },
            ])
            yield* storage.commit([{ _tag: 'document.retire', id: historyId }])
            const retired = yield* inspect
            assert.isBelow(retired.retained, image.length + 64 * 1024)
            console.info(JSON.stringify({ backend, initial, deltas, retired }))
            assert.strictEqual(
              Option.getOrThrow(yield* storage.document(historyId, first)).value.count,
              0,
            )
            // Copying a retired historical document at a live sequence remains valid.
            yield* storage.commit([
              {
                _tag: 'document.copy',
                record: {
                  id: Record.DocumentId.make(5),
                  kind: 'history',
                  scope: { _tag: 'conversation', conversationId: root },
                  history: 'rewindable',
                  fork: 'asOf',
                },
                source: { id: historyId, at: first },
              },
            ])
          }).pipe(Effect.provide(layer), Effect.provideService(FileSystem.FileSystem, measuredFs))
          yield* Effect.gen(function* () {
            const storage = yield* Storage.Storage
            assert.isTrue(Option.isNone(yield* storage.document(currentId)))
            assert.strictEqual(
              Option.getOrThrow(yield* storage.document(Record.DocumentId.make(5))).value.count,
              0,
            )
            assert.strictEqual(
              Option.getOrThrow(yield* storage.task(taskId)).state.status,
              'terminal',
            )
            assert.strictEqual(yield* storage.mintId(), 6)
            yield* storage.commit([
              {
                _tag: 'document.change',
                id: Record.DocumentId.make(5),
                content: {
                  _tag: 'delta',
                  version: 1,
                  ops: [['set', ['count'], 99]],
                },
              },
            ])
          }).pipe(Effect.provide(layer))
          yield* Effect.gen(function* () {
            const storage = yield* Storage.Storage
            assert.strictEqual(
              Option.getOrThrow(yield* storage.document(Record.DocumentId.make(5))).value.count,
              99,
            )
          }).pipe(Effect.provide(layer))
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
  )

for (const point of [
  'sidecar-write',
  'sidecar-sync',
  'directory-sync',
  'marker-partial',
  'marker-sync',
  'reclaim',
  'reclaim-partial',
  'reclaim-sync',
] as const)
  it.live(
    `JSONL crash boundary ${point} preserves a complete old or new commit and reclaims orphans on reopen`,
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const { fs, filename } = yield* fixture
          const layer = Storage.layerJsonl({ filePath: filename })
          yield* Effect.service(Storage.Storage).pipe(Effect.flatMap(seed), Effect.provide(layer))
          const fault = yield* fs.readFile(`${filename}.missing`).pipe(Effect.flip)
          let armed = false
          let directorySyncs = 0
          let removed = 0
          const injected: FileSystem.FileSystem = {
            ...fs,
            remove: (path, options) =>
              Effect.suspend(() => {
                if (
                  armed &&
                  (point === 'reclaim' || (point === 'reclaim-partial' && ++removed === 2))
                )
                  return Effect.fail(fault)
                return fs.remove(path, options)
              }),
            open: (path, options) =>
              fs.open(path, options).pipe(
                Effect.map((file) => ({
                  ...file,
                  truncate: file.truncate,
                  sync: Effect.suspend(() => {
                    if (!armed) return file.sync
                    if (path === `${filename}.parts`) {
                      directorySyncs++
                      if (
                        (point === 'directory-sync' && directorySyncs === 1) ||
                        (point === 'reclaim-sync' && directorySyncs === 2)
                      )
                        return Effect.fail(fault)
                    }
                    if (
                      (point === 'sidecar-sync' && path.endsWith('.row')) ||
                      (point === 'marker-sync' && path === filename)
                    )
                      return file.sync.pipe(Effect.andThen(Effect.fail(fault)))
                    return file.sync
                  }),
                  writeAll: (bytes) => {
                    if (
                      armed &&
                      ((point === 'marker-partial' && path === filename) ||
                        (point === 'sidecar-write' && path.endsWith('.row')))
                    )
                      return file
                        .writeAll(bytes.subarray(0, 12))
                        .pipe(Effect.andThen(Effect.fail(fault)))
                    return file.writeAll(bytes)
                  },
                })),
              ),
          }
          yield* Effect.gen(function* () {
            const storage = yield* Storage.Storage
            armed = true
            assert.strictEqual((yield* replace(storage, 1).pipe(Effect.flip)).reason, 'uncertain')
            assert.strictEqual((yield* storage.mintId().pipe(Effect.flip)).reason, 'uncertain')
            assert.strictEqual(
              (yield* storage.document(currentId).pipe(Effect.flip)).reason,
              'uncertain',
            )
          }).pipe(Effect.provide(layer), Effect.provideService(FileSystem.FileSystem, injected))
          const committed =
            point === 'marker-sync' ||
            point === 'reclaim' ||
            point === 'reclaim-partial' ||
            point === 'reclaim-sync'
          yield* Effect.gen(function* () {
            const storage = yield* Storage.Storage
            assert.strictEqual(
              Option.getOrThrow(yield* storage.document(currentId)).value.count,
              committed ? 1 : 0,
            )
            const task = Option.getOrThrow(yield* storage.task(taskId))
            assert.strictEqual(task.state.status, 'pending')
            if (task.state.status === 'pending')
              assert.deepEqual(task.state.checkpoint, {
                phase: 'image',
                image,
                step: committed ? 1 : 0,
              })
            assert.isBelow(yield* retainedJsonl(fs, filename), 3 * image.length + 16 * 1024)
            assert.strictEqual(yield* replace(storage, 2), committed ? 3 : 2)
          }).pipe(Effect.provide(layer))
          yield* Effect.gen(function* () {
            const storage = yield* Storage.Storage
            assert.strictEqual(Option.getOrThrow(yield* storage.document(currentId)).value.count, 2)
          }).pipe(Effect.provide(layer))
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
  )

for (const recoveryFails of [false, true])
  it.live(
    `JSONL recovery ${recoveryFails ? 'sync failure preserves old sidecars' : 'syncs complete markers before reclamation'} through a second crash`,
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const { fs, filename } = yield* fixture
          const layer = Storage.layerJsonl({ filePath: filename })
          yield* Effect.service(Storage.Storage).pipe(Effect.flatMap(seed), Effect.provide(layer))
          let durable = yield* fs.readFile(filename)
          const oldFiles = yield* fs.readDirectory(`${filename}.parts`)
          const fault = yield* fs.readFile(`${filename}.missing`).pipe(Effect.flip)
          let phase: 'opening' | 'commit' | 'recovery' = 'opening'
          let recoverySynced = false
          const injected: FileSystem.FileSystem = {
            ...fs,
            open: (path, options) =>
              fs.open(path, options).pipe(
                Effect.map((file) => ({
                  ...file,
                  writeAll: (bytes) => file.writeAll(bytes),
                  truncate: file.truncate,
                  sync: Effect.suspend(() => {
                    if (path !== filename) return file.sync
                    if (phase === 'commit' || (phase === 'recovery' && recoveryFails))
                      return Effect.fail(fault)
                    return file.sync.pipe(
                      Effect.andThen(fs.readFile(filename)),
                      Effect.map((bytes) => {
                        durable = bytes
                        if (phase === 'recovery') recoverySynced = true
                      }),
                    )
                  }),
                })),
              ),
          }
          yield* Effect.gen(function* () {
            const storage = yield* Storage.Storage
            phase = 'commit'
            assert.strictEqual((yield* replace(storage, 1).pipe(Effect.flip)).reason, 'uncertain')
          }).pipe(Effect.provide(layer), Effect.provideService(FileSystem.FileSystem, injected))
          assert.isAbove((yield* fs.readFile(filename)).length, durable.length)
          phase = 'recovery'
          const reopened = Effect.gen(function* () {
            const storage = yield* Storage.Storage
            assert.strictEqual(Option.getOrThrow(yield* storage.document(currentId)).value.count, 1)
          }).pipe(Effect.provide(layer), Effect.provideService(FileSystem.FileSystem, injected))
          if (recoveryFails) {
            assert.strictEqual((yield* reopened.pipe(Effect.flip)).reason, 'io')
            for (const name of oldFiles)
              assert.isTrue(yield* fs.exists(`${filename}.parts/${name}`))
          } else yield* reopened
          // Discard all bytes after the last successful fsync, as a second power loss can.
          yield* fs.writeFile(filename, durable)
          yield* Effect.gen(function* () {
            const storage = yield* Storage.Storage
            assert.strictEqual(
              Option.getOrThrow(yield* storage.document(currentId)).value.count,
              recoveryFails ? 0 : 1,
            )
            assert.strictEqual(yield* replace(storage, 2), recoveryFails ? 2 : 3)
          }).pipe(Effect.provide(layer))
          assert.strictEqual(recoverySynced, !recoveryFails)
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
  )

it.live(
  'JSONL missing committed sidecars fail closed without deleting evidence or truncating the tail',
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { fs, filename } = yield* fixture
        const layer = Storage.layerJsonl({ filePath: filename })
        yield* Effect.service(Storage.Storage).pipe(Effect.flatMap(seed), Effect.provide(layer))
        yield* fs.remove(`${filename}.parts/3-1.revision`)
        yield* fs.writeFileString(filename, '{torn', { flag: 'a' })
        const before = yield* fs.readFile(filename)
        const files = yield* fs.readDirectory(`${filename}.parts`)
        const error = yield* Effect.service(Storage.Storage).pipe(
          Effect.provide(layer),
          Effect.flip,
        )
        assert.strictEqual(error.reason, 'corrupt')
        assert.deepEqual(yield* fs.readFile(filename), before)
        assert.deepEqual(yield* fs.readDirectory(`${filename}.parts`), files)
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
)

it.live('rejects legacy physical layouts without migration or content changes', () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { fs, filename } = yield* fixture
      const legacy = '{"metadata":{"format":1,"nextId":2,"nextSeq":1},"rows":[]}\n'
      yield* fs.writeFileString(filename, legacy)
      const error = yield* Effect.service(Storage.Storage).pipe(
        Effect.provide(Storage.layerJsonl({ filePath: filename })),
        Effect.flip,
      )
      assert.strictEqual(error.reason, 'unsupportedFormat')
      assert.strictEqual(yield* fs.readFileString(filename), legacy)
      assert.isFalse(yield* fs.exists(`${filename}.parts`))
      yield* Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient
        yield* sql`CREATE TABLE effect_harness_metadata(singleton INTEGER PRIMARY KEY, format INTEGER, next_id INTEGER, next_seq INTEGER)`
        yield* sql`INSERT INTO effect_harness_metadata VALUES(1,1,2,1)`
        const before = yield* sql`SELECT * FROM effect_harness_metadata`
        const failed = yield* Layer.build(Storage.layerSql).pipe(Effect.flip)
        assert.strictEqual(failed.reason, 'unsupportedFormat')
        assert.deepEqual(yield* sql`SELECT * FROM effect_harness_metadata`, before)
        assert.lengthOf(
          yield* sql`SELECT name FROM sqlite_master WHERE name='effect_harness_revisions'`,
          0,
        )
      }).pipe(Effect.provide(SqliteClient.layer({ filename: ':memory:' })))
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
)

it.live(
  'SQL revision keys and ownership reject impossible histories, and failed pruning rolls back atomically',
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient
        const storage = Context.get(yield* Layer.build(Storage.layerSql), Storage.Storage)
        yield* sql`PRAGMA foreign_keys=ON`
        yield* seed(storage)
        for (const [id, seq] of [
          [3, 1],
          [3, 0],
          [999, 1],
        ])
          yield* sql`INSERT INTO effect_harness_revisions VALUES(${id},${seq},'{}')`.pipe(
            Effect.flip,
          )
        yield* sql`CREATE TRIGGER reject_revision BEFORE INSERT ON effect_harness_revisions
      WHEN NEW.seq = 2 BEGIN SELECT RAISE(ABORT, 'injected failure after pruning'); END`
        assert.strictEqual((yield* replace(storage, 1).pipe(Effect.flip)).reason, 'uncertain')
        const reopened = Context.get(yield* Layer.build(Storage.layerSql), Storage.Storage)
        assert.deepEqual(Option.getOrThrow(yield* reopened.document(currentId)).value, {
          image,
          count: 0,
        })
        assert.strictEqual(
          (yield* sql`SELECT COUNT(*) AS n FROM effect_harness_revisions WHERE document_id=3`)[0]
            ?.n,
          1,
        )
        yield* sql`DROP TRIGGER reject_revision`
        assert.strictEqual(yield* replace(reopened, 2), 2)
      }),
    ).pipe(Effect.provide(SqliteClient.layer({ filename: ':memory:' }))),
)

it.live('a corrupt JSONL prune marker cannot erase rewindable history during recovery', () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { fs, filename } = yield* fixture
      const layer = Storage.layerJsonl({ filePath: filename })
      yield* Effect.gen(function* () {
        const storage = yield* Storage.Storage
        yield* seed(storage)
        yield* storage.commit([
          {
            _tag: 'document.change',
            id: historyId,
            content: {
              _tag: 'delta',
              version: 1,
              ops: [['set', ['count'], 1]],
            },
          },
        ])
      }).pipe(Effect.provide(layer))
      const lines = (yield* fs.readFileString(filename)).trimEnd().split('\n')
      const frame = yield* Schema.decodeEffect(Schema.fromJsonString(Schema.JsonObject))(lines[1]!)
      yield* fs.writeFileString(
        filename,
        `${lines[0]}\n${JSON.stringify({
          ...frame,
          rows: [{ id: 2, kind: 'document', seq: 2, firstRevision: 2, revisions: [2] }],
        })}\n`,
      )
      const before = yield* fs.readFile(filename)
      const base = yield* fs.readFile(`${filename}.parts/2-1.revision`)
      assert.strictEqual(
        (yield* Effect.service(Storage.Storage).pipe(Effect.provide(layer), Effect.flip)).reason,
        'corrupt',
      )
      assert.deepEqual(yield* fs.readFile(filename), before)
      assert.deepEqual(yield* fs.readFile(`${filename}.parts/2-1.revision`), base)
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
)

for (const scope of ['session', 'latest'] as const)
  for (const boundary of ['delta', 'false-retirement', 'retired-opaque', 'live-opaque'] as const)
    it.live(`JSONL ${scope} ${boundary} validates pruning before deleting prior evidence`, () =>
      Effect.scoped(
        Effect.gen(function* () {
          const { fs, filename } = yield* fixture
          const layer = Storage.layerJsonl({ filePath: filename })
          yield* Effect.gen(function* () {
            const storage = yield* Storage.Storage
            yield* storage.commit([
              { _tag: 'conversation', value: { id: root } },
              {
                _tag: 'document.create',
                record: {
                  id: historyId,
                  kind: 'prune',
                  ...(scope === 'session'
                    ? { scope: { _tag: 'session' as const } }
                    : {
                        scope: { _tag: 'conversation' as const, conversationId: root },
                        history: 'latest' as const,
                        fork: 'current' as const,
                      }),
                },
                content: { _tag: 'base', version: 1, value: { count: 0 } },
              },
            ])
            yield* storage.commit([
              {
                _tag: 'document.change',
                id: historyId,
                content: { _tag: 'delta', version: 1, ops: [['set', ['count'], 1]] },
              },
            ])
            if (boundary === 'live-opaque')
              yield* storage.commit([
                {
                  _tag: 'document.change',
                  id: historyId,
                  content: { _tag: 'base', version: 2, value: { count: 2 } },
                },
              ])
            if (boundary === 'retired-opaque')
              yield* storage.commit([{ _tag: 'document.retire', id: historyId }])
          }).pipe(Effect.provide(layer))
          if (boundary === 'live-opaque') {
            yield* fs.writeFileString(
              `${filename}.parts/2-3.revision`,
              JSON.stringify({
                seq: 3,
                content: PackedBody.empty({
                  _tag: 'base',
                  version: 2,
                  value: 'obsolete codec payload',
                }),
              }),
            )
            yield* Effect.gen(function* () {
              const storage = yield* Storage.Storage
              yield* storage.commit([{ _tag: 'document.retire', id: historyId }])
            }).pipe(Effect.provide(layer))
            assert.isFalse(yield* fs.exists(`${filename}.parts/2-3.revision`))
            return
          }
          if (boundary === 'retired-opaque') {
            // Interrupted cleanup may leave obsolete bodies that cannot be decoded.
            yield* fs.writeFileString(`${filename}.parts/2-1.revision`, '{malformed obsolete body')
            yield* Effect.gen(function* () {
              const storage = yield* Storage.Storage
              assert.isTrue(Option.isNone(yield* storage.document(historyId)))
            }).pipe(Effect.provide(layer))
            assert.isFalse(yield* fs.exists(`${filename}.parts/2-1.revision`))
            return
          }
          const lines = (yield* fs.readFileString(filename)).trimEnd().split('\n')
          const frame = yield* Schema.decodeEffect(Schema.fromJsonString(Schema.JsonObject))(
            lines[1]!,
          )
          yield* fs.writeFileString(
            filename,
            `${lines[0]}\n${JSON.stringify({
              ...frame,
              rows: [
                {
                  id: 2,
                  kind: 'document',
                  seq: 2,
                  firstRevision: boundary === 'delta' ? 2 : null,
                  revisions: boundary === 'delta' ? [2] : [],
                },
              ],
            })}\n`,
          )
          const before = yield* fs.readFile(filename)
          const base = yield* fs.readFile(`${filename}.parts/2-1.revision`)
          assert.strictEqual(
            (yield* Effect.service(Storage.Storage).pipe(Effect.provide(layer), Effect.flip))
              .reason,
            'corrupt',
          )
          assert.deepEqual(yield* fs.readFile(filename), before)
          assert.deepEqual(yield* fs.readFile(`${filename}.parts/2-1.revision`), base)
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
    )

it.live('JSONL does not recreate a lost journal or erase its surviving sidecars', () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { fs, filename } = yield* fixture
      const layer = Storage.layerJsonl({ filePath: filename })
      yield* Effect.service(Storage.Storage).pipe(Effect.flatMap(seed), Effect.provide(layer))
      const before = yield* fs.readDirectory(`${filename}.parts`)
      yield* fs.remove(filename)
      assert.strictEqual(
        (yield* Effect.service(Storage.Storage).pipe(Effect.provide(layer), Effect.flip)).reason,
        'corrupt',
      )
      assert.isFalse(yield* fs.exists(filename))
      assert.deepEqual(yield* fs.readDirectory(`${filename}.parts`), before)
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
)
