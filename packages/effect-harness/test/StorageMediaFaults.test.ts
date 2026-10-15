import { assert, it } from '@effect/vitest'
import { vi } from 'vitest'
import { NodeServices } from '@effect/platform-node'
import * as SqliteClient from '@effect/sql-sqlite-node/SqliteClient'
import * as Effect from 'effect/Effect'
import * as Context from 'effect/Context'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Result from 'effect/Result'
import * as Schema from 'effect/Schema'
import * as SqlClient from 'effect/sql/SqlClient'
import * as SqlError from 'effect/sql/SqlError'
import * as Statement from 'effect/sql/Statement'
import * as Reactivity from 'effect/reactivity/Reactivity'
import * as Prompt from 'effect/ai/Prompt'
import * as Storage from 'effect-harness/Storage'
import * as Record from 'effect-harness/Record'
import * as Sequence from 'effect-harness/Sequence'
import * as Wire from 'effect-harness/PromptWire'
import * as Memory from '../src/internal/Memory.js'
import * as Kernel from '../src/internal/Kernel.js'

const root = Record.ROOT_CONVERSATION_ID
const id = Record.DocumentId.make(2)
const body = (byte: number): Record.Content => ({
  _tag: 'base',
  version: 1,
  value: {
    request: {
      prompt: Schema.encodeSync(Schema.toCodecJson(Wire.Prompt))(
        Prompt.fromMessages([
          Prompt.userMessage({
            content: [
              Prompt.filePart({ mediaType: 'image/png', data: new Uint8Array(8192).fill(byte) }),
            ],
          }),
        ]),
      ),
    },
  },
})
const seed = (storage: Storage.Storage['Service']) =>
  storage.commit([
    { _tag: 'conversation', value: { id: root } },
    {
      _tag: 'document.create',
      record: { id, kind: 'harness.request', scope: { _tag: 'session' } },
      content: body(1),
    },
  ])
const replace = (storage: Storage.Storage['Service']) =>
  storage.commit([{ _tag: 'document.change', id, content: body(2) }])
const steps = [
  'object.write',
  'object.sync',
  'media.sync',
  'row.write',
  'row.sync',
  'revision.write',
  'revision.sync',
  'parts.sync',
  'marker.write',
  'marker.sync',
  'sidecar.delete',
  'object.delete',
] as const
for (const step of steps)
  for (const timing of ['before', 'after'] as const)
    it.live(
      `JSONL ${timing} ${step}: reopen selects a complete state and sweeps only after prefix sync`,
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const fs = yield* FileSystem.FileSystem
            const directory = yield* fs.makeTempDirectoryScoped({ prefix: 'media-fault-' })
            const filename = `${directory}/journal`
            const layer = Storage.layerJsonl({ filePath: filename })
            yield* Storage.Storage.pipe(Effect.flatMap(seed), Effect.provide(layer))
            const fault = yield* fs.readFile(`${directory}/absent`).pipe(Effect.flip)
            let armed = false
            let fired = false
            const inject = <A, E, R>(name: string, effect: Effect.Effect<A, E, R>) =>
              Effect.suspend(() => {
                if (!armed || fired || name !== step) return effect
                fired = true
                return timing === 'before'
                  ? Effect.fail(fault)
                  : effect.pipe(Effect.andThen(Effect.fail(fault)))
              })
            const label = (path: string): string => {
              if (path === filename) return 'marker'
              if (path === `${filename}.parts`) return 'parts'
              if (path === `${filename}.media`) return 'media'
              if (path.startsWith(`${filename}.media/`)) return 'object'
              if (path.endsWith('.revision')) return 'revision'
              return 'row'
            }
            const injected: FileSystem.FileSystem = {
              ...fs,
              open: (path, options) =>
                fs.open(path, options).pipe(
                  Effect.map((file) => ({
                    ...file,
                    writeAll: (bytes) => inject(`${label(path)}.write`, file.writeAll(bytes)),
                    sync: inject(`${label(path)}.sync`, file.sync),
                  })),
                ),
              remove: (path, options) =>
                inject(
                  path.startsWith(`${filename}.media/`) ? 'object.delete' : 'sidecar.delete',
                  fs.remove(path, options),
                ),
            }
            yield* Effect.gen(function* () {
              const storage = yield* Storage.Storage
              armed = true
              const failure = yield* replace(storage).pipe(Effect.flip)
              assert.strictEqual(failure.reason, 'uncertain')
              assert.isTrue(fired)
              assert.strictEqual((yield* storage.mintId().pipe(Effect.flip)).reason, 'uncertain')
            }).pipe(Effect.provide(layer), Effect.provideService(FileSystem.FileSystem, injected))
            let transientObjects = 0
            for (const name of yield* fs.readDirectory(`${filename}.media`))
              transientObjects += Number((yield* fs.stat(`${filename}.media/${name}`)).size)
            assert.isAtMost(transientObjects, 2 * 8192)
            const recovery: Array<string> = []
            const observed: FileSystem.FileSystem = {
              ...fs,
              open: (path, options) =>
                fs.open(path, options).pipe(
                  Effect.map((file) => ({
                    ...file,
                    sync: file.sync.pipe(
                      Effect.tap(() =>
                        Effect.sync(() => {
                          if (path === filename) recovery.push('prefix.sync')
                        }),
                      ),
                    ),
                  })),
                ),
              remove: (path, options) =>
                fs.remove(path, options).pipe(
                  Effect.tap(() =>
                    Effect.sync(() => {
                      recovery.push('remove')
                    }),
                  ),
                ),
            }
            for (let reopen = 0; reopen < 2; reopen++)
              yield* Effect.gen(function* () {
                const storage = yield* Storage.Storage
                const value = Option.getOrThrow(yield* storage.document(id)).value
                const expected = [body(1), body(2)].map((item) =>
                  item._tag === 'base' ? item.value : {},
                )
                assert.isTrue(
                  expected.some((item) => JSON.stringify(item) === JSON.stringify(value)),
                )
                assert.strictEqual((yield* fs.readDirectory(`${filename}.media`)).length, 1)
              }).pipe(Effect.provide(layer), Effect.provideService(FileSystem.FileSystem, observed))
            assert.strictEqual(recovery[0], 'prefix.sync')
          }),
        ).pipe(Effect.provide(NodeServices.layer)),
    )

for (const corruption of ['missing', 'bytes', 'manifest'] as const)
  it.live(`JSONL ${corruption} refuses recovery without reclamation`, () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem
        const directory = yield* fs.makeTempDirectoryScoped({ prefix: 'media-corruption-' })
        const filename = `${directory}/journal`
        const layer = Storage.layerJsonl({ filePath: filename })
        yield* Storage.Storage.pipe(Effect.flatMap(seed), Effect.provide(layer))
        const object = (yield* fs.readDirectory(`${filename}.media`))[0]!
        if (corruption === 'missing') yield* fs.remove(`${filename}.media/${object}`)
        else if (corruption === 'bytes')
          yield* fs.writeFile(`${filename}.media/${object}`, new Uint8Array(8192).fill(9))
        else {
          const path = `${filename}.parts/2-1.revision`
          const json = JSON.parse(yield* fs.readFileString(path)) as {
            content: { media: Array<{ path: Array<string | number> }> }
          }
          json.content.media[0]!.path = ['__proto__', 'stolen']
          yield* fs.writeFileString(path, JSON.stringify(json))
        }
        const before = yield* fs.readFile(filename)
        const sidecars = yield* fs.readDirectory(`${filename}.parts`)
        const result = yield* Storage.Storage.pipe(Effect.provide(layer), Effect.flip)
        assert.strictEqual(result.reason, 'corrupt')
        assert.deepEqual(yield* fs.readFile(filename), before)
        assert.deepEqual(yield* fs.readDirectory(`${filename}.parts`), sidecars)
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  )

// Recovery must establish complete ownership before classifying any object as garbage.
for (const owner of ['entry', 'base', 'delta', 'history', 'retired-history'] as const)
  for (const corruption of ['empty', 'missing', 'partial'] as const)
    it.live(
      `JSONL ${owner} ${corruption} manifest preserves evidence until metadata is repaired`,
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const fs = yield* FileSystem.FileSystem
            const directory = yield* fs.makeTempDirectoryScoped({ prefix: 'media-coverage-' })
            const filename = `${directory}/journal`
            const layer = Storage.layerJsonl({ filePath: filename })
            const message = {
              role: 'user',
              content: ['AQID', 'BAUG'].map((data) => ({
                type: 'file',
                mediaType: 'image/png',
                data,
              })),
            }
            const value = { request: { prompt: { content: [message] } } }
            const history = owner === 'history' || owner === 'retired-history'
            yield* Effect.gen(function* () {
              const storage = yield* Storage.Storage
              yield* storage.commit([
                { _tag: 'conversation', value: { id: root } },
                ...(owner === 'entry'
                  ? [
                      {
                        _tag: 'entry' as const,
                        value: {
                          id: Record.EntryId.make(2),
                          conversationId: root,
                          kind: 'user',
                          model: [message],
                        },
                      },
                    ]
                  : [
                      {
                        _tag: 'document.create' as const,
                        record: history
                          ? {
                              id,
                              kind: 'harness.request',
                              scope: { _tag: 'conversation' as const, conversationId: root },
                              history: 'rewindable' as const,
                              fork: 'asOf' as const,
                            }
                          : { id, kind: 'harness.request', scope: { _tag: 'session' as const } },
                        content: {
                          _tag: 'base' as const,
                          version: 1,
                          value: owner === 'delta' ? {} : value,
                        },
                      },
                    ]),
              ])
              if (owner === 'delta')
                yield* storage.commit([
                  {
                    _tag: 'document.change',
                    id,
                    content: {
                      _tag: 'delta',
                      version: 1,
                      ops: [['set', ['request'], value.request]],
                    },
                  },
                ])
              if (history)
                yield* storage.commit([
                  { _tag: 'document.change', id, content: { _tag: 'base', version: 1, value: {} } },
                ])
              if (owner === 'retired-history')
                yield* storage.commit([{ _tag: 'document.retire', id }])
            }).pipe(Effect.provide(layer))
            const path = `${filename}.parts/2-${owner === 'delta' ? 2 : 1}.${owner === 'entry' ? 'row' : 'revision'}`
            const original = yield* fs.readFileString(path)
            const json = JSON.parse(original) as {
              value?: { media?: Array<unknown> }
              content?: { media?: Array<unknown> }
            }
            const packed = (owner === 'entry' ? json.value : json.content)!
            assert.strictEqual(packed.media!.length, 2)
            if (corruption === 'missing') delete packed.media
            else packed.media = corruption === 'empty' ? [] : packed.media!.slice(0, 1)
            const read = Effect.gen(function* () {
              const storage = yield* Storage.Storage
              if (owner === 'entry')
                assert.deepEqual(
                  Option.getOrThrow(yield* storage.entry(Record.EntryId.make(2))).entry.model,
                  [message],
                )
              else
                assert.deepEqual(
                  Option.getOrThrow(
                    yield* storage.document(id, history ? Sequence.make(1) : undefined),
                  ).value,
                  value,
                )
            }).pipe(Effect.provide(layer))
            // Negative control: identical corruption repaired before recovery still reads exactly.
            yield* fs.writeFileString(path, JSON.stringify(json))
            yield* fs.writeFileString(path, original)
            yield* read
            yield* fs.writeFileString(path, JSON.stringify(json))
            const before = yield* fs.readFile(filename)
            const names = (yield* fs.readDirectory(`${filename}.media`)).sort()
            const evidence = yield* Effect.forEach(names, (name) =>
              fs.readFile(`${filename}.media/${name}`),
            )
            const sidecars = (yield* fs.readDirectory(`${filename}.parts`)).sort()
            const failure = yield* Storage.Storage.pipe(Effect.provide(layer), Effect.result)
            assert.deepEqual(yield* fs.readFile(filename), before)
            assert.deepEqual((yield* fs.readDirectory(`${filename}.parts`)).sort(), sidecars)
            assert.deepEqual((yield* fs.readDirectory(`${filename}.media`)).sort(), names)
            assert.deepEqual(
              yield* Effect.forEach(names, (name) => fs.readFile(`${filename}.media/${name}`)),
              evidence,
            )
            assert.isTrue(Result.isFailure(failure))
            if (Result.isFailure(failure)) assert.strictEqual(failure.failure.reason, 'corrupt')
            yield* fs.writeFileString(path, original)
            // The same repair must work after the refused recovery, including retained asOf data.
            yield* read
            yield* read
          }),
        ).pipe(Effect.provide(NodeServices.layer)),
    )

for (const history of [false, true])
  it.live(
    `JSONL complete ownership permits opaque ${history ? 'retired history' : 'current body'} and safe orphan cleanup`,
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem
          const directory = yield* fs.makeTempDirectoryScoped({ prefix: 'media-opaque-' })
          const filename = `${directory}/journal`
          const layer = Storage.layerJsonl({ filePath: filename })
          yield* Effect.gen(function* () {
            const storage = yield* Storage.Storage
            yield* storage.commit([
              { _tag: 'conversation', value: { id: root } },
              {
                _tag: 'document.create',
                record: history
                  ? {
                      id,
                      kind: 'harness.request',
                      scope: { _tag: 'conversation', conversationId: root },
                      history: 'rewindable',
                      fork: 'asOf',
                    }
                  : { id, kind: 'harness.request', scope: { _tag: 'session' } },
                content: body(1),
              },
            ])
            yield* replace(storage)
            if (history) yield* storage.commit([{ _tag: 'document.retire', id }])
          }).pipe(Effect.provide(layer))
          const path = `${filename}.parts/2-${history ? 1 : 2}.revision`
          const original = yield* fs.readFileString(path)
          const malformed = JSON.parse(original) as {
            content: { body: { _tag: string; ops?: string } }
          }
          // Keep media slots and ownership intact but invalidate the document codec.
          malformed.content.body._tag = 'delta'
          malformed.content.body.ops = 'unreadable obsolete operations'
          yield* fs.writeFileString(path, JSON.stringify(malformed))
          const orphan = `${filename}.media/b-${'0'.repeat(64)}`
          yield* fs.writeFile(orphan, new Uint8Array([99]))
          // These sidecars are excluded by the durable marker, regardless of their contents.
          const obsolete = `${filename}.parts/2-9999.revision`
          yield* fs.writeFileString(obsolete, '{malformed pruned payload')
          for (let reopen = 0; reopen < 2; reopen++)
            yield* Effect.gen(function* () {
              const storage = yield* Storage.Storage
              assert.strictEqual(Option.isSome(yield* storage.documentMetadata!(id)), !history)
              assert.isFalse(yield* fs.exists(orphan))
              assert.isFalse(yield* fs.exists(obsolete))
              assert.strictEqual(
                (yield* fs.readDirectory(`${filename}.media`)).length,
                history ? 2 : 1,
              )
              if (!history && reopen === 1) {
                // Retirement must not invoke the obsolete body's codec.
                yield* storage.commit([{ _tag: 'document.retire', id }])
                assert.strictEqual((yield* fs.readDirectory(`${filename}.media`)).length, 0)
              }
            }).pipe(Effect.provide(layer))
          if (history) {
            yield* fs.writeFileString(path, original)
            yield* Effect.gen(function* () {
              const storage = yield* Storage.Storage
              const expected = body(1)
              assert.strictEqual(expected._tag, 'base')
              if (expected._tag === 'base')
                assert.deepEqual(
                  Option.getOrThrow(yield* storage.document(id, Sequence.make(1))).value,
                  expected.value,
                )
            }).pipe(Effect.provide(layer))
          }
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
  )

for (const corruption of ['missing', 'invalid', 'mismatch', 'old-layout'] as const)
  it.live(`JSONL ${corruption} ownership commitment refuses all recovery mutations`, () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem
        const directory = yield* fs.makeTempDirectoryScoped({ prefix: 'media-marker-' })
        const filename = `${directory}/journal`
        const layer = Storage.layerJsonl({ filePath: filename })
        yield* Storage.Storage.pipe(Effect.flatMap(seed), Effect.provide(layer))
        const original = yield* fs.readFileString(filename)
        const marker = JSON.parse(original) as {
          persistence: string
          rows: Array<{ ownership?: string }>
        }
        if (corruption === 'missing') delete marker.rows[1]!.ownership
        else if (corruption === 'invalid') marker.rows[1]!.ownership = 'invalid'
        else if (corruption === 'mismatch') marker.rows[1]!.ownership = '0'.repeat(64)
        else marker.persistence = 'sidecars-v2'
        const damaged = JSON.stringify(marker) + '\n'
        yield* fs.writeFileString(filename, damaged)
        const files = (yield* fs.readDirectory(`${filename}.media`)).sort()
        const parts = (yield* fs.readDirectory(`${filename}.parts`)).sort()
        const failure = yield* Storage.Storage.pipe(Effect.provide(layer), Effect.flip)
        assert.strictEqual(
          failure.reason,
          corruption === 'old-layout' ? 'unsupportedFormat' : 'corrupt',
        )
        assert.strictEqual(yield* fs.readFileString(filename), damaged)
        assert.deepEqual((yield* fs.readDirectory(`${filename}.media`)).sort(), files)
        assert.deepEqual((yield* fs.readDirectory(`${filename}.parts`)).sort(), parts)
        yield* fs.writeFileString(filename, original)
        yield* Effect.gen(function* () {
          const storage = yield* Storage.Storage
          assert.isTrue(Option.isSome(yield* storage.document(id)))
        }).pipe(Effect.provide(layer))
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  )

it.effect('hash collisions fail closed without publishing rows or replacing shared bytes', () =>
  Effect.scoped(
    Effect.gen(function* () {
      const collision = vi
        .spyOn(globalThis.crypto.subtle, 'digest')
        .mockResolvedValue(new ArrayBuffer(32))
      yield* Effect.addFinalizer(() => Effect.sync(() => collision.mockRestore()))
      const access = yield* Memory.makeAccess
      const storage = yield* Kernel.make(access)
      yield* seed(storage)
      const before = yield* access.snapshot
      const error = yield* replace(storage).pipe(Effect.flip)
      assert.strictEqual(error.operation, 'media.integrity')
      assert.strictEqual(yield* access.snapshot, before)
    }),
  ),
)

it.live(
  'SQL rollback leaves old owners and objects intact; another writer can then replace them',
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient
        const context = yield* Layer.build(Storage.layerSql)
        const storage = Context.get(context, Storage.Storage)
        yield* seed(storage)
        yield* sql`CREATE TRIGGER fail_media BEFORE INSERT ON effect_harness_media_owners BEGIN SELECT RAISE(ABORT,'injected owner failure'); END`
        assert.strictEqual((yield* replace(storage).pipe(Effect.flip)).reason, 'uncertain')
        assert.strictEqual((yield* sql`SELECT COUNT(*) AS n FROM effect_harness_media`)[0]?.n, 1)
        yield* sql`DROP TRIGGER fail_media`
        yield* Storage.Storage.pipe(Effect.flatMap(replace), Effect.provide(Storage.layerSql))
        assert.strictEqual((yield* sql`SELECT COUNT(*) AS n FROM effect_harness_media`)[0]?.n, 1)
      }),
    ).pipe(Effect.provide(SqliteClient.layer({ filename: ':memory:' }))),
)

it.live('independent SQL writers share images and retain a consistent read closure during GC', () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: 'media-sql-writers-' })
      const filename = `${directory}/store.sqlite`
      const client = yield* Layer.build(SqliteClient.layer({ filename }))
      const open = () => Layer.build(Storage.layerSql).pipe(Effect.provideContext(client))
      const first = Context.get(yield* open(), Storage.Storage)
      yield* seed(first)
      const second = Context.get(yield* open(), Storage.Storage)
      const other = Record.DocumentId.make(3)
      yield* second.commit([
        {
          _tag: 'document.create',
          record: { id: other, kind: 'harness.request', key: 'other', scope: { _tag: 'session' } },
          content: body(1),
        },
      ])
      yield* Effect.all(
        [
          Effect.forEach(
            Array.from({ length: 12 }, (_, i) => i),
            (i) => first.commit([{ _tag: 'document.change', id, content: body((i % 2) + 1) }]),
            { discard: true },
          ),
          Effect.forEach(
            Array.from({ length: 12 }, (_, i) => i),
            (i) =>
              second.commit([{ _tag: 'document.change', id: other, content: body((i % 2) + 1) }]),
            { discard: true },
          ),
          Effect.forEach(
            Array.from({ length: 24 }),
            () =>
              Effect.gen(function* () {
                const document = Option.getOrThrow(yield* second.document(id))
                assert.isTrue(
                  [body(1), body(2)].some(
                    (expected) =>
                      expected._tag === 'base' &&
                      JSON.stringify(document.value) === JSON.stringify(expected.value),
                  ),
                )
              }),
            { discard: true },
          ),
        ],
        { concurrency: 'unbounded' },
      )
      yield* first.commit([{ _tag: 'document.retire', id }])
      assert.isTrue(Option.isSome(yield* second.document(other)))
      yield* second.commit([{ _tag: 'document.retire', id: other }])
      yield* Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient
        assert.strictEqual((yield* sql`SELECT COUNT(*) AS n FROM effect_harness_media`)[0]?.n, 0)
        assert.strictEqual(
          (yield* sql`SELECT COUNT(*) AS n FROM effect_harness_media_owners`)[0]?.n,
          0,
        )
      }).pipe(Effect.provide(SqliteClient.layer({ filename })))
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
)

it.live(
  'SQL verifies persisted binary objects and rejects corruption before returning records',
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient
        const storage = Context.get(yield* Layer.build(Storage.layerSql), Storage.Storage)
        yield* seed(storage)
        yield* sql`UPDATE effect_harness_media SET bytes = ${new Uint8Array(8192).fill(9)}`
        const error = yield* storage.document(id).pipe(Effect.flip)
        assert.strictEqual(error.operation, 'media.integrity')
        assert.strictEqual((yield* sql`SELECT COUNT(*) AS n FROM effect_harness_media`)[0]?.n, 1)
      }),
    ).pipe(Effect.provide(SqliteClient.layer({ filename: ':memory:' }))),
)

for (const timing of ['before', 'after'] as const)
  it.live(
    `SQL lost commit acknowledgment ${timing} publication reopens a complete object closure`,
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem
          const directory = yield* fs.makeTempDirectoryScoped({ prefix: 'media-sql-commit-' })
          const filename = `${directory}/store.sqlite`
          const clientLayer = SqliteClient.layer({ filename })
          const storageLayer = Storage.layerSql.pipe(Layer.provide(clientLayer))
          yield* Storage.Storage.pipe(Effect.flatMap(seed), Effect.provide(storageLayer))
          yield* Effect.scoped(
            Effect.gen(function* () {
              const native = yield* SqlClient.SqlClient
              const lost = Effect.fail(
                new SqlError.SqlError({
                  reason: new SqlError.ConnectionError({ cause: 'lost commit acknowledgment' }),
                }),
              )
              const client = yield* SqlClient.make({
                acquirer: Effect.succeed(yield* native.reserve),
                compiler: Statement.makeCompilerSqlite(),
                spanAttributes: [],
                commit: (connection) =>
                  timing === 'before'
                    ? lost
                    : connection
                        .executeUnprepared('COMMIT', [], undefined)
                        .pipe(Effect.andThen(lost)),
              }).pipe(Effect.provide(Reactivity.layer))
              const storage = Context.get(
                yield* Layer.build(Storage.layerSql).pipe(
                  Effect.provideService(SqlClient.SqlClient, client),
                ),
                Storage.Storage,
              )
              assert.strictEqual((yield* replace(storage).pipe(Effect.flip)).reason, 'uncertain')
              assert.strictEqual(
                (yield* storage.document(id).pipe(Effect.flip)).reason,
                'uncertain',
              )
            }),
          ).pipe(Effect.provide(clientLayer))
          yield* Effect.gen(function* () {
            const storage = yield* Storage.Storage
            const expected = body(timing === 'before' ? 1 : 2)
            assert.deepEqual(
              Option.getOrThrow(yield* storage.document(id)).value,
              expected._tag === 'base' ? expected.value : {},
            )
          }).pipe(Effect.provide(storageLayer))
          yield* Effect.gen(function* () {
            const sql = yield* SqlClient.SqlClient
            assert.strictEqual(
              (yield* sql`SELECT COUNT(*) AS n FROM effect_harness_media`)[0]?.n,
              1,
            )
            assert.strictEqual(
              (yield* sql`SELECT COUNT(*) AS n FROM effect_harness_media_owners`)[0]?.n,
              1,
            )
          }).pipe(Effect.provide(clientLayer))
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
  )

it.live('SQL rejects unguarded nonempty containers before creating any physical tables', () =>
  Effect.scoped(
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient
      yield* sql`CREATE TABLE effect_harness_records (id INTEGER)`
      yield* sql`INSERT INTO effect_harness_records VALUES (1)`
      const before = yield* sql`SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name`
      assert.strictEqual((yield* Layer.build(Storage.layerSql).pipe(Effect.flip)).reason, 'corrupt')
      assert.deepEqual(
        yield* sql`SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name`,
        before,
      )
    }),
  ).pipe(Effect.provide(SqliteClient.layer({ filename: ':memory:' }))),
)
