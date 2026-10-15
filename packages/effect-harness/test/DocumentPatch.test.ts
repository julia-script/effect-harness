import { assert, describe, it } from '@effect/vitest'
import { NodeServices } from '@effect/platform-node'
import * as SqliteClient from '@effect/sql-sqlite-node/SqliteClient'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as SchemaGetter from 'effect/SchemaGetter'
import * as Stream from 'effect/Stream'
import * as SqlClient from 'effect/sql/SqlClient'
import * as Document from 'effect-harness/Document'
import * as Record from 'effect-harness/Record'
import * as Session from 'effect-harness/Session'
import { SessionError } from 'effect-harness/SessionError'
import * as Storage from 'effect-harness/Storage'
import * as StorageFormat from 'effect-harness/StorageFormat'
import * as Transaction from 'effect-harness/Transaction'
import * as ConversationInitializer from 'effect-harness/ConversationInitializer'
import * as Kernel from '../src/internal/Kernel.js'
import * as Memory from '../src/internal/Memory.js'
import * as Metadata from '../src/internal/Metadata.js'
import * as Reader2 from './fixtures/DocumentPatch/MetadataV2.js'
import { program as example } from '../../../apps/example/src/tour/DocumentPatch.js'

const root = Record.ROOT_CONVERSATION_ID
const target = { scope: { _tag: 'conversation', conversationId: root } } satisfies Document.Target
const ownerless = { ownership: { _tag: 'ownerless' } } satisfies Transaction.ConversationOptions
const Value = Schema.Struct({
  nested: Schema.Struct({ n: Schema.Natural }),
  items: Schema.Array(Schema.Int),
  label: Schema.String,
  optional: Schema.optionalKey(Schema.String),
})
const baseDefinition = {
  kind: 'test.document.patch',
  version: 1,
  scope: 'conversation',
  history: 'rewindable',
  fork: 'asOf',
  schema: Value,
  initial: () => ({
    nested: { n: 0 },
    items: Array.from({ length: 1000 }, (_, i) => i),
    label: 'initial',
    optional: 'remove',
  }),
} satisfies Document.Definition<typeof Value>
const some = Option.getOrThrow
const contentWrites = (writes: ReadonlyArray<Record.StorageWrite>) =>
  writes.filter((write) => write._tag === 'document.change' || write._tag === 'document.create')
const backendLayer = (backend: 'memory' | 'sqlite' | 'jsonl', file: string) => {
  if (backend === 'memory') return Storage.layerMemory
  if (backend === 'sqlite')
    return Storage.layerSql.pipe(Layer.provide(SqliteClient.layer({ filename: file })))
  return Storage.layerJsonl({ filePath: file })
}
const fixture = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem
  const directory = yield* fs.makeTempDirectoryScoped({ prefix: 'document-patch-' })
  return { fs, file: `${directory}/store` }
})
const trackedSession = Effect.gen(function* () {
  const native = yield* Storage.Storage
  const format = yield* StorageFormat.StorageFormat
  const batches: Array<ReadonlyArray<Record.StorageWrite>> = []
  const storage: Storage.Storage['Service'] = {
    ...native,
    commit: (input) =>
      Effect.suspend(() => {
        const writes = Array.from(input)
        return native.commit(writes).pipe(
          Effect.tap(() =>
            Effect.sync(() => {
              batches.push(writes)
            }),
          ),
        )
      }),
  }
  const session = yield* Session.make().pipe(
    Effect.provideService(Storage.Storage, storage),
    Effect.provideService(StorageFormat.StorageFormat, { ...format, storage }),
  )
  return { session, storage, batches }
})
const initialize = (session: Session.Session, document: Document.Document<typeof Value>) =>
  Session.commit(session, (tx) =>
    Effect.gen(function* () {
      yield* Transaction.ensureRoot(tx)
      yield* Transaction.ensureDocument(tx, document, target)
      return yield* Transaction.appendEntry(tx, root, { kind: 'patch.initial' })
    }),
  )

for (const backend of ['memory', 'sqlite', 'jsonl'] as const) {
  describe(`Document patches: ${backend}`, () => {
    it.live(
      'persists compact update splices, ordered patches and base/delta/delta/base checkpoints',
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const { file } = yield* fixture
            const layer = backendLayer(backend, file)
            const observed: Array<{
              value: typeof Value.Type
              operations: ReadonlyArray<Record.Op>
              count: number
            }> = []
            const document = Document.define({
              ...baseDefinition,
              checkpointWhen: (value, operations, info) => {
                observed.push({ value, operations, count: info.deltasSinceBase })
                return info.deltasSinceBase >= 2
              },
            })
            const run = Effect.gen(function* () {
              const { session, storage, batches } = yield* trackedSession
              const initial = yield* initialize(session, document)
              const id = some(yield* Session.snapshot(session, document, target)).record.id
              const pull = yield* Session.watch(session, document, target).pipe(
                Stream.rechunk(1),
                Stream.toPull,
              )
              assert.strictEqual((yield* pull)[0]?.deltasSinceBase, 0)
              const first = yield* Session.commit(session, (tx) =>
                Effect.gen(function* () {
                  yield* Transaction.updateDocument(tx, document, target, (value) => {
                    const items = [...value.items]
                    items.splice(500, 0, -1, -2)
                    return { ...value, items }
                  })
                  return yield* Transaction.appendEntry(tx, root, { kind: 'patch.first' })
                }),
              )
              const splice = contentWrites(batches[1] ?? []).find(
                (write) => write._tag === 'document.change',
              )
              assert.deepEqual(splice?.content, {
                _tag: 'delta',
                version: 1,
                ops: [['splice', ['items'], 500, 0, [-1, -2]]],
              })
              assert.isBelow(JSON.stringify(splice).length, 200)
              assert.strictEqual((yield* pull)[0]?.deltasSinceBase, 1)
              yield* Session.commit(session, (tx) =>
                Effect.gen(function* () {
                  yield* tx.pipe(
                    Transaction.patchDocument(document, target, [
                      ['delete', ['items', 500]],
                      ['set', ['nested', 'n'], 1],
                    ]),
                  )
                  yield* Transaction.patchDocument(tx, document, target, [
                    ['set', ['items', 500], -3],
                    ['delete', ['optional']],
                  ])
                }),
              )
              const second = contentWrites(batches[2] ?? []).find(
                (write) => write._tag === 'document.change',
              )
              assert.deepEqual(second?.content, {
                _tag: 'delta',
                version: 1,
                ops: [
                  ['delete', ['items', 500]],
                  ['set', ['nested', 'n'], 1],
                  ['set', ['items', 500], -3],
                  ['delete', ['optional']],
                ],
              })
              const revision = (yield* pull)[0]
              assert.strictEqual(revision?.deltasSinceBase, 2)
              assert.strictEqual(revision?.value.items[500], -3)
              const last = yield* Session.commit(session, (tx) =>
                Effect.gen(function* () {
                  yield* Transaction.patchDocument(tx, document, target, [
                    ['splice', ['items'], 500, 1, [88]],
                  ])
                  yield* Transaction.updateDocument(tx, document, target, (value) => ({
                    ...value,
                    nested: { n: 3 },
                    label: 'final',
                  }))
                  return yield* Transaction.appendEntry(tx, root, { kind: 'patch.checkpoint' })
                }),
              )
              const writes = contentWrites(batches[3] ?? [])
              assert.lengthOf(writes, 1)
              assert.strictEqual(writes[0]?.content._tag, 'base')
              const checkpoint = writes[0]
              assert.isTrue(
                checkpoint?._tag === 'document.change' &&
                  checkpoint.publicationOps?.[0]?.[0] === 'splice',
              )
              const final = some(yield* Session.snapshot(session, document, target))
              assert.deepEqual((yield* pull)[0], final)
              assert.strictEqual(final.deltasSinceBase, 0)
              assert.deepEqual(some(yield* storage.document(id)).value, final.value)
              assert.deepEqual(
                observed.map((item) => item.count),
                [0, 1, 2],
              )
              assert.strictEqual(observed[2]?.value.label, 'final')
              assert.lengthOf(observed[2]?.operations ?? [], 3)
              assert.deepEqual(
                some(yield* Session.snapshotAsOf(session, document, target, initial.id)).value,
                baseDefinition.initial(),
              )
              assert.strictEqual(
                some(yield* Session.snapshotAsOf(session, document, target, first.id)).value
                  .items[500],
                -1,
              )
              assert.strictEqual(
                some(yield* Session.snapshotAsOf(session, document, target, last.id)).value
                  .items[500],
                88,
              )
              assert.deepEqual(yield* StorageFormat.inspect, {
                formatVersion: 3,
                minimumReaderVersion: 3,
              })
              assert.deepEqual(
                yield* StorageFormat.requireSupportedFormat({
                  formatVersion: 2,
                  minimumReaderVersion: 2,
                }),
                { formatVersion: 3, minimumReaderVersion: 3 },
              )
              return { id, final, initial, first, last }
            })
            if (backend === 'memory') {
              // Retain the underlying native storage across independent Session scopes.
              yield* Effect.gen(function* () {
                const result = yield* Effect.scoped(run)
                const reopened = yield* Session.make()
                assert.deepEqual(
                  some(yield* Session.snapshot(reopened, document, target)),
                  result.final,
                )
              }).pipe(Effect.provide(layer))
            } else {
              const result = yield* Effect.scoped(run.pipe(Effect.provide(layer)))
              yield* Effect.gen(function* () {
                const session = yield* Session.make()
                assert.deepEqual(
                  some(yield* Session.snapshot(session, document, target)),
                  result.final,
                )
                assert.strictEqual(
                  some(yield* Session.snapshotAsOf(session, document, target, result.first.id))
                    .value.items[500],
                  -1,
                )
                const child = yield* Session.commit(session, (tx) =>
                  Transaction.forkConversation(tx, root, result.first.id, ownerless),
                )
                const childTarget = {
                  scope: { _tag: 'conversation', conversationId: child.id },
                } satisfies Document.Target
                const forked = some(yield* Session.snapshot(session, document, childTarget))
                assert.strictEqual(forked.value.items[500], -1)
                assert.strictEqual(forked.deltasSinceBase, 0)
              }).pipe(Effect.provide(layer))
            }
          }),
        ).pipe(Effect.provide(NodeServices.layer)),
    )

    it.live(
      'caught invalid patches preserve prior drafts; throwing predicates write no batch or marker',
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const { file } = yield* fixture
            yield* Effect.gen(function* () {
              const document = Document.define(baseDefinition)
              const { session, storage, batches } = yield* trackedSession
              yield* initialize(session, document)
              yield* Session.commit(session, (tx) =>
                Effect.gen(function* () {
                  yield* Transaction.patchDocument(tx, document, target, [
                    ['set', ['nested', 'n'], 1],
                  ])
                  for (const operations of [
                    [['splice', ['items'], 1001, 0, [1]]],
                    [['splice', ['items'], 2, 999, []]],
                    [['splice', ['label'], 0, 0, []]],
                    [['set', ['missing', 'n'], 2]],
                    [['set', ['nested', 'n'], -1]],
                    [['set', ['items', -1], 9]],
                    [['set', ['items', 1001], 9]],
                  ] satisfies ReadonlyArray<ReadonlyArray<Record.Op>>) {
                    yield* Transaction.patchDocument(tx, document, target, operations).pipe(
                      Effect.flip,
                    )
                    assert.strictEqual(
                      some(yield* Transaction.snapshot(tx, document, target)).value.nested.n,
                      1,
                    )
                  }
                  yield* Transaction.appendEntry(tx, root, { kind: 'patch.caught' })
                }),
              )
              assert.lengthOf(batches, 2)
              assert.strictEqual(
                some(yield* Session.snapshot(session, document, target)).deltasSinceBase,
                1,
              )
              assert.deepEqual(yield* StorageFormat.inspect, {
                formatVersion: 1,
                minimumReaderVersion: 1,
              })
              const throwing = Document.define({
                ...baseDefinition,
                checkpointWhen: () => JSON.parse('invalid'),
              })
              const failed = yield* Session.commit(session, (tx) =>
                Effect.gen(function* () {
                  yield* Transaction.appendEntry(tx, root, { kind: 'must.not.persist' })
                  yield* Transaction.patchDocument(tx, throwing, target, [
                    ['splice', ['items'], 0, 0, [42]],
                  ])
                }),
              ).pipe(Effect.flip)
              assert.instanceOf(failed, SessionError)
              if (failed instanceof SessionError)
                assert.strictEqual(failed.operation, 'document.checkpoint')
              assert.lengthOf(batches, 2)
              assert.strictEqual(
                (yield* storage.scanEntries({ conversationId: root }).pipe(Stream.runCollect)).some(
                  (entry) => entry.kind === 'must.not.persist',
                ),
                false,
              )
              assert.strictEqual(
                some(yield* Session.snapshot(session, document, target)).value.items.length,
                1000,
              )
              assert.deepEqual(yield* StorageFormat.inspect, {
                formatVersion: 1,
                minimumReaderVersion: 1,
              })
              yield* Session.commit(session, (tx) =>
                Transaction.patchDocument(tx, document, target, [
                  ['set', ['label'], 'still usable'],
                ]),
              )
            }).pipe(Effect.provide(backendLayer(backend, file)))
          }),
        ).pipe(Effect.provide(NodeServices.layer)),
    )
  })
}

for (const backend of ['sqlite', 'jsonl'] as const) {
  it.live(`${backend} persists an upgraded marker rejected by the actual format2 decoder`, () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { fs, file } = yield* fixture
        yield* Effect.gen(function* () {
          const session = yield* Session.make()
          const document = Document.define(baseDefinition)
          yield* initialize(session, document)
          yield* Session.commit(session, (tx) =>
            Transaction.patchDocument(tx, document, target, [['splice', ['items'], 400, 0, [42]]]),
          )
        }).pipe(Effect.provide(backendLayer(backend, file)))
        if (backend === 'jsonl') {
          const frames = (yield* fs.readFileString(file)).trim().split('\n')
          const frame = yield* Schema.decodeUnknownEffect(
            Schema.fromJsonString(Schema.Struct({ metadata: Reader2.Metadata })),
          )(frames.at(-1))
          assert.strictEqual(
            (yield* Reader2.requireSupported(frame.metadata).pipe(Effect.flip)).reason,
            'unsupportedFormat',
          )
        } else {
          yield* Effect.gen(function* () {
            const sql = yield* SqlClient.SqlClient
            const marker =
              (yield* sql`SELECT format, minimum_reader AS minimumReader, next_id AS nextId, next_seq AS nextSeq FROM effect_harness_metadata`)[0]
            const decoded = yield* Schema.decodeUnknownEffect(Reader2.Metadata)(marker)
            assert.strictEqual(
              (yield* Reader2.requireSupported(decoded).pipe(Effect.flip)).reason,
              'unsupportedFormat',
            )
          }).pipe(Effect.provide(SqliteClient.layer({ filename: file })))
        }
        yield* StorageFormat.inspect.pipe(
          Effect.provide(backendLayer(backend, file)),
          Effect.map((format) => assert.strictEqual(format.formatVersion, 3)),
        )
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  )
}

it.effect(
  'raw native splice writes require explicit activation, legacy operations remain supported',
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const access = yield* Memory.makeAccess
        const storage = yield* Kernel.make(access)
        const id = Record.DocumentId.make(2)
        yield* storage.commit([
          {
            _tag: 'document.create',
            record: {
              id,
              ...target,
              kind: baseDefinition.kind,
              history: 'rewindable',
              fork: 'asOf',
            },
            content: { _tag: 'base', version: 1, value: { items: [1, 2], n: 0 } },
          },
        ])
        const splice: Record.StorageWrite = {
          _tag: 'document.change',
          id,
          content: { _tag: 'delta', version: 1, ops: [['splice', ['items'], 1, 0, [3]]] },
        }
        assert.strictEqual(
          (yield* storage.commit([splice]).pipe(Effect.flip)).reason,
          'unsupportedFormat',
        )
        assert.strictEqual((yield* access.metadata).format, 1)
        assert.deepEqual(some(yield* storage.document(id)).value, { items: [1, 2], n: 0 })
        yield* storage.commit([
          {
            _tag: 'document.change',
            id,
            content: {
              _tag: 'delta',
              version: 1,
              ops: [
                ['set', ['n'], 1],
                ['delete', ['items', 0]],
              ],
            },
          },
        ])
        yield* storage.storageFormat.requireSupportedFormat({
          formatVersion: 3,
          minimumReaderVersion: 3,
        })
        yield* storage.commit([splice])
        assert.deepEqual(some(yield* storage.document(id)).value, { items: [2, 3], n: 1 })
        assert.strictEqual(some(yield* storage.document(id)).deltasSinceBase, 2)
        assert.strictEqual(
          (yield* Metadata.requireSupported({
            format: 12,
            minimumReader: 12,
            nextId: 3,
            nextSeq: 1,
          }).pipe(Effect.flip)).reason,
          'unsupportedFormat',
        )
      }),
    ),
)

class Decode extends Context.Service<Decode, string>()('test/DocumentPatch/Decode') {}
class Encode extends Context.Service<Encode, string>()('test/DocumentPatch/Encode') {}
const ServiceCodec = Schema.Struct({ encodedCount: Schema.Natural }).pipe(
  Schema.decodeTo(Schema.Struct({ count: Schema.Natural, label: Schema.String }), {
    decode: SchemaGetter.transformEffect(({ encodedCount }) =>
      Decode.pipe(Effect.map((label) => ({ count: encodedCount, label }))),
    ),
    encode: SchemaGetter.transformEffect(({ count }) =>
      Encode.pipe(Effect.as({ encodedCount: count })),
    ),
  }),
)
it.effect(
  'patches encoded transformed fields, validates both codec services and gives checkpoint decoded values',
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const seen: Array<string> = []
        const document = Document.define({
          kind: 'test.service.patch',
          version: 1,
          scope: 'session',
          schema: ServiceCodec,
          initial: () => ({ count: 0, label: 'initial' }),
          checkpointWhen: (value) => {
            seen.push(`${value.count}:${value.label}`)
            return false
          },
        })
        const target = { scope: { _tag: 'session' } } satisfies Document.Target
        const session = yield* Session.make()
        yield* Session.commit(session, (tx) => Transaction.ensureDocument(tx, document, target))
        yield* Session.commit(session, (tx) =>
          Transaction.patchDocument(tx, document, target, [['set', ['encodedCount'], 5]]),
        )
        assert.deepEqual(some(yield* Session.snapshot(session, document, target)).value, {
          count: 5,
          label: 'decoded',
        })
        assert.deepEqual(seen, ['5:decoded'])
      }),
    ).pipe(
      Effect.provide(Storage.layerMemory),
      Effect.provideService(Decode, 'decoded'),
      Effect.provideService(Encode, 'encoded'),
    ),
)

it.effect('runs the serviceful public patch/checkpoint example through source mapping', () =>
  example.pipe(
    Effect.map(({ first, checkpoint, format }) => {
      assert.deepEqual(first.value, {
        count: 1,
        items: ['a', 'b', 'c'],
        label: 'decoded with a service',
      })
      assert.strictEqual(first.deltasSinceBase, 1)
      assert.strictEqual(checkpoint.value.count, 3)
      assert.strictEqual(checkpoint.deltasSinceBase, 0)
      assert.deepEqual(format, { formatVersion: 3, minimumReaderVersion: 3 })
    }),
  ),
)

for (const policy of [
  { history: 'rewindable', fork: 'asOf' },
  { history: 'rewindable', fork: 'current' },
  { history: 'rewindable', fork: 'initial' },
  { history: 'latest', fork: 'current' },
  { history: 'latest', fork: 'initial' },
] as const) {
  it.effect(`preserves ${policy.history}/${policy.fork} forks before and after checkpoints`, () =>
    Effect.scoped(
      Effect.gen(function* () {
        const session = yield* Session.make()
        const document = Document.define({
          ...baseDefinition,
          ...policy,
          checkpointWhen: (_value, _ops, info) => info.deltasSinceBase >= 1,
        })
        const initial = yield* initialize(session, document)
        const delta = yield* Session.commit(session, (tx) =>
          Effect.gen(function* () {
            yield* Transaction.patchDocument(tx, document, target, [['set', ['nested', 'n'], 1]])
            return yield* Transaction.appendEntry(tx, root, { kind: 'patch.delta' })
          }),
        )
        const before = yield* Session.commit(session, (tx) =>
          Transaction.forkConversation(tx, root, initial.id, ownerless),
        )
        const beforeTarget = {
          scope: { _tag: 'conversation', conversationId: before.id },
        } satisfies Document.Target
        if (policy.fork === 'initial') {
          assert.isTrue(Option.isNone(yield* Session.snapshot(session, document, beforeTarget)))
          yield* Session.commit(session, (tx) =>
            Transaction.ensureDocument(tx, document, beforeTarget),
          )
        }
        assert.strictEqual(
          some(yield* Session.snapshot(session, document, beforeTarget)).value.nested.n,
          policy.fork === 'current' ? 1 : 0,
        )
        const checkpointEntry = yield* Session.commit(session, (tx) =>
          Effect.gen(function* () {
            yield* Transaction.patchDocument(tx, document, target, [['set', ['nested', 'n'], 2]])
            return yield* Transaction.appendEntry(tx, root, { kind: 'patch.base' })
          }),
        )
        const historical = yield* Session.commit(session, (tx) =>
          Transaction.forkConversation(tx, root, delta.id, ownerless),
        )
        const current = yield* Session.commit(session, (tx) =>
          Transaction.forkConversation(tx, root, checkpointEntry.id, ownerless),
        )
        for (const [child, expected] of [
          [historical, policy.fork === 'asOf' ? 1 : 2],
          [current, 2],
        ] as const) {
          const childTarget = {
            scope: { _tag: 'conversation', conversationId: child.id },
          } satisfies Document.Target
          if (policy.fork === 'initial')
            yield* Session.commit(session, (tx) =>
              Transaction.ensureDocument(tx, document, childTarget),
            )
          const snapshot = some(yield* Session.snapshot(session, document, childTarget))
          assert.strictEqual(snapshot.value.nested.n, policy.fork === 'initial' ? 0 : expected)
          assert.strictEqual(snapshot.deltasSinceBase, 0)
        }
        assert.strictEqual(
          some(yield* Session.snapshot(session, document, beforeTarget)).value.nested.n,
          policy.fork === 'current' ? 1 : 0,
        )
      }),
    ).pipe(Effect.provide(Storage.layerMemory)),
  )
}

it.effect(
  'new documents, explicit set/replace and migrations persist bases; no-op updates avoid writes',
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        let predicates = 0
        const document = Document.define({
          ...baseDefinition,
          checkpointWhen: () => {
            predicates++
            return false
          },
        })
        const { session, batches } = yield* trackedSession
        yield* Session.commit(session, (tx) =>
          Effect.gen(function* () {
            yield* Transaction.ensureRoot(tx)
            yield* Transaction.ensureDocument(tx, document, target)
            yield* Transaction.patchDocument(tx, document, target, [
              ['splice', ['items'], 500, 1, [55]],
            ])
          }),
        )
        assert.strictEqual(
          contentWrites(batches[0] ?? []).find(
            (write) =>
              write._tag === 'document.create' && write.record.kind === document.definition.kind,
          )?.content._tag,
          'base',
        )
        assert.deepEqual(yield* StorageFormat.inspect, {
          formatVersion: 1,
          minimumReaderVersion: 1,
        })
        yield* Session.commit(session, (tx) =>
          Transaction.updateDocument(tx, document, target, (value) => value),
        )
        yield* Session.commit(session, (tx) => Transaction.patchDocument(tx, document, target, []))
        assert.lengthOf(batches, 1)
        yield* Session.commit(session, (tx) =>
          Effect.gen(function* () {
            yield* Transaction.patchDocument(tx, document, target, [['set', ['nested', 'n'], 1]])
            yield* Transaction.setDocument(tx, document, target, baseDefinition.initial())
            yield* Transaction.patchDocument(tx, document, target, [['set', ['nested', 'n'], 2]])
          }),
        )
        assert.strictEqual(contentWrites(batches[1] ?? [])[0]?.content._tag, 'base')
        yield* Session.commit(session, (tx) =>
          Transaction.patchDocument(tx, document, target, [
            ['replace', { ...baseDefinition.initial(), label: 'replacement' }],
          ]),
        )
        assert.strictEqual(contentWrites(batches[2] ?? [])[0]?.content._tag, 'base')
        const upgraded = Document.define({
          ...document.definition,
          version: 2,
          migrations: { 1: Effect.succeed },
        })
        yield* Session.commit(session, (tx) =>
          Transaction.updateDocument(tx, upgraded, target, (value) => ({
            ...value,
            label: 'migrated',
          })),
        )
        assert.deepEqual(contentWrites(batches[3] ?? [])[0]?.content, {
          _tag: 'base',
          version: 2,
          value: { ...baseDefinition.initial(), label: 'migrated' },
        })
        assert.strictEqual(predicates, 0)
        assert.strictEqual(
          some(yield* Session.snapshot(session, upgraded, target)).deltasSinceBase,
          0,
        )
      }),
    ).pipe(Effect.provide(Storage.layerMemory)),
)

it.effect(
  'initializer savepoints discard only failed child patch operations and retain earlier ordered drafts',
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const document = Document.define(baseDefinition)
        yield* Effect.scoped(
          Effect.gen(function* () {
            const session = yield* Session.make()
            yield* initialize(session, document)
          }),
        )
        const session = yield* Session.make({
          initializers: [
            ConversationInitializer.make({
              execute: (tx) =>
                Effect.gen(function* () {
                  yield* Transaction.patchDocument(tx, document, target, [
                    ['splice', ['items'], 5, 1, [99]],
                  ])
                  return yield* new SessionError({
                    reason: 'invalid',
                    operation: 'test.initializer',
                    message: 'discard child',
                  })
                }),
            }),
          ],
        })
        yield* Session.commit(session, (tx) =>
          Effect.gen(function* () {
            yield* Transaction.patchDocument(tx, document, target, [['set', ['nested', 'n'], 1]])
            yield* Transaction.createConversation(tx, ownerless).pipe(Effect.flip)
            assert.strictEqual(
              some(yield* Transaction.snapshot(tx, document, target)).value.items[5],
              5,
            )
            yield* Transaction.patchDocument(tx, document, target, [['set', ['label'], 'retained']])
          }),
        )
        const stored = some(yield* Session.snapshot(session, document, target))
        assert.strictEqual(stored.value.nested.n, 1)
        assert.strictEqual(stored.value.label, 'retained')
        assert.strictEqual(stored.value.items[5], 5)
        assert.strictEqual(stored.deltasSinceBase, 1)
        assert.deepEqual(yield* StorageFormat.inspect, {
          formatVersion: 1,
          minimumReaderVersion: 1,
        })
      }),
    ).pipe(Effect.provide(Storage.layerMemory)),
)

it.effect(
  'retirement/recreation isolates patch journals per incarnation and ends the old watch',
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const document = Document.define(baseDefinition)
        const { session, storage, batches } = yield* trackedSession
        const initial = yield* initialize(session, document)
        const old = some(yield* Session.snapshot(session, document, target))
        const pull = yield* Session.watch(session, document, target).pipe(
          Stream.rechunk(1),
          Stream.toPull,
        )
        yield* pull
        yield* Session.commit(session, (tx) =>
          Effect.gen(function* () {
            yield* Transaction.patchDocument(tx, document, target, [['set', ['nested', 'n'], 4]])
            yield* Transaction.retireDocument(tx, document, target)
            yield* Transaction.ensureDocument(tx, document, target)
            yield* Transaction.patchDocument(tx, document, target, [['set', ['nested', 'n'], 9]])
          }),
        )
        yield* pull.pipe(Effect.flip)
        const fresh = some(yield* Session.snapshot(session, document, target))
        assert.notStrictEqual(old.record.id, fresh.record.id)
        assert.strictEqual(fresh.value.nested.n, 9)
        assert.strictEqual(fresh.deltasSinceBase, 0)
        const changed = contentWrites(batches[1] ?? [])
        assert.lengthOf(changed, 2)
        assert.strictEqual(
          changed.find((write) => write._tag === 'document.change')?.content._tag,
          'delta',
        )
        assert.strictEqual(
          changed.find((write) => write._tag === 'document.create')?.content._tag,
          'base',
        )
        assert.isTrue(Option.isNone(yield* storage.document(old.record.id)))
        assert.strictEqual(
          some(yield* Session.snapshotAsOf(session, document, target, initial.id)).value.nested.n,
          0,
        )
      }),
    ).pipe(Effect.provide(Storage.layerMemory)),
)

it.effect(
  'missing/mismatched guard capabilities fail preparation without storage or publication',
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const native = yield* Storage.Storage
        const document = Document.define(baseDefinition)
        let commits = 0
        const custom: Storage.Storage['Service'] = {
          ...native,
          commit: (writes) =>
            Effect.sync(() => {
              commits++
            }).pipe(Effect.andThen(native.commit(writes))),
        }
        const session = yield* Session.make().pipe(Effect.provideService(Storage.Storage, custom))
        yield* initialize(session, document)
        const before = commits
        const failure = yield* Session.commit(session, (tx) =>
          Transaction.patchDocument(tx, document, target, [['splice', ['items'], 2, 0, [42]]]),
        ).pipe(Effect.flip)
        assert.instanceOf(failure, SessionError)
        if (failure instanceof SessionError)
          assert.strictEqual(failure.operation, 'document.format')
        assert.strictEqual(commits, before)
        assert.strictEqual(
          some(yield* Session.snapshot(session, document, target)).value.items.length,
          1000,
        )
        assert.deepEqual(yield* StorageFormat.inspect, {
          formatVersion: 1,
          minimumReaderVersion: 1,
        })
        yield* Session.commit(session, (tx) =>
          Transaction.patchDocument(tx, document, target, [['set', ['label'], 'usable']]),
        )
      }),
    ).pipe(Effect.provide(Storage.layerMemory)),
)

it.effect('uncertain format activation poisons the Session and closes its document observers', () =>
  Effect.scoped(
    Effect.gen(function* () {
      const storage = yield* Storage.Storage
      const nativeFormat = yield* StorageFormat.StorageFormat
      const uncertainty = new Storage.StorageError({
        reason: 'uncertain',
        operation: 'test.format',
        message: 'lost marker acknowledgment',
      })
      const session = yield* Session.make().pipe(
        Effect.provideService(StorageFormat.StorageFormat, {
          ...nativeFormat,
          storage,
          requireSupportedFormat: () => Effect.fail(uncertainty),
        }),
      )
      const document = Document.define(baseDefinition)
      yield* initialize(session, document)
      const pull = yield* Session.watch(session, document, target).pipe(
        Stream.rechunk(1),
        Stream.toPull,
      )
      yield* pull
      assert.strictEqual(
        yield* Session.commit(session, (tx) =>
          Transaction.patchDocument(tx, document, target, [['splice', ['items'], 5, 0, [9]]]),
        ).pipe(Effect.flip),
        uncertainty,
      )
      assert.strictEqual(
        yield* Session.snapshot(session, document, target).pipe(Effect.flip),
        uncertainty,
      )
      const observerFailure = yield* pull.pipe(Effect.flip)
      assert.instanceOf(observerFailure, SessionError)
      if (observerFailure instanceof SessionError)
        assert.strictEqual(observerFailure.reason, 'closed')
      assert.deepEqual(yield* StorageFormat.inspect, { formatVersion: 1, minimumReaderVersion: 1 })
    }),
  ).pipe(Effect.provide(Storage.layerMemory)),
)

it.effect(
  'a later invalid operation discards its whole patch and a later predicate blocks all guarded writes',
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const document = Document.define(baseDefinition)
        const second = Document.define({
          ...baseDefinition,
          kind: 'test.patch.second',
          checkpointWhen: () => JSON.parse('invalid'),
        })
        const { session, batches } = yield* trackedSession
        yield* initialize(session, document)
        yield* Session.commit(session, (tx) => Transaction.ensureDocument(tx, second, target))
        yield* Session.commit(session, (tx) =>
          Effect.gen(function* () {
            yield* Transaction.patchDocument(tx, document, target, [['set', ['nested', 'n'], 1]])
            yield* Transaction.patchDocument(tx, document, target, [
              ['set', ['nested', 'n'], 99],
              ['splice', ['items'], 1001, 0, [5]],
            ]).pipe(Effect.flip)
            assert.strictEqual(
              some(yield* Transaction.snapshot(tx, document, target)).value.nested.n,
              1,
            )
          }),
        )
        const before = batches.length
        yield* Session.commit(session, (tx) =>
          Effect.gen(function* () {
            yield* Transaction.patchDocument(tx, document, target, [
              ['splice', ['items'], 500, 0, [7]],
            ])
            yield* Transaction.patchDocument(tx, second, target, [
              ['set', ['label'], 'must not commit'],
            ])
          }),
        ).pipe(Effect.flip)
        assert.strictEqual(batches.length, before)
        assert.strictEqual(
          some(yield* Session.snapshot(session, document, target)).value.items.length,
          1000,
        )
        assert.strictEqual(
          some(yield* Session.snapshot(session, second, target)).value.label,
          'initial',
        )
        assert.deepEqual(yield* StorageFormat.inspect, {
          formatVersion: 1,
          minimumReaderVersion: 1,
        })
      }),
    ).pipe(Effect.provide(Storage.layerMemory)),
)

for (const backend of ['sqlite', 'jsonl'] as const) {
  for (const marker of [
    { format: 3, minimumReader: 2 },
    { format: 2, minimumReader: 3 },
  ]) {
    it.live(
      `${backend} rejects mismatched supported reader pair ${marker.format}/${marker.minimumReader} before replay`,
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const { fs, file } = yield* fixture
            if (backend === 'jsonl') {
              const bytes =
                JSON.stringify({ metadata: { ...marker, nextId: 2, nextSeq: 1 }, rows: [] }) + '\n'
              yield* fs.writeFileString(file, bytes)
              const failure = yield* StorageFormat.inspect.pipe(
                Effect.provide(backendLayer(backend, file)),
                Effect.flip,
              )
              assert.strictEqual(failure._tag, 'StorageError')
              if (failure._tag === 'StorageError') assert.strictEqual(failure.reason, 'corrupt')
              assert.strictEqual(yield* fs.readFileString(file), bytes)
            } else {
              yield* Effect.gen(function* () {
                const sql = yield* SqlClient.SqlClient
                yield* Effect.scoped(Layer.build(Storage.layerSql))
                yield* sql`UPDATE effect_harness_metadata SET format=${marker.format},minimum_reader=${marker.minimumReader}`
                const before = yield* sql`SELECT * FROM effect_harness_metadata`
                const failure = yield* StorageFormat.inspect.pipe(
                  Effect.provide(Storage.layerSql),
                  Effect.flip,
                )
                assert.strictEqual(failure._tag, 'StorageError')
                if (failure._tag === 'StorageError') assert.strictEqual(failure.reason, 'corrupt')
                assert.deepEqual(yield* sql`SELECT * FROM effect_harness_metadata`, before)
              }).pipe(Effect.provide(SqliteClient.layer({ filename: file })))
            }
          }),
        ).pipe(Effect.provide(NodeServices.layer)),
    )
  }
}
