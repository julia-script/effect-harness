import { assert, it } from '@effect/vitest'
import { NodeServices } from '@effect/platform-node'
import * as SqliteClient from '@effect/sql-sqlite-node/SqliteClient'
import * as SqlClient from 'effect/sql/SqlClient'
import * as Effect from 'effect/Effect'
import * as Brand from 'effect/Brand'
import * as Domain from '../src/Record.js'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as SchemaTransformation from 'effect/SchemaTransformation'
import * as Struct from 'effect/Struct'
import * as Document from 'effect-harness/Document'
import * as Retirement from 'effect-harness/DocumentRetirement'
import * as Session from 'effect-harness/Session'
import * as Sequence from 'effect-harness/Sequence'
import * as Storage from 'effect-harness/Storage'
import * as Transaction from 'effect-harness/Transaction'
import * as Kernel from '../src/internal/Kernel.js'
import * as Memory from '../src/internal/Memory.js'
import * as Metadata from '../src/internal/Metadata.js'
import * as Row from '../src/internal/Row.js'

const definition = Document.define({
  kind: 'native.obsolete',
  version: 1,
  scope: 'session',
  schema: Schema.Struct({ count: Schema.Natural }),
  initial: () => ({ count: 1 }),
})
const policy = { ...definition.definition, version: 3 }
// The test seam must inspect even sequences rejected by the product's stored-row schema.
const RawRow = Schema.Union([
  Row.Row.members[0],
  Row.Row.members[1],
  Row.Row.members[2],
  Row.Row.members[3],
  Schema.TaggedStruct('document', {
    value: Schema.Struct({
      record: Row.StoredDocument.fields.record,
      revisions: Schema.Array(
        Schema.Struct({
          seq: Schema.Finite.pipe(Schema.brand('@effect-harness/durable/Record/Seq')),
          content: Schema.Json,
        }),
      ),
    }),
  }),
])
const PackedContent = Schema.Struct({
  version: Schema.Literal(1),
  body: Schema.Json,
  media: Schema.Array(Schema.Json),
}).pipe(
  Schema.decodeTo(
    Schema.Json,
    SchemaTransformation.transform({
      decode: (packed) => packed.body,
      encode: (body) => ({ version: 1 as const, body, media: [] }),
    }),
  ),
)
const RowJson = Schema.fromJsonString(
  Schema.Struct({
    category: Schema.Literals(['conversation', 'entry', 'task', 'submission', 'document']),
    value: PackedContent,
    revisions: Schema.Array(Schema.Json),
  }).pipe(
    Schema.decodeTo(
      Schema.toType(RawRow),
      SchemaTransformation.transform({
        decode: (packed) =>
          Schema.decodeUnknownSync(RawRow)({ _tag: packed.category, value: packed.value }),
        encode: (row) => ({ category: row._tag, value: row.value, revisions: [] }),
      }),
    ),
  ),
)
const FrameJson = Schema.fromJsonString(
  Schema.Struct({
    metadata: Metadata.Metadata,
    rows: Schema.Array(
      Schema.Struct({
        id: Schema.Finite,
        seq: Schema.Finite,
        firstRevision: Schema.NullOr(Schema.Finite),
        revisions: Schema.Array(Schema.Finite),
      }),
    ),
  }),
)
const testError = (cause: unknown) =>
  new Storage.StorageError({
    reason: 'io',
    operation: 'test',
    message: 'Native fixture access failed',
    cause,
  })
const readSql = (id: number) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient
    const rows = yield* sql`SELECT payload FROM effect_harness_records WHERE id=${id}`
    const row = yield* Schema.decodeUnknownEffect(RowJson)(rows[0]?.payload)
    if (row._tag !== 'document') return row
    const revisions =
      yield* sql`SELECT CAST(seq AS TEXT) AS seq, content FROM effect_harness_revisions
    WHERE document_id=${id} ORDER BY seq`
    return {
      ...row,
      value: {
        ...row.value,
        revisions: yield* Effect.forEach(revisions, (revision) =>
          Schema.decodeUnknownEffect(Schema.fromJsonString(PackedContent))(revision.content).pipe(
            Effect.map((content) => ({ seq: damagedSeq(Number(revision.seq)), content })),
          ),
        ),
      },
    }
  }).pipe(Effect.mapError(testError))
const writeSql = (row: Row.Stored) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient
    const header =
      row._tag === 'document' ? { ...row, value: { ...row.value, revisions: [] } } : row
    const payload = yield* Schema.encodeEffect(RowJson)(header)
    yield* sql`UPDATE effect_harness_records SET payload=${payload} WHERE id=${Row.idOf(row)}`
    if (row._tag === 'document') {
      yield* sql`DELETE FROM effect_harness_revisions WHERE document_id=${row.value.record.id}`
      for (const revision of row.value.revisions) {
        const content = JSON.stringify({ version: 1, body: revision.content, media: [] })
        yield* sql`INSERT INTO effect_harness_revisions (document_id,seq,content)
        VALUES (${row.value.record.id},${revision.seq},${content})`
      }
    }
  }).pipe(Effect.mapError(testError))
const jsonlReference = (filePath: string, id: number) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem
    const text = yield* fs.readFileString(filePath)
    const frames = yield* Effect.forEach(text.trimEnd().split('\n'), (line) =>
      Schema.decodeEffect(FrameJson)(line),
    )
    const references = frames.flatMap((frame) => frame.rows).filter((ref) => ref.id === id)
    let revisions: ReadonlyArray<number> = []
    for (const ref of references)
      revisions = [
        ...revisions.filter((seq) => ref.firstRevision !== null && seq >= ref.firstRevision),
        ...ref.revisions,
      ]
    return { ...references.at(-1)!, revisions }
  })
const readJsonl = (filePath: string, id: number) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem
    const ref = yield* jsonlReference(filePath, id)
    const row = yield* fs
      .readFileString(`${filePath}.parts/${id}-${ref.seq}.row`)
      .pipe(Effect.flatMap(Schema.decodeEffect(RowJson)))
    if (row._tag !== 'document') return row
    const revisions = yield* Effect.forEach(ref.revisions, (seq) =>
      fs.readFileString(`${filePath}.parts/${id}-${seq}.revision`).pipe(
        Effect.flatMap(
          Schema.decodeEffect(
            Schema.fromJsonString(
              RawRow.members[4].fields.value.fields.revisions.value.mapFields((fields) => ({
                ...fields,
                content: PackedContent,
              })),
            ),
          ),
        ),
      ),
    )
    return { ...row, value: { ...row.value, revisions } }
  }).pipe(Effect.mapError(testError))
const writeJsonl = (filePath: string, row: Row.Stored) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem
    const id = Row.idOf(row)
    const ref = yield* jsonlReference(filePath, id)
    const header =
      row._tag === 'document' ? { ...row, value: { ...row.value, revisions: [] } } : row
    yield* fs.writeFileString(
      `${filePath}.parts/${id}-${ref.seq}.row`,
      yield* Schema.encodeEffect(RowJson)(header),
    )
    if (row._tag === 'document')
      for (const [index, seq] of ref.revisions.entries())
        yield* fs.writeFileString(
          `${filePath}.parts/${id}-${seq}.revision`,
          JSON.stringify({
            seq: row.value.revisions[index]!.seq,
            content: { version: 1, body: row.value.revisions[index]!.content, media: [] },
          }),
        )
  }).pipe(Effect.mapError(testError))
const readMemory = (access: Effect.Success<typeof Memory.makeAccess>, id: number) =>
  access.snapshot.pipe(
    Effect.map((state) => {
      const packed = state.rows.get(id)!
      const value = packed.value.body
      return Schema.decodeUnknownSync(RawRow)({
        _tag: packed.category,
        value:
          packed.category === 'document'
            ? {
                ...Struct.assign(value as Schema.JsonObject, {}),
                revisions: packed.revisions.map((revision) => ({
                  seq: revision.seq,
                  content: revision.content.body,
                })),
              }
            : value,
      })
    }),
  )
const writeMemory = (
  access: Effect.Success<typeof Memory.makeAccess>,
  row: Row.Stored,
  metadata?: Metadata.Metadata,
) =>
  Effect.gen(function* () {
    const state = yield* access.snapshot
    const rows = new Map(state.rows)
    rows.set(Row.idOf(row), {
      category: row._tag,
      value: {
        version: 1,
        body: row._tag === 'document' ? { record: row.value.record, revisions: [] } : row.value,
        media: [],
      },
      revisions:
        row._tag === 'document'
          ? row.value.revisions.map((revision) => ({
              seq: revision.seq,
              content: { version: 1, body: revision.content, media: [] },
            }))
          : [],
    })
    yield* access.publish({ ...state, rows, metadata: metadata ?? state.metadata })
  })
const modes = [
  'missing-body',
  'malformed-body',
  'malformed-delta',
  'zero-version',
  'missing-version',
  'future-version',
  'wrong-kind',
  'wrong-policy',
  'valid',
  'earlier-tag',
  'earlier-zero-version',
  'earlier-negative-version',
  'earlier-missing-version',
  'earlier-fraction-version',
  'earlier-unsafe-version',
  'duplicate-sequence',
  'descending-sequence',
  'zero-sequence',
  'fraction-sequence',
  'unsafe-sequence',
  'future-sequence',
  'revision-before-creation',
  'unallocated-creation',
  'retirement-before-creation',
  'retirement-before-revision',
  'unallocated-retirement',
] as const
type Mode = (typeof modes)[number]
// These corruptions cannot be represented by the SQL primary key, CHECK and ORDER BY.
const representable = (backend: string, mode: Mode) =>
  backend !== 'sqlite' ||
  !['duplicate-sequence', 'descending-sequence', 'zero-sequence'].includes(mode)
const successful = (mode: Mode) =>
  ['missing-body', 'malformed-body', 'malformed-delta', 'valid'].includes(mode)
// Deliberately bypass sequence validation only when damaging a native test fixture.
const damagedSeq = Brand.nominal<Domain.Seq>()
const corrupt = Effect.fnUntraced(function* (row: Row.Stored, mode: Mode) {
  if (row._tag !== 'document') return row
  const revisions = yield* Effect.forEach(
    row.value.revisions,
    Effect.fnUntraced(function* (revision, index) {
      const content = yield* Schema.decodeUnknownEffect(Schema.JsonObject)(revision.content)
      let next: Schema.JsonObject = content
      if (mode === 'missing-body') next = Struct.omit(content, ['value'])
      if (mode === 'malformed-body') next = { ...content, value: 'unreadable obsolete body' }
      if (mode === 'malformed-delta')
        next = { _tag: 'delta', version: 1, ops: 'unreadable old operations' }
      if (mode === 'zero-version') next = { ...content, version: 0 }
      if (mode === 'missing-version') next = Struct.omit(content, ['version'])
      if (mode === 'future-version') next = { ...content, version: 4 }
      if (index === 0) {
        if (mode === 'earlier-tag') next = { ...content, _tag: 'alien' }
        if (mode === 'earlier-zero-version') next = { ...content, version: 0 }
        if (mode === 'earlier-negative-version') next = { ...content, version: -1 }
        if (mode === 'earlier-missing-version') next = Struct.omit(content, ['version'])
        if (mode === 'earlier-fraction-version') next = { ...content, version: 1.5 }
        if (mode === 'earlier-unsafe-version')
          next = { ...content, version: Number.MAX_SAFE_INTEGER + 1 }
      }
      let seq = revision.seq
      if (index === 1) {
        if (mode === 'duplicate-sequence') seq = row.value.revisions[0]!.seq
        if (mode === 'zero-sequence') seq = damagedSeq(0)
        if (mode === 'fraction-sequence') seq = damagedSeq(1.5)
        if (mode === 'unsafe-sequence') seq = damagedSeq(Number.MAX_SAFE_INTEGER + 1)
        if (mode === 'future-sequence') seq = damagedSeq(999)
      }
      return { seq, content: next }
    }),
  )
  let record = row.value.record
  if (mode === 'wrong-kind') record = { ...record, kind: 'other.native.kind' }
  if (mode === 'wrong-policy') record = { ...record, history: 'rewindable' }
  if (mode === 'revision-before-creation') record = { ...record, createdAt: damagedSeq(2) }
  if (mode === 'unallocated-creation') record = { ...record, createdAt: damagedSeq(3) }
  if (mode === 'retirement-before-creation')
    record = { ...record, createdAt: damagedSeq(2), retiredAt: damagedSeq(1) }
  if (mode === 'retirement-before-revision') record = { ...record, retiredAt: damagedSeq(1) }
  if (mode === 'unallocated-retirement') record = { ...record, retiredAt: damagedSeq(3) }
  if (mode === 'descending-sequence') revisions.reverse()
  return { ...row, value: { record, revisions } } satisfies Row.Stored
})
const seed = Effect.scoped(
  Effect.gen(function* () {
    const session = yield* Session.make()
    const snapshot = yield* Session.commit(session, (tx) =>
      Transaction.ensureDocument(tx, definition, { scope: { _tag: 'session' } }),
    )
    const storage = yield* Storage.Storage
    yield* storage.commit([
      {
        _tag: 'document.change',
        id: snapshot.record.id,
        content: { _tag: 'delta', version: 1, ops: [['set', ['count'], 2]] },
      },
    ])
    return Retirement.target(snapshot)
  }),
)
const verify = <R>(
  selected: Retirement.Target,
  mode: Mode,
  read: Effect.Effect<Row.Stored, Storage.StorageError, R>,
  unchanged: Effect.Effect<void, Storage.StorageError, R>,
) =>
  Effect.gen(function* () {
    const storage = yield* Storage.Storage
    const before = yield* read
    assert.isDefined(storage.documentMetadata)
    const metadata = yield* storage.documentMetadata!(selected.id).pipe(Effect.result)
    yield* unchanged
    if (!successful(mode) && mode !== 'future-version' && mode !== 'wrong-kind')
      assert.strictEqual(metadata._tag, 'Failure')
    else assert.strictEqual(metadata._tag, 'Success')
    if (
      mode === 'missing-body' ||
      mode === 'malformed-body' ||
      mode === 'malformed-delta' ||
      mode === 'zero-version' ||
      mode === 'missing-version'
    ) {
      const ordinary = yield* storage.document(selected.id).pipe(Effect.flip)
      assert.strictEqual(ordinary.reason, 'corrupt')
    }
    const session = yield* Session.make() // A fresh cache, never the healthy seed snapshot.
    const retired = yield* Session.commit(session, (tx) =>
      Transaction.retireDocumentMetadata(tx, policy, selected),
    ).pipe(Effect.result)
    if (!successful(mode)) {
      assert.strictEqual(retired._tag, 'Failure')
      yield* unchanged
      assert.deepEqual(yield* read, before)
      return
    }
    assert.strictEqual(retired._tag, 'Success')
    const after = yield* read
    assert.strictEqual(after._tag, 'document')
    if (after._tag !== 'document' || before._tag !== 'document') return
    assert.isDefined(after.value.record.retiredAt)
    assert.deepEqual(after.value.revisions, [])
    assert.deepEqual(Struct.omit(after.value.record, ['retiredAt']), before.value.record)
    assert.isTrue(Option.isNone(yield* storage.findDocument(selected.address)))
    if (mode !== 'valid')
      assert.strictEqual(
        (yield* storage.document(selected.id, Sequence.make(1)).pipe(Effect.flip)).reason,
        'invalid',
      )
    else assert.isTrue(Option.isNone(yield* storage.document(selected.id)))
  })

const verifyCommit = <E, R>(
  storage: Storage.Storage['Service'],
  mode: Mode,
  read: (id: Storage.DocumentId) => Effect.Effect<Row.Stored, E, R>,
  write: (row: Row.Stored) => Effect.Effect<unknown, E, R>,
  counters: Effect.Effect<unknown, E, R>,
) =>
  Effect.gen(function* () {
    const selected = yield* seed
    const original = yield* read(selected.id)
    const changed = yield* corrupt(original, mode)
    const session = yield* Session.make()
    let admitted: unknown
    const result = yield* Session.commit(session, (tx) =>
      Effect.gen(function* () {
        yield* Transaction.ensureRoot(tx)
        yield* Transaction.retireDocumentMetadata(tx, policy, selected)
        yield* write(changed)
        admitted = yield* counters
      }),
    ).pipe(Effect.result)
    assert.strictEqual(result._tag, successful(mode) ? 'Success' : 'Failure')
    const after = yield* read(selected.id)
    if (successful(mode)) {
      if (after._tag !== 'document' || changed._tag !== 'document') return
      assert.isDefined(after.value.record.retiredAt)
      assert.deepEqual(after.value.revisions, [])
      assert.isTrue(Option.isSome(yield* storage.conversation(Storage.ROOT_CONVERSATION_ID)))
    } else {
      assert.deepEqual(after, changed)
      assert.deepEqual(yield* counters, admitted)
      assert.isTrue(Option.isNone(yield* storage.conversation(Storage.ROOT_CONVERSATION_ID)))
    }
  }).pipe(Effect.provideService(Storage.Storage, storage))

for (const backend of ['memory', 'sqlite', 'jsonl'] as const) {
  for (const mode of modes.filter(
    (mode) => representable(backend, mode) && (successful(mode) || modes.indexOf(mode) >= 9),
  )) {
    it.effect(
      `native ${backend} revalidates ${mode} after staging and rolls back mixed writes`,
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const fs = yield* FileSystem.FileSystem
            const directory = yield* fs.makeTempDirectoryScoped({ prefix: 'native-header-commit-' })
            if (backend === 'memory') {
              const access = yield* Memory.makeAccess
              const storage = yield* Kernel.make(access)
              yield* verifyCommit(
                storage,
                mode,
                (id) => readMemory(access, id),
                (row) => writeMemory(access, row),
                access.metadata,
              )
            } else if (backend === 'sqlite') {
              yield* Effect.gen(function* () {
                const sql = yield* SqlClient.SqlClient
                const storage = yield* Storage.Storage
                yield* verifyCommit(
                  storage,
                  mode,
                  readSql,
                  writeSql,
                  sql`SELECT * FROM effect_harness_metadata`.pipe(Effect.mapError(testError)),
                )
              }).pipe(
                Effect.provide(
                  Storage.layerSql.pipe(
                    Layer.provideMerge(
                      SqliteClient.layer({ filename: `${directory}/store.sqlite` }),
                    ),
                  ),
                ),
              )
            } else {
              const filePath = `${directory}/store.jsonl`
              const layer = Storage.layerJsonl({ filePath })
              yield* Effect.gen(function* () {
                const original = yield* Storage.Storage
                // JSONL is single-writer: stage with a healthy reader, then cold-reopen the
                // authoritative journal for commit. Editing an already-open writer's file
                // is not a foreign-writer fencing guarantee.
                const storage: Storage.Storage['Service'] = {
                  ...original,
                  commit: (writes) =>
                    Effect.scoped(
                      Effect.gen(function* () {
                        const fresh = yield* Storage.Storage
                        return yield* fresh.commit(writes)
                      }).pipe(
                        Effect.provide(Storage.layerJsonl({ filePath })),
                        Effect.provideService(FileSystem.FileSystem, fs),
                      ),
                    ),
                  conversation: (id) =>
                    Effect.scoped(
                      Effect.gen(function* () {
                        const fresh = yield* Storage.Storage
                        return yield* fresh.conversation(id)
                      }).pipe(
                        Effect.provide(Storage.layerJsonl({ filePath })),
                        Effect.provideService(FileSystem.FileSystem, fs),
                      ),
                    ),
                }
                // Seed through the original writer so the staging reader sees the history.
                const staging = { ...storage, commit: original.commit }
                const selected = yield* seed.pipe(Effect.provideService(Storage.Storage, staging))
                const read = readJsonl(filePath, selected.id)
                const changed = yield* corrupt(yield* read, mode)
                const session = yield* Session.make().pipe(
                  Effect.provideService(Storage.Storage, storage),
                )
                let damaged = ''
                const result = yield* Session.commit(session, (tx) =>
                  Effect.gen(function* () {
                    yield* Transaction.ensureRoot(tx)
                    yield* Transaction.retireDocumentMetadata(tx, policy, selected)
                    yield* writeJsonl(filePath, changed)
                    damaged = yield* fs.readFileString(filePath)
                  }),
                ).pipe(Effect.result)
                assert.strictEqual(result._tag, successful(mode) ? 'Success' : 'Failure')
                if (successful(mode)) {
                  const after = yield* read
                  if (after._tag !== 'document' || changed._tag !== 'document') return
                  assert.isDefined(after.value.record.retiredAt)
                  assert.deepEqual(after.value.revisions, [])
                  assert.isTrue((yield* fs.readFileString(filePath)).startsWith(damaged))
                  assert.isTrue(
                    Option.isSome(yield* storage.conversation(Storage.ROOT_CONVERSATION_ID)),
                  )
                } else {
                  assert.deepEqual(yield* read, changed)
                  assert.strictEqual(yield* fs.readFileString(filePath), damaged)
                }
              }).pipe(Effect.provide(layer))
            }
          }),
        ).pipe(Effect.provide(NodeServices.layer)),
    )
  }
}

for (const backend of ['memory', 'jsonl', 'sqlite'] as const) {
  for (const mode of modes.filter((mode) => representable(backend, mode))) {
    it.effect(
      `native ${backend} ${mode} validates fresh headers and persists only retirement`,
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const fs = yield* FileSystem.FileSystem
            const directory = yield* fs.makeTempDirectoryScoped({ prefix: 'native-retirement-' })
            if (backend === 'memory') {
              const access = yield* Memory.makeAccess
              const storage = yield* Kernel.make(access)
              const selected = yield* seed.pipe(Effect.provideService(Storage.Storage, storage))
              const changed = yield* corrupt(yield* readMemory(access, selected.id), mode)
              const counters = yield* access.metadata
              yield* writeMemory(access, changed, counters)
              const read = readMemory(access, selected.id)
              const unchanged = Effect.gen(function* () {
                assert.deepEqual(yield* access.metadata, counters)
                assert.deepEqual(yield* read, changed)
              })
              yield* verify(selected, mode, read, unchanged).pipe(
                Effect.provideService(Storage.Storage, storage),
              )
            } else if (backend === 'sqlite') {
              yield* Effect.gen(function* () {
                const sql = yield* SqlClient.SqlClient
                const selected = yield* seed
                const changed = yield* corrupt(yield* readSql(selected.id), mode)
                yield* writeSql(changed)
                const counters = yield* sql`SELECT * FROM effect_harness_metadata`
                const read = readSql(selected.id)
                const unchanged = Effect.gen(function* () {
                  assert.deepEqual(yield* sql`SELECT * FROM effect_harness_metadata`, counters)
                  assert.deepEqual(yield* read, changed)
                }).pipe(Effect.mapError(testError))
                yield* verify(selected, mode, read, unchanged)
              }).pipe(
                Effect.provide(
                  Storage.layerSql.pipe(
                    Layer.provideMerge(
                      SqliteClient.layer({ filename: `${directory}/store.sqlite` }),
                    ),
                  ),
                ),
              )
            } else {
              const filePath = `${directory}/store.jsonl`
              const layer = Storage.layerJsonl({ filePath })
              const selected = yield* seed.pipe(Effect.provide(layer))
              const read = readJsonl(filePath, selected.id)
              const changed = yield* corrupt(yield* read, mode)
              yield* writeJsonl(filePath, changed)
              const bytes = yield* fs.readFileString(filePath)
              const unchanged = Effect.gen(function* () {
                assert.strictEqual(yield* fs.readFileString(filePath), bytes)
                assert.deepEqual(yield* read, changed)
              }).pipe(Effect.mapError(testError))
              const outcome = yield* verify(selected, mode, read, unchanged).pipe(
                Effect.provide(layer),
                Effect.result,
              )
              if (outcome._tag === 'Failure') {
                assert.isTrue(
                  [
                    'zero-sequence',
                    'fraction-sequence',
                    'unsafe-sequence',
                    'duplicate-sequence',
                    'descending-sequence',
                    'future-sequence',
                  ].includes(mode),
                )
                assert.instanceOf(outcome.failure, Storage.StorageError)
                assert.strictEqual(outcome.failure.reason, 'corrupt')
                yield* unchanged
              }
              assert.isTrue((yield* fs.readFileString(filePath)).startsWith(bytes)) // Existing journal bytes are immutable.
              if (successful(mode))
                yield* Effect.gen(function* () {
                  const storage = yield* Storage.Storage
                  assert.isTrue(Option.isNone(yield* storage.findDocument(selected.address)))
                }).pipe(Effect.provide(layer))
            }
          }),
        ).pipe(Effect.provide(NodeServices.layer)),
    )
  }
}

it.effect(
  'revalidates native version headers at commit and preserves earlier writes on refusal',
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const access = yield* Memory.makeAccess
        const storage = yield* Kernel.make(access)
        const selected = yield* seed.pipe(Effect.provideService(Storage.Storage, storage))
        const original = yield* readMemory(access, selected.id)
        const changed = yield* corrupt(original, 'missing-body')
        const counters = yield* access.metadata
        yield* writeMemory(access, changed, counters)
        const session = yield* Session.make().pipe(Effect.provideService(Storage.Storage, storage))
        const outcome = yield* Session.commit(session, (tx) =>
          Effect.gen(function* () {
            yield* Transaction.ensureRoot(tx)
            yield* Transaction.retireDocumentMetadata(tx, policy, selected)
            // A native seam mutation tests commit's own validation, rather than a reader override.
            yield* writeMemory(access, yield* corrupt(changed, 'missing-version'), counters)
          }),
        ).pipe(Effect.flip)
        assert.strictEqual(outcome._tag, 'StorageError')
        assert.deepEqual(yield* access.metadata, counters)
        assert.isTrue(Option.isNone(yield* storage.conversation(Storage.ROOT_CONVERSATION_ID)))
        const after = yield* readMemory(access, selected.id)
        if (after._tag === 'document') assert.isUndefined(after.value.record.retiredAt)
      }),
    ),
)

for (const backend of ['memory', 'jsonl', 'sqlite'] as const) {
  for (const shape of ['reset', 'asOf', 'current', 'initial'] as const) {
    it.effect(`native ${backend} preserves migrated ${shape} history and copied fork headers`, () =>
      Effect.scoped(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem
          const directory = yield* fs.makeTempDirectoryScoped({ prefix: 'native-valid-history-' })
          const run = Effect.gen(function* () {
            const storage = yield* Storage.Storage
            const id = Domain.DocumentId.make(2)
            const childId = Domain.ConversationId.make(3)
            const copyId = Domain.DocumentId.make(4)
            const root = Storage.ROOT_CONVERSATION_ID
            const record: Storage.DocumentCreate = {
              id,
              kind: 'native.history',
              scope:
                shape === 'reset'
                  ? { _tag: 'session' }
                  : { _tag: 'conversation', conversationId: root },
              ...(shape === 'reset' ? {} : { history: 'rewindable', fork: shape }),
            }
            const first = yield* storage.commit([
              { _tag: 'conversation', value: { id: root } },
              {
                _tag: 'document.create',
                record,
                content: { _tag: 'base', version: 1, value: { count: 1 } },
              },
            ])
            yield* storage.commit([
              {
                _tag: 'document.change',
                id,
                content: { _tag: 'delta', version: 1, ops: [['set', ['count'], 2]] },
              },
            ])
            yield* storage.commit([
              {
                _tag: 'document.change',
                id,
                content: { _tag: 'base', version: 2, value: { count: 3 } },
              },
            ])
            yield* storage.commit([
              {
                _tag: 'document.change',
                id,
                content: { _tag: 'delta', version: 2, ops: [['set', ['count'], 4]] },
              },
            ])
            assert.strictEqual(Option.getOrThrow(yield* storage.documentMetadata!(id)).version, 2)
            assert.deepEqual(Option.getOrThrow(yield* storage.document(id)).value, { count: 4 })
            const writes: Array<Storage.StorageWrite> = [{ _tag: 'document.retire', id }]
            if (shape !== 'reset') {
              assert.deepEqual(Option.getOrThrow(yield* storage.document(id, first)).value, {
                count: 1,
              })
              yield* storage.commit([
                { _tag: 'conversation', value: { id: childId } },
                {
                  _tag: 'document.copy',
                  record: {
                    ...record,
                    id: copyId,
                    scope: { _tag: 'conversation', conversationId: childId },
                  },
                  source: { id, at: first },
                },
              ])
              const copied = Option.getOrThrow(yield* storage.documentMetadata!(copyId))
              assert.strictEqual(copied.version, 1)
              assert.deepEqual(Option.getOrThrow(yield* storage.document(copyId)).value, {
                count: 1,
              })
              writes.push({ _tag: 'document.retire', id: copyId })
            }
            yield* storage.commit(writes)
            // Native content+retire may share a sequence; ordinary current-only
            // retirement clears history while rewindable retirement retains it.
            for (const history of ['latest', 'rewindable'] as const) {
              const retiredId = Domain.DocumentId.make(history === 'latest' ? 5 : 6)
              yield* storage.commit([
                {
                  _tag: 'document.create',
                  record: {
                    id: retiredId,
                    kind: `native.same-commit.${history}`,
                    scope: { _tag: 'conversation', conversationId: root },
                    history,
                    fork: 'current',
                  },
                  content: { _tag: 'base', version: 1, value: { count: 1 } },
                },
              ])
              yield* storage.commit([
                {
                  _tag: 'document.change',
                  id: retiredId,
                  content: { _tag: 'base', version: 1, value: { count: 2 } },
                },
                { _tag: 'document.retire', id: retiredId },
              ])
              assert.isTrue(Option.isNone(yield* storage.documentMetadata!(retiredId)))
            }
            assert.isTrue(Option.isNone(yield* storage.documentMetadata!(id)))
            if (shape !== 'reset')
              assert.deepEqual(Option.getOrThrow(yield* storage.document(id, first)).value, {
                count: 1,
              })
          })
          if (backend === 'memory') yield* run.pipe(Effect.provide(Storage.layerMemory))
          else if (backend === 'sqlite')
            yield* run.pipe(
              Effect.provide(
                Storage.layerSql.pipe(
                  Layer.provideMerge(SqliteClient.layer({ filename: `${directory}/store.sqlite` })),
                ),
              ),
            )
          else
            yield* run.pipe(
              Effect.provide(Storage.layerJsonl({ filePath: `${directory}/store.jsonl` })),
            )
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
    )
  }
}

it.effect(
  'native malformed retirement and recreation share one atomic batch and reject stale targets',
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const access = yield* Memory.makeAccess
        const storage = yield* Kernel.make(access)
        const selected = yield* seed.pipe(Effect.provideService(Storage.Storage, storage))
        const changed = yield* corrupt(yield* readMemory(access, selected.id), 'malformed-body')
        yield* writeMemory(access, changed)
        const session = yield* Session.make().pipe(Effect.provideService(Storage.Storage, storage))
        const recreated = yield* Session.commit(session, (tx) =>
          Effect.gen(function* () {
            yield* Transaction.ensureRoot(tx)
            const entry = yield* Transaction.appendEntry(tx, Storage.ROOT_CONVERSATION_ID, {
              kind: 'native.prior',
            })
            yield* Transaction.retireDocumentMetadata(tx, policy, selected)
            const recreated = yield* Transaction.ensureDocument(tx, definition, {
              scope: { _tag: 'session' },
            })
            assert.strictEqual(
              Option.getOrThrow(yield* Transaction.entry(tx, entry.id)).kind,
              'native.prior',
            )
            return recreated
          }),
        )
        assert.notStrictEqual(recreated.record.id, selected.id)
        const stale = yield* Session.commit(session, (tx) =>
          Transaction.retireDocumentMetadata(tx, policy, selected),
        ).pipe(Effect.flip)
        assert.strictEqual(stale._tag, 'SessionError')
        assert.strictEqual(
          Option.getOrThrow(yield* storage.findDocument(selected.address)).id,
          recreated.record.id,
        )
        const retired = yield* readMemory(access, selected.id)
        if (changed._tag === 'document' && retired._tag === 'document')
          assert.deepEqual(retired.value.revisions, [])
      }),
    ),
)
