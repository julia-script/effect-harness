import { assert, it } from '@effect/vitest'
import { NodeServices } from '@effect/platform-node'
import * as SqliteClient from '@effect/sql-sqlite-node/SqliteClient'
import * as SqlClient from 'effect/sql/SqlClient'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
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
const RowJson = Schema.fromJsonString(Row.Stored)
const FrameJson = Schema.fromJsonString(
  Schema.Struct({ metadata: Metadata.Metadata, rows: Schema.Array(Row.Stored) }),
)
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
] as const
type Mode = (typeof modes)[number]
const successful = (mode: Mode) =>
  ['missing-body', 'malformed-body', 'malformed-delta', 'valid'].includes(mode)
const corrupt = Effect.fnUntraced(function* (row: Row.Stored, mode: Mode) {
  if (row._tag !== 'document') return row
  const revisions = yield* Effect.forEach(
    row.value.revisions,
    Effect.fnUntraced(function* (revision) {
      const content = yield* Schema.decodeUnknownEffect(Schema.JsonObject)(revision.content)
      let next: Schema.JsonObject = content
      if (mode === 'missing-body') next = Struct.omit(content, ['value'])
      if (mode === 'malformed-body') next = { ...content, value: 'unreadable obsolete body' }
      if (mode === 'malformed-delta')
        next = { _tag: 'delta', version: 1, ops: 'unreadable old operations' }
      if (mode === 'zero-version') next = { ...content, version: 0 }
      if (mode === 'missing-version') next = Struct.omit(content, ['version'])
      if (mode === 'future-version') next = { ...content, version: 4 }
      return { ...revision, content: next }
    }),
  )
  let record = row.value.record
  if (mode === 'wrong-kind') record = { ...record, kind: 'other.native.kind' }
  if (mode === 'wrong-policy') record = { ...record, history: 'rewindable' }
  return { ...row, value: { record, revisions } } satisfies Row.Stored
})
const seed = Effect.scoped(
  Effect.gen(function* () {
    const session = yield* Session.make()
    const snapshot = yield* Session.commit(session, (tx) =>
      Transaction.ensureDocument(tx, definition, { scope: { _tag: 'session' } }),
    )
    return Retirement.target(snapshot)
  }),
)
const verify = (
  selected: Retirement.Target,
  mode: Mode,
  read: Effect.Effect<Row.Stored, Storage.StorageError>,
  unchanged: Effect.Effect<void, Storage.StorageError>,
) =>
  Effect.gen(function* () {
    const storage = yield* Storage.Storage
    const before = yield* read
    assert.isDefined(storage.documentMetadata)
    const metadata = yield* storage.documentMetadata!(selected.id).pipe(Effect.result)
    yield* unchanged
    if (mode === 'zero-version' || mode === 'missing-version' || mode === 'wrong-policy')
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
    assert.deepEqual(after.value.revisions, before.value.revisions)
    assert.deepEqual(Struct.omit(after.value.record, ['retiredAt']), before.value.record)
    assert.isTrue(Option.isNone(yield* storage.findDocument(selected.address)))
    if (mode !== 'valid')
      assert.strictEqual(
        (yield* storage.document(selected.id, Sequence.make(1)).pipe(Effect.flip)).reason,
        'corrupt',
      )
    else assert.isTrue(Option.isNone(yield* storage.document(selected.id)))
  })

for (const backend of ['memory', 'jsonl', 'sqlite'] as const) {
  for (const mode of modes) {
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
              const changed = yield* corrupt(
                Option.getOrThrow(yield* access.get(selected.id)),
                mode,
              )
              const counters = yield* access.metadata
              yield* access.save([changed], counters)
              const read = access.get(selected.id).pipe(Effect.map(Option.getOrThrow))
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
                const raw =
                  yield* sql`SELECT payload FROM effect_harness_records WHERE id=${selected.id}`
                const original = yield* Schema.decodeUnknownEffect(Schema.String)(raw[0]?.payload)
                const changed = yield* corrupt(yield* Schema.decodeEffect(RowJson)(original), mode)
                const bytes = yield* Schema.encodeEffect(RowJson)(changed)
                yield* sql`UPDATE effect_harness_records SET payload=${bytes} WHERE id=${selected.id}`
                const counters = yield* sql`SELECT * FROM effect_harness_metadata`
                const read = Effect.gen(function* () {
                  const rows =
                    yield* sql`SELECT payload FROM effect_harness_records WHERE id=${selected.id}`
                  return yield* Schema.decodeUnknownEffect(RowJson)(rows[0]?.payload)
                }).pipe(
                  Effect.mapError(
                    (cause) =>
                      new Storage.StorageError({
                        reason: 'io',
                        operation: 'test',
                        message: 'read',
                        cause,
                      }),
                  ),
                )
                const unchanged = Effect.gen(function* () {
                  assert.deepEqual(yield* sql`SELECT * FROM effect_harness_metadata`, counters)
                  assert.deepEqual(
                    yield* sql`SELECT payload FROM effect_harness_records WHERE id=${selected.id}`,
                    [{ payload: bytes }],
                  )
                }).pipe(
                  Effect.mapError(
                    (cause) =>
                      new Storage.StorageError({
                        reason: 'io',
                        operation: 'test',
                        message: 'read',
                        cause,
                      }),
                  ),
                )
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
              const original = yield* fs.readFileString(filePath)
              const frames = yield* Effect.forEach(original.trimEnd().split('\n'), (line) =>
                Schema.decodeEffect(FrameJson)(line),
              )
              const changedFrames = yield* Effect.forEach(
                frames,
                Effect.fnUntraced(function* (frame) {
                  return {
                    ...frame,
                    rows: yield* Effect.forEach(frame.rows, (row) =>
                      Row.idOf(row) === selected.id ? corrupt(row, mode) : Effect.succeed(row),
                    ),
                  }
                }),
              )
              const bytes =
                (yield* Effect.forEach(changedFrames, (frame) =>
                  Schema.encodeEffect(FrameJson)(frame),
                )).join('\n') + '\n'
              yield* fs.writeFileString(filePath, bytes)
              const read = Effect.gen(function* () {
                const text = yield* fs.readFileString(filePath)
                const all = yield* Effect.forEach(text.trimEnd().split('\n'), (line) =>
                  Schema.decodeEffect(FrameJson)(line),
                )
                const row = all
                  .flatMap((frame) => frame.rows)
                  .findLast((row) => Row.idOf(row) === selected.id)
                return yield* Schema.decodeUnknownEffect(Row.Stored)(row)
              }).pipe(
                Effect.mapError(
                  (cause) =>
                    new Storage.StorageError({
                      reason: 'io',
                      operation: 'test',
                      message: 'read',
                      cause,
                    }),
                ),
              )
              const unchanged = fs.readFileString(filePath).pipe(
                Effect.tap((text) => Effect.sync(() => assert.strictEqual(text, bytes))),
                Effect.asVoid,
                Effect.mapError(
                  (cause) =>
                    new Storage.StorageError({
                      reason: 'io',
                      operation: 'test',
                      message: 'read',
                      cause,
                    }),
                ),
              )
              yield* verify(selected, mode, read, unchanged).pipe(Effect.provide(layer))
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
        const original = Option.getOrThrow(yield* access.get(selected.id))
        const changed = yield* corrupt(original, 'missing-body')
        const counters = yield* access.metadata
        yield* access.save([changed], counters)
        const session = yield* Session.make().pipe(Effect.provideService(Storage.Storage, storage))
        const outcome = yield* Session.commit(session, (tx) =>
          Effect.gen(function* () {
            yield* Transaction.ensureRoot(tx)
            yield* Transaction.retireDocumentMetadata(tx, policy, selected)
            // A native seam mutation tests commit's own validation, rather than a reader override.
            yield* access.save([yield* corrupt(changed, 'missing-version')], counters)
          }),
        ).pipe(Effect.flip)
        assert.strictEqual(outcome._tag, 'StorageError')
        assert.deepEqual(yield* access.metadata, counters)
        assert.isTrue(Option.isNone(yield* storage.conversation(Storage.ROOT_CONVERSATION_ID)))
        const after = Option.getOrThrow(yield* access.get(selected.id))
        if (after._tag === 'document') assert.isUndefined(after.value.record.retiredAt)
      }),
    ),
)

it.effect(
  'native malformed retirement and recreation share one atomic batch and reject stale targets',
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const access = yield* Memory.makeAccess
        const storage = yield* Kernel.make(access)
        const selected = yield* seed.pipe(Effect.provideService(Storage.Storage, storage))
        const changed = yield* corrupt(
          Option.getOrThrow(yield* access.get(selected.id)),
          'malformed-body',
        )
        yield* access.save([changed], yield* access.metadata)
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
        const retired = Option.getOrThrow(yield* access.get(selected.id))
        if (changed._tag === 'document' && retired._tag === 'document')
          assert.deepEqual(retired.value.revisions, changed.value.revisions)
      }),
    ),
)
