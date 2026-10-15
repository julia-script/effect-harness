import { assert, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as Context from 'effect/Context'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Stream from 'effect/Stream'
import * as SqlClient from 'effect/sql/SqlClient'
import * as SqliteClient from '@effect/sql-sqlite-node/SqliteClient'
import * as Storage from 'effect-harness/Storage'
import * as Media from 'effect-harness/StorageMedia'
import * as Record from 'effect-harness/Record'
import * as Prompt from 'effect/ai/Prompt'
import * as Wire from 'effect-harness/PromptWire'
import * as MediaRow from '../src/internal/MediaRow.js'

const root = Record.ROOT_CONVERSATION_ID
const messages = (kind: 'envelope' | 'native') => {
  const file = Prompt.filePart({ mediaType: 'image/png', data: new Uint8Array([1, 2, 3]) })
  return kind === 'envelope'
    ? [
        Prompt.toolMessage({
          content: ['first', 'second'].map((id) =>
            Prompt.toolResultPart({
              id,
              name: 'image',
              result: {
                _tag: '@effect-harness/ToolContent',
                content: [{ type: 'file', mediaType: 'image/png', data: 'AQID' }],
              },
              isFailure: false,
              providerExecuted: false,
            }),
          ),
        }),
      ]
    : [Prompt.userMessage({ content: [file, file] })]
}
const model = (kind: 'envelope' | 'native') =>
  Schema.encodeSync(Schema.Array(Schema.toCodecJson(Wire.Message)))(messages(kind))
const omit = (
  packed: Media.PackedBody,
  corruption: 'partial' | 'empty' | 'missing' | 'coverage',
) => {
  const { media, coverage, ...rest } = packed
  if (corruption === 'missing') return { ...rest, coverage }
  if (corruption === 'coverage') return { ...rest, media }
  return { ...rest, coverage, media: corruption === 'empty' ? [] : media.slice(0, 1) }
}

for (const kind of ['envelope', 'native'] as const)
  it.live(`SQL omitted same-digest occurrence must fail closed: ${kind}`, () =>
    Effect.scoped(
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient
        const storage = Context.get(yield* Layer.build(Storage.layerSql), Storage.Storage)
        const original = {
          id: Record.EntryId.make(2),
          conversationId: root,
          kind: 'input',
          model: model(kind),
        }
        yield* storage.commit([
          { _tag: 'conversation', value: { id: root } },
          { _tag: 'entry', value: original },
        ])
        const read = storage.scanEntries({ conversationId: root }).pipe(Stream.runCollect)
        assert.deepEqual(yield* read, [original])
        const row = (yield* sql`SELECT payload FROM effect_harness_records WHERE id = 2`)[0]!
        const originalPayload =
          typeof row.payload === 'string' ? row.payload : JSON.stringify(row.payload)
        const packed = yield* Schema.decodeEffect(Schema.fromJsonString(MediaRow.PackedRow))(
          originalPayload,
        )
        assert.strictEqual(packed.value.media.length, 2)
        assert.strictEqual(packed.value.media[0]!.digest, packed.value.media[1]!.digest)
        for (const corruption of ['partial', 'empty', 'missing'] as const) {
          yield* sql`UPDATE effect_harness_records SET payload = ${JSON.stringify({ ...packed, value: omit(packed.value, corruption) })} WHERE id = 2`
          const result = yield* read.pipe(Effect.result)
          // Restore only metadata; neither the retained object nor its unique owner changes.
          yield* sql`UPDATE effect_harness_records SET payload = ${originalPayload} WHERE id = 2`
          assert.deepEqual(yield* read, [original])
          assert.strictEqual(
            result._tag,
            'Failure',
            `Incomplete physical ${corruption} manifest must not return changed application JSON`,
          )
        }
      }),
    ).pipe(Effect.provide(SqliteClient.layer({ filename: ':memory:' }))),
  )

it.live(
  'SQL retained document revisions reject omitted repeated envelope occurrences and recover after metadata repair',
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient
        const storage = Context.get(yield* Layer.build(Storage.layerSql), Storage.Storage)
        const id = Record.DocumentId.make(2)
        const value = { request: { prompt: { content: model('envelope') } } }
        const seq = yield* storage.commit([
          { _tag: 'conversation', value: { id: root } },
          {
            _tag: 'document.create',
            record: {
              id,
              kind: 'harness.request',
              scope: { _tag: 'conversation', conversationId: root },
              history: 'rewindable',
              fork: 'asOf',
            },
            content: { _tag: 'base', version: 1, value },
          },
        ])
        yield* storage.commit([
          { _tag: 'document.change', id, content: { _tag: 'base', version: 1, value: {} } },
          { _tag: 'document.retire', id },
        ])
        const read = storage.document(id, seq)
        assert.deepEqual(Option.getOrThrow(yield* read).value, value)
        const row =
          (yield* sql`SELECT content FROM effect_harness_revisions WHERE document_id = 2 AND seq = ${seq}`)[0]!
        const original = typeof row.content === 'string' ? row.content : JSON.stringify(row.content)
        const packed = yield* Schema.decodeEffect(Schema.fromJsonString(Media.PackedBodySchema))(
          original,
        )
        assert.strictEqual(packed.media.length, 2)
        for (const corruption of ['partial', 'empty', 'missing', 'coverage'] as const) {
          yield* sql`UPDATE effect_harness_revisions SET content = ${JSON.stringify(omit(packed, corruption))} WHERE document_id = 2 AND seq = ${seq}`
          const result = yield* read.pipe(Effect.result)
          yield* sql`UPDATE effect_harness_revisions SET content = ${original} WHERE document_id = 2 AND seq = ${seq}`
          assert.deepEqual(Option.getOrThrow(yield* read).value, value)
          assert.strictEqual(result._tag, 'Failure')
        }
      }),
    ).pipe(Effect.provide(SqliteClient.layer({ filename: ':memory:' }))),
)

it.effect(
  'shared manifest checks physical occurrences before resolution while malformed application envelopes remain opaque',
  () =>
    Effect.gen(function* () {
      const opaque = {
        _tag: '@effect-harness/ToolContent',
        content: [{ type: 'file', mediaType: 'image/png', data: null }],
      }
      const encoded = [...model('envelope')]
      // Put malformed envelope JSON at the actual tool-result protocol position, too.
      encoded.push(
        yield* Schema.encodeEffect(Schema.toCodecJson(Wire.Message))(
          Prompt.toolMessage({
            content: [
              Prompt.toolResultPart({
                id: 'opaque',
                name: 'image',
                result: opaque,
                isFailure: false,
                providerExecuted: false,
              }),
            ],
          }),
        ),
      )
      const original = { entry: { kind: 'input', model: encoded, data: { opaque } } }
      const { packed, objects } = yield* Media.pack(original, { category: 'entry' })
      assert.strictEqual(packed.media.length, 2)
      assert.strictEqual(objects.length, 1)
      let reads = 0
      const read = () =>
        Effect.sync(() => {
          reads++
          return objects[0]!.bytes
        })
      for (const corruption of ['partial', 'empty', 'missing', 'coverage'] as const) {
        const result = yield* Media.manifest(omit(packed, corruption)).pipe(
          Effect.flatMap((body) => Media.hydrate(body, { category: 'entry' }, read)),
          Effect.result,
        )
        assert.strictEqual(result._tag, 'Failure')
        assert.strictEqual(reads, 0)
      }
      assert.deepEqual(yield* Media.hydrate(packed, { category: 'entry' }, read), original)
      assert.strictEqual(reads, 2)
      // Physical validation must not interpret an opaque retired document's damaged body.
      const empty = yield* Media.pack({ malformed: true }, { category: 'conversation' })
      assert.deepEqual(yield* Media.manifest(empty.packed), empty.packed)
      assert.strictEqual(
        (yield* Media.manifest({ ...empty.packed, version: 1 }).pipe(Effect.result))._tag,
        'Failure',
      )
    }),
)

it.live('SQL rejects the previous physical version before mutating stored records', () =>
  Effect.scoped(
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient
      yield* Layer.build(Storage.layerSql)
      yield* sql`UPDATE effect_harness_metadata SET persistence_version = 3`
      const before = yield* sql`SELECT * FROM effect_harness_metadata`
      const result = yield* Storage.Storage.pipe(Effect.provide(Storage.layerSql), Effect.result)
      assert.strictEqual(result._tag, 'Failure')
      if (result._tag === 'Failure') assert.strictEqual(result.failure.reason, 'unsupportedFormat')
      assert.deepEqual(yield* sql`SELECT * FROM effect_harness_metadata`, before)
    }),
  ).pipe(Effect.provide(SqliteClient.layer({ filename: ':memory:' }))),
)
