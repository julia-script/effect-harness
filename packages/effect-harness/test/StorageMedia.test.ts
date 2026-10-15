import { assert, it } from '@effect/vitest'
import { NodeServices } from '@effect/platform-node'
import * as SqliteClient from '@effect/sql-sqlite-node/SqliteClient'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as SqlClient from 'effect/sql/SqlClient'
import * as Prompt from 'effect/ai/Prompt'
import * as Base64 from 'effect/encoding/Base64'
import * as Storage from 'effect-harness/Storage'
import * as Media from 'effect-harness/StorageMedia'
import * as Wire from 'effect-harness/PromptWire'
import * as Record from 'effect-harness/Record'
import * as Projection from 'effect-harness/ContextProjection'
import * as ToolResult from 'effect-harness/ToolResult'
import * as ResultEntry from '../src/internal/ToolResultEntry.js'
import * as Memory from '../src/internal/Memory.js'
import * as Kernel from '../src/internal/Kernel.js'

const root = Record.ROOT_CONVERSATION_ID
const native = (data: Uint8Array | string | URL, index = 0) =>
  Prompt.filePart({
    mediaType: 'image/png',
    data,
    fileName: `image-${index}.png`,
    options: {
      openai: { imageDetail: 'high' },
      anthropic: { cacheControl: { type: 'ephemeral' } },
    },
  })
const encodePrompt = (parts: ReadonlyArray<Prompt.UserMessagePart>) =>
  Schema.encodeSync(Schema.toCodecJson(Wire.Prompt))(
    Prompt.fromMessages([Prompt.userMessage({ content: parts })]),
  )
const content = (parts: ReadonlyArray<Prompt.UserMessagePart>): Record.Content => ({
  _tag: 'base',
  version: 1,
  value: { request: { prompt: encodePrompt(parts) } },
})
const imageBytes = Array.from({ length: 4 }, (_, index) => new Uint8Array(835584).fill(index + 21))
const parts = imageBytes.map((data, index) => native(data, index))
const refs = (value: unknown) => JSON.stringify(value)

it.effect(
  'deduplicates bytes/base64/data URLs and preserves literal UTF-16, URLs and application JSON',
  () =>
    Effect.gen(function* () {
      const data = new Uint8Array([0, 255, 127, 128])
      const base64 = Base64.encode(data)
      const marker = {
        version: 1,
        body: null,
        media: [{ path: ['anything'], digest: 'b-' + '0'.repeat(64) }],
      }
      const body = {
        entry: {
          kind: 'user',
          model: yield* Schema.encodeEffect(Schema.Array(Schema.toCodecJson(Wire.Message)))([
            Prompt.userMessage({
              content: [
                native(data),
                native(base64),
                native(`data:image/png;name=X;base64,${base64}`),
                native(new URL('file:///never-read')),
                native('https://never-fetch/image'),
                native('opaque\ud800payload'),
              ],
            }),
          ]),
          data: { marker, unrelated: 'q'.repeat(128 * 1024) },
        },
      }
      const packed = yield* Media.pack(body, { category: 'entry' })
      assert.strictEqual(packed.objects.length, 2)
      assert.strictEqual(packed.packed.media.length, 4)
      assert.strictEqual(packed.packed.media[0]?.digest, packed.packed.media[2]?.digest)
      const objects = new Map(packed.objects.map((object) => [object.digest, object.bytes]))
      const hydrated = yield* Media.hydrate(packed.packed, { category: 'entry' }, (key) =>
        Effect.succeed(objects.get(key)!),
      )
      assert.deepEqual(hydrated, body)
      assert.deepEqual((hydrated as typeof body).entry.data, body.entry.data)
    }),
)

it.effect(
  'rejects corrupt bytes, duplicate, overlapping, missing and unrelated manifest paths',
  () =>
    Effect.gen(function* () {
      const value = {
        entry: {
          kind: 'user',
          model: yield* Schema.encodeEffect(Schema.Array(Schema.toCodecJson(Wire.Message)))([
            Prompt.userMessage({ content: [native(new Uint8Array([7, 8, 9]))] }),
          ]),
          data: null,
        },
      }
      const { packed, objects } = yield* Media.pack(value, { category: 'entry' })
      const ref = packed.media[0]!
      const read = () => Effect.succeed(objects[0]!.bytes)
      assert.strictEqual(
        (yield* Media.manifest({
          ...packed,
          media: [
            ref,
            { ...ref, path: ref.path.map((key) => (typeof key === 'number' ? String(key) : key)) },
          ],
        }).pipe(Effect.result))._tag,
        'Failure',
      )
      assert.strictEqual(
        (yield* Media.hydrate(packed, { category: 'entry' }, () =>
          Effect.succeed(new Uint8Array([7, 8, 10])),
        ).pipe(Effect.flip)).operation,
        'media.integrity',
      )
      for (const media of [
        [ref, ref],
        [{ ...ref, path: ['entry', 'data'] as const }],
        [{ ...ref, path: ['__proto__', 'x'] as const }],
        [{ ...ref, byteLength: -1 }],
        [{ ...ref, digest: '../escape' }],
        [],
      ]) {
        const result = yield* Media.hydrate({ ...packed, media }, { category: 'entry' }, read).pipe(
          Effect.result,
        )
        assert.strictEqual(result._tag, 'Failure')
      }
    }),
)

for (const backend of ['memory', 'jsonl', 'sqlite'] as const) {
  it.live(
    `${backend}: four images across 100 requests, canonical results and forks have one object each and bounded growth`,
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem
          const directory = yield* fs.makeTempDirectoryScoped({ prefix: 'shared-images-' })
          const filename = `${directory}/store`
          const memory = yield* Memory.makeAccess
          const memoryStorage = yield* Kernel.make(memory)
          const sqlLayer = SqliteClient.layer({ filename })
          const layer =
            backend === 'jsonl'
              ? Storage.layerJsonl({ filePath: filename })
              : Storage.layerSql.pipe(Layer.provide(sqlLayer))
          const fixture = (withImages: boolean) =>
            Effect.gen(function* () {
              const storage = yield* Storage.Storage
              const selected = withImages ? parts : []
              const result = yield* ResultEntry.encode({
                content: selected,
                isError: true,
                diagnostics: [],
                callId: 'call-image',
                name: 'image',
                explicitEnvelope: true,
              })
              yield* storage.commit([
                { _tag: 'conversation', value: { id: root } },
                {
                  _tag: 'entry',
                  value: {
                    id: Record.EntryId.make(2),
                    conversationId: root,
                    kind: 'tool.result',
                    data: result,
                  },
                },
                ...Array.from({ length: 3 }, (_, index): Record.StorageWrite => ({
                  _tag: 'conversation',
                  value: {
                    id: Record.ConversationId.make(index + 110),
                    parent: { conversationId: root, at: Record.EntryId.make(2) },
                  },
                })),
              ])
              const requestContent = content(selected)
              for (let index = 0; index < 100; index++)
                yield* storage.commit([
                  {
                    _tag: 'document.create',
                    record: {
                      id: Record.DocumentId.make(index + 10),
                      kind: 'harness.request',
                      scope: { _tag: 'task', taskId: Record.TaskId.make(3) },
                      key: `request-${index}`,
                    },
                    content: requestContent,
                  },
                ])
              for (let i = 0; i < 3; i++) {
                const entries = yield* storage
                  .scanEntries({ conversationId: Record.ConversationId.make(110 + i) })
                  .pipe(Stream.runCollect)
                assert.strictEqual(entries.length, 1)
                assert.deepEqual((yield* ResultEntry.read(entries[0]!)).content, selected)
              }
              const returned = yield* Projection.messages(
                Option.getOrThrow(yield* storage.entry(Record.EntryId.make(2))).entry,
              )
              const request = Option.getOrThrow(yield* storage.document(Record.DocumentId.make(10)))
              assert.deepEqual(
                request.value,
                content(selected)._tag === 'base'
                  ? { request: { prompt: encodePrompt(selected) } }
                  : {},
              )
              return returned
            })
          const baselineMemory = yield* Memory.makeAccess
          const baselineFile = `${directory}/baseline`
          const baselineStorage = yield* Kernel.make(baselineMemory)
          const baselineLayer = (() => {
            if (backend === 'memory') return Layer.succeed(Storage.Storage, baselineStorage)
            if (backend === 'jsonl') return Storage.layerJsonl({ filePath: baselineFile })
            return Storage.layerSql.pipe(
              Layer.provide(SqliteClient.layer({ filename: baselineFile })),
            )
          })()
          yield* fixture(false).pipe(Effect.provide(baselineLayer))
          const baseline = yield* Effect.gen(function* () {
            if (backend === 'memory')
              return refs([...(yield* baselineMemory.snapshot).rows.values()]).length
            if (backend === 'sqlite') return Number((yield* fs.stat(baselineFile)).size)
            let size = Number((yield* fs.stat(baselineFile)).size)
            for (const name of yield* fs.readDirectory(`${baselineFile}.parts`))
              size += Number((yield* fs.stat(`${baselineFile}.parts/${name}`)).size)
            return size
          })
          if (backend === 'memory') {
            yield* fixture(true).pipe(Effect.provideService(Storage.Storage, memoryStorage))
            const state = yield* memory.snapshot
            assert.strictEqual(state.objects.size, 4)
            const raw = [...state.objects.values()].reduce((n, bytes) => n + bytes.length, 0)
            const physical = raw + refs([...state.rows.values()]).length
            assert.strictEqual(raw, 3342336)
            assert.isBelow(physical, raw + 404 * 1024 + 256 * 1024)
            console.info(
              JSON.stringify({
                backend,
                objects: 4,
                raw,
                physical,
                baseline,
                imageGrowth: physical - baseline,
              }),
            )
            return
          }
          yield* fixture(true).pipe(Effect.provide(layer))
          // Cold reads hydrate actual native content, including bytes and options, without a live resolver.
          const messages = yield* Effect.gen(function* () {
            const storage = yield* Storage.Storage
            const entry = Option.getOrThrow(yield* storage.entry(Record.EntryId.make(2))).entry
            assert.deepEqual((yield* ResultEntry.read(entry)).content, parts)
            for (let i = 0; i < 100; i++) {
              const document = Option.getOrThrow(
                yield* storage.document(Record.DocumentId.make(10 + i)),
              )
              assert.deepEqual(document.value, { request: { prompt: encodePrompt(parts) } })
            }
            return yield* Projection.messages(entry)
          }).pipe(Effect.provide(layer))
          const envelope =
            messages[0]?.role === 'tool' && messages[0].content[0]?.type === 'tool-result'
              ? messages[0].content[0].result
              : null
          const restored = yield* Schema.decodeUnknownEffect(
            Schema.toCodecJson(ToolResult.Envelope),
          )(envelope)
          assert.deepEqual(restored.content, parts)
          if (backend === 'jsonl') {
            const names = yield* fs.readDirectory(`${filename}.media`)
            assert.strictEqual(names.length, 4)
            let sidecars = 0
            for (const name of yield* fs.readDirectory(`${filename}.parts`))
              sidecars += Number((yield* fs.stat(`${filename}.parts/${name}`)).size)
            const journal = Number((yield* fs.stat(filename)).size)
            const raw = imageBytes.reduce((n, bytes) => n + bytes.length, 0)
            assert.isBelow(sidecars + journal + raw, raw + 404 * 1024 + 256 * 1024)
            console.info(
              JSON.stringify({
                backend,
                objects: names.length,
                journal,
                sidecars,
                raw,
                baseline,
                imageGrowth: journal + sidecars + raw - baseline,
              }),
            )
          } else
            yield* Effect.gen(function* () {
              const sql = yield* SqlClient.SqlClient
              const objects =
                yield* sql`SELECT COUNT(*) AS n, SUM(LENGTH(bytes)) AS bytes FROM effect_harness_media`
              assert.strictEqual(objects[0]?.n, 4)
              assert.strictEqual(objects[0]?.bytes, 3342336)
              const live =
                yield* sql`SELECT (SELECT SUM(LENGTH(payload)) FROM effect_harness_records) + (SELECT SUM(LENGTH(content)) FROM effect_harness_revisions) + (SELECT SUM(LENGTH(bytes)) FROM effect_harness_media) AS bytes`
              const pages = yield* sql`PRAGMA page_count`
              const size = yield* sql`PRAGMA page_size`
              const physical = Number(pages[0]?.page_count) * Number(size[0]?.page_size)
              const free = yield* sql`PRAGMA freelist_count`
              const wal = (yield* fs.exists(`${filename}-wal`))
                ? Number((yield* fs.stat(`${filename}-wal`)).size)
                : 0
              assert.isBelow(physical, 3342336 + 404 * 1024 + 256 * 1024)
              console.info(
                JSON.stringify({
                  backend,
                  objects: objects[0]?.n,
                  raw: objects[0]?.bytes,
                  live: live[0]?.bytes,
                  physical,
                  baseline,
                  imageGrowth: physical - baseline,
                  wal,
                  reusablePages: Number(free[0]?.freelist_count),
                }),
              )
            }).pipe(Effect.provide(sqlLayer))
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
  )
}

it.effect('visits only explicit queue, round, live-progress and nested content protocols', () =>
  Effect.gen(function* () {
    const file = yield* Schema.encodeEffect(Schema.toCodecJson(Wire.FilePart))(
      native(new Uint8Array([21, 22, 23])),
    )
    const marker = { _tag: '@effect-harness/Bytes', data: 'FRYX' }
    const entry = {
      kind: 'input',
      model: [{ role: 'user', content: [file] }],
      data: { file, marker },
    }
    const documents = [
      { kind: 'harness.inbox', value: { items: [{ payload: entry }] } },
      {
        kind: 'harness.tool.round',
        value: {
          round: {
            slots: [{ result: { content: [file], details: file, structuredOutput: file } }],
          },
        },
      },
      {
        kind: 'harness.model.live',
        value: { generation: { partial: { message: { content: entry.model } } } },
      },
      {
        kind: 'harness.tool.nested.result',
        value: {
          receipt: { result: { valueKind: 'content', structuredOutput: file, details: marker } },
        },
      },
      {
        kind: 'harness.tool.nested.result',
        value: {
          receipt: { result: { valueKind: 'content', valueFromDetails: true, details: file } },
        },
      },
      {
        kind: 'harness.tool.nested.result',
        value: {
          receipt: {
            result: {
              valueKind: 'content',
              valueFromContent: true,
              details: { _tag: '@effect-harness/ToolContent', content: [file] },
            },
          },
        },
      },
    ]
    for (const item of documents) {
      const body: Schema.Json = { _tag: 'base', version: 1, value: item.value }
      const context = { category: 'document' as const, kind: item.kind }
      const packed = yield* Media.pack(body, context)
      assert.strictEqual(packed.objects.length, 1, item.kind)
      assert.strictEqual(packed.packed.media.length, 1, item.kind)
      assert.deepEqual(
        yield* Media.hydrate(packed.packed, context, () =>
          Effect.succeed(packed.objects[0]!.bytes),
        ),
        body,
      )
    }
    for (const checkpoint of [
      { _tag: 'queued', draft: { content: [file] } },
      { partial: { message: { content: entry.model } } },
    ]) {
      const body = { kind: 'harness.run', state: { checkpoint }, input: { file, marker } }
      const packed = yield* Media.pack(body, { category: 'task' })
      assert.strictEqual(packed.packed.media.length, 1)
    }
    for (const kind of ['app.document', 'harness.tool.nested.result']) {
      const packed = yield* Media.pack(
        {
          _tag: 'base',
          version: 1,
          value: {
            receipt: { result: { valueKind: 'decoded', structuredOutput: file } },
            file,
            marker,
          },
        },
        { category: 'document', kind },
      )
      assert.strictEqual(packed.objects.length, 0)
    }
  }),
)

it.effect('owns resolver bytes before asynchronous integrity verification', () =>
  Effect.gen(function* () {
    const value = content([native(new Uint8Array([7, 8, 9]))])
    const context = { category: 'document' as const, kind: 'harness.request' }
    const packed = yield* Media.pack(value, context)
    const borrowed = packed.objects[0]!.bytes.slice()
    const restored = yield* Media.hydrate(packed.packed, context, () =>
      Effect.sync(() => {
        queueMicrotask(() => borrowed.fill(99))
        return borrowed
      }),
    )
    assert.deepEqual(restored, value)
    assert.strictEqual(borrowed[0], 99)
  }),
)
