import { assert, it } from '@effect/vitest'
import { NodeServices } from '@effect/platform-node'
import * as SqliteClient from '@effect/sql-sqlite-node/SqliteClient'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as SqlClient from 'effect/sql/SqlClient'
import * as Prompt from 'effect/ai/Prompt'
import * as Storage from 'effect-harness/Storage'
import * as Record from 'effect-harness/Record'
import * as Wire from 'effect-harness/PromptWire'
import * as ToolResult from 'effect-harness/ToolResult'
import * as Memory from '../src/internal/Memory.js'
import * as Kernel from '../src/internal/Kernel.js'

const root = Record.ROOT_CONVERSATION_ID
const id = (value: number) => Record.DocumentId.make(value)
const taskId = Record.TaskId.make(3)
const images = Array.from({ length: 4 }, (_, i) =>
  Prompt.filePart({ mediaType: 'image/png', data: new Uint8Array(4096).fill(i + 1) }),
)
const prompt = (index: number) =>
  Schema.encodeSync(Schema.toCodecJson(Wire.Prompt))(
    Prompt.fromMessages([Prompt.userMessage({ content: [images[index]!] })]),
  )
const base = (index: number): Record.Content => ({
  _tag: 'base',
  version: 1,
  value: { request: { prompt: prompt(index) } },
})
const task = (checkpoint: Schema.Json): Record.Task => ({
  id: taskId,
  conversationId: root,
  kind: 'harness.tool',
  version: 1,
  input: null,
  background: false,
  abortRequested: false,
  state: { status: 'pending', checkpoint },
})

for (const backend of ['memory', 'jsonl', 'sqlite'] as const)
  it.live(
    `${backend}: owners follow exact revisions, checkpoints, nested receipts, copies and asOf history`,
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem
          const directory = yield* fs.makeTempDirectoryScoped({ prefix: 'media-lifetime-' })
          const filename = `${directory}/journal`
          const access = yield* Memory.makeAccess
          const memory = yield* Kernel.make(access)
          const layer: Layer.Layer<
            Storage.Storage,
            Storage.StorageError,
            FileSystem.FileSystem | SqlClient.SqlClient
          > = (() => {
            if (backend === 'memory') return Layer.succeed(Storage.Storage, memory)
            if (backend === 'jsonl') return Storage.layerJsonl({ filePath: filename })
            return Storage.layerSql
          })()
          const count = Effect.gen(function* () {
            if (backend === 'memory') return (yield* access.snapshot).objects.size
            if (backend === 'jsonl') return (yield* fs.readDirectory(`${filename}.media`)).length
            return Number(
              (yield* (yield* SqlClient.SqlClient)`SELECT COUNT(*) AS n FROM effect_harness_media`)[0]
                ?.n,
            )
          })
          yield* Effect.gen(function* () {
            const storage = yield* Storage.Storage
            const observed = yield* Schema.encodeEffect(
              Schema.toCodecJson(ToolResult.ResultSchema),
            )({ content: [images[0]!], diagnostics: [], isError: false })
            const nested = yield* Schema.encodeEffect(Schema.toCodecJson(ToolResult.NestedSchema))({
              valueKind: 'content',
              structuredOutput: images[1]!,
              diagnostics: [],
              isError: true,
            })
            const first = yield* storage.commit([
              { _tag: 'conversation', value: { id: root } },
              { _tag: 'conversation', value: { id: Record.ConversationId.make(4) } },
              { _tag: 'task', value: task({ phase: 'execute', observedResult: observed }) },
              {
                _tag: 'document.create',
                record: { id: id(10), kind: 'harness.request', scope: { _tag: 'task', taskId } },
                content: base(0),
              },
              {
                _tag: 'document.create',
                record: {
                  id: id(11),
                  kind: 'harness.tool.nested.result',
                  scope: { _tag: 'task', taskId },
                },
                content: { _tag: 'base', version: 1, value: { receipt: { result: nested } } },
              },
              {
                _tag: 'document.create',
                record: {
                  id: id(12),
                  kind: 'harness.request',
                  scope: { _tag: 'conversation', conversationId: root },
                  history: 'rewindable',
                  fork: 'asOf',
                },
                content: base(0),
              },
              {
                _tag: 'document.create',
                record: {
                  id: id(13),
                  kind: 'harness.request',
                  key: 'copy',
                  scope: { _tag: 'conversation', conversationId: root },
                  history: 'latest',
                  fork: 'current',
                },
                content: base(0),
              },
            ])
            assert.strictEqual(yield* count, 2)
            // A leaf replacement has no MIME/type discriminator in its own operation.
            const wire = yield* Schema.encodeEffect(Schema.toCodecJson(Wire.FilePart))(images[2]!)
            const data = yield* Schema.decodeUnknownEffect(
              Schema.Struct({ data: Schema.Struct({ data: Schema.String }) }),
            )(wire)
            yield* storage.commit([
              {
                _tag: 'document.change',
                id: id(13),
                content: {
                  _tag: 'delta',
                  version: 1,
                  ops: [
                    [
                      'set',
                      ['request', 'prompt', 'content', 0, 'content', 0, 'data', 'data'],
                      data.data.data,
                    ],
                  ],
                },
              },
            ])
            assert.strictEqual(yield* count, 3)
            yield* storage.commit([
              {
                _tag: 'document.change',
                id: id(13),
                content: {
                  _tag: 'delta',
                  version: 1,
                  ops: [['splice', ['request', 'prompt', 'content', 0, 'content'], 1, 0, [wire]]],
                },
              },
            ])
            const delta = Option.getOrThrow(yield* storage.document(id(13)))
            assert.strictEqual(delta.deltasSinceBase, 2)
            const restored = yield* Schema.decodeUnknownEffect(
              Schema.Struct({
                request: Schema.Struct({ prompt: Schema.toCodecJson(Wire.Prompt) }),
              }),
            )(delta.value)
            assert.deepEqual(restored.request.prompt.content[0]?.content, [images[2]!, images[2]!])
            yield* storage.commit([{ _tag: 'document.change', id: id(13), content: base(3) }])
            assert.strictEqual(yield* count, 3) // image 2 reclaimed; images 0, 1, 3 retained
            yield* storage.commit([
              {
                _tag: 'document.copy',
                record: {
                  id: id(14),
                  kind: 'harness.request',
                  key: 'copy',
                  scope: { _tag: 'conversation', conversationId: Record.ConversationId.make(4) },
                  history: 'latest',
                  fork: 'current',
                },
                source: { id: id(13), at: 'current' },
              },
            ])
            yield* storage.commit([{ _tag: 'document.retire', id: id(13) }])
            assert.strictEqual(yield* count, 3)
            yield* storage.commit([{ _tag: 'document.retire', id: id(14) }])
            assert.strictEqual(yield* count, 2)
            yield* storage.commit([
              { _tag: 'document.retire', id: id(10) },
              { _tag: 'document.retire', id: id(11) },
              {
                _tag: 'task',
                value: {
                  ...task(null),
                  state: { status: 'terminal', outcome: { status: 'completed', result: null } },
                },
              },
            ])
            assert.strictEqual(yield* count, 1)
            yield* storage.commit([{ _tag: 'document.retire', id: id(12) }])
            assert.strictEqual(yield* count, 1)
            assert.deepEqual(Option.getOrThrow(yield* storage.document(id(12), first)).value, {
              request: { prompt: prompt(0) },
            })
            // Readers race final-owner replacement; every returned snapshot owns its bytes.
            yield* storage.commit([
              {
                _tag: 'document.create',
                record: { id: id(15), kind: 'harness.request', scope: { _tag: 'task', taskId } },
                content: base(1),
              },
            ])
            yield* Effect.all(
              [
                Effect.forEach(
                  Array.from({ length: 16 }, (_, n) => n),
                  (n) =>
                    storage.commit([
                      { _tag: 'document.change', id: id(15), content: base((n % 2) + 1) },
                    ]),
                  { discard: true },
                ),
                Effect.forEach(
                  Array.from({ length: 32 }),
                  () =>
                    Effect.gen(function* () {
                      const value = Option.getOrThrow(yield* storage.document(id(15))).value
                      const json = JSON.stringify(value)
                      assert.isTrue(
                        json === JSON.stringify({ request: { prompt: prompt(1) } }) ||
                          json === JSON.stringify({ request: { prompt: prompt(2) } }),
                      )
                    }),
                  { discard: true },
                ),
              ],
              { concurrency: 'unbounded' },
            )
            yield* storage.commit([{ _tag: 'document.retire', id: id(15) }])
            assert.strictEqual(yield* count, 1)
            const returned = restored.request.prompt.content[0]?.content[0]
            if (
              typeof returned !== 'object' ||
              returned.type !== 'file' ||
              !(returned.data instanceof Uint8Array)
            )
              throw new Error('Missing bytes')
            returned.data[0] = 99
            assert.strictEqual((images[2]!.data as Uint8Array)[0], 3)
          }).pipe(Effect.provide(layer))
        }),
      ).pipe(
        Effect.provide(
          Layer.merge(SqliteClient.layer({ filename: ':memory:' }), NodeServices.layer),
        ),
      ),
  )
