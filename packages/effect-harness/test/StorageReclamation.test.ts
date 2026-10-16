import { assert, it } from '@effect/vitest'
import { NodeServices } from '@effect/platform-node'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Option from 'effect/Option'
import * as Storage from 'effect-harness/Storage'
import * as Record from 'effect-harness/Record'
import * as Kernel from '../src/internal/Kernel.js'
import * as Projection from '../src/internal/Projection.js'

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
        const access = yield* Projection.makeAccess
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

for (const backend of ['jsonl'] as const)
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
          const layer = Storage.layerJsonl({ filePath: filename })
          const inspect = Effect.gen(function* () {
            return { written, retained: yield* retainedJsonl(fs, filename) }
          })
          yield* Effect.gen(function* () {
            const storage = yield* Storage.Storage
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
