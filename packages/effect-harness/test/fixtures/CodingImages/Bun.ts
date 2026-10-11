import assert from 'node:assert/strict'
import * as BunFileSystem from '@effect/platform-bun/BunFileSystem'
import * as BunPath from '@effect/platform-bun/BunPath'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as FileReader from 'effect-harness/FileReader'
import * as Record from 'effect-harness/Record'
import { ToolExecution } from 'effect-harness/ToolExecution'
import * as ToolkitFileSystem from 'effect-harness/ToolkitFileSystem'
import * as Read from 'effect-harness/coding/Read'
import * as ImageRead from 'effect-harness/coding/ImageRead'
import * as Policy from 'effect-harness/coding/ImageReadPolicy'
import * as BunImages from 'effect-harness/images-bun'

const native = Layer.merge(BunFileSystem.layer, BunPath.layer)
const tools = Read.withImages({ maxSourceBytes: 1024 * 1024 })
const execution = Layer.succeed(ToolExecution, {
  conversationId: Schema.decodeSync(Record.ConversationId)(1),
  taskId: Schema.decodeSync(Record.TaskId)(1),
  callId: 'bun-image-read',
  snapshot: () => Effect.die('Unexpected snapshot'),
  watch: () => Stream.die('Unexpected watch'),
  commit: () => Effect.die('Unexpected commit'),
  output: () => Effect.die('Unexpected output'),
  details: () => Effect.die('Unexpected details'),
  diagnostic: () => Effect.die('Unexpected diagnostic'),
})
await Effect.runPromise(
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem
      const root = yield* fs.realPath(yield* fs.makeTempDirectoryScoped())
      const files = FileReader.layer.pipe(
        Layer.provideMerge(ToolkitFileSystem.layer({ root }).pipe(Layer.provide(native))),
      )
      const handlers = tools.layer.pipe(
        Layer.provideMerge(
          Layer.mergeAll(
            files,
            BunImages.layer,
            Policy.layer({
              vision: true,
              limits: { maxWidth: 30, maxHeight: 30, maxBytes: 100000, maxInputPixels: 100 },
            }),
          ),
        ),
      )
      for (const name of [
        'opaque.png',
        'opaque.jpg',
        'animated.gif',
        'animated.webp',
        'opaque.bmp',
      ]) {
        const bytes = yield* fs.readFile(
          new URL(`../images-native/${name}`, import.meta.url).pathname,
        )
        yield* fs.writeFile(`${root}/${name}`, bytes)
        const result = yield* Effect.flatMap(tools.toolkit, (ready) =>
          ready.invoke({ id: name, name: 'read', arguments: { path: name } }),
        ).pipe(Effect.provide(Layer.merge(handlers, execution)))
        assert.equal(result.isError, false)
        const outcome = yield* Schema.decodeUnknownEffect(ImageRead.Outcome)(
          result.structuredOutput,
        )
        assert.deepEqual(outcome.dimensions, { width: 3, height: 2 })
        assert.ok(
          result.content.some(
            (part) => part.type === 'file' && part.mediaType === outcome.mimeType,
          ),
        )
        if (name.endsWith('.bmp')) assert.equal(outcome.convertedFrom, 'image/bmp')
      }
    }),
  ).pipe(Effect.provide(native)),
)
console.log('Bun bound image Read matrix passed')
