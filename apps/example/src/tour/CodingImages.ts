import * as NodeFileSystem from '@effect/platform-node/NodeFileSystem'
import * as NodePath from '@effect/platform-node/NodePath'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as FileReader from 'effect-harness/FileReader'
import * as Record from 'effect-harness/Record'
import { ToolExecution } from 'effect-harness/ToolExecution'
import * as ToolkitFileSystem from 'effect-harness/ToolkitFileSystem'
import * as ImageRead from 'effect-harness/coding/ImageRead'
import * as Policy from 'effect-harness/coding/ImageReadPolicy'
import * as Read from 'effect-harness/coding/Read'
import * as NodeImages from 'effect-harness/images-node'

// After the integration build: node apps/example/dist/tour/CodingImages.js
// Invokes the public ready-made handler with explicit model policy and native
// file/codec Layers. Runtime selected-model policy wiring is a later integration.
const native = Layer.merge(NodeFileSystem.layer, NodePath.layer)
const tools = Read.withImages({ maxSourceBytes: 1024 * 1024 })
const policy = Policy.fromCapabilities(
  { images: true },
  {
    maxWidth: 2,
    maxHeight: 2,
    maxBytes: 10000,
    maxInputPixels: 100,
  },
)
// Direct toolkit invocation needs execution access. This read has no durable
// writes, so unused execution methods deliberately fail if accidentally called.
const execution = Layer.succeed(ToolExecution, {
  conversationId: Schema.decodeSync(Record.ConversationId)(1),
  taskId: Schema.decodeSync(Record.TaskId)(1),
  callId: 'read-image-example',
  snapshot: () => Effect.die('Unexpected snapshot'),
  watch: () => Stream.die('Unexpected watch'),
  commit: () => Effect.die('Unexpected commit'),
  output: () => Effect.die('Unexpected progress'),
  details: () => Effect.die('Unexpected details'),
  diagnostic: () => Effect.die('Unexpected diagnostic'),
})

await Effect.runPromise(
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem
      const root = yield* fs.realPath(yield* fs.makeTempDirectoryScoped())
      const png = yield* Schema.decodeEffect(Schema.Uint8ArrayFromBase64)(
        'iVBORw0KGgoAAAANSUhEUgAAAAMAAAACCAYAAACddGYaAAAACXBIWXMAAAPoAAAD6AG1e1JrAAAAFElEQVQImWP4z8DwHwwZ/v9HRv8Bqm8O8jjYJxEAAAAASUVORK5CYII=',
      )
      yield* fs.writeFile(`${root}/sample.png`, png)
      const files = FileReader.layer.pipe(
        Layer.provideMerge(ToolkitFileSystem.layer({ root }).pipe(Layer.provide(native))),
      )
      const handlers = tools.layer.pipe(
        Layer.provideMerge(Layer.mergeAll(files, NodeImages.layer, Policy.layer(policy))),
      )
      const result = yield* Effect.flatMap(tools.toolkit, (ready) =>
        ready.invoke({
          id: 'read-image-example',
          name: 'read',
          arguments: { path: 'sample.png' },
        }),
      ).pipe(Effect.provide(Layer.merge(handlers, execution)))
      if (result.isError) return yield* Effect.die(result)
      const image = yield* Schema.decodeUnknownEffect(ImageRead.Outcome)(result.structuredOutput)
      if (image.dimensions.width !== 2 || !result.content.some((part) => part.type === 'file'))
        return yield* Effect.die('Image read did not return prepared native file content')
      yield* Effect.log(image)
    }),
  ).pipe(Effect.provide(native)),
)
