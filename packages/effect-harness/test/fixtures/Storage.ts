/** Isolated real JSONL storage for behavior tests that do not need a named path. */
import { NodeServices } from '@effect/platform-node'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Storage from 'effect-harness/Storage'

export const layer = Layer.unwrap(
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: 'harness-test-' })
    return Storage.layerJsonl({ filePath: `${directory}/main.jsonl` })
  }),
).pipe(Layer.provide(NodeServices.layer), Layer.orDie)
