import * as Effect from 'effect/Effect'
import * as Storage from 'effect-harness/Storage'
import * as StorageFormat from 'effect-harness/StorageFormat'

/** The runtime that introduces v2 work must complete this guard before its first write. */
export const program = Effect.gen(function* () {
  const storage = yield* Storage.Storage
  const before = yield* StorageFormat.inspect
  const activated = yield* StorageFormat.requireSupportedFormat({
    formatVersion: 2,
    minimumReaderVersion: 2,
  })
  const sequence = yield* storage.commit([])
  if (
    before.formatVersion !== 1 ||
    before.minimumReaderVersion !== 1 ||
    activated.formatVersion !== 2 ||
    activated.minimumReaderVersion !== 2
  )
    return yield* Effect.die('Storage format activation contract failed')
  return { before, activated, sequence }
}).pipe(Effect.provide(Storage.layerMemory))
import * as NodeRuntime from '@effect/platform-node/NodeRuntime'
import { pathToFileURL } from 'node:url'

if (process.argv[1] !== undefined && import.meta.url === pathToFileURL(process.argv[1]).href)
  NodeRuntime.runMain(program.pipe(Effect.tap((result) => Effect.log(result))))
