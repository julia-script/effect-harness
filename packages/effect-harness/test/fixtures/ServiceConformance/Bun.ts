import * as BunFileSystem from '@effect/platform-bun/BunFileSystem'
import * as BunPath from '@effect/platform-bun/BunPath'
import * as BunSpawner from '@effect/platform-bun/BunChildProcessSpawner'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Directory from 'effect-harness/DirectoryReaderBun'
import * as Watch from 'effect-harness/FileWatchBun'
import * as FileChecks from 'effect-harness/testing/FileSystem'
import * as ProcessChecks from 'effect-harness/testing/Process'
import { fileFixture, processFixture } from './Native.js'

const backend = Layer.merge(BunFileSystem.layer, BunPath.layer)
const cases: ReadonlyArray<
  FileChecks.ConformanceCase<
    FileChecks.FileSystemError | ProcessChecks.ProcessError | FileChecks.ServiceConformanceError
  >
> = [
  ...FileChecks.fileSystemConformance({
    make: fileFixture(
      (root) => Directory.layer({ root }),
      (root) => Watch.layer({ root }),
      backend,
    ),
    capabilities: { noFollow: 'unsupported', boundedDirectory: true, watch: true },
  }),
  ...ProcessChecks.processConformance({
    make: processFixture(false, backend, BunSpawner.layer),
    capabilities: { strictOwnership: true, spill: true, spillLifetime: 'persistent' },
  }),
  ...FileChecks.fileSystemConformance({
    make: fileFixture(),
    capabilities: { unavailableDirectory: true },
  }).filter((c) => c.name === 'filesystem: directory pages and capability'),
]
await Effect.runPromise(
  Effect.forEach(cases, (c) => c.run.pipe(Effect.tap(() => Effect.log(c.name))), { discard: true }),
)
