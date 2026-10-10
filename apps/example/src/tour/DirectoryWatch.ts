import * as NodeFileSystem from '@effect/platform-node/NodeFileSystem'
import * as NodeRuntime from '@effect/platform-node/NodeRuntime'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Stream from 'effect/Stream'
import * as DirectoryReader from 'effect-harness/DirectoryReader'
import * as DirectoryReaderNode from 'effect-harness/DirectoryReaderNode'
import * as FileWatch from 'effect-harness/FileWatch'
import * as FileWatchNode from 'effect-harness/FileWatchNode'

// After integration build: node apps/example/dist/tour/DirectoryWatch.js
NodeRuntime.runMain(
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem
      const root = yield* fs.makeTempDirectoryScoped({ prefix: 'directory-watch-example-' })
      yield* fs.writeFileString(`${root}/first`, 'first')
      const reader = yield* Effect.flatMap(DirectoryReader.DirectoryReader, (s) =>
        s.open('.', { requireOpenedIdentity: true, requireBoundedEnumeration: true }),
      ).pipe(Effect.provide(DirectoryReaderNode.layer({ root })))
      yield* Effect.log({ page: yield* reader.next(1), capabilities: reader.capabilities })
      yield* reader.close
      const watcher = yield* Effect.flatMap(FileWatch.FileWatch, (s) =>
        s.open([{ path: 'missing' }], { rescanIntervalMs: 100 }),
      ).pipe(Effect.provide(FileWatchNode.layer({ root })))
      yield* fs.writeFileString(`${root}/missing`, 'created')
      // Native events are invalidations; a rescan also covers native startup/coalescing gaps.
      const invalidation = yield* watcher.events.pipe(
        Stream.take(1),
        Stream.runCollect,
        Effect.timeout('3 seconds'),
      )
      yield* Effect.log({ invalidation, coverage: watcher.coverage })
      yield* watcher.close
    }),
  ).pipe(Effect.provide(NodeFileSystem.layer)),
)
