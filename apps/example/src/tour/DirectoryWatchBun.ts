import * as BunFileSystem from '@effect/platform-bun/BunFileSystem'
import * as BunRuntime from '@effect/platform-bun/BunRuntime'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Stream from 'effect/Stream'
import * as DirectoryReader from 'effect-harness/DirectoryReader'
import * as DirectoryReaderBun from 'effect-harness/DirectoryReaderBun'
import * as FileWatch from 'effect-harness/FileWatch'
import * as FileWatchBun from 'effect-harness/FileWatchBun'

// After integration build: bun apps/example/dist/tour/DirectoryWatchBun.js
// Native directory handle ABI is explicitly Darwin/Linux64; other ABIs return unsupported.
BunRuntime.runMain(
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem
      const root = yield* fs.makeTempDirectoryScoped({ prefix: 'directory-watch-bun-' })
      yield* fs.makeDirectory(`${root}/opened`)
      yield* fs.writeFileString(`${root}/opened/first`, 'first')
      const reader = yield* Effect.flatMap(DirectoryReader.DirectoryReader, (s) =>
        s.open('opened'),
      ).pipe(Effect.provide(DirectoryReaderBun.layer({ root })))
      yield* fs.rename(`${root}/opened`, `${root}/renamed`)
      yield* fs.makeDirectory(`${root}/opened`)
      yield* fs.writeFileString(`${root}/opened/replacement`, '')
      const page = yield* reader.next(8)
      if (page.entries.length !== 1 || page.entries[0]?.name !== 'first')
        return yield* Effect.die('Native reader lost opened identity')
      yield* Effect.log({ page, capabilities: reader.capabilities })
      yield* reader.close
      const watcher = yield* Effect.flatMap(FileWatch.FileWatch, (s) =>
        s.open([{ path: 'missing' }], { rescanIntervalMs: 100 }),
      ).pipe(Effect.provide(FileWatchBun.layer({ root })))
      yield* fs.writeFileString(`${root}/missing`, 'created')
      yield* Effect.log({
        invalidation: yield* watcher.events.pipe(
          Stream.take(1),
          Stream.runCollect,
          Effect.timeout('3 seconds'),
        ),
        coverage: watcher.coverage,
      })
      yield* watcher.close
    }),
  ).pipe(Effect.provide(BunFileSystem.layer)),
)
