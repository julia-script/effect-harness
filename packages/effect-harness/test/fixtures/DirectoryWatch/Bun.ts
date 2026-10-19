import * as BunFileSystem from '@effect/platform-bun/BunFileSystem'
import { strict as assert } from 'node:assert'
import * as Effect from 'effect/Effect'
import * as Deferred from 'effect/Deferred'
import * as Fiber from 'effect/Fiber'
import * as FileSystem from 'effect/FileSystem'
import * as Result from 'effect/Result'
import * as Stream from 'effect/Stream'
import * as DirectoryReader from '../../../src/DirectoryReader.ts'
import * as DirectoryReaderBun from '../../../src/DirectoryReaderBun.ts'
import * as DirectoryReaderNode from '../../../src/DirectoryReaderNode.ts'
import * as FileWatch from '../../../src/FileWatch.ts'
import * as FileWatchBun from '../../../src/FileWatchBun.ts'

const failure = <A, E>(result: Result.Result<A, E>): E => {
  if (Result.isFailure(result)) return result.failure
  return assert.fail('Expected a typed failure')
}

await Effect.runPromise(
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem
      const root = yield* fs.makeTempDirectoryScoped({ prefix: 'native-bun-directory-' })
      yield* fs.makeDirectory(`${root}/original`)
      const names = Array.from({ length: 2081 }, (_, i) => `file-${i}`)
      yield* Effect.forEach(names, (name) => fs.writeFileString(`${root}/original/${name}`, ''), {
        concurrency: 16,
        discard: true,
      })
      const reader = yield* Effect.flatMap(DirectoryReader.DirectoryReader, (s) =>
        s.open('original', { requireOpenedIdentity: true, requireBoundedEnumeration: true }),
      ).pipe(Effect.provide(DirectoryReaderBun.layer({ root })))
      yield* fs.rename(`${root}/original`, `${root}/moved`)
      yield* fs.makeDirectory(`${root}/original`)
      yield* fs.writeFileString(`${root}/original/new`, '')
      const seen: Array<string> = []
      let done = false
      while (!done) {
        const page = yield* reader.next(29)
        assert.ok(page.entries.length <= 29)
        seen.push(...page.entries.map((e) => e.name))
        done = page.done
      }
      assert.deepEqual(seen.sort(), names.sort())
      yield* reader.close
      yield* reader.close
      assert.equal(failure(yield* reader.next(1).pipe(Effect.result)).reason, 'closed')
      const missing = yield* Effect.flatMap(DirectoryReader.DirectoryReader, (s) =>
        s.open('missing'),
      ).pipe(Effect.provide(DirectoryReaderBun.layer({ root })), Effect.result)
      assert.equal(failure(missing).reason, 'notFound')
      const file = yield* Effect.flatMap(DirectoryReader.DirectoryReader, (s) =>
        s.open('original/new'),
      ).pipe(Effect.provide(DirectoryReaderBun.layer({ root })), Effect.result)
      assert.equal(failure(file).reason, 'notDirectory')
      const wrongRuntime = yield* Effect.flatMap(DirectoryReader.DirectoryReader, (s) =>
        s.open('.'),
      ).pipe(Effect.provide(DirectoryReaderNode.layer({ root })), Effect.result)
      assert.equal(failure(wrongRuntime).reason, 'unsupported')
      // Native directory descriptors are actually released, including early scope closure.
      const before = (yield* fs.readDirectory('/dev/fd')).length
      for (let i = 0; i < 64; i++) {
        const handle = yield* Effect.scoped(
          Effect.flatMap(DirectoryReader.DirectoryReader, (s) => s.open('moved')),
        ).pipe(Effect.provide(DirectoryReaderBun.layer({ root })))
        assert.equal(failure(yield* handle.next(1).pipe(Effect.result)).reason, 'closed')
      }
      const ready = yield* Deferred.make<DirectoryReader.Reader>()
      const interrupted = yield* Effect.scoped(
        Effect.gen(function* () {
          const handle = yield* Effect.flatMap(DirectoryReader.DirectoryReader, (service) =>
            service.open('moved'),
          )
          yield* Deferred.succeed(ready, handle)
          return yield* Effect.never
        }),
      ).pipe(Effect.provide(DirectoryReaderBun.layer({ root })), Effect.forkChild)
      const interruptedHandle = yield* Deferred.await(ready)
      yield* Fiber.interrupt(interrupted)
      assert.equal(failure(yield* interruptedHandle.next(1).pipe(Effect.result)).reason, 'closed')
      const after = (yield* fs.readDirectory('/dev/fd')).length
      assert.ok(after <= before + 1, `Native handle leak: ${before} to ${after}`)
      const watch = yield* Effect.flatMap(FileWatch.FileWatch, (s) =>
        s.open([
          { path: 'missing/deep/file' },
          { path: 'tree', recursive: true, exclude: { hidden: true } },
        ]),
      ).pipe(Effect.provide(FileWatchBun.layer({ root })))
      assert.equal(watch.coverage.mode, 'native')
      const events: Array<FileWatch.Event> = []
      yield* watch.events.pipe(
        Stream.runForEach((event) => Effect.sync(() => events.push(event))),
        Effect.forkScoped,
      )
      yield* Effect.sleep('600 millis')
      const expectChange = <A, E>(path: string, mutate: Effect.Effect<A, E>) =>
        Effect.gen(function* () {
          events.length = 0
          yield* mutate
          for (let i = 0; i < 150; i++) {
            if (
              events.some((e) =>
                e._tag === 'Change'
                  ? e.path === `${root}/${path}`
                  : e.paths.some(
                      (covered) =>
                        `${root}/${path}` === covered ||
                        `${root}/${path}`.startsWith(`${covered}/`),
                    ),
              )
            )
              return
            yield* Effect.sleep('20 millis')
          }
          assert.fail(`Missing Bun native invalidation: ${path}; ${JSON.stringify(events)}`)
        })
      yield* expectChange(
        'missing/deep/file',
        fs
          .makeDirectory(`${root}/missing/deep`, { recursive: true })
          .pipe(Effect.andThen(fs.writeFileString(`${root}/missing/deep/file`, 'one'))),
      )
      yield* expectChange('missing/deep/file', fs.remove(`${root}/missing/deep/file`))
      yield* expectChange(
        'missing/deep/file',
        fs.writeFileString(`${root}/missing/deep/file`, 'two'),
      )
      yield* expectChange('tree', fs.makeDirectory(`${root}/tree/a`, { recursive: true }))
      yield* Effect.sleep('600 millis')
      yield* expectChange('tree/a/file', fs.writeFileString(`${root}/tree/a/file`, 'file'))
      yield* expectChange('tree', fs.rename(`${root}/tree`, `${root}/old-tree`))
      yield* expectChange('tree', fs.makeDirectory(`${root}/tree`))
      yield* Effect.sleep('600 millis')
      yield* expectChange('tree/new', fs.writeFileString(`${root}/tree/new`, 'new'))
      events.length = 0
      yield* fs.writeFileString(`${root}/tree/.hidden`, 'hidden')
      yield* Effect.sleep('100 millis')
      assert.ok(!events.some((e) => e._tag === 'Change' && e.path.endsWith('/.hidden')))
      yield* watch.close
      const length = events.length
      yield* fs.writeFileString(`${root}/tree/closed`, 'closed')
      yield* Effect.sleep('100 millis')
      assert.equal(events.length, length)
      yield* Effect.log('Native Bun directory and watch checks passed')
    }),
  ).pipe(Effect.provide(BunFileSystem.layer)),
)
