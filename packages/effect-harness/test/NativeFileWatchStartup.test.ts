import { assert, it } from '@effect/vitest'
import * as NodeFileSystem from '@effect/platform-node/NodeFileSystem'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as TestClock from 'effect/testing/TestClock'
import * as FileWatch from '../src/FileWatch.ts'
import * as FileWatchNode from '../src/FileWatchNode.ts'

it.effect('native settling coverage follows startup writes instead of preceding them', () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem
      const root = yield* fs.makeTempDirectoryScoped({ prefix: 'native-startup-' })
      const adapter = yield* FileWatch.Adapter
      const events: Array<FileWatch.Event> = []
      yield* adapter.subscribe(
        [{ path: root }],
        { emit: (event) => events.push(event), fail: (error) => assert.fail(error.message) },
        {},
      )
      yield* TestClock.adjust('0 millis')
      const beforeWrites = events.filter(
        (event) => event._tag === 'Rescan' && event.reason === 'coverage',
      )
      yield* Effect.forEach(
        Array.from({ length: 128 }, (_, index) => index),
        (index) => fs.writeFileString(`${root}/burst-${index}`, `burst:${index}`),
        { concurrency: 16, discard: true },
      )
      assert.deepStrictEqual(
        beforeWrites,
        [],
        'Coverage must follow the native startup settling interval',
      )
      yield* TestClock.adjust('499 millis')
      assert.deepStrictEqual(
        events.filter((event) => event._tag === 'Rescan' && event.reason === 'coverage'),
        [],
      )
      yield* TestClock.adjust('1 millis')
      assert.deepStrictEqual(
        events.filter((event) => event._tag === 'Rescan' && event.reason === 'coverage'),
        [{ _tag: 'Rescan', paths: [root], reason: 'coverage' }],
      )
      assert.strictEqual((yield* fs.readDirectory(root)).length, 128)
      for (let index = 0; index < 128; index++)
        assert.strictEqual(yield* fs.readFileString(`${root}/burst-${index}`), `burst:${index}`)
    }),
  ).pipe(Effect.provide([NodeFileSystem.layer, FileWatchNode.adapter])),
)
