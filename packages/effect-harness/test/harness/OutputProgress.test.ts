// Output vectors adapted from pi-durable (MIT), pinned 636703a0.
import { assert, describe, it } from '@effect/vitest'
import * as Clock from 'effect/Clock'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Fiber from 'effect/Fiber'
import * as Exit from 'effect/Exit'
import * as Ref from 'effect/Ref'
import { TestClock } from 'effect/testing'
import * as Output from 'effect-harness/Output'
import { OutputError, OutputFailure } from 'effect-harness/OutputError'
import * as Progress from 'effect-harness/Progress'
describe('OutputProgress', () => {
  const limits = (retain: 'head' | 'tail', maxLines = 2, maxBytes = 100): Output.OutputLimits => ({
    retain,
    maxLines,
    maxBytes,
  })
  const text = (value: string, config: Output.OutputLimits): string =>
    Output.boundOutput(value, config).text

  describe('bounded UTF8 output and adaptive progress', () => {
    it('head/tail retain exact whole lines and zero limits retain nothing', () => {
      assert.strictEqual(text('a\nb\nc\n', limits('head')), 'a\nb\n')
      assert.strictEqual(text('a\nb\nc\n', limits('tail')), 'b\nc\n')
      assert.strictEqual(text('a\nb\nc\n\n', limits('tail', 3)), 'b\nc\n\n')
      assert.strictEqual(text('abc', limits('head', 2, 0)), '')
      assert.strictEqual(text('abc', limits('tail', 0)), '')
    })
    it('byte limits respect line boundaries and single long UTF8 character boundaries', () => {
      assert.strictEqual(text('aa\nbb\ncc\n', limits('head', 10, 7)), 'aa\nbb\n')
      assert.strictEqual(text('aa\nbb\ncc\n', limits('tail', 10, 7)), 'bb\ncc\n')
      assert.strictEqual(text('ééé\n', limits('head', 10, 5)), 'éé')
      assert.strictEqual(text('x\néééé', limits('tail', 10, 5)), 'éé')
      assert.strictEqual(text('x\ufeffa', limits('tail', 10, 4)), '\ufeffa')
    })
    it('removes display controls while preserving tabs/newlines and unicode', () => {
      assert.strictEqual(
        Output.sanitizeOutput('a\0b\tc\nd\re\u0007f\ufff9g\ufffbh😀'),
        'ab\tc\ndefgh😀',
      )
    })
    it.effect(
      'streaming tail is invariant under every snapshot cadence and byte chunk boundary',
      () =>
        Effect.gen(function* () {
          for (const source of ['a\nb\nc\n\n', 'ééé\n🙂tail', 'a\ufeffb\nc', 'x'.repeat(200)]) {
            const encoded = new TextEncoder().encode(source)
            for (let maxBytes = 0; maxBytes < 16; maxBytes++) {
              const config = limits('tail', 2, maxBytes)
              const buffer = Output.make(config)
              for (const byte of encoded) {
                yield* Output.push(buffer, Uint8Array.of(byte))
                Output.snapshotUnsafe(buffer)
              }
              Output.endUnsafe(buffer)
              assert.strictEqual(
                Output.snapshotUnsafe(buffer).text,
                Output.sanitizeOutput(Output.boundOutput(source, config).text),
              )
              assert.strictEqual(buffer.storedBytes <= maxBytes + 5, true)
            }
          }
        }),
    )
    it.effect(
      'initial byte BOM drops, later BOM stays and incomplete bytes flush at string/end boundaries',
      () =>
        Effect.gen(function* () {
          const buffer = Output.make(limits('head', 10))
          yield* Output.push(buffer, new TextEncoder().encode('\ufeffa'))
          yield* Output.push(buffer, Uint8Array.of(0xc3))
          yield* Output.push(buffer, 'b\ufeff')
          yield* Output.push(buffer, Uint8Array.of(0xe2))
          Output.endUnsafe(buffer)
          assert.strictEqual(Output.snapshotUnsafe(buffer).text, 'a�b\ufeff�')
        }),
    )
    it.effect('skip requires tail, clears prior stored suffix and retains raw skipped counts', () =>
      Effect.gen(function* () {
        const failure = yield* Effect.flip(
          Output.push(Output.make(limits('head')), 'x', {
            bytes: 3,
            newlines: 1,
            endsWithNewline: true,
          }),
        )
        assert.instanceOf(failure, OutputError)
        assert.instanceOf(failure.reason, OutputFailure)
        assert.strictEqual(failure.message, 'Skipped output requires tail retention')
        assert.strictEqual(failure.cause, undefined)
        const buffer = Output.make(limits('tail', 1, 3))
        yield* Output.push(buffer, 'old\n')
        yield* Output.push(buffer, 'new', { bytes: 100, newlines: 10, endsWithNewline: true })
        assert.deepStrictEqual(Output.snapshotUnsafe(buffer), {
          text: 'new',
          droppedBytes: 104,
          droppedLines: 11,
        })
      }),
    )
    it('deltas reconstruct append/trim and bounded fallback', () => {
      for (const [before, after] of [
        ['abc', 'abcdef'],
        ['abcde', 'cdef'],
        ['old', 'new'],
        ['x'.repeat(30), 'z'.repeat(30)],
      ]) {
        if (before === undefined || after === undefined) continue
        const delta = Output.delta(before, after, 20)
        assert.strictEqual(
          delta._tag === 'set' ? delta.text : before.slice(delta.trimStart) + delta.text,
          after,
        )
      }
    })
    it.effect(
      'first write immediate, later updates coalesce behind interval and written-byte pacing',
      () =>
        Effect.gen(function* () {
          const calls = yield* Ref.make<ReadonlyArray<number>>([])
          const dirty = yield* Ref.make(0)
          const written = yield* Deferred.make<void>()
          const progress = yield* Progress.make(
            Effect.gen(function* () {
              const now = yield* Clock.currentTimeMillis
              yield* Ref.update(calls, (old) => [...old, now])
              yield* Deferred.succeed(written, undefined)
              return Progress.bytesPerSecond
            }),
            { minIntervalMs: 100 },
          )
          yield* progress.mark
          yield* Deferred.await(written)
          yield* progress.mark
          yield* progress.mark
          yield* Ref.set(dirty, 1)
          yield* TestClock.adjust(999)
          assert.strictEqual((yield* Ref.get(calls)).length, 1)
          yield* TestClock.adjust(1)
          assert.deepStrictEqual(yield* Ref.get(calls), [0, 1000])
          assert.strictEqual(yield* Ref.get(dirty), 1)
          yield* progress.stop
        }),
    )
    it.effect(
      'stop joins admitted write and returns undelivered waiters for final settlement',
      () =>
        Effect.gen(function* () {
          const started = yield* Deferred.make<void>()
          const release = yield* Deferred.make<void>()
          const progress = yield* Progress.make(
            Deferred.succeed(started, undefined).pipe(
              Effect.andThen(Deferred.await(release)),
              Effect.as(0),
            ),
            { minIntervalMs: 100 },
          )
          const waiting = yield* progress.markAndWait.pipe(Effect.forkChild)
          yield* Deferred.await(started)
          const stopped = yield* Ref.make(false)
          const stop = yield* progress.stop.pipe(
            Effect.tap(() => Ref.set(stopped, true)),
            Effect.forkChild,
          )
          yield* Effect.yieldNow
          assert.strictEqual(yield* Ref.get(stopped), false)
          yield* Deferred.succeed(release, undefined)
          assert.strictEqual((yield* Fiber.join(stop)).length, 0)
          yield* Fiber.join(waiting)
          const pending = yield* progress.markAndWait.pipe(Effect.forkChild)
          yield* Effect.yieldNow
          const final = yield* progress.stop
          assert.strictEqual(final.length, 1)
          yield* Progress.settle(final, Exit.succeed(undefined))
          yield* Fiber.join(pending)
        }),
    )
  })
})
