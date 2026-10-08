import { assertSuccess } from '@effect/vitest/utils'
import { assert, describe, it } from '@effect/vitest'

import * as Effect from 'effect/Effect'

import * as OutputError from 'effect-harness/OutputError'

import * as Result from 'effect/Result'

import * as Output from 'effect-harness/Output'

import * as EditDiff from 'effect-harness/tools/EditDiff'

describe('OutputEditDiff', () => {
  it('dual deltas retain UTF-16 offsets and safe diff preserves unchanged CRLF bytes', () => {
    const self = '🙂alpha'
    const that = 'alpha🙂'
    const dataFirst = Output.delta(self, that, 32)
    assert.deepStrictEqual(Output.delta(that, 32)(self), dataFirst)
    assert.strictEqual(
      Output.Delta.$match(dataFirst, {
        set: ({ text }) => text,
        append: ({ trimStart, text }) => self.slice(trimStart) + text,
      }),
      that,
    )
    const edits = Object.freeze([{ oldText: 'old', newText: 'new' }])
    const fixed = EditDiff.applyEditsToNormalizedContent(edits, 'file')('one\nold\nthree')
    assertSuccess(fixed, {
      baseContent: 'one\nold\nthree',
      newContent: 'one\nnew\nthree',
    })
    assert.strictEqual(EditDiff.restoreLineEndings('\r\n')('a\nb'), 'a\r\nb')
    assert.strictEqual(
      EditDiff.applyEditsToNormalizedContentUnsafe(edits, 'file')('one\nold\nthree').newContent,
      'one\nnew\nthree',
    )
  })
  it.effect('safe output commands retain the actual hostile decoder failure', () =>
    Effect.gen(function* () {
      const original = new Error('output decoder accessor')
      const buffer = Output.make()
      Object.defineProperty(buffer, 'decoder', {
        // effect-nit-allow P7-v4-data-type-naming: PropertyDescriptor requires the exact get callback name; this hostile fixture must throw the original foreign cause to exercise the guarded boundary.
        get() {
          throw original
        },
      })
      const snapshotBuffer = Output.make()
      Object.defineProperty(snapshotBuffer, 'chunks', {
        // effect-nit-allow P7-v4-data-type-naming: PropertyDescriptor requires the exact get callback name; this hostile fixture must throw the original foreign cause to exercise the guarded boundary.
        get() {
          throw original
        },
      })
      for (const operation of [
        Output.push(buffer, 'text'),
        Output.end(buffer),
        Output.snapshot(snapshotBuffer),
      ]) {
        const failure = yield* Effect.flip(operation)
        assert.instanceOf(failure, OutputError.OutputError)
        assert.strictEqual(failure.reason._tag, 'OutputFailureError')
        assert.strictEqual(failure.cause, original)
      }
      assert.deepStrictEqual(
        Result.try(() => Output.endUnsafe(buffer)),
        Result.fail(original),
      )
    }),
  )
})
