import { assert, describe, it } from '@effect/vitest'

import * as Effect from 'effect/Effect'

import * as Cause from 'effect/Cause'

import * as Result from 'effect/Result'

import * as Decode from 'effect-harness/env/Decode'

describe('DecodeBoundary', () => {
  it.effect(
    'safe decoder catches an actual native accessor fault while Unsafe keeps synchronous throwing',
    () =>
      Effect.gen(function* () {
        const original = new Error('decoder accessor')
        const decoder = Decode.make()
        Object.defineProperty(decoder, 'decoder', {
          // effect-nit-allow P7-v4-data-type-naming: PropertyDescriptor requires the exact get callback name; this hostile fixture must throw the original foreign cause to exercise the guarded boundary.
          get() {
            throw original
          },
        })
        assert.deepStrictEqual(
          Result.try(() => Decode.decodeUnsafe(decoder)),
          Result.fail(original),
        )
        const exit = yield* Effect.exit(Decode.decode(decoder))
        assert.strictEqual(exit._tag, 'Failure')
        if (exit._tag === 'Failure') {
          const failure = Cause.squash(exit.cause)
          assert.instanceOf(failure, FileError.FileError)
          assert.strictEqual((failure as FileError.FileError).cause, original)
          assert.strictEqual((failure as FileError.FileError).code, 'unknown')
        }
      }),
  )
})

import * as FileError from 'effect-harness/FileError'
