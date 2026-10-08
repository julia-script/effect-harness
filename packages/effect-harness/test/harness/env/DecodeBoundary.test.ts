import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Cause from 'effect/Cause'
import * as Result from 'effect/Result'
import * as Decode from 'effect-harness/env/Decode'
import * as Env from 'effect-harness/Env'

describe('DecodeBoundary', () => {
  it.effect(
    'safe decoder catches an actual native accessor fault while Unsafe keeps synchronous throwing',
    () =>
      Effect.gen(function* () {
        const original = new Error('decoder accessor')
        const decoder = Decode.make()
        Object.defineProperty(decoder, 'decoder', {
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
          assert.instanceOf(failure, Env.FileError)
          assert.strictEqual((failure as Env.FileError).cause, original)
          assert.strictEqual((failure as Env.FileError).code, 'unknown')
        }
      }),
  )
})
