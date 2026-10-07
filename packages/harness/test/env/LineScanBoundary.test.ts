import { assert, describe, it } from '@effect/vitest'
import * as Result from 'effect/Result'
import * as LineScan from '@effect-harness/harness/env/LineScan'
import * as Env from '@effect-harness/harness/Env'

describe('LineScanBoundary', () => {
  it('safe scanner construction catches the real options getter and Unsafe throws the same typed failure', () => {
    const cause = new Error('range getter')
    const options = {
      get endLine(): number {
        throw cause
      },
    }
    const result = LineScan.make(0, options)
    assert.strictEqual(result._tag, 'Failure')
    Result.match(result, {
      onSuccess: () => assert.fail('Expected typed range failure'),
      onFailure: (error) => {
        assert.strictEqual(error.code, 'invalid')
        assert.strictEqual(error.cause, cause)
      },
    })
    Result.match(
      Result.try(() => LineScan.makeUnsafe(0, options)),
      {
        onSuccess: () => assert.fail('Expected synchronous typed failure'),
        onFailure: (error) => {
          if (!(error instanceof Env.FileError)) return assert.fail('Expected FileError')
          assert.strictEqual(error.cause, cause)
          assert.strictEqual(error.code, 'invalid')
        },
      },
    )
  })
})
