import { assertFailure } from '@effect/vitest/utils'
import { FileInvalidError } from 'effect-harness/FileError'
import { assert, describe, it } from '@effect/vitest'

import * as Result from 'effect/Result'

import * as LineScan from 'effect-harness/env/LineScan'

describe('LineScanBoundary', () => {
  it('safe scanner construction catches the real options getter and Unsafe throws the same typed failure', () => {
    const cause = new Error('range getter')
    const options = {
      get endLine(): number {
        throw cause
      },
    }
    const result = LineScan.make(0, options)
    assertFailure(
      result,
      new FileError.FileError({
        reason: new FileInvalidError({ message: 'Invalid line range', cause }),
      }),
    )
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
          if (!(error instanceof FileError.FileError)) return assert.fail('Expected FileError')
          assert.strictEqual(error.cause, cause)
          assert.strictEqual(error.code, 'invalid')
        },
      },
    )
  })
})

import * as FileError from 'effect-harness/FileError'
