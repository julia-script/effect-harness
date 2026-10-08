import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as TestSchema from 'effect/testing/TestSchema'
import * as StorageError from 'effect-harness/durable/StorageError'

describe('StorageError', () => {
  it.effect('keeps validation cause-free and rejects retrying uncertain writes', () =>
    Effect.gen(function* () {
      const invalid = StorageError.rejected('caller validation')
      assert.strictEqual(invalid.reason._tag, 'InvalidError')
      assert.strictEqual(invalid.certainty, 'rejected')
      assert.strictEqual(invalid.isRetryable, false)
      assert.strictEqual(Object.hasOwn(invalid.reason, 'cause'), false)
      assert.strictEqual(invalid.cause, undefined)
      const codec = Schema.toCodecJson(StorageError.StorageError)
      const assertions = new TestSchema.Asserts(codec)
      yield* assertions
        .decoding()
        .succeedEffect(
          { _tag: 'StorageError', reason: { _tag: 'InvalidError', message: 'caller validation' } },
          StorageError.rejected('caller validation'),
        )
      yield* assertions.encoding().succeedEffect(invalid, {
        _tag: 'StorageError',
        reason: { _tag: 'InvalidError', message: 'caller validation' },
      })
      const json = yield* Schema.encodeEffect(codec)(invalid)
      assert.deepStrictEqual(json, {
        _tag: 'StorageError',
        reason: { _tag: 'InvalidError', message: 'caller validation' },
      })
      const cause = new Error('settlement unavailable')
      const uncertain = StorageError.uncertain('reconcile durable facts', cause)
      assert.strictEqual(uncertain.reason._tag, 'IoError')
      assert.strictEqual(uncertain.certainty, 'uncertain')
      assert.strictEqual(uncertain.isRetryable, false)
      const wire = {
        _tag: 'StorageError',
        reason: {
          _tag: 'IoError',
          message: 'reconcile durable facts',
          cause: { name: 'Error', message: 'settlement unavailable' },
          certainty: 'uncertain',
        },
      }
      const expected = StorageError.uncertain(
        'reconcile durable facts',
        new Error('settlement unavailable'),
      )
      yield* assertions.encoding().succeedEffect(uncertain, wire)
      yield* assertions.decoding().succeedEffect(wire, expected)
      const decoded = yield* Schema.decodeEffect(codec)(wire)
      assert.strictEqual(decoded.certainty, 'uncertain')
      assert.strictEqual(decoded.reason.isRetryable, false)
      assert.strictEqual(decoded.isRetryable, false)
      assert.ok(decoded.cause instanceof Error)
      assert.strictEqual(decoded.cause.message, cause.message)
      const missing = StorageError.rejected('record absent', StorageError.NotFoundError)
      assert.strictEqual(missing.reason._tag, 'NotFoundError')
      assert.strictEqual(missing.message, 'record absent')
    }),
  )
  it.effect('carries structured reasons and preserves wrapped Error messages across JSON', () =>
    Effect.gen(function* () {
      const cause = new Error('disk failure', { cause: new Error('device offline') })
      const error = StorageError.rejected('write failed', StorageError.IoError, cause)
      assert.strictEqual(error.reason._tag, 'IoError')
      assert.strictEqual(error.message, 'write failed')
      assert.strictEqual(error.cause, cause)
      const codec = Schema.toCodecJson(StorageError.StorageError)
      const wire = {
        _tag: 'StorageError',
        reason: {
          _tag: 'IoError',
          message: 'write failed',
          certainty: 'rejected',
          cause: {
            name: 'Error',
            message: 'disk failure',
            cause: { name: 'Error', message: 'device offline' },
          },
        },
      }
      const expected = StorageError.rejected(
        'write failed',
        StorageError.IoError,
        new Error('disk failure', { cause: new Error('device offline') }),
      )
      const assertions = new TestSchema.Asserts(codec)
      yield* assertions.encoding().succeedEffect(error, wire)
      yield* assertions.decoding().succeedEffect(wire, expected)
      const decoded = yield* Schema.decodeEffect(codec)(wire)
      assert.ok(decoded.cause instanceof Error)
      assert.strictEqual(decoded.cause.message, 'disk failure')
      assert.ok(decoded.cause.cause instanceof Error)
      assert.strictEqual(decoded.cause.cause.message, 'device offline')
    }),
  )
})
