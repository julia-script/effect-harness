import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as StorageError from 'effect-harness/durable/StorageError'

describe('StorageError', () => {
  it.effect('keeps validation cause-free and rejects retrying uncertain writes', () =>
    Effect.gen(function* () {
      const invalid = StorageError.rejected('caller validation')
      assert.strictEqual(invalid.reason._tag, 'Invalid')
      assert.strictEqual(invalid.certainty, 'rejected')
      assert.strictEqual(invalid.isRetryable, false)
      assert.strictEqual(Object.hasOwn(invalid.reason, 'cause'), false)
      assert.strictEqual(invalid.cause, undefined)
      const codec = Schema.toCodecJson(StorageError.StorageError)
      const json = yield* Schema.encodeEffect(codec)(invalid)
      assert.deepStrictEqual(json, {
        _tag: 'StorageError',
        reason: { _tag: 'Invalid', message: 'caller validation' },
      })
      const cause = new Error('settlement unavailable')
      const uncertain = StorageError.uncertain('reconcile durable facts', cause)
      assert.strictEqual(uncertain.reason._tag, 'Io')
      assert.strictEqual(uncertain.certainty, 'uncertain')
      assert.strictEqual(uncertain.isRetryable, false)
      const decoded = yield* Schema.decodeEffect(codec)(
        yield* Schema.encodeEffect(codec)(uncertain),
      )
      assert.strictEqual(decoded.certainty, 'uncertain')
      assert.strictEqual(decoded.reason.isRetryable, false)
      assert.strictEqual(decoded.isRetryable, false)
      assert.ok(decoded.cause instanceof Error)
      assert.strictEqual(decoded.cause.message, cause.message)
      const legacy = StorageError.rejectedLegacy('legacy caller', 'not_found')
      assert.strictEqual(legacy.reason._tag, 'NotFound')
      assert.strictEqual(legacy.message, 'legacy caller')
    }),
  )
  it.effect('carries structured reasons and preserves wrapped Error messages across JSON', () =>
    Effect.gen(function* () {
      const cause = new Error('disk failure', { cause: new Error('device offline') })
      const error = StorageError.rejected('write failed', StorageError.Io, cause)
      assert.strictEqual(error.reason._tag, 'Io')
      assert.strictEqual(error.message, 'write failed')
      assert.strictEqual(error.cause, cause)
      const codec = Schema.toCodecJson(StorageError.StorageError)
      const encoded = yield* Schema.encodeEffect(codec)(error)
      const decoded = yield* Schema.decodeEffect(codec)(encoded)
      assert.ok(decoded.cause instanceof Error)
      assert.strictEqual(decoded.cause.message, 'disk failure')
      assert.ok(decoded.cause.cause instanceof Error)
      assert.strictEqual(decoded.cause.cause.message, 'device offline')
    }),
  )
})
