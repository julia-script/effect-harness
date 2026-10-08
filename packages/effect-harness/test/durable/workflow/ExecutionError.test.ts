import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Cause from 'effect/Cause'
import * as Exit from 'effect/Exit'
import * as Result from 'effect/Result'
import * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import * as StorageError from 'effect-harness/durable/StorageError'
import * as Ownership from 'effect-harness/durable/Ownership'
import * as WorkflowEngine from 'effect/workflow/WorkflowEngine'
import * as ExecutionError from 'effect-harness/durable/workflow/ExecutionError'
import { Generation } from 'effect-harness/durable/workflow/Generation'
import { Submission } from 'effect-harness/durable/workflow/Submission'
import { storageError } from 'effect-harness/durable/workflow/SubmissionExecutor'

const codec: Schema.Codec<ExecutionError.ExecutionError, Schema.Json> = Schema.toCodecJson(
  ExecutionError.ExecutionError,
)
describe('ExecutionError', () => {
  it.effect('retains the native declaration decoder failure when wrapping it', () =>
    Effect.gen(function* () {
      const exit = yield* Ownership.execute({
        workflow: Generation._tag,
        executionId: 'malformed-binding',
        payload: { invalid: true },
      }).pipe(Effect.exit)
      assert.ok(Exit.isFailure(exit))
      const error = Result.getOrThrow(Cause.findError(exit.cause))
      assert.strictEqual(error.reason._tag, 'InvalidState')
      assert.strictEqual(error.message, `Native workflow ${Generation._tag} failed`)
      assert.ok(error.cause instanceof Schema.SchemaError)
    }).pipe(
      Effect.provide(
        Layer.merge(Ownership.layerDeclarations([Generation]), WorkflowEngine.layerMemory),
      ),
    ),
  )

  it.effect('round-trips structured reasons and preserves real foreign causes', () =>
    Effect.gen(function* () {
      const cause = new TypeError('provider parsing failed', { cause: new Error('upstream') })
      const error = new ExecutionError.ExecutionError({
        reason: new ExecutionError.ModelError({
          message: 'model failed',
          detail: { retained: true },
          cause,
        }),
      })
      assert.strictEqual(error.cause, cause)
      const encoded = yield* Schema.encodeEffect(codec)(error)
      const decoded = yield* Schema.decodeEffect(codec)(encoded)
      assert.ok(decoded instanceof ExecutionError.ExecutionError)
      assert.ok(decoded.reason instanceof ExecutionError.ModelError)
      assert.strictEqual(decoded.reason._tag, 'ModelError')
      assert.deepStrictEqual(decoded.detail, { retained: true })
      assert.ok(decoded.cause instanceof Error)
      assert.strictEqual(decoded.cause.message, 'provider parsing failed')
      assert.ok(decoded.cause.cause instanceof Error)
      assert.strictEqual(decoded.cause.cause.message, 'upstream')
    }),
  )

  it.effect(
    'persists structured errors in native declarations and rejects owned undefined detail',
    () =>
      Effect.gen(function* () {
        const error = new ExecutionError.ExecutionError({
          reason: new ExecutionError.Aborted({ message: 'Activity aborted' }),
        })
        const encoded = {
          _tag: 'ExecutionError',
          reason: { _tag: 'Aborted', message: 'Activity aborted' },
        }
        for (const declaration of [Generation, Submission]) {
          const errorSchema: Schema.Codec<ExecutionError.ExecutionError, unknown> =
            declaration.errorSchema
          const persisted = yield* Schema.encodeEffect(errorSchema)(error)
          assert.deepStrictEqual(persisted, encoded)
          const decoded = yield* Schema.decodeEffect(errorSchema)(persisted)
          assert.ok(decoded.reason instanceof ExecutionError.Aborted)
          assert.strictEqual(decoded.reason._tag, 'Aborted')
          assert.strictEqual(decoded.message, 'Activity aborted')
        }
        const invalid = yield* Schema.decodeUnknownEffect(codec)({
          ...encoded,
          reason: { ...encoded.reason, detail: undefined },
        }).pipe(Effect.flip)
        assert.ok(invalid instanceof Schema.SchemaError)
      }),
  )

  it.effect('retains storage uncertainty, original cause and permanent retry policy', () =>
    Effect.gen(function* () {
      const cause = new Error('write settlement lost')
      const storage = StorageError.uncertain('commit uncertain', cause)
      const wrapped = storageError(storage)
      assert.strictEqual(wrapped.reason._tag, 'Storage')
      assert.strictEqual(wrapped.cause, storage)
      assert.ok(wrapped.cause instanceof StorageError.StorageError)
      assert.strictEqual(wrapped.cause.cause, cause)
      assert.deepStrictEqual(wrapped.detail, { reason: 'io', certainty: 'uncertain' })
      assert.strictEqual(storage.certainty, 'uncertain')
      assert.strictEqual(storage.isRetryable, false)
      assert.strictEqual(wrapped.isRetryable, false)
      const decoded = yield* Schema.decodeEffect(codec)(yield* Schema.encodeEffect(codec)(wrapped))
      assert.deepStrictEqual(decoded.detail, wrapped.detail)
      assert.ok(decoded.cause instanceof Error)
      assert.strictEqual(decoded.cause.message, 'commit uncertain')
      assert.ok(decoded.cause.cause instanceof Error)
      assert.strictEqual(decoded.cause.cause.message, 'write settlement lost')
    }),
  )
})
