import { assertExitFailure } from '@effect/vitest/utils'
import * as Cause from 'effect/Cause'
import * as TestSchema from 'effect/testing/TestSchema'
import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
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
      const decoderFailure = yield* Schema.decodeUnknownEffect(Generation.payloadSchema)({
        invalid: true,
      }).pipe(Effect.flip)
      assertExitFailure(
        exit,
        Cause.fail(
          new ExecutionError.ExecutionError({
            reason: new ExecutionError.InvalidStateError({
              message: `Native workflow ${Generation._tag} failed`,
              cause: decoderFailure,
            }),
          }),
        ),
      )
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
      const wire = {
        _tag: 'ExecutionError',
        reason: {
          _tag: 'ModelError',
          message: 'model failed',
          detail: { retained: true },
          cause: {
            name: 'TypeError',
            message: 'provider parsing failed',
            cause: { name: 'Error', message: 'upstream' },
          },
        },
      }
      const expectedCause = new Error('provider parsing failed', { cause: new Error('upstream') })
      expectedCause.name = 'TypeError'
      const expected = new ExecutionError.ExecutionError({
        reason: new ExecutionError.ModelError({
          message: 'model failed',
          detail: { retained: true },
          cause: expectedCause,
        }),
      })
      const assertions = new TestSchema.Asserts(codec)
      yield* assertions.encoding().succeedEffect(error, wire)
      yield* assertions.decoding().succeedEffect(wire, expected)
      const decoded = yield* Schema.decodeEffect(codec)(wire)
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
          reason: new ExecutionError.AbortedError({ message: 'Activity aborted' }),
        })
        const encoded = {
          _tag: 'ExecutionError',
          reason: { _tag: 'AbortedError', message: 'Activity aborted' },
        }
        for (const declaration of [Generation, Submission]) {
          const errorSchema: Schema.Codec<ExecutionError.ExecutionError, unknown> =
            declaration.errorSchema
          const asserts = new TestSchema.Asserts(errorSchema)
          yield* asserts.encoding().succeedEffect(error, encoded)
          yield* asserts.decoding().succeedEffect(encoded, error)
        }
        yield* new TestSchema.Asserts(codec).decoding().failEffect(
          {
            ...encoded,
            reason: { ...encoded.reason, detail: undefined },
          },
          'Expected JSON value\n  at ["reason"]["detail"]',
        )
      }),
  )

  it.effect('retains storage uncertainty, original cause and permanent retry policy', () =>
    Effect.gen(function* () {
      const cause = new Error('write settlement lost')
      const storage = StorageError.uncertain('commit uncertain', cause)
      const wrapped = storageError(storage)
      assert.strictEqual(wrapped.reason._tag, 'StorageError')
      assert.strictEqual(wrapped.cause, storage)
      assert.ok(wrapped.cause instanceof StorageError.StorageError)
      assert.strictEqual(wrapped.cause.cause, cause)
      assert.deepStrictEqual(wrapped.detail, { reason: 'IoError', certainty: 'uncertain' })
      assert.strictEqual(storage.certainty, 'uncertain')
      assert.strictEqual(storage.isRetryable, false)
      assert.strictEqual(wrapped.isRetryable, false)
      const wire = {
        _tag: 'ExecutionError',
        reason: {
          _tag: 'StorageError',
          message: 'commit uncertain',
          detail: { reason: 'IoError', certainty: 'uncertain' },
          cause: {
            name: '@effect-harness/durable/StorageError',
            message: 'commit uncertain',
            cause: { name: 'Error', message: 'write settlement lost' },
          },
        },
      }
      const expectedCause = new Error('commit uncertain', {
        cause: new Error('write settlement lost'),
      })
      expectedCause.name = '@effect-harness/durable/StorageError'
      const expected = new ExecutionError.ExecutionError({
        reason: new ExecutionError.StorageError({
          message: 'commit uncertain',
          detail: { reason: 'IoError', certainty: 'uncertain' },
          cause: expectedCause,
        }),
      })
      const assertions = new TestSchema.Asserts(codec)
      yield* assertions.encoding().succeedEffect(wrapped, wire)
      yield* assertions.decoding().succeedEffect(wire, expected)
      const decoded = yield* Schema.decodeEffect(codec)(wire)
      assert.deepStrictEqual(decoded.detail, wrapped.detail)
      assert.ok(decoded.cause instanceof Error)
      assert.strictEqual(decoded.cause.message, 'commit uncertain')
      assert.ok(decoded.cause.cause instanceof Error)
      assert.strictEqual(decoded.cause.cause.message, 'write settlement lost')
    }),
  )
})
