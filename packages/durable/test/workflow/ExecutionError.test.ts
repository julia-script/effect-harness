import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Cause from 'effect/Cause'
import * as Exit from 'effect/Exit'
import * as Result from 'effect/Result'
import * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import * as StorageError from '@effect-harness/durable/StorageError'
import * as Ownership from '@effect-harness/durable/Ownership'
import * as WorkflowEngine from 'effect/workflow/WorkflowEngine'
import * as ExecutionError from '@effect-harness/durable/workflow/ExecutionError'
import { Generation } from '@effect-harness/durable/workflow/Generation'
import { Submission } from '@effect-harness/durable/workflow/Submission'
import { storageError } from '@effect-harness/durable/workflow/SubmissionExecutor'

const codec: Schema.Codec<ExecutionError.ExecutionError, Schema.Json> = Schema.toCodecJson(
  ExecutionError.ExecutionErrorCodec,
)
describe('workflow.ExecutionError', () => {
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

  it.effect('decodes every frozen reason and preserves the original Workflow wire shape', () =>
    Effect.gen(function* () {
      const fixtures = [
        ['no_model', 'NoModel'],
        ['conversation_busy', 'ConversationBusy'],
        ['request_conflict', 'RequestConflict'],
        ['tool_unavailable', 'ToolUnavailable'],
        ['invalid_arguments', 'InvalidArguments'],
        ['model_error', 'ModelError'],
        ['context_overflow', 'ContextOverflow'],
        ['aborted', 'Aborted'],
        ['closed', 'Closed'],
        ['storage', 'Storage'],
        ['invalid_state', 'InvalidState'],
      ] as const
      for (const [reason, tag] of fixtures) {
        for (const detail of [undefined, null, { certainty: 'uncertain', reason: 'io' }]) {
          const legacy = {
            _tag: 'ExecutionError' as const,
            reason,
            message: `original ${reason}`,
            ...(detail === undefined ? {} : { detail }),
          }
          const incompatibleCodec: Schema.Codec<ExecutionError.ExecutionError, Schema.Json> =
            Schema.toCodecJson(ExecutionError.ExecutionError)
          const rejectedLegacy = yield* Schema.decodeEffect(incompatibleCodec)(legacy).pipe(
            Effect.flip,
          )
          assert.ok(rejectedLegacy instanceof Schema.SchemaError)
          const decoded = yield* Schema.decodeEffect(codec)(legacy)
          assert.ok(decoded instanceof ExecutionError.ExecutionError)
          assert.strictEqual(decoded.reason._tag, tag)
          assert.strictEqual(decoded.message, legacy.message)
          assert.strictEqual(decoded.code, reason)
          assert.strictEqual(decoded.isRetryable, false)
          assert.strictEqual(Object.hasOwn(decoded.reason, 'detail'), detail !== undefined)
          assert.strictEqual(Object.hasOwn(decoded.reason, 'cause'), false)
          assert.deepStrictEqual(yield* Schema.encodeEffect(codec)(decoded), legacy)
        }
      }
    }),
  )

  it.effect('accepts current structured encoding and preserves real foreign causes', () =>
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
      const currentCodec: Schema.Codec<ExecutionError.ExecutionError, Schema.Json> =
        Schema.toCodecJson(ExecutionError.ExecutionError)
      const current = yield* Schema.encodeEffect(currentCodec)(error)
      const decoded = yield* Schema.decodeEffect(codec)(current)
      assert.strictEqual(decoded.reason._tag, 'ModelError')
      assert.deepStrictEqual(decoded.detail, { retained: true })
      assert.ok(decoded.cause instanceof Error)
      assert.strictEqual(decoded.cause.message, 'provider parsing failed')
      assert.ok(decoded.cause.cause instanceof Error)
      assert.strictEqual(decoded.cause.cause.message, 'upstream')
      const legacy = yield* Schema.encodeEffect(codec)(decoded)
      assert.ok(legacy !== null && typeof legacy === 'object' && !Array.isArray(legacy))
      assert.ok('reason' in legacy && 'message' in legacy)
      assert.strictEqual(legacy.reason, 'model_error')
      assert.strictEqual(legacy.message, 'model failed')
      const roundtrip = yield* Schema.decodeEffect(codec)(legacy)
      assert.ok(roundtrip.cause instanceof Error)
      assert.strictEqual(roundtrip.cause.message, 'provider parsing failed')
    }),
  )

  it.effect(
    'uses compatible codecs in native declarations and rejects owned undefined detail',
    () =>
      Effect.gen(function* () {
        const legacy = { _tag: 'ExecutionError', reason: 'aborted', message: 'old activity' }
        for (const declaration of [Generation, Submission]) {
          const errorSchema: Schema.Codec<ExecutionError.ExecutionError, unknown> =
            declaration.errorSchema
          const decoded = yield* Schema.decodeEffect(errorSchema)(legacy)
          assert.strictEqual(decoded.reason._tag, 'Aborted')
          assert.strictEqual(decoded.message, 'old activity')
        }
        const invalid = yield* Schema.decodeUnknownEffect(codec)({
          ...legacy,
          detail: undefined,
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
