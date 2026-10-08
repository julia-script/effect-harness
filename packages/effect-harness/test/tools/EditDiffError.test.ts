import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as ErrorReporter from 'effect/ErrorReporter'
import * as Schema from 'effect/Schema'
import * as TestSchema from 'effect/testing/TestSchema'
import * as Result from 'effect/Result'
import * as EditDiff from 'effect-harness/tools/EditDiff'

describe('EditDiffError', () => {
  it.effect('foreign edit getters retain their exact typed rejection cause', () =>
    Effect.sync(() => {
      const cause = new Error('foreign edit getter')
      const edit: EditDiff.Edit = {
        // effect-nit-allow P7-v4-data-type-naming: oldText is the fixed Edit input field; this throwing descriptor exercises the foreign synchronous rejection boundary.
        get oldText(): string {
          throw cause
        },
        newText: 'next',
      }
      const result = EditDiff.applyEditsToNormalizedContent('initial', [edit], 'file')
      assert.isTrue(Result.isFailure(result))
      if (Result.isFailure(result)) {
        assert.strictEqual(result.failure.reason._tag, 'EditRangeError')
        assert.strictEqual(result.failure.cause, cause)
        assert.strictEqual(result.failure.reason.cause, cause)
      }
    }),
  )
  it.effect('native construction retains every tagged leaf and cause by identity', () =>
    Effect.gen(function* () {
      const cause = new Error('kept leaf cause')
      const leaves = [
        new EditDiff.EditEmptyError({ message: 'empty', cause }),
        new EditDiff.EditNotFoundError({ message: 'not found', cause }),
        new EditDiff.EditDuplicateError({ message: 'duplicate', cause }),
        new EditDiff.EditOverlapError({ message: 'overlap', cause }),
        new EditDiff.EditNoChangeError({ message: 'same', cause }),
        new EditDiff.EditRangeError({ message: 'range', cause }),
      ]
      for (const reason of leaves) {
        const error = new EditDiff.EditError({ reason })
        assert.strictEqual(error.reason, reason)
        assert.strictEqual(error.cause, cause)
        assert.strictEqual(error.message, reason.message)
        const wire = yield* Schema.encodeEffect(Schema.toCodecJson(EditDiff.EditError))(error)
        assert.deepStrictEqual(wire, {
          _tag: 'EditError',
          reason: {
            _tag: reason._tag,
            message: reason.message,
            cause: { name: 'Error', message: cause.message },
          },
        })
        assert.isFalse(ErrorReporter.isIgnored(error))
        assert.strictEqual(ErrorReporter.getSeverity(error), 'Info')
      }
    }),
  )
  it.effect('schemas encode one structured error shape and hydrate native reasons', () =>
    Effect.gen(function* () {
      const schema = Schema.toCodecJson(EditDiff.EditError)
      const checks = new TestSchema.Asserts(schema)
      const error = new EditDiff.EditError({
        reason: new EditDiff.EditEmptyError({ message: 'empty' }),
      })
      const wire = { _tag: 'EditError', reason: { _tag: 'EditEmptyError', message: 'empty' } }
      yield* checks.encoding().succeedEffect(error, wire)
      yield* checks.decoding().succeedEffect(wire, error)
      const cause = new TypeError('native defect')
      const codec = Schema.fromJsonString(EditDiff.EditError)
      const serialized = yield* Schema.encodeEffect(codec)(
        new EditDiff.EditError({
          reason: new EditDiff.EditRangeError({ message: 'wrapped', cause }),
        }),
      )
      const restored = yield* Schema.decodeEffect(codec)(serialized)
      assert.instanceOf(restored, EditDiff.EditError)
      assert.instanceOf(restored.reason, EditDiff.EditRangeError)
      assert.instanceOf(restored.cause, Error)
      assert.strictEqual(restored.cause.message, cause.message)
      assert.strictEqual(restored.reason.cause, restored.cause)
      const rejected = yield* Schema.decodeEffect(codec)(
        '{"_tag":"EditError","code":"range","message":"flat"}',
      ).pipe(Effect.flip)
      assert.strictEqual(rejected._tag, 'SchemaError')
    }),
  )
})
