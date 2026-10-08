import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Result from 'effect/Result'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Root from 'effect-harness'
import * as Transcript from 'effect-harness/Transcript'
import * as ResponseAccumulator from 'effect-harness/ResponseAccumulator'
import * as Usage from 'effect-harness/Usage'
import * as Decode from 'effect-harness/env/Decode'
import * as Output from 'effect-harness/Output'
import * as EditDiff from 'effect-harness/tools/EditDiff'
import * as Registry from 'effect-harness/Registry'

describe('TreeNamingConsistency', () => {
  it.effect('constructors create independent mutable state', () =>
    Effect.sync(() => {
      const a = Transcript.make()
      const b = Transcript.make()
      assert.notStrictEqual(a, b)
      assert.notStrictEqual(a.entries, b.entries)
      assert.notStrictEqual(a.messages, b.messages)
      const r = ResponseAccumulator.make()
      const r2 = ResponseAccumulator.make()
      assert.notStrictEqual(r, r2)
      assert.notStrictEqual(r.order, r2.order)
      const usage = Usage.make()
      const other = Usage.make()
      assert.notStrictEqual(usage.cost, other.cost)
      const totals = Usage.makeState()
      const otherTotals = Usage.makeState()
      assert.notStrictEqual(totals.models, otherTotals.models)
      assert.strictEqual(Object.getPrototypeOf(totals.models), null)
      const decoder = Decode.makeRangeDecoder()
      const otherDecoder = Decode.makeRangeDecoder()
      assert.notStrictEqual(decoder, otherDecoder)
      const bomText = new TextEncoder().encode('\ufeffretained')
      assert.strictEqual(decoder.decode(bomText), '\ufeffretained')
      assert.strictEqual(otherDecoder.decode(bomText), '\ufeffretained')
    }),
  )
  it.effect('Registry preserves failure causes and schema round trips', () =>
    Effect.gen(function* () {
      assert.strictEqual(Root.Registry.RegistryError, Registry.RegistryError)
      const cause = new Error('original native cause')
      const caused = new Registry.RegistryError({
        reason: new Registry.RegistryFailureError({ message: 'retained cause', cause }),
      })
      assert.strictEqual(caused.cause, cause)
      assert.strictEqual(caused.reason.cause, cause)
      const failure = new Registry.RegistryFailureError({ message: 'original message' })
      const error = new Registry.RegistryError({ reason: failure })
      const wire = yield* Schema.encodeEffect(Schema.toCodecJson(Registry.RegistryError))(error)
      assert.deepStrictEqual(wire, {
        _tag: 'RegistryError',
        reason: { _tag: 'RegistryFailureError', message: 'original message' },
      })
      const restored = yield* Schema.decodeEffect(Schema.toCodecJson(Registry.RegistryError))(wire)
      assert.instanceOf(restored, Registry.RegistryError)
      assert.instanceOf(restored.reason, Registry.RegistryFailureError)
      assert.strictEqual(restored.message, 'original message')
      const rejected = yield* Registry.make([{ name: '' }]).pipe(Effect.flip)
      assert.instanceOf(rejected, Registry.RegistryError)
      assert.instanceOf(rejected.reason, Registry.RegistryFailureError)
    }),
  )
  it.effect('private Unsafe naming retains typed edit faults and native output offsets', () =>
    Effect.sync(() => {
      const fault = new Error('original accessor failure')
      const hostile: EditDiff.Edit = {
        // effect-nit-allow P7-v4-data-type-naming: Edit.oldText is the fixed public input field; this descriptor getter must retain its synchronous throw to probe the existing Result boundary.
        get oldText(): string {
          throw fault
        },
        newText: 'next',
      }
      const result = EditDiff.applyEditsToNormalizedContent('initial', [hostile], 'file')
      assert.isTrue(Result.isFailure(result))
      if (Result.isFailure(result)) {
        assert.strictEqual(result.failure.cause, fault)
        assert.strictEqual(result.failure.reason._tag, 'EditRangeError')
      }
      const match = EditDiff.fuzzyFindText('α\noriginal\n', 'original')
      assert.isTrue(Option.isSome(match))
      if (Option.isSome(match)) assert.strictEqual(match.value.index, 2)
      const slice = Output.boundOutput('one\ntwo\n', { maxBytes: 4, maxLines: 1, retain: 'tail' })
      assert.deepStrictEqual(slice, { text: 'two\n', bytes: 4, droppedBytes: 4, droppedLines: 1 })
    }),
  )
})
