import { assertFailure, assertSuccess } from '@effect/vitest/utils'

import { assert, describe, it } from '@effect/vitest'

import * as Effect from 'effect/Effect'

import * as Schema from 'effect/Schema'

import * as Document from 'effect-harness/durable/Document'

import * as Record from 'effect-harness/durable/Record'

import * as View from 'effect-harness/durable/View'

const primitiveId = Schema.decodeSync(Record.TaskId)(2)

const view: View.Value = {
  conversation: { id: Record.ROOT_CONVERSATION_ID },
  entries: [],
  docs: {},
}
// The own-property regression deliberately checks the same prototype-key contract through Document.copy and View.apply in one original test case.

describe('DocumentCopy', () => {
  it.effect(
    'preserves proxy clone fallback, primitive brands, cycles and owned prototype keys',
    () =>
      Effect.sync(() => {
        const source: {
          readonly __proto__: { readonly retained: boolean }
          readonly nested: number[]
        } = {
          ['__proto__']: { retained: true },
          nested: [1, 2],
        }
        const cloneResult = Document.copy(new Proxy(source, {}))
        assertSuccess(cloneResult, { ['__proto__']: { retained: true }, nested: [1, 2] })
        const clone = cloneResult.success
        assert.notStrictEqual(clone, source)
        assert.notStrictEqual(clone.nested, source.nested)
        assert.strictEqual(Object.hasOwn(clone, '__proto__'), true)
        assert.deepStrictEqual(clone.__proto__, { retained: true })
        assert.strictEqual(Object.getPrototypeOf(clone), Object.prototype)
        const cycle: { self?: object } = {}
        const cycleProxy = new Proxy(cycle, {})
        cycle.self = cycleProxy
        const expectedCycle: { self?: object } = {}
        expectedCycle.self = expectedCycle
        const cyclicResult = Document.copy(cycleProxy)
        assertSuccess(cyclicResult, expectedCycle)
        const cyclic = cyclicResult.success
        assert.strictEqual(cyclic.self, cyclic)
        const copiedResult = Document.copy(primitiveId)
        assertSuccess(copiedResult, primitiveId)
        const copied = copiedResult.success
        assert.strictEqual(copied, primitiveId)
        const appliedResult = View.apply(view, [['set', ['docs', '__proto__'], { retained: true }]])
        const expectedView = {
          conversation: { id: Record.ROOT_CONVERSATION_ID },
          entries: [],
          docs: {},
        }
        Object.defineProperty(expectedView.docs, '__proto__', {
          value: { retained: true },
          enumerable: true,
          configurable: true,
          writable: true,
        })
        assertSuccess(appliedResult, expectedView)
        const applied = appliedResult.success
        assert.strictEqual(Object.hasOwn(applied.docs, '__proto__'), true)
        assert.strictEqual(Object.getPrototypeOf(applied.docs), Object.prototype)
        assert.strictEqual(Object.hasOwn(view.docs, '__proto__'), false)
      }),
  )

  it.effect('wraps native clone and fallback getter failures with the actual cause', () =>
    Effect.sync(() => {
      const cause = new Error('getter failed')
      const native = {
        get value(): number {
          throw cause
        },
      }
      const proxy = new Proxy(native, {})
      assertFailure(
        Document.copy(native),
        new Document.CloneError({ message: 'Cannot detach durable value', cause }),
      )
      assertFailure(
        Document.copy(proxy),
        new Document.CloneError({ message: 'Cannot detach durable proxy', cause }),
      )
    }),
  )
})
