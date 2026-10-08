import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Output from 'effect-harness/Output'

describe('OutputWindowLaziness', () => {
  it.effect(
    'constructing or repeating commands keeps decoder/state work inside serialized execution',
    () =>
      Effect.gen(function* () {
        const window = yield* Output.makeWindow()
        const first = window.push(new Uint8Array([0xe2]))
        const second = window.push(new Uint8Array([0x82, 0xac]))
        const text = window.push('!')
        assert.strictEqual((yield* window.snapshot).text, '')
        yield* first
        yield* second
        yield* text
        yield* text
        yield* window.end
        assert.strictEqual((yield* window.snapshot).text, '€!!')
        yield* window.reset
        const pending = window.push('later')
        assert.strictEqual((yield* window.snapshot).text, '')
        yield* pending
        assert.strictEqual((yield* window.snapshot).text, 'later')
      }),
  )
})
