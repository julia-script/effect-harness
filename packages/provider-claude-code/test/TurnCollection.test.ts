import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Stream from 'effect/Stream'
import * as Turn from '@effect-harness/provider-claude-code/Turn'

describe('TurnCollection', () => {
  it.effect('collection preserves index zero, map insertion order and part metadata', () =>
    Effect.gen(function* () {
      const parts = yield* Turn.collect(
        Stream.make(
          { type: 'text-start', id: 'first' },
          { type: 'text-delta', id: 'first', delta: 'a' },
          { type: 'reasoning-start', id: 'second' },
          { type: 'reasoning-delta', id: 'second', delta: 'b' },
          { type: 'text-delta', id: 'first', delta: 'c' },
          { type: 'text-end', id: 'first', metadata: { claudeCode: { retained: 'first' } } },
          {
            type: 'reasoning-end',
            id: 'second',
            metadata: { claudeCode: { signature: 'second' } },
          },
          { type: 'text-delta', id: 'absent', delta: 'ignored' },
        ),
      )
      assert.deepStrictEqual(parts, [
        { type: 'text', text: 'ac', metadata: { claudeCode: { retained: 'first' } } },
        { type: 'reasoning', text: 'b', metadata: { claudeCode: { signature: 'second' } } },
      ])
    }),
  )
})
