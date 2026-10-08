import { assert, describe, it } from '@effect/vitest'

import * as Effect from 'effect/Effect'

import * as Edit from 'effect-harness/tools/Edit'

describe('EditRepair', () => {
  it.effect(
    'tolerant Edit repair preserves unrelated keys, arbitrary arrays and unchanged input fallback',
    () =>
      Effect.gen(function* () {
        const original = [{ oldText: 'a', newText: 'b' }]
        assert.strictEqual(yield* Edit.repair(original), original)
        const arbitrary = [1, null, { other: true }]
        const value = { path: 'file', edits: arbitrary, extra: { keep: true } }
        assert.deepStrictEqual(yield* Edit.repair(value), value)
        assert.deepStrictEqual(
          yield* Edit.repair({ path: 'file', edits: '[1,null,{"other":true}]', extra: 'kept' }),
          { path: 'file', edits: arbitrary, extra: 'kept' },
        )
        assert.deepStrictEqual(
          yield* Edit.repair({
            path: 'file',
            edits: '{"oldText":"a","newText":"b"}',
            extra: 'kept',
          }),
          { path: 'file', edits: original, extra: 'kept' },
        )
        assert.deepStrictEqual(
          yield* Edit.repair({
            path: 'file',
            edits: original[0],
            oldText: 'c',
            newText: 'd',
            extra: 9,
          }),
          { path: 'file', edits: original, oldText: 'c', newText: 'd', extra: 9 },
        )
        const invalid = { path: 'file', edits: 'not JSON', oldText: 1, newText: 'd', extra: 9 }
        assert.deepStrictEqual(yield* Edit.repair(invalid), invalid)
        assert.strictEqual(yield* Edit.repair('unrelated'), 'unrelated')
      }),
  )
})
