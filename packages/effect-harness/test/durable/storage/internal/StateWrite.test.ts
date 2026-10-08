import { assert, describe, it } from '@effect/vitest'

import * as Effect from 'effect/Effect'

import * as Record from 'effect-harness/durable/Record'

// effect-nit-allow P8-tests-import-public-specifiers: these adversarial tests exercise private storage validation seams that intentionally have no public package export; all public behavior uses package specifiers.
// effect-nit-allow P9-no-internal-cross-import: these adversarial tests exercise private storage validation seams that intentionally have no public package export; all public behavior uses package specifiers.
import * as state from '../../../../src/durable/storage/internal/state.ts'

describe('StateWrite', () => {
  it.effect('retains both immutable state write forms and deferred validation', () =>
    Effect.gen(function* () {
      const self = Record.emptyState()
      const before = structuredClone(self)
      const writes = [
        { _tag: 'conversation', value: { id: Record.ROOT_CONVERSATION_ID } },
      ] as const satisfies ReadonlyArray<Record.Write>
      const dataFirst = yield* state.applyWrites(self, writes)
      const dataLast = yield* state.applyWrites(writes)(self)
      assert.deepStrictEqual(dataFirst, dataLast)
      assert.deepStrictEqual(self, before)
      assert.notStrictEqual(dataFirst, self)
      assert.deepStrictEqual(dataFirst.conversations, [{ id: Record.ROOT_CONVERSATION_ID }])
      const late: Array<Record.Write> = []
      const delayed = state.applyWrites(late)(self)
      late.push(...writes)
      assert.deepStrictEqual(yield* delayed, dataFirst)
    }),
  )
})
