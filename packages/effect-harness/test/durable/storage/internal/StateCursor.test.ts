import { assert, describe, it } from '@effect/vitest'

import * as Effect from 'effect/Effect'

import * as Schema from 'effect/Schema'

import * as Record from 'effect-harness/durable/Record'

// effect-nit-allow P8-tests-import-public-specifiers: these adversarial tests exercise private storage validation seams that intentionally have no public package export; all public behavior uses package specifiers.
// effect-nit-allow P9-no-internal-cross-import: these adversarial tests exercise private storage validation seams that intentionally have no public package export; all public behavior uses package specifiers.
import { cursor } from '../../../../src/durable/storage/internal/state.ts'

describe('StateCursor', () => {
  it.effect('validates zero/last-safe journal cursors and the exhausted allocator sentinel', () =>
    Effect.gen(function* () {
      assert.strictEqual(yield* cursor(1), 0)
      assert.strictEqual(yield* cursor(Number.MAX_SAFE_INTEGER + 1), Number.MAX_SAFE_INTEGER)
      assert.strictEqual((yield* cursor(0).pipe(Effect.flip)).reason._tag, 'CorruptError')
      const state = {
        ...Record.emptyState(),
        nextId: Number.MAX_SAFE_INTEGER + 1,
        nextSeq: Number.MAX_SAFE_INTEGER + 1,
      }
      assert.isTrue(Schema.is(Record.State)(state))
    }),
  )
})
