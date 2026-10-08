import { assertFailure, assertSuccess, assertNone } from '@effect/vitest/utils'

import { assert, describe, it } from '@effect/vitest'

import * as Effect from 'effect/Effect'

import * as Schema from 'effect/Schema'

import * as Record from 'effect-harness/durable/Record'

import * as View from 'effect-harness/durable/View'

// effect-nit-allow P8-tests-import-public-specifiers: these adversarial tests exercise private storage validation seams that intentionally have no public package export; all public behavior uses package specifiers.
// effect-nit-allow P9-no-internal-cross-import: these adversarial tests exercise private storage validation seams that intentionally have no public package export; all public behavior uses package specifiers.
import * as state from '../../src/durable/storage/internal/state.ts'

describe('ViewReplayPaths', () => {
  it.effect(
    'decoded change guards preserve opaque values while refining the completed structure',
    () =>
      Effect.sync(() => {
        const value: View.Value = {
          conversation: { id: Record.ROOT_CONVERSATION_ID },
          entries: [],
          docs: {},
        }
        const opaque = () => 'opaque'
        const change: View.Change = {
          seq: Record.JournalCursor.make(Record.Seq.make(1)),
          before: value,
          value,
          ops: [['set', ['docs'], opaque]],
          reset: false,
        }
        assert.isTrue(View.isChange(change))
        assert.strictEqual(change.ops[0]?.[2], opaque)
        assert.isFalse(View.isChange({ ...change, reset: 'false' }))
        assert.isFalse(View.isChange({ ...change, before: { ...value, entries: [{}] } }))
        assert.isFalse(View.isChange(undefined))
      }),
  )

  it.effect('rejects malformed empty persisted paths and permits root View replacement', () =>
    Effect.gen(function* () {
      const malformed: unknown = [['delete', []]]
      assert.isFalse(Schema.is(Schema.Array(Record.Op))(malformed))
      const error = yield* state
        .applyOps({ count: 1 }, malformed as ReadonlyArray<Record.Op>)
        .pipe(Effect.flip)
      assert.strictEqual(error._tag, 'StorageError')
      assert.strictEqual(error.reason._tag, 'CorruptError')
      assert.strictEqual(error.message, 'Invalid document operation')
      const view: View.Value = {
        conversation: { id: Record.ROOT_CONVERSATION_ID },
        entries: [],
        docs: {},
      }
      const rejected = View.apply(view, malformed as ReadonlyArray<View.Op>)
      assertFailure(
        rejected,
        new View.ViewOperationError({
          message: 'Invalid view operation',
          cause: new TypeError('Invalid view operation'),
        }),
      )
      const replacement = { ...view, docs: {} }
      const applied = View.apply(view, [['set', [], replacement]])
      assertSuccess(applied, replacement)
      assert.strictEqual(applied.success, replacement)
      assertNone(
        state.findDocument(
          Record.emptyState(),
          { kind: 'missing', scope: { _tag: 'session' as const } },
          'current',
        ),
      )
    }),
  )
})
