// effect-nit-allow P8-tests-import-public-specifiers: this same-package private overload proof reads actual emitted state.js → state.d.ts declarations; the production storage seam remains null-exported.
// effect-nit-allow P9-no-internal-cross-import: this same-package private overload proof reads actual emitted state.js → state.d.ts declarations; the production storage seam remains null-exported.
// Boundary.tst.ts separately rejects the private package specifiers. No source alias is used.
import { expect, test } from 'tstyche'
import * as state from '../../dist/durable/storage/internal/state.js'
import type * as Record from 'effect-harness/durable/Record'
import type * as StorageError from 'effect-harness/durable/StorageError'
import type * as Effect from 'effect/Effect'

declare const self: Record.State
declare const writes: ReadonlyArray<Record.Write>
test('private immutable applyWrites preserves both forms and exact channels', () => {
  expect(state.applyWrites(self, writes)).type.toBe<
    Effect.Effect<Record.State, StorageError.StorageError>
  >()
  expect(state.applyWrites(writes)(self)).type.toBe<
    Effect.Effect<Record.State, StorageError.StorageError>
  >()
  expect(state.applyWrites).type.not.toBeCallableWith(self, 1)
  expect(state.applyWrites(writes)).type.not.toBeCallableWith(1)
})
