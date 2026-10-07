// This private implementation proof imports actual emitted declarations by a relative path.
// Boundary.tst.ts separately rejects the private package specifiers. No source alias is used.
import { expect, test } from 'tstyche'
import * as State from '../dist/storage/internal/state.js'
import type * as Record from '@effect-harness/durable/Record'
import type * as StorageError from '@effect-harness/durable/StorageError'
import type * as Effect from 'effect/Effect'

declare const self: Record.State
declare const writes: ReadonlyArray<Record.Write>
test('private immutable applyWrites preserves both forms and exact channels', () => {
  expect(State.applyWrites(self, writes)).type.toBe<
    Effect.Effect<Record.State, StorageError.StorageError>
  >()
  expect(State.applyWrites(writes)(self)).type.toBe<
    Effect.Effect<Record.State, StorageError.StorageError>
  >()
  expect(State.applyWrites).type.not.toBeCallableWith(self, 1)
  expect(State.applyWrites(writes)).type.not.toBeCallableWith(1)
})
