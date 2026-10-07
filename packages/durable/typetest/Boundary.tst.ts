import { expect, test } from 'tstyche'
import * as Store from '@effect-harness/durable/Store'
import * as Record from '@effect-harness/durable/Record'
import * as Document from '@effect-harness/durable/Document'
import type * as Effect from 'effect/Effect'
import type * as StorageError from '@effect-harness/durable/StorageError'

// There is no module-import negative matcher. These exact TS2307 guards are checked
// by tstyche's checkSuppressedErrors against actual emitted public package exports.
// @ts-expect-error Cannot find module '@effect-harness/durable/storage/State'
import type * as OldState from '@effect-harness/durable/storage/State'
// @ts-expect-error Cannot find module '@effect-harness/durable/storage/Backend'
import type * as OldBackend from '@effect-harness/durable/storage/Backend'
// @ts-expect-error Cannot find module '@effect-harness/durable/storage/internal/state'
import type * as PrivateState from '@effect-harness/durable/storage/internal/state'
// @ts-expect-error Cannot find module '@effect-harness/durable/storage/internal/backend'
import type * as PrivateBackend from '@effect-harness/durable/storage/internal/backend'

// Referencing the suppressed import namespaces keeps noUnusedLocals honest without
// asserting the compiler's error-recovery any as a real public type.
export type RejectedImports = [
  typeof OldState,
  typeof OldBackend,
  typeof PrivateState,
  typeof PrivateBackend,
]

test('supported Store extension contracts and Document copy errors remain public', () => {
  expect(Store.makeCandidate({ state: Record.emptyState(), writes: [], result: 42 })).type.toBe<
    Store.Candidate<number>
  >()
  expect(Store.mintId(Record.EntryId)).type.toBe<
    Effect.Effect<Record.EntryId, StorageError.StorageError, Store.Store>
  >()
  expect(
    new Document.CloneError({ message: 'clone failure', cause: new TypeError('actual cause') }),
  ).type.toBe<Document.CloneError>()
})
