/** Independent behavior probes for persistence and transaction integrity. */
import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as Document from '../../src/Document.ts'
import { Persistence } from '../../src/Persistence.ts'
import * as Record from '../../src/Record.ts'
import * as Session from '../../src/internal/Session.ts'
import * as Memory from '../../src/storage/Memory.ts'

const Counter = Document.defineUnsafe({
  kind: 'test/review-counter',
  version: 1,
  scope: 'conversation',
  history: 'rewindable',
  fork: 'asOf',
  schema: Schema.Struct({ count: Schema.Int }),
  initial: () => ({ count: 0 }),
})
const fixture = Effect.gen(function* () {
  const store = yield* Memory.make
  const session = yield* Session.make.pipe(Effect.provideService(Persistence, store))
  yield* session.initialize
  return { session, store }
})

describe('independent persistence review', () => {
  it.effect(
    'retiring then reacquiring an unacquired document creates a fresh incarnation in one transaction',
    () =>
      Effect.gen(function* () {
        const { session } = yield* fixture
        yield* session.transaction(
          Effect.fn('review.initialDocument')(function* (tx) {
            const draft = yield* tx.doc(Counter, { owner: Record.ROOT_CONVERSATION_ID })
            draft.count = 2
          }),
        )
        const previous = Option.getOrThrow(
          yield* session.snapshot(Counter, { owner: Record.ROOT_CONVERSATION_ID }),
        )
        yield* session.transaction(
          Effect.fn('review.replaceDocument')(function* (tx) {
            yield* tx.retire(Counter, { owner: Record.ROOT_CONVERSATION_ID })
            const fresh = yield* tx.doc(Counter, { owner: Record.ROOT_CONVERSATION_ID })
            assert.strictEqual(fresh.count, 0)
            fresh.count = 10
          }),
        )
        const current = Option.getOrThrow(
          yield* session.snapshot(Counter, { owner: Record.ROOT_CONVERSATION_ID }),
        )
        assert.isAbove(current.record.id, previous.record.id)
        assert.strictEqual(current.value.count, 10)
      }),
  )

  it.effect(
    'a transaction scan acquired on the mutation line is revoked after its callback finishes',
    () =>
      Effect.gen(function* () {
        const { session } = yield* fixture
        yield* session.transaction(
          Effect.fn('review.seedScan')(function* (tx) {
            for (let index = 0; index < 70; index++)
              yield* tx.createTask({
                conversationId: Record.ROOT_CONVERSATION_ID,
                kind: 'test/review',
                version: 1,
                input: null,
                background: false,
                abortRequested: false,
                state: { status: 'pending', checkpoint: { phase: 'run' } },
              })
          }),
        )
        const pull = yield* session.transaction(
          Effect.fn('review.acquireScan')(function* (tx) {
            const pull = yield* tx.scanTasks().pipe(Stream.toPull)
            assert.lengthOf(yield* pull, 1)
            return pull
          }),
        )
        const value = yield* Effect.result(pull)
        assert.strictEqual(value._tag, 'Failure')
      }),
  )
})
