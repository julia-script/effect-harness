import { assert, describe, it } from '@effect/vitest'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Option from 'effect/Option'
import * as Result from 'effect/Result'
import * as Schema from 'effect/Schema'
import * as Conversation from '../src/Conversation.ts'
import * as Executor from '../src/Executor.ts'
import * as Harness from '../src/Harness.ts'
import * as Identity from '../src/Identity.ts'
import { Persistence } from '../src/Persistence.ts'
import * as Record from '../src/Record.ts'
import * as Submission from '../src/Submission.ts'
import * as Memory from '../src/storage/Memory.ts'
import * as Native from './embedded/NativeFixture.ts'

const open = Effect.fn('test.open')(function* (
  store: Persistence['Service'],
  options: Native.Options = {},
) {
  const { executor } = yield* Native.makeExecutor(options)
  return yield* Harness.make({
    agent: { model: Native.ref },
    settings: { retry: { enabled: false }, compaction: { enabled: false } },
  }).pipe(
    Effect.provideService(Persistence, store),
    Effect.provideService(Executor.Executor, executor),
  )
})

const gate = Effect.gen(function* () {
  const entered = yield* Deferred.make<void>()
  const release = yield* Deferred.make<void>()
  const handle: NonNullable<Native.Options['handle']> = ({ text }) =>
    Deferred.succeed(entered, undefined).pipe(
      Effect.andThen(Deferred.await(release)),
      Effect.as(text.toUpperCase()),
    )
  return { entered, release, handle }
})

describe('submission admission and withdrawal', () => {
  it.effect('rejects busy admission without writes and deduplicates before the busy policy', () =>
    Effect.gen(function* () {
      const store = yield* Memory.make
      const blocked = yield* gate
      const harness = yield* open(store, { handle: blocked.handle })
      const conversation = yield* harness.root
      const requestId = yield* Schema.decodeEffect(Identity.RequestId)('admission/request')
      const first = yield* Conversation.submit(conversation, 'first', { requestId })
      yield* Deferred.await(blocked.entered)
      const failure = yield* Effect.result(
        Conversation.submit(conversation, 'rejected', { mode: 'reject' }),
      )
      assert.isTrue(Result.isFailure(failure))
      if (Result.isFailure(failure)) {
        assert.strictEqual(failure.failure._tag, 'StorageError')
        if (failure.failure._tag === 'StorageError')
          assert.strictEqual(failure.failure.reason._tag, 'ConflictError')
      }
      const retry = yield* Conversation.submit(conversation, 'first', {
        requestId,
        mode: 'reject',
      })
      assert.strictEqual(retry.id, first.id)
      assert.strictEqual((yield* Conversation.snapshot(conversation)).submissions.length, 1)
      yield* Deferred.succeed(blocked.release, undefined)
      yield* Submission.wait(first)
      const accepted = yield* Conversation.submit(conversation, 'idle', { mode: 'reject' })
      assert.strictEqual((yield* Submission.wait(accepted)).status, 'done')
    }),
  )

  it.effect(
    'withdraws only queued input, preserving the active turn and immutable settlement',
    () =>
      Effect.gen(function* () {
        const store = yield* Memory.make
        const blocked = yield* gate
        const harness = yield* open(store, { handle: blocked.handle })
        const conversation = yield* harness.root
        const active = yield* Conversation.submit(conversation, 'active')
        yield* Deferred.await(blocked.entered)
        const queued = yield* Conversation.submit(conversation, 'withdraw this input')
        assert.strictEqual(yield* Submission.withdraw(active), 'already_placed')
        const result = yield* Schema.decodeEffect(Submission.WithdrawalResult)(
          yield* Submission.withdraw(queued),
        )
        assert.strictEqual(result, 'aborted')
        const settled = yield* Submission.wait(queued)
        assert.strictEqual(settled.status, 'unanswered')
        if (settled.status === 'unanswered') assert.strictEqual(settled.reason, 'aborted')
        assert.strictEqual(yield* Submission.withdraw(queued), 'settled')
        yield* Deferred.succeed(blocked.release, undefined)
        assert.strictEqual((yield* Submission.wait(active)).status, 'done')
        yield* Conversation.awaitIdle(conversation)
        const snapshot = yield* Conversation.snapshot(conversation)
        assert.strictEqual(
          snapshot.entries.filter((entry) => entry.kind === 'harness.user').length,
          1,
        )
        assert.strictEqual(snapshot.submissions.length, 2)
      }),
  )

  it.effect('withdraws saved queued input before explicit resume and persists the settlement', () =>
    Effect.gen(function* () {
      const store = yield* Memory.make
      const blocked = yield* gate
      const first = yield* open(store, { handle: blocked.handle })
      const conversation = yield* first.root
      const active = yield* Conversation.submit(conversation, 'active')
      yield* Deferred.await(blocked.entered)
      const queued = yield* Conversation.submit(conversation, 'saved queued input')
      yield* first.close
      const second = yield* open(store)
      assert.strictEqual(yield* second.withdraw(queued.id), 'aborted')
      yield* second.resume
      assert.strictEqual((yield* second.awaitSubmission(active.id)).status, 'done')
      yield* second.awaitIdle(conversation.id)
      yield* second.close
      const third = yield* open(store)
      const saved = yield* third.submission(queued.id)
      assert.isTrue(Option.isSome(saved))
      if (Option.isSome(saved)) {
        const encoded = yield* Schema.encodeEffect(Record.Submission)(saved.value)
        const decoded = yield* Schema.decodeEffect(Record.Submission)(encoded)
        assert.strictEqual(decoded.status, 'unanswered')
        if (decoded.status === 'unanswered') assert.strictEqual(decoded.reason, 'aborted')
      }
      assert.strictEqual(yield* third.withdraw(queued.id), 'settled')
    }),
  )
})
