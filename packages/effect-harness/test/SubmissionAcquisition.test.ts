import { assert, it } from '@effect/vitest'
import { NodeServices } from '@effect/platform-node'
import * as Context from 'effect/Context'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Path from 'effect/Path'
import * as Scope from 'effect/Scope'
import * as Stream from 'effect/Stream'
import * as LanguageModel from 'effect/ai/LanguageModel'
import type * as Response from 'effect/ai/Response'
import * as Conversation from 'effect-harness/Conversation'
import * as Harness from 'effect-harness/Harness'
import { HarnessBackend } from 'effect-harness/HarnessBackend'
import * as HarnessRuntime from 'effect-harness/HarnessRuntime'
import * as Record from 'effect-harness/Record'
import * as Storage from 'effect-harness/Storage'
import * as Submission from 'effect-harness/Submission'

const answer: Array<Response.PartEncoded> = [
  { type: 'text', text: 'answer' },
  {
    type: 'finish',
    reason: 'stop',
    usage: {
      inputTokens: { total: 1, uncached: 1, cacheRead: undefined, cacheWrite: undefined },
      outputTokens: { total: 1, text: 1, reasoning: undefined },
    },
    response: undefined,
  },
]

it.live(
  'saved IDs reacquire every input state after reopening durable storage without writes',
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem
        const path = yield* Path.Path
        const directory = yield* fs.makeTempDirectoryScoped({ prefix: 'submission-acquisition-' })
        const filePath = path.join(directory, 'records.jsonl')
        const entered = yield* Deferred.make<void>()
        const release = yield* Deferred.make<void>()
        const model = yield* LanguageModel.make({
          generateText: ({ prompt }) => {
            const user = prompt.content.findLast((message) => message.role === 'user')
            if (user?.content.some((part) => part.type === 'text' && part.text === 'complete'))
              return Effect.succeed(answer)
            return Deferred.succeed(entered, undefined).pipe(
              Effect.andThen(Deferred.await(release)),
              Effect.as(answer),
            )
          },
          streamText: () => Stream.empty,
        })
        const open = Effect.gen(function* () {
          const storage = Context.get(
            yield* Layer.build(Storage.layerJsonl({ filePath })),
            Storage.Storage,
          )
          const runtime = yield* HarnessRuntime.make().pipe(
            Effect.provideService(Storage.Storage, storage),
            Effect.provideService(LanguageModel.LanguageModel, model),
          )
          return yield* Harness.make.pipe(Effect.provideService(HarnessBackend, runtime.backend))
        })
        const firstScope = yield* Scope.make()
        yield* Effect.addFinalizer(() => Scope.close(firstScope, Exit.void))
        const first = yield* open.pipe(Effect.provideService(Scope.Scope, firstScope))
        const root = yield* first.root
        const completed = yield* Conversation.submit(root, { type: 'input', content: 'complete' })
        const completedResult = yield* Submission.wait(completed)
        const conversation = yield* first.create()
        const draft = { type: 'input', content: 'active', requestId: 'saved-active' } as const
        const active = yield* Conversation.submit(conversation, draft)
        yield* Deferred.await(entered)
        const queued = yield* Conversation.submit(conversation, {
          type: 'input',
          content: 'queued',
          requestId: 'saved-queued',
        })
        const withdrawn = yield* Conversation.submit(conversation, {
          type: 'input',
          content: 'withdrawn',
        })
        assert.strictEqual(yield* Submission.withdraw(withdrawn), 'aborted')
        const withdrawnResult = yield* Submission.wait(withdrawn)
        yield* Scope.close(firstScope, Exit.void)

        const reopened = yield* open
        const before = yield* fs.readFileString(filePath)
        const savedCompleted = yield* reopened.submission(completed.id)
        const savedActive = yield* reopened.submission(active.id)
        const savedQueued = yield* reopened.submission(queued.id)
        const savedWithdrawn = yield* reopened.submission(withdrawn.id)
        for (const [original, saved] of [
          [completed, savedCompleted],
          [active, savedActive],
          [queued, savedQueued],
          [withdrawn, savedWithdrawn],
        ] as const) {
          assert.strictEqual(saved.id, original.id)
          assert.strictEqual(saved.conversationId, original.conversationId)
        }
        const missing = yield* reopened
          .submission(Record.SubmissionId.make(999999))
          .pipe(Effect.flip)
        assert.strictEqual(missing._tag, 'HarnessError')
        assert.strictEqual(missing.reason, 'notFound')
        assert.strictEqual(missing.operation, 'submission.read')
        assert.strictEqual(yield* fs.readFileString(filePath), before)
        assert.strictEqual((yield* Submission.read(savedActive)).status, 'placed')
        assert.strictEqual((yield* Submission.read(savedQueued)).status, 'queued')
        assert.deepEqual(yield* Submission.wait(savedCompleted), completedResult)
        assert.deepEqual(yield* Submission.wait(savedWithdrawn), withdrawnResult)
        assert.strictEqual(yield* Submission.withdraw(savedCompleted), 'settled')
        assert.strictEqual(yield* Submission.withdraw(savedWithdrawn), 'settled')
        assert.strictEqual(yield* Submission.withdraw(savedActive), 'already_placed')
        assert.strictEqual(yield* Submission.withdraw(savedQueued), 'aborted')
        const queuedResult = yield* Submission.wait(savedQueued)
        assert.strictEqual(queuedResult.status, 'unanswered')
        assert.strictEqual(queuedResult.reason, 'withdrawn')

        const restoredConversation = Option.getOrThrow(
          yield* reopened.conversation(conversation.id),
        )
        yield* Deferred.succeed(release, undefined)
        assert.strictEqual((yield* Submission.wait(savedActive)).status, 'done')
        assert.strictEqual(yield* Submission.withdraw(savedActive), 'settled')
        const repeated = yield* Conversation.submit(restoredConversation, draft)
        assert.strictEqual(repeated.id, active.id)
        const repeatedQueued = yield* Conversation.submit(restoredConversation, {
          type: 'input',
          content: 'queued',
          requestId: 'saved-queued',
        })
        assert.strictEqual(repeatedQueued.id, queued.id)
        assert.deepEqual(yield* Submission.wait(repeatedQueued), queuedResult)
        yield* reopened.waitForIdle
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
)
