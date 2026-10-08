/** Two conversations run concurrently while sharing an immutable historical prefix. */
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Fiber from 'effect/Fiber'
import * as Ref from 'effect/Ref'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as Conversation from 'effect-harness/Conversation'
import * as Runtime from './Runtime.ts'

export const Result = Schema.Struct({
  channel: Conversation.Id,
  thread: Conversation.Id,
  inheritedAnswer: Conversation.EntryId,
  concurrentRequests: Schema.Int,
  answers: Schema.Array(Schema.String),
})

export const run = Effect.scoped(
  Effect.gen(function* () {
    const entered = yield* Deferred.make<void>()
    const release = yield* Deferred.make<void>()
    const concurrent = yield* Ref.make(0)
    const provider: Runtime.Provider = {
      generateText: () =>
        Effect.succeed([{ type: 'text', text: 'summary' }, Runtime.finish('stop')]),
      streamText: ({ prompt }) =>
        Stream.unwrap(
          Effect.gen(function* () {
            const text = Runtime.lastUserText(prompt)
            if (text !== 'Why did the deployment fail?') {
              if ((yield* Ref.updateAndGet(concurrent, (count) => count + 1)) === 2)
                yield* Deferred.succeed(entered, undefined)
              yield* Deferred.await(release)
            }
            return Runtime.answer(`Reply: ${text}`)
          }),
        ),
    }
    const { harness } = yield* Runtime.open({ provider })
    const channel = yield* harness.root
    const initial = yield* Runtime.ask(channel, 'Why did the deployment fail?')
    const thread = yield* Conversation.fork(channel, initial.submission.answer)
    yield* Conversation.configure(thread, { instructions: 'Review rollback options.' })
    const work = yield* Effect.all(
      [Runtime.ask(thread, 'Can we roll it back?'), Runtime.ask(channel, 'Who is on call?')],
      { concurrency: 'unbounded' },
    ).pipe(Effect.forkChild)
    // Both requests must have reached the model before either can finish.
    yield* Deferred.await(entered)
    yield* Deferred.succeed(release, undefined)
    const answers = yield* Fiber.join(work)
    const snapshot = yield* Conversation.snapshot(thread)
    yield* Runtime.check(
      snapshot.entries.some((entry) => entry.id === initial.submission.answer),
      'The fork lost its historical prefix',
    )
    yield* Runtime.check(
      snapshot.conversation.parent?.conversationId === channel.id,
      'The fork is not linked to its parent',
    )
    return yield* Schema.decodeEffect(Result)({
      channel: channel.id,
      thread: thread.id,
      inheritedAnswer: initial.submission.answer,
      concurrentRequests: yield* Ref.get(concurrent),
      answers: answers.map((answer) => answer.text),
    })
  }),
)
