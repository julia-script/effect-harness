/** Real native chunks publish committed progress and one terminal response. */
import * as Console from 'effect/Console'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Stream from 'effect/Stream'
import * as LanguageModel from 'effect/ai/LanguageModel'
import type * as Response from 'effect/ai/Response'
import * as HarnessRuntime from 'effect-harness/HarnessRuntime'
import * as LiveState from 'effect-harness/LiveState'
import * as Session from 'effect-harness/Session'
import * as Storage from 'effect-harness/Storage'

const program = Effect.scoped(
  Effect.gen(function* () {
    const entered = yield* Deferred.make<void>()
    const parts: Response.StreamPartEncoded[] = [
      { type: 'text-start', id: 'answer' },
      { type: 'text-delta', id: 'answer', delta: 'Committed progress' },
      { type: 'text-end', id: 'answer' },
      {
        type: 'finish',
        reason: 'stop',
        usage: {
          inputTokens: { total: 1, uncached: 1, cacheRead: undefined, cacheWrite: undefined },
          outputTokens: { total: 2, text: 2, reasoning: undefined },
        },
        response: undefined,
      },
    ]
    const model = yield* LanguageModel.make({
      generateText: () => Effect.die('This example requires native streaming'),
      streamText: () =>
        Stream.concat(
          Stream.fromIterable(parts.slice(0, 2)),
          Stream.unwrap(
            Deferred.succeed(entered, undefined).pipe(
              Effect.andThen(Effect.sleep('150 millis')),
              Effect.as(Stream.fromIterable(parts.slice(2))),
            ),
          ),
        ),
    })
    const runtime = yield* HarnessRuntime.make().pipe(
      Effect.provideService(LanguageModel.LanguageModel, model),
    )
    const conversationId = yield* runtime.backend.root
    const job = yield* runtime.backend.submit({
      conversationId,
      draft: { type: 'input', content: 'Stream a reply.' },
    })
    yield* Deferred.await(entered)
    const progress = yield* Session.watch(
      runtime.session,
      LiveState.document,
      LiveState.target(conversationId),
    ).pipe(
      Stream.filter((snapshot) => snapshot.value.generation?.partial !== undefined),
      Stream.take(1),
      Stream.runHead,
    )
    yield* Console.log(progress)
    yield* Console.log((yield* runtime.backend.wait(job.id)).status)
    yield* Console.log(yield* Session.usage(runtime.session, conversationId))
  }),
).pipe(Effect.provide(Storage.layerMemory))

await Effect.runPromise(program)
