/** Manual compaction returns a durable data handle; queued placement has its own submission. */
import * as Console from 'effect/Console'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as LanguageModel from 'effect/ai/LanguageModel'
import * as Response from 'effect/ai/Response'
import * as CompactionTask from 'effect-harness/CompactionTask'
import * as Conversation from 'effect-harness/Conversation'
import * as Harness from 'effect-harness/Harness'
import { HarnessBackend } from 'effect-harness/HarnessBackend'
import * as HarnessRuntime from 'effect-harness/HarnessRuntime'
import * as Model from 'effect-harness/Model'
import * as Settings from 'effect-harness/Settings'
import * as Storage from 'effect-harness/Storage'
import * as Submission from 'effect-harness/Submission'
import { CompactionRequest } from 'effect-harness/CompactionRequest'
import { CompactionError } from 'effect-harness/CompactionError'

const program = Effect.scoped(
  Effect.gen(function* () {
    // Real provider adapters consume CompactionRequest at the captured client boundary.
    // This offline model verifies that the durable summary invocation receives its controls.
    const native = yield* LanguageModel.make({
      generateText: () =>
        Effect.gen(function* () {
          const request = Option.getOrThrow(yield* Effect.serviceOption(CompactionRequest))
          yield* Console.log(`Summary budget: ${request.maxOutputTokens}`)
          return [
            Response.makePart('text', {
              text: 'Earlier work established the API and kept exact paths.',
            }),
            Response.makePart('finish', {
              reason: 'stop',
              usage: {
                inputTokens: {
                  total: undefined,
                  uncached: undefined,
                  cacheRead: undefined,
                  cacheWrite: undefined,
                },
                outputTokens: { total: undefined, text: undefined, reasoning: undefined },
              },
              response: undefined,
            }),
          ]
        }),
      streamText: () => Stream.die('This example only runs manual summary work'),
    })
    const compaction = yield* Model.makeCompaction({
      binding: { id: 'example/summary', version: 1 },
      configure: (context) => Effect.succeed(context),
    })
    const model = Model.make({
      definition: {
        ref: { provider: 'example', modelId: 'summary' },
        capabilities: { tools: false, images: false, reasoning: false, structuredOutput: false },
      },
      options: Schema.Struct({}),
      languageModel: native,
      compaction,
      configure: () => Effect.succeed(Context.empty()),
    })
    const settings = yield* Settings.make({
      compaction: { enabled: false, reserveTokens: 20, keepRecentTokens: 10 },
    })
    const runtime = yield* HarnessRuntime.make({ models: [model] }).pipe(
      Effect.provideService(Settings.Settings, settings),
    )
    const client = yield* Harness.make.pipe(Effect.provideService(HarnessBackend, runtime.backend))
    const conversation = yield* client.root
    for (const text of ['Earlier work '.repeat(30), 'Recent context '.repeat(10)]) {
      const write = yield* Conversation.submit(conversation, {
        type: 'write',
        entry: { kind: 'input', model: [{ role: 'user', content: text }] },
      })
      yield* Submission.wait(write)
    }
    const handle = yield* Conversation.compact(conversation, {
      instructions: 'Keep exact file paths.',
    })
    const outcome = yield* CompactionTask.wait(handle).pipe(
      Effect.provideService(HarnessBackend, runtime.backend),
    )
    yield* Console.log(outcome.status)
    if (outcome.status === 'completed' && outcome.result._tag === 'Queued') {
      const placement = yield* client.submission(outcome.result.submissionId)
      yield* Console.log((yield* Submission.wait(placement)).status)
    } else
      return yield* new CompactionError({
        reason: 'invalidSummary',
        message: 'Example summary did not reach queued placement',
      })
  }),
).pipe(Effect.provide(Storage.layerMemory))

await Effect.runPromise(program)
