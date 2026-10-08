import * as Model from 'effect-harness/Model'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Stream from 'effect/Stream'
import * as LanguageModel from 'effect/ai/LanguageModel'
import type * as Response from 'effect/ai/Response'

export const ref: Model.Descriptor['ref'] = { provider: 'example', modelId: 'deterministic' }

export const finish = (reason: Response.FinishReason): Response.FinishPartEncoded => ({
  type: 'finish',
  reason,
  usage: {
    inputTokens: { total: 10, uncached: 10, cacheRead: undefined, cacheWrite: undefined },
    outputTokens: { total: 5, text: 5, reasoning: undefined },
  },
  response: undefined,
})

// A local provider keeps this example runnable without credentials. It asks for
// a tool, then answers once the prompt contains the committed tool result.
export const provider: Parameters<typeof LanguageModel.make>[0] = {
  generateText: () => Effect.succeed([{ type: 'text', text: 'summary' }, finish('stop')]),
  streamText: ({ prompt }) => {
    const answered = prompt.content.some(
      (message) =>
        message.role === 'tool' &&
        message.content.some((part) => part.type === 'tool-result' && part.name === 'uppercase'),
    )
    return Stream.fromIterable<Response.StreamPartEncoded>(
      answered
        ? [
            { type: 'text-start', id: 'answer' },
            { type: 'text-delta', id: 'answer', delta: 'HELLO' },
            { type: 'text-end', id: 'answer' },
            finish('stop'),
          ]
        : [
            {
              type: 'tool-call',
              id: 'uppercase-call',
              name: 'uppercase',
              params: { text: 'hello' },
              providerExecuted: false,
            },
            finish('tool-calls'),
          ],
    )
  },
}

export const layer = Layer.effect(LanguageModel.LanguageModel, LanguageModel.make(provider))

// Any native LanguageModel can supply the catalogue; the application chooses
// its concrete provider when composing Layers.
export const layerCatalogue = Layer.unwrap(
  Effect.gen(function* () {
    const model = yield* LanguageModel.LanguageModel
    return Model.layer([
      {
        ref,
        model,
        contextWindow: 100000,
        maxOutputTokens: 1000,
        configure: () => Effect.succeed(Context.empty()),
      },
    ])
  }),
)
