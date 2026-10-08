import { Model } from 'effect-harness'
import { Context, Effect, Layer, Stream } from 'effect'
import { LanguageModel, type Response } from 'effect/ai'

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

export const layer: Layer.Layer<LanguageModel.LanguageModel> = Layer.effect(
  LanguageModel.LanguageModel,
  LanguageModel.make(provider),
)

// Any native LanguageModel can supply the catalogue; the application chooses
// its concrete provider when composing Layers.
export const layerCatalogue: Layer.Layer<Model.Catalog, never, LanguageModel.LanguageModel> =
  Layer.unwrap(
    Effect.gen(function* () {
      const model = yield* LanguageModel.LanguageModel
      // effect-nit-allow B-no-layer-arguments: catalogue registration retains the already-acquired model instance and its caller-owned lifetime; the public registration contract supports heterogeneous provider/model descriptors without rebuilding or collapsing them.
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
