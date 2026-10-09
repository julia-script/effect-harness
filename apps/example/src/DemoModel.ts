import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Stream from 'effect/Stream'
import * as LanguageModel from 'effect/ai/LanguageModel'
import type * as Response from 'effect/ai/Response'

const finish = (reason: Response.FinishReason): Response.FinishPartEncoded => ({
  type: 'finish',
  reason,
  usage: {
    inputTokens: { total: 1, uncached: 1, cacheRead: undefined, cacheWrite: undefined },
    outputTokens: { total: 1, text: 1, reasoning: undefined },
  },
  response: undefined,
})

/** Offline native provider: asks for uppercase, then answers with its committed result. */
export const layer = Layer.effect(
  LanguageModel.LanguageModel,
  LanguageModel.make({
    generateText: ({ prompt, tools }) => {
      const lastUser = prompt.content.findLastIndex((message) => message.role === 'user')
      const result = prompt.content
        .slice(lastUser + 1)
        .flatMap((message) => (message.role === 'tool' ? message.content : []))
        .find((part) => part.type === 'tool-result')
      if (result !== undefined) {
        const text = result.isFailure ? 'The tool was interrupted.' : String(result.result)
        return Effect.succeed([{ type: 'text', text }, finish('stop')])
      }
      const input = prompt.content[lastUser]
      const text =
        input?.role === 'user'
          ? input.content
              .filter((part) => part.type === 'text')
              .map((part) => part.text)
              .join('\n')
          : 'hello'
      if (!tools.some((tool) => tool.name === 'uppercase'))
        return Effect.succeed([{ type: 'text', text }, finish('stop')])
      return Effect.succeed([
        {
          type: 'tool-call',
          id: `uppercase-${prompt.content.length}`,
          name: 'uppercase',
          params: { text },
          providerExecuted: false,
        },
        finish('tool-calls'),
      ])
    },
    streamText: () => Stream.empty,
  }),
)
