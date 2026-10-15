import { assert, it } from '@effect/vitest'
import * as AnthropicClient from '@effect/ai-anthropic/AnthropicClient'
import * as Effect from 'effect/Effect'
import type * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as HttpClient from 'effect/http/HttpClient'
import * as HttpClientResponse from 'effect/http/HttpClientResponse'
import * as Anthropic from '../../src/provider-anthropic/AnthropicLanguageModel.js'
import * as Claude from '../../src/provider-anthropic/OAuthLanguageModel.js'
import * as Store from '../../src/provider-auth/CredentialStore.js'
import {
  binding,
  credentials,
  claudeResponse,
  claudeEvents,
  payload,
  sse,
} from '../fixtures/SubscriptionProtocol.js'

const cases = [
  { name: 'unspecified thinking', thinking: undefined, display: undefined, expected: undefined },
  {
    name: 'disabled thinking',
    thinking: { type: 'disabled' },
    display: undefined,
    expected: { type: 'disabled' },
  },
  {
    name: 'adaptive thinking',
    thinking: { type: 'adaptive' },
    display: undefined,
    expected: { type: 'adaptive', display: 'summarized' },
  },
  {
    name: 'budgeted thinking',
    thinking: { type: 'enabled', budget_tokens: 1024 },
    display: undefined,
    expected: { type: 'enabled', budget_tokens: 1024, display: 'summarized' },
  },
  {
    name: 'explicit omission',
    thinking: { type: 'adaptive' },
    display: 'omitted',
    expected: { type: 'adaptive', display: 'omitted' },
  },
] as const

for (const transport of ['oauth', 'api-key'] as const) {
  for (const scenario of cases) {
    it.effect(`${transport} sends the thinking display contract for ${scenario.name}`, () =>
      Effect.gen(function* () {
        const bodies: Schema.JsonObject[] = []
        const http = HttpClient.make((request) =>
          Effect.sync(() => {
            const body = payload(request)
            bodies.push(body)
            return HttpClientResponse.fromWeb(
              request,
              new Response(body.stream ? sse(claudeEvents) : JSON.stringify(claudeResponse)),
            )
          }),
        )
        const store = yield* credentials('anthropic')
        const model = yield* (
          transport === 'oauth'
            ? Claude.make({
                model: 'claude-sonnet-4-6',
                binding: binding,
                thinkingModes: ['disabled', 'enabled', 'adaptive'],
                config: {
                  ...(scenario.thinking === undefined ? {} : { thinking: scenario.thinking }),
                  ...(scenario.display === undefined ? {} : { thinkingDisplay: scenario.display }),
                },
              }).pipe(Effect.provideService(Store.CredentialStore, store))
            : Anthropic.make({
                model: 'claude-sonnet-4-6',
                config:
                  scenario.thinking === undefined
                    ? {}
                    : {
                        thinking: {
                          ...scenario.thinking,
                          ...(scenario.display === undefined ? {} : { display: scenario.display }),
                        },
                      },
              }).pipe(Effect.provide(AnthropicClient.layer({})))
        ).pipe(Effect.provideService(HttpClient.HttpClient, http))
        yield* model.generateText({ prompt: 'hello' })
        yield* model.streamText({ prompt: 'hello' }).pipe(Stream.runDrain)
        assert.lengthOf(bodies, 2)
        for (const body of bodies) assert.deepEqual(body.thinking, scenario.expected)
      }),
    )
  }
}
