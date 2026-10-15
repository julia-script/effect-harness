import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as Tool from 'effect/ai/Tool'
import * as Toolkit from 'effect/ai/Toolkit'
import * as Stream from 'effect/Stream'
import * as Prompt from 'effect/ai/Prompt'
import * as HttpClient from 'effect/http/HttpClient'
import type * as HttpClientRequest from 'effect/http/HttpClientRequest'
import * as HttpClientResponse from 'effect/http/HttpClientResponse'
import * as Store from '../src/provider-auth/CredentialStore.js'
import * as Codex from '../src/provider-openai-codex/CodexLanguageModel.js'
import { ProviderAffinity } from '../src/ProviderAffinity.js'
import {
  codexToken,
  binding,
  codexEvents,
  codexResponse,
  sse,
  payload,
  credentials,
} from './fixtures/SubscriptionProtocol.js'

describe('Codex public LanguageModel protocol', () => {
  for (const terminalType of ['response.completed', 'response.done'] as const)
    it.effect(
      `Codex ${terminalType} completion and streaming share prompt, usage and affinity contracts`,
      () =>
        Effect.gen(function* () {
          const captured: HttpClientRequest.HttpClientRequest[] = []
          const signals: AbortSignal[] = []
          const store = yield* credentials('openai-codex')
          const http = HttpClient.make((request, _url, signal) =>
            Effect.sync(() => {
              signals.push(signal)
              captured.push(request)
              return HttpClientResponse.fromWeb(
                request,
                new Response(
                  sse(
                    codexEvents.map((event) =>
                      event.type === 'response.completed'
                        ? { ...event, type: terminalType }
                        : event,
                    ),
                  ),
                ),
              )
            }),
          )
          const model = yield* Codex.make({ model: 'gpt-5.4', binding }).pipe(
            Effect.provideService(Store.CredentialStore, store),
            Effect.provideService(HttpClient.HttpClient, http),
          )
          const prompt = Prompt.make([
            { role: 'system', content: 'Application instructions' },
            { role: 'user', content: 'Hello' },
          ])
          const completed = yield* model
            .generateText({ prompt })
            .pipe(Effect.provideService(ProviderAffinity, { id: 'conversation-1' }))
          const streamed = yield* model
            .streamText({ prompt })
            .pipe(
              Stream.runCollect,
              Effect.provideService(ProviderAffinity, { id: 'conversation-1' }),
            )
          assert.strictEqual(completed.text, 'Olá 🌍')
          assert.strictEqual(
            streamed
              .filter((part) => part.type === 'text-delta')
              .map((part) => part.delta)
              .join(''),
            completed.text,
          )
          assert.deepEqual(streamed.find((part) => part.type === 'finish')?.usage, completed.usage)
          assert.strictEqual(captured.length, 2)
          assert.isTrue(
            signals.every((signal) => signal.aborted),
            'completion and stream release their HTTP scopes',
          )
          for (const request of captured) {
            assert.strictEqual(request.url, 'https://chatgpt.com/backend-api/codex/responses')
            assert.strictEqual(request.headers.authorization, `Bearer ${codexToken}`)
            assert.strictEqual(request.headers['chatgpt-account-id'], 'account-1')
            assert.strictEqual(request.headers.accept, 'text/event-stream')
            assert.strictEqual(request.headers['session-id'], 'conversation-1')
            const body = payload(request)
            assert.strictEqual(body.stream, true)
            assert.strictEqual(body.store, false)
            assert.strictEqual(body.instructions, 'Application instructions')
            assert.strictEqual(body.prompt_cache_key, 'conversation-1')
            assert.deepEqual(body.include, ['reasoning.encrypted_content'])
            assert.isUndefined(body.max_output_tokens)
          }
          assert.notInclude(JSON.stringify(completed), 'SECRET')
          assert.notInclude(JSON.stringify(streamed), 'SECRET')
        }),
    )

  for (const failure of ['truncated', 'malformed', 'provider-error'] as const)
    it.effect(`Codex rejects ${failure} streams instead of returning a successful completion`, () =>
      Effect.gen(function* () {
        const store = yield* credentials('openai-codex')
        let events: ReadonlyArray<unknown> = codexEvents.slice(0, -1)
        if (failure === 'malformed')
          events = [{ type: 'response.completed', response: 'SECRET-invalid' }]
        if (failure === 'provider-error')
          events = [
            {
              type: 'response.failed',
              response: {
                ...codexResponse,
                error: { code: 'server_error', message: 'SECRET-server-body' },
              },
            },
          ]
        const model = yield* Codex.make({ model: 'gpt-5.4', binding }).pipe(
          Effect.provideService(Store.CredentialStore, store),
          Effect.provideService(
            HttpClient.HttpClient,
            HttpClient.make((request) =>
              Effect.succeed(HttpClientResponse.fromWeb(request, new Response(sse(events)))),
            ),
          ),
        )
        const error = yield* Effect.flip(model.generateText({ prompt: 'hello' }))
        assert.strictEqual(error._tag, 'AiError')
        assert.notInclude(JSON.stringify(error), 'SECRET')
      }),
    )
})

it.effect(
  'Codex preserves prepared dynamic tool schemas without imposing strict OpenAI semantics',
  () =>
    Effect.gen(function* () {
      const store = yield* credentials('openai-codex')
      const captured: HttpClientRequest.HttpClientRequest[] = []
      const model = yield* Codex.make({ model: 'gpt-6.1-sol', binding }).pipe(
        Effect.provideService(Store.CredentialStore, store),
        Effect.provideService(
          HttpClient.HttpClient,
          HttpClient.make((request) =>
            Effect.sync(() => {
              captured.push(request)
              return HttpClientResponse.fromWeb(request, new Response(sse(codexEvents)))
            }),
          ),
        ),
      )
      const result = yield* model.generateText({
        prompt: 'hello',
        toolkit: Toolkit.make(
          Tool.dynamic('read', {
            parameters: {
              type: 'object',
              properties: { path: { type: 'string' }, offset: { type: 'number' } },
              required: ['path'],
              additionalProperties: true,
            },
          }),
        ),
        disableToolCallResolution: true,
      })
      const body = payload(captured[0]!)
      const tools = yield* Schema.decodeUnknownEffect(
        Schema.Array(
          Schema.Struct({
            strict: Schema.Boolean,
            parameters: Schema.Struct({
              required: Schema.Array(Schema.String),
              additionalProperties: Schema.Boolean,
            }),
          }),
        ),
      )(body.tools)
      assert.strictEqual(
        tools[0]?.strict,
        false,
        'OpenAI rejects these ordinary schemas in strict mode',
      )
      assert.deepEqual(tools[0]?.parameters.required, ['path'])
      assert.strictEqual(tools[0]?.parameters.additionalProperties, true)
      assert.strictEqual(result.text, 'Olá 🌍')
    }),
)

it.effect('Codex classifies invalid function parameters as a non-retryable request error', () =>
  Effect.gen(function* () {
    const store = yield* credentials('openai-codex')
    const model = yield* Codex.make({ model: 'gpt-6.1-sol', binding }).pipe(
      Effect.provideService(Store.CredentialStore, store),
      Effect.provideService(
        HttpClient.HttpClient,
        HttpClient.make((request) =>
          Effect.succeed(
            HttpClientResponse.fromWeb(
              request,
              new Response(
                sse([
                  {
                    type: 'error',
                    code: 'invalid_function_parameters',
                    message: 'SECRET provider validation details',
                  },
                ]),
              ),
            ),
          ),
        ),
      ),
    )
    const error = yield* model.generateText({ prompt: 'hello' }).pipe(Effect.flip)
    assert.strictEqual(error.reason._tag, 'InvalidRequestError')
    assert.notInclude(JSON.stringify(error), 'SECRET')
  }),
)
