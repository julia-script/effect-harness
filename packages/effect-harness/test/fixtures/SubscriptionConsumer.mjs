// Runs against built package exports, with no test-runner aliases or live accounts.
import * as Effect from 'effect/Effect'
import * as Redacted from 'effect/Redacted'
import * as HttpClient from 'effect/http/HttpClient'
import * as HttpClientResponse from 'effect/http/HttpClientResponse'
import { CredentialStore, MemoryCredentialStore } from 'effect-harness/provider-auth'
import { CodexLanguageModel } from 'effect-harness/provider-openai-codex'
import { OAuthLanguageModel } from 'effect-harness/provider-anthropic'

const program = Effect.gen(function* () {
  const store = yield* MemoryCredentialStore.make
  const binding = { provider: 'openai-codex', profile: 'consumer', identity: 'login' }
  const token = `header.${btoa(JSON.stringify({ 'https://api.openai.com/auth': { chatgpt_account_id: 'consumer-account' } }))}.signature`
  yield* CredentialStore.save({
    type: 'oauth',
    binding,
    accessToken: Redacted.make(token),
    refreshToken: Redacted.make('refresh'),
    accountId: 'consumer-account',
    expiresAt: 9000000000000,
  }).pipe(Effect.provideService(CredentialStore.CredentialStore, store))
  const http = HttpClient.make((request) =>
    Effect.succeed(
      HttpClientResponse.fromWeb(
        request,
        new Response(
          `data: ${JSON.stringify({ type: 'response.completed', response: { id: 'consumer', model: 'gpt-5.4', created_at: 0, output: [{ type: 'message', id: 'message', role: 'assistant', status: 'completed', content: [{ type: 'output_text', text: 'packaged consumer works', annotations: [], logprobs: [] }] }] } })}\n\n`,
        ),
      ),
    ),
  )
  const model = yield* CodexLanguageModel.make({ model: 'gpt-5.4', binding }).pipe(
    Effect.provideService(CredentialStore.CredentialStore, store),
    Effect.provideService(HttpClient.HttpClient, http),
  )
  const response = yield* model.generateText({ prompt: 'test' })
  if (response.text !== 'packaged consumer works' || typeof OAuthLanguageModel.make !== 'function')
    throw new Error('Published provider consumer failed')
  return response.text
})

console.log(await Effect.runPromise(program))
