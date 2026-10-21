/**
 * The Codex provider: the Responses API at ChatGPT's backend, over `@effect/ai-openai` with the
 * stored OAuth access token, the account id header, `store: false`, and the system prompt as
 * `instructions`, as pi-ai sends them.
 */
import { OpenAiClient, OpenAiLanguageModel } from '@effect/ai-openai'
import { Effect, Redacted, Semaphore } from 'effect'
import { HttpClient, HttpClientRequest } from 'effect/http'
import * as CredentialStore from '../../../credentials/CredentialStore.ts'
import * as LanguageModelProvider from '../../../model/LanguageModelProvider.ts'
import type { Info, Listable, Request } from '../../../model/Model.ts'
import { ModelError, Rejected } from '../../../model/ModelError.ts'
import * as Transcript from '../../../model/Transcript.ts'
import type { Options } from '../../OpenAiCodex.ts'
import { config as responsesConfig } from '../provider.ts'
import { AUTH_URL, accountId, refresh } from './auth.ts'
import { models as catalog } from './catalog.ts'
import { list } from './models.ts'

const BASE_URL = 'https://chatgpt.com/backend-api'

const rejected = (message: string, cause?: unknown) =>
  new ModelError({ message, reason: new Rejected({}), ...(cause === undefined ? {} : { cause }) })

/** The request options Codex takes: no output limit, nothing stored, instructions required. */
const config = (model: Info, request: Request) => {
  const { max_output_tokens: _limit, ...rest } = responsesConfig(model, request, catalog)
  const sessionId = request.options?.sessionId
  return {
    ...rest,
    store: false,
    instructions: Transcript.systemPrompt(request.messages) || 'You are a helpful assistant.',
    include: ['reasoning.encrypted_content' as const],
    ...(sessionId === undefined ? {} : { prompt_cache_key: sessionId }),
  }
}

/** The fresh access token of the account stored under `name`, and the account id it carries. */
const account = (name: string, authUrl: string, line: Semaphore.Semaphore) =>
  Effect.gen(function* () {
    const http = yield* HttpClient.HttpClient
    const resolve = yield* CredentialStore.makeResolver({
      refresh: (_provider, stored) =>
        Effect.provideService(refresh(authUrl, stored), HttpClient.HttpClient, http),
    })
    const credential = yield* resolve(name).pipe(
      Semaphore.withPermit(line),
      Effect.mapError((cause) => rejected(`Codex credentials: ${cause.message}`, cause)),
    )
    if (credential?._tag !== 'OAuth') {
      return yield* rejected(`Sign in to Codex first: no ${name} tokens are stored`)
    }
    const access = Redacted.value(credential.access)
    const id = accountId(access)
    if (id === undefined) return yield* rejected('The Codex token has no account id')
    return { access, id }
  })

export const make = (
  options: Options,
): Listable<HttpClient.HttpClient | CredentialStore.CredentialStore> => {
  const name = options.name ?? 'openai-codex'
  const baseUrl = options.baseUrl ?? BASE_URL
  const originator = options.originator ?? 'effect-harness'
  // One refresh at a time for this provider, so concurrent requests near expiry refresh once.
  const tokens = account(name, options.authUrl ?? AUTH_URL, Semaphore.makeUnsafe(1))
  const provider = LanguageModelProvider.make({
    name,
    api: 'openai-codex-responses',
    models: options.models ?? catalog,
    system: 'separate',
    languageModel: (model, request) =>
      Effect.gen(function* () {
        const { access, id } = yield* tokens
        const sessionId = request.options?.sessionId
        const client = yield* OpenAiClient.make({
          apiKey: Redacted.make(access),
          apiUrl: `${baseUrl}/codex`,
          transformClient: HttpClient.mapRequest(
            HttpClientRequest.setHeaders({
              'chatgpt-account-id': id,
              'OpenAI-Beta': 'responses=experimental',
              originator,
              ...(sessionId === undefined ? {} : { 'session-id': sessionId }),
            }),
          ),
        })
        return yield* OpenAiLanguageModel.make({
          model: model.id,
          config: config(model, request),
        }).pipe(Effect.provideService(OpenAiClient.OpenAiClient, client))
      }),
  })
  const listModels = Effect.flatMap(tokens, (found) =>
    list({ baseUrl, originator, name, served: provider.models, ...found, ...options }),
  )
  return { ...provider, listModels }
}
