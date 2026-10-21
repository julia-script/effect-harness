/** Fresh subscription credentials shared by inference and paginated discovery. */
import { AnthropicClient, AnthropicLanguageModel } from '@effect/ai-anthropic'
import { Effect, Redacted, Schema, Semaphore } from 'effect'
import { HttpClient, HttpClientRequest } from 'effect/http'
import * as CredentialStore from '../../../credentials/CredentialStore.ts'
import * as LanguageModelProvider from '../../../model/LanguageModelProvider.ts'
import type { Info, Listable, Listed, Request } from '../../../model/Model.ts'
import { ModelError, Rejected } from '../../../model/ModelError.ts'
import { getJson, listed } from '../../../internal/provider/listing.ts'
import type { Options } from '../../AnthropicOAuth.ts'
import { models as catalog } from '../catalog.ts'
import { config } from '../provider.ts'
import { AUTH_URL, refresh } from './auth.ts'
import * as transport from './transport.ts'

const Page = Schema.Struct({
  data: Schema.Array(
    Schema.Struct({
      id: Schema.NonEmptyString,
      display_name: Schema.NonEmptyString,
      max_input_tokens: Schema.optionalKey(
        Schema.NullOr(Schema.Finite.check(Schema.isGreaterThan(0))),
      ),
    }),
  ),
  has_more: Schema.Boolean,
  last_id: Schema.NullOr(Schema.String),
})
const rejected = (message: string) => new ModelError({ message, reason: new Rejected({}) })

const account = Effect.fnUntraced(
  function* (name: string, authUrl: string) {
    const http = yield* HttpClient.HttpClient
    const resolve = yield* CredentialStore.makeResolver({
      refresh: (_provider, stored) =>
        refresh(authUrl, stored).pipe(Effect.provideService(HttpClient.HttpClient, http)),
    })
    const stored = yield* resolve(name).pipe(
      Effect.mapError(() => rejected('Claude credentials could not be resolved')),
    )
    if (stored?._tag !== 'OAuth')
      return yield* rejected(`Sign in to Claude first: no ${name} OAuth tokens are stored`)
    return stored.access
  },
  Semaphore.withPermit(Semaphore.makeUnsafe(1)),
)

const authorize = (access: Redacted.Redacted, request: HttpClientRequest.HttpClientRequest) => {
  // Native streaming uses /v1/messages; the generated JSON endpoint includes ?beta=true.
  const streaming = request.url.endsWith('/v1/messages')
  return (streaming ? HttpClientRequest.setUrlParam(request, 'beta', 'true') : request).pipe(
    HttpClientRequest.setHeaders({
      authorization: `Bearer ${Redacted.value(access)}`,
      'anthropic-beta': [
        ...new Set([
          'claude-code-20250219',
          'oauth-2025-04-20',
          ...(request.headers['anthropic-beta']?.split(',').filter(Boolean) ?? []),
        ]),
      ].join(','),
      'user-agent': 'claude-cli/2.1.280',
      'x-app': 'cli',
      'anthropic-dangerous-direct-browser-access': 'true',
      accept: streaming ? 'text/event-stream' : 'application/json',
    }),
  )
}

const discover = Effect.fnUntraced(function* (
  name: string,
  apiUrl: string,
  served: ReadonlyArray<Info>,
  access: Redacted.Redacted,
) {
  const result: Array<Listed> = []
  const seen = new Set<string>()
  let after: string | undefined
  for (;;) {
    const page = yield* getJson(
      'The Claude subscription model list',
      HttpClientRequest.get(`${apiUrl}/v1/models`).pipe(
        HttpClientRequest.setUrlParams({
          limit: '1000',
          ...(after === undefined ? {} : { after_id: after }),
        }),
        HttpClientRequest.setHeaders({
          authorization: `Bearer ${Redacted.value(access)}`,
          'anthropic-version': '2023-06-01',
          'anthropic-beta': 'oauth-2025-04-20',
        }),
      ),
      Page,
    )
    for (const model of page.data)
      result.push({
        ...listed(name, served, catalog, { id: model.id, name: model.display_name }),
        ...(model.max_input_tokens === undefined || model.max_input_tokens === null
          ? {}
          : { contextWindow: model.max_input_tokens }),
      })
    if (!page.has_more) return result
    if (page.last_id === null || page.last_id === '' || seen.has(page.last_id))
      return yield* rejected('Claude returned an invalid model pagination cursor')
    seen.add(page.last_id)
    after = page.last_id
  }
})

const configure = (model: Info, request: Request, options: Options) => {
  const configured = config(model, request)
  if (options.adaptiveModels?.includes(model.id) !== true) return configured
  const level = request.options?.reasoning
  let effort: 'low' | 'medium' | 'high' = 'high'
  if (level === 'low' || level === 'medium') effort = level
  if (level === 'minimal') effort = 'low'
  return {
    strictJsonSchema: configured.strictJsonSchema,
    max_tokens: configured.max_tokens,
    thinking: { type: 'adaptive' as const },
    ...(level === undefined ? {} : { output_config: { effort } }),
  }
}

export const make = (
  options: Options,
): Listable<HttpClient.HttpClient | CredentialStore.CredentialStore> => {
  const name = options.name ?? 'anthropic-oauth'
  const apiUrl = (options.apiUrl ?? 'https://api.anthropic.com').replace(/\/$/u, '')
  const tokens = account(name, options.authUrl ?? AUTH_URL)
  const provider = LanguageModelProvider.make({
    name,
    api: 'anthropic-messages',
    models: options.models ?? catalog,
    languageModel: Effect.fnUntraced(function* (model, request) {
      const access = yield* tokens
      const native = yield* AnthropicClient.make({
        apiUrl,
        transformClient: HttpClient.mapRequest((httpRequest) => authorize(access, httpRequest)),
      })
      return yield* AnthropicLanguageModel.make({
        model: model.id,
        config: configure(model, request, options),
      }).pipe(
        Effect.provideService(AnthropicClient.AnthropicClient, transport.client(native, request)),
      )
    }),
  })
  const listModels = Effect.flatMap(tokens, (access) =>
    discover(name, apiUrl, provider.models, access),
  )
  return { ...provider, listModels }
}
