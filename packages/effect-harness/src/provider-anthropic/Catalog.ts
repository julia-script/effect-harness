/** Claude metadata for OAuth and API-key discovery. */
import * as Effect from 'effect/Effect'
import * as Redacted from 'effect/Redacted'
import * as Schema from 'effect/Schema'
import * as HttpClient from 'effect/http/HttpClient'
import * as HttpClientRequest from 'effect/http/HttpClientRequest'
import * as ModelInfo from '../ModelInfo.js'
import type * as ModelCatalog from '../ModelCatalog.js'
import { ModelCatalogError } from '../ModelCatalogError.js'
import * as Http from '../internal/SubscriptionHttp.js'
import * as Native from './OAuthLanguageModel.js'
import * as OAuth from './OAuth.js'
import * as Resolver from '../provider-auth/Resolver.js'
import { toAiError } from '../provider-auth/AuthError.js'

const Support = Schema.Struct({ supported: Schema.Boolean })
const Capabilities = Schema.Struct({
  image_input: Schema.optionalKey(Support),
  structured_outputs: Schema.optionalKey(Support),
  thinking: Schema.optionalKey(
    Schema.Struct({
      supported: Schema.Boolean,
      types: Schema.Struct({
        disabled: Schema.optionalKey(Support),
        enabled: Schema.optionalKey(Support),
        adaptive: Schema.optionalKey(Support),
      }),
    }),
  ),
  effort: Schema.optionalKey(
    Schema.Struct({
      supported: Schema.Boolean,
      low: Schema.optionalKey(Support),
      medium: Schema.optionalKey(Support),
      high: Schema.optionalKey(Support),
      xhigh: Schema.optionalKey(Support),
      max: Schema.optionalKey(Support),
    }),
  ),
})
const Entry = Schema.Struct({
  id: Schema.NonEmptyString,
  display_name: Schema.NonEmptyString,
  capabilities: Schema.optionalKey(Schema.NullOr(Capabilities)),
  max_input_tokens: Schema.optionalKey(Schema.NullOr(Schema.Int.check(Schema.isGreaterThan(0)))),
  max_tokens: Schema.optionalKey(Schema.NullOr(Schema.Int.check(Schema.isGreaterThan(0)))),
})
const Page = Schema.Struct({
  data: Schema.Array(
    Schema.Struct({ id: Schema.NonEmptyString, display_name: Schema.NonEmptyString }),
  ),
  has_more: Schema.Boolean,
  last_id: Schema.NullOr(Schema.String),
})

export interface ApiKeyOptions {
  readonly apiKey: Redacted.Redacted<string>
  /** API origin, default https://api.anthropic.com. */
  readonly apiUrl?: string
  readonly apiVersion?: string
}

const request = Effect.fnUntraced(function* (
  options: Pick<ApiKeyOptions, 'apiUrl' | 'apiVersion'>,
  headers: Readonly<Record<string, string>>,
  path: string,
  input?: ModelCatalog.ListOptions,
) {
  if (input?.limit !== undefined && input.limit > 1000)
    return yield* new ModelCatalogError({
      reason: 'unsupportedOptions',
      provider: 'anthropic',
      message: 'Claude model pages are limited to 1000 entries',
    })
  const client = yield* HttpClient.HttpClient
  let request = HttpClientRequest.get(
    `${(options.apiUrl ?? 'https://api.anthropic.com').replace(/\/$/, '')}/v1${path}`,
  ).pipe(
    HttpClientRequest.setHeaders({
      ...headers,
      'anthropic-version': options.apiVersion ?? '2023-06-01',
    }),
  )
  if (input?.cursor !== undefined)
    request = HttpClientRequest.setUrlParam(request, 'after_id', input.cursor)
  if (input?.limit !== undefined)
    request = HttpClientRequest.setUrlParam(request, 'limit', String(input.limit))
  const response = yield* Http.execute(client, request)
  yield* Http.checkStatus(response, 'anthropic')
  return yield* response.json.pipe(Effect.mapError(() => Http.malformed()))
})

const normalize = (entry: typeof Entry.Type, oauth: boolean) => {
  const capabilities = entry.capabilities
  const efforts = ['low', 'medium', 'high', 'xhigh', 'max'].filter(
    (effort) =>
      Schema.is(Native.Effort)(effort) &&
      capabilities?.effort?.supported &&
      capabilities.effort[effort]?.supported,
  )
  const modes = (['disabled', 'enabled', 'adaptive'] as const).filter(
    (mode) => capabilities?.thinking?.types[mode]?.supported,
  )
  return ModelInfo.decode({
    id: entry.id,
    name: entry.display_name,
    capabilities: {
      tools: true,
      ...(capabilities?.image_input === undefined
        ? {}
        : { images: capabilities.image_input.supported }),
      ...(capabilities?.thinking === undefined
        ? {}
        : { reasoning: capabilities.thinking.supported || efforts.length > 0 }),
      ...(oauth ? { structuredOutput: false } : {}),
      ...(!oauth && capabilities?.structured_outputs !== undefined
        ? { structuredOutput: capabilities.structured_outputs.supported }
        : {}),
    },
    ...(entry.max_input_tokens == null ? {} : { contextWindow: entry.max_input_tokens }),
    ...(entry.max_tokens == null ? {} : { maxOutputTokens: entry.max_tokens }),
    ...(efforts.length === 0 && modes.length === 0
      ? {}
      : {
          reasoning: {
            ...(efforts.length === 0 ? {} : { effort: { values: efforts } }),
            ...(modes.length === 0
              ? {}
              : {
                  thinking: {
                    modes,
                    ...(modes.includes('enabled')
                      ? {
                          budgetTokens: {
                            min: 1024,
                            ...(entry.max_tokens == null ? {} : { max: entry.max_tokens - 1 }),
                          },
                        }
                      : {}),
                  },
                }),
          },
        }),
  })
}

const source = <E, R>(
  options: Pick<ApiKeyOptions, 'apiUrl' | 'apiVersion'>,
  auth: Effect.Effect<Readonly<Record<string, string>>, E, R>,
  oauth: boolean,
) => ({
  list: Effect.fnUntraced(function* (input: ModelCatalog.ListOptions) {
    const page = yield* request(options, yield* auth, '/models', input).pipe(
      Effect.flatMap((value) =>
        Schema.decodeUnknownEffect(Page)(value).pipe(Effect.mapError(() => Http.malformed())),
      ),
    )
    if (page.has_more && !page.last_id) return yield* Http.malformed()
    const query = input.query?.toLowerCase() ?? ''
    return {
      items: page.data
        .filter((entry) => `${entry.id} ${entry.display_name}`.toLowerCase().includes(query))
        .map((entry) => ({ id: entry.id, name: entry.display_name })),
      ...(page.has_more && page.last_id !== null ? { nextCursor: page.last_id } : {}),
    }
  }),
  get: Effect.fnUntraced(function* (modelId: string) {
    const entry = yield* request(
      options,
      yield* auth,
      `/models/${encodeURIComponent(modelId)}`,
    ).pipe(
      Effect.flatMap((value) =>
        Schema.decodeUnknownEffect(Entry)(value).pipe(Effect.mapError(() => Http.malformed())),
      ),
    )
    return yield* normalize(entry, oauth)
  }),
})

/** API-key discovery uses one server page or direct lookup; queries filter that page only. */
export const apiKey = (options: ApiKeyOptions) =>
  source(
    options,
    Effect.sync(() => ({ 'x-api-key': Redacted.value(options.apiKey) })),
    false,
  )

export interface OAuthOptions extends Pick<
  Native.Options,
  | 'binding'
  | 'apiUrl'
  | 'apiVersion'
  | 'tokenUrl'
  | 'minimumValidityMs'
  | 'refreshTimeoutMs'
  | 'automaticRefresh'
> {}

/** Live subscription discovery using refreshed account credentials, with no eager IO. */
export const oauth = (options: OAuthOptions) =>
  source(
    options,
    Effect.gen(function* () {
      const refresh = yield* OAuth.makeRefresh(options)
      const auth = yield* Resolver.make({
        binding: options.binding,
        refresh: options.automaticRefresh === false ? undefined : refresh,
        ...(options.minimumValidityMs === undefined
          ? {}
          : { minimumValidityMs: options.minimumValidityMs }),
        ...(options.refreshTimeoutMs === undefined
          ? {}
          : { refreshTimeoutMs: options.refreshTimeoutMs }),
      }).pipe(Effect.mapError(toAiError))
      const credential = yield* auth.resolve.pipe(Effect.mapError(toAiError))
      return {
        authorization: `Bearer ${Redacted.value(credential.accessToken)}`,
        'anthropic-beta': 'oauth-2025-04-20',
      }
    }),
    true,
  )
