/** Claude metadata for OAuth and API-key discovery. */
import * as Effect from 'effect/Effect'
import * as Redacted from 'effect/Redacted'
import * as Schema from 'effect/Schema'
import * as HttpClient from 'effect/http/HttpClient'
import * as HttpClientRequest from 'effect/http/HttpClientRequest'
import * as ModelInfo from '../ModelInfo.js'
import * as Provider from '../Provider.js'
import { ModelCatalogError } from '../ModelCatalogError.js'
import * as Http from '../internal/SubscriptionHttp.js'
import * as Native from './OAuthLanguageModel.js'

/**
 * OAuth-compatible inventory, verified 2026-10-14 against the model overview and effort guide:
 * https://platform.claude.com/docs/en/models/overview
 * https://platform.claude.com/docs/en/build-with-claude/effort
 * Adaptive-only controls reflect the transport's safe thinking-history policy.
 * These declarations do not assert subscription/account availability.
 */
export const models: ReadonlyArray<ModelInfo.ModelInfo> = (
  [
    ['claude-fable-5-1', 'Claude Fable 5.1', 'high'],
    ['claude-opus-5-5', 'Claude Opus 5.5', 'medium'],
    ['claude-sonnet-5-5', 'Claude Sonnet 5.5', 'high'],
    ['claude-haiku-5-5', 'Claude Haiku 5.5', 'medium'],
  ] as const
).map(([id, name, defaultEffort]) => ({
  id,
  name,
  contextWindow: 1_000_000,
  maxOutputTokens: 128_000,
  capabilities: { tools: true, reasoning: true, images: true, structuredOutput: false },
  reasoning: {
    effort: { values: ['low', 'medium', 'high', 'xhigh', 'max'], default: defaultEffort },
    thinking: { modes: ['adaptive'] },
  },
}))

/** Subscription discovery is local; it never sends OAuth tokens to the Models API. */
export const bundled = () => Provider.fromModels(models)

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
  /** API base, default https://api.anthropic.com/v1. */
  readonly apiUrl?: string
  readonly apiVersion?: string
}

const request = Effect.fnUntraced(function* (
  options: ApiKeyOptions,
  path: string,
  input?: Provider.ListOptions,
) {
  if (input?.limit !== undefined && input.limit > 1000)
    return yield* new ModelCatalogError({
      reason: 'unsupportedOptions',
      provider: 'anthropic',
      message: 'Claude model pages are limited to 1000 entries',
    })
  const client = yield* HttpClient.HttpClient
  let request = HttpClientRequest.get(
    `${(options.apiUrl ?? 'https://api.anthropic.com/v1').replace(/\/$/, '')}${path}`,
  ).pipe(
    HttpClientRequest.setHeaders({
      'x-api-key': Redacted.value(options.apiKey),
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

const normalize = (entry: typeof Entry.Type) => {
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
      ...(capabilities?.structured_outputs === undefined
        ? {}
        : { structuredOutput: capabilities.structured_outputs.supported }),
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

/** API-key discovery uses one server page or a direct GET /models/:id. Queries filter that page only. */
export const apiKey = (options: ApiKeyOptions) => ({
  list: Effect.fnUntraced(function* (input: Provider.ListOptions) {
    const page = yield* request(options, '/models', input).pipe(
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
    const entry = yield* request(options, `/models/${encodeURIComponent(modelId)}`).pipe(
      Effect.flatMap((value) =>
        Schema.decodeUnknownEffect(Entry)(value).pipe(Effect.mapError(() => Http.malformed())),
      ),
    )
    return yield* normalize(entry)
  }),
})
