/** Authenticated Codex model discovery; the backend exposes a bulk catalog. */
import * as Cache from 'effect/Cache'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import type * as Duration from 'effect/Duration'
import * as Redacted from 'effect/Redacted'
import * as Schema from 'effect/Schema'
import * as HttpClient from 'effect/http/HttpClient'
import * as HttpClientRequest from 'effect/http/HttpClientRequest'
import * as ModelInfo from '../ModelInfo.js'
import * as ModelCatalog from '../ModelCatalog.js'
import { ModelCatalogError } from '../ModelCatalogError.js'
import * as Resolver from '../provider-auth/Resolver.js'
import { toAiError } from '../provider-auth/AuthError.js'
import * as AuthProtocol from '../internal/ProviderOAuth.js'
import * as Http from '../internal/SubscriptionHttp.js'
import * as OAuth from './OAuth.js'
import * as Native from './CodexLanguageModel.js'

export interface Options extends Pick<
  Native.Options,
  'binding' | 'authBaseUrl' | 'minimumValidityMs' | 'refreshTimeoutMs' | 'automaticRefresh'
> {
  /** Codex catalog compatibility version sent as client_version. */
  readonly clientVersion: string
  /** Full catalog endpoint; independent of the inference endpoint. */
  readonly catalogUrl?: string
}

const Entry = Schema.Struct({
  slug: Schema.NonEmptyString,
  display_name: Schema.NonEmptyString,
  visibility: Schema.optionalKey(Schema.String),
  supported_reasoning_levels: Schema.Array(Schema.Struct({ effort: Schema.NonEmptyString })),
  default_reasoning_level: Schema.optionalKey(Schema.NullOr(Schema.String)),
  input_modalities: Schema.optionalKey(Schema.Array(Schema.String)),
  context_window: Schema.optionalKey(Schema.NullOr(Schema.Int.check(Schema.isGreaterThan(0)))),
  max_context_window: Schema.optionalKey(Schema.NullOr(Schema.Int.check(Schema.isGreaterThan(0)))),
})
const Response = Schema.Struct({ models: Schema.Array(Entry) })

const fetch = Effect.fnUntraced(function* (options: Options) {
  if (options.clientVersion.length === 0)
    return yield* Http.invalid('Codex discovery requires an openai-codex binding and clientVersion')
  const client = yield* HttpClient.HttpClient
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
  const account = yield* AuthProtocol.accountId(credential.accessToken).pipe(
    Effect.mapError(toAiError),
  )
  if (!credential.accountId || account !== credential.accountId)
    return yield* Http.invalid('Codex discovery account does not match token routing metadata')
  const response = yield* Http.execute(
    client,
    HttpClientRequest.get(
      options.catalogUrl ?? 'https://chatgpt.com/backend-api/codex/models',
    ).pipe(
      HttpClientRequest.setUrlParam('client_version', options.clientVersion),
      HttpClientRequest.setHeaders({
        authorization: `Bearer ${Redacted.value(credential.accessToken)}`,
        'chatgpt-account-id': credential.accountId,
        originator: 'effect-harness',
        accept: 'application/json',
      }),
    ),
  )
  yield* Http.checkStatus(response, 'openai-codex')
  const result = yield* response.json.pipe(
    Effect.flatMap(Schema.decodeUnknownEffect(Response)),
    Effect.mapError(() => Http.malformed()),
  )
  return result.models
})

const normalize = (entry: typeof Entry.Type) => {
  const efforts = [
    ...new Set(
      entry.supported_reasoning_levels
        .map((level) => level.effort)
        .filter(Schema.is(Native.ReasoningEffort)),
    ),
  ]
  const contextWindow = entry.context_window ?? entry.max_context_window
  return ModelInfo.decode({
    id: entry.slug,
    name: entry.display_name,
    capabilities: {
      tools: true,
      reasoning: entry.supported_reasoning_levels.length > 0,
      structuredOutput: false,
      ...(entry.input_modalities === undefined
        ? {}
        : { images: entry.input_modalities.includes('image') }),
    },
    ...(contextWindow === undefined || contextWindow === null ? {} : { contextWindow }),
    ...(efforts.length === 0
      ? {}
      : {
          reasoning: {
            effort: {
              values: efforts,
              ...(entry.default_reasoning_level != null &&
              efforts.some((effort) => effort === entry.default_reasoning_level)
                ? { default: entry.default_reasoning_level }
                : {}),
            },
          },
        }),
  })
}

const source = <E, R>(snapshot: Effect.Effect<ReadonlyArray<typeof Entry.Type>, E, R>) => ({
  list: Effect.fnUntraced(function* (input: ModelCatalog.ListOptions) {
    const models = yield* snapshot
    return yield* ModelCatalog.fromModels(
      models
        .filter((entry) => entry.visibility === undefined || entry.visibility === 'list')
        .map((entry) => ({ id: entry.slug, name: entry.display_name })),
    ).list(input)
  }),
  get: Effect.fnUntraced(function* (modelId: string) {
    const models = yield* snapshot
    const entry = models.find((entry) => entry.slug === modelId)
    if (entry === undefined)
      return yield* new ModelCatalogError({
        reason: 'modelNotFound',
        modelId,
        provider: 'openai-codex',
        message: 'Model is absent from the Codex catalog',
      })
    return yield* normalize(entry)
  }),
})

/** No network or credential lookup runs until list/get is executed. */
export const make = (options: Options) => source(fetch(options))

/**
 * Bind discovery services without fetching, then share one successful bulk snapshot
 * across all pages and detail lookups. Create one source per account and endpoint.
 * Failures are not retained; invalidate clears the entire snapshot.
 */
export const cached = (options: Options, cache: { readonly timeToLive: Duration.Input }) =>
  Effect.gen(function* () {
    const context = yield* Effect.context<Effect.Services<ReturnType<typeof fetch>>>()
    const snapshots = yield* Cache.makeWith(
      (_key: void) => fetch(options).pipe(Effect.provideContext(context)),
      {
        capacity: 1,
        timeToLive: (exit) => (Exit.isSuccess(exit) ? cache.timeToLive : 0),
      },
    )
    return {
      ...source(Cache.get(snapshots, undefined)),
      invalidate: Cache.invalidateAll(snapshots),
    }
  })
