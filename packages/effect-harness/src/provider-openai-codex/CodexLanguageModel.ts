/** Codex SSE adapter retaining Effect AI prompt, tool, reasoning and response translation. */
import * as Native from '@effect/ai-openai/OpenAiLanguageModel'
import * as OpenAiClient from '@effect/ai-openai/OpenAiClient'
import * as OpenAiSchema from '@effect/ai-openai/OpenAiSchema'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Redacted from 'effect/Redacted'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as LanguageModel from 'effect/ai/LanguageModel'
import * as Sse from 'effect/encoding/Sse'
import * as HttpClient from 'effect/http/HttpClient'
import * as HttpClientRequest from 'effect/http/HttpClientRequest'
import * as ToolResult from '../provider-openai/ToolResult.js'
import type * as Credential from '../provider-auth/Credential.js'
import { toAiError } from '../provider-auth/AuthError.js'
import * as Resolver from '../provider-auth/Resolver.js'
import * as OAuth from './OAuth.js'
import { ProviderAffinity } from '../ProviderAffinity.js'
import * as Http from '../internal/SubscriptionHttp.js'
import * as AuthProtocol from '../internal/ProviderOAuth.js'

export const ReasoningEffort = Schema.Literals([
  'none',
  'minimal',
  'low',
  'medium',
  'high',
  'xhigh',
  'max',
])
export const Settings = Schema.Struct({
  reasoningEffort: Schema.optionalKey(ReasoningEffort),
  reasoningSummary: Schema.optionalKey(Schema.Literals(['auto', 'concise', 'detailed'])),
  verbosity: Schema.optionalKey(Schema.Literals(['low', 'medium', 'high'])),
  serviceTier: Schema.optionalKey(Schema.Literals(['auto', 'default', 'flex', 'priority'])),
  temperature: Schema.optionalKey(
    Schema.Finite.check(Schema.isBetween({ minimum: 0, maximum: 2 })),
  ),
  cache: Schema.optionalKey(Schema.Literals(['affinity', 'none'])),
  cacheKey: Schema.optionalKey(Schema.String),
})
export type Settings = typeof Settings.Type
export class Config extends Context.Service<Config, Settings>()(
  'effect-harness/provider-openai-codex/Config',
) {}
export interface Options extends OAuth.Endpoints {
  readonly model: string
  readonly binding: Credential.Binding
  readonly config?: Settings
  /** Full endpoint or backend base; defaults to ChatGPT's Codex Responses endpoint. */
  readonly apiUrl?: string
  readonly transport?: 'sse'
  readonly minimumValidityMs?: number
  readonly refreshTimeoutMs?: number
  readonly automaticRefresh?: boolean
  /** Host declarations from the selected model's verified compatibility metadata. */
  readonly reasoningEfforts?: ReadonlyArray<NonNullable<Settings['reasoningEffort']>>
  readonly supportsTemperature?: boolean
}
const endpoint = (base = 'https://chatgpt.com/backend-api') => {
  const url = base.replace(/\/$/, '')
  if (url.endsWith('/codex/responses')) return url
  return `${url}${url.endsWith('/codex') ? '' : '/codex'}/responses`
}

/** Provides only LanguageModel. Unsupported native compatibility methods stay private. */
export const make = Effect.fnUntraced(function* (options: Options) {
  if (
    options.binding.provider !== 'openai-codex' ||
    (options.transport !== undefined && options.transport !== 'sse')
  )
    return yield* Http.invalid('Codex requires openai-codex OAuth and SSE transport')
  const http = yield* HttpClient.HttpClient
  const refresh = yield* OAuth.makeRefresh(options)
  const auth = yield* Resolver.make({
    binding: options.binding,
    ...(options.minimumValidityMs === undefined
      ? {}
      : { minimumValidityMs: options.minimumValidityMs }),
    ...(options.refreshTimeoutMs === undefined
      ? {}
      : { refreshTimeoutMs: options.refreshTimeoutMs }),
    refresh: options.automaticRefresh === false ? undefined : refresh,
  }).pipe(Effect.mapError(toAiError))
  const streamRequest: OpenAiClient.Service['createResponseStream'] = (input) =>
    Effect.gen(function* () {
      const nativeConfig = yield* Effect.serviceOption(Native.Config)
      if (Option.isSome(nativeConfig) && Object.keys(nativeConfig.value).length > 0)
        return yield* Http.invalid(
          'Use Codex Config; ordinary OpenAI request overrides are unsupported',
        )
      const dynamic = yield* Effect.serviceOption(Config)
      const config = yield* Schema.decodeEffect(Settings, { onExcessProperty: 'error' })(
        Option.getOrElse(dynamic, () => options.config ?? {}),
      ).pipe(Effect.mapError(() => Http.invalid('Unsupported Codex settings')))
      if (
        config.reasoningEffort !== undefined &&
        !options.reasoningEfforts?.includes(config.reasoningEffort)
      )
        return yield* Http.invalid('Reasoning effort is not declared supported for this model')
      if (config.temperature !== undefined && options.supportsTemperature !== true)
        return yield* Http.invalid('Temperature is not declared supported for this model')
      const allowed = new Set([
        'model',
        'input',
        'include',
        'text',
        'tools',
        'tool_choice',
        'store',
        'useItemReferences',
      ])
      for (const [key, value] of Object.entries(input))
        if (value !== undefined && !allowed.has(key))
          return yield* Http.invalid(`Unsupported Codex request option: ${key}`)
      if (input.model !== options.model || input.store === true)
        return yield* Http.invalid('Codex model and storage policy are fixed')
      if (input.text?.format?.type !== undefined && input.text.format.type !== 'text')
        return yield* Http.invalid('Codex structured final output is unsupported')
      if (typeof input.tool_choice === 'object')
        return yield* Http.invalid('Codex named tool choice is unsupported')
      if (input.tools?.some((tool) => tool.type !== 'function'))
        return yield* Http.invalid('Codex supports application function tools only')
      const translated = yield* ToolResult.request(input)
      const affinity = yield* Effect.serviceOption(ProviderAffinity)
      const cacheKey =
        config.cache === 'none'
          ? undefined
          : (config.cacheKey ?? Option.getOrUndefined(affinity)?.id)
      const messages =
        typeof translated.input === 'string'
          ? [{ role: 'user' as const, content: translated.input }]
          : (translated.input ?? [])
      const instructions: string[] = []
      const history = messages.filter((item) => {
        if ('role' in item && (item.role === 'system' || item.role === 'developer')) {
          if (typeof item.content === 'string') instructions.push(item.content)
          else
            for (const part of item.content)
              if (part.type === 'input_text') instructions.push(part.text)
          return false
        }
        return true
      })
      const credential = yield* auth.resolve.pipe(Effect.mapError(toAiError))
      if (!credential.accountId)
        return yield* Http.invalid('Codex credential has no validated account identifier')
      const tokenAccount = yield* AuthProtocol.accountId(credential.accessToken).pipe(
        Effect.mapError(toAiError),
      )
      if (tokenAccount !== credential.accountId)
        return yield* Http.invalid('Codex credential account does not match token routing metadata')
      const request = HttpClientRequest.post(endpoint(options.apiUrl)).pipe(
        HttpClientRequest.setHeaders({
          authorization: `Bearer ${Redacted.value(credential.accessToken)}`,
          'chatgpt-account-id': credential.accountId,
          'openai-beta': 'responses=experimental',
          accept: 'text/event-stream',
          originator: 'effect-harness',
          ...(cacheKey ? { 'session-id': cacheKey, 'x-client-request-id': cacheKey } : {}),
        }),
        HttpClientRequest.bodyJsonUnsafe({
          model: options.model,
          store: false,
          stream: true,
          instructions: instructions.join('\n\n') || 'You are a helpful assistant.',
          input: history,
          include: ['reasoning.encrypted_content'],
          text: { verbosity: config.verbosity ?? 'low' },
          tool_choice: translated.tool_choice ?? 'auto',
          parallel_tool_calls: true,
          ...(translated.tools === undefined ? {} : { tools: translated.tools }),
          ...(cacheKey === undefined ? {} : { prompt_cache_key: cacheKey }),
          ...(config.reasoningEffort === undefined && config.reasoningSummary === undefined
            ? {}
            : {
                reasoning: {
                  effort: config.reasoningEffort,
                  summary: config.reasoningSummary ?? 'auto',
                },
              }),
          ...(config.temperature === undefined ? {} : { temperature: config.temperature }),
          ...(config.serviceTier === undefined ? {} : { service_tier: config.serviceTier }),
        }),
      )
      const response = yield* Http.execute(http, request)
      yield* Http.checkStatus(response, 'openai-codex')
      const stream = Stream.suspend(() => {
        let terminal = false
        return response.stream.pipe(
          Stream.decodeText(),
          Stream.pipeThroughChannel(
            Sse.decodeDataSchema(Schema.Record(Schema.String, Schema.Unknown)),
          ),
          Stream.map((event) => event.data),
          Stream.mapError(Http.malformed),
          Stream.mapEffect((event) => {
            if (event.type === 'error') {
              const failure = Schema.decodeUnknownOption(Schema.Struct({ code: Schema.String }))(
                typeof event.code === 'string' ? event : event.error,
              )
              return Effect.fail(
                Http.providerFailure(
                  Option.isSome(failure) ? failure.value.code : undefined,
                  'openai-codex',
                ),
              )
            }
            if (event.type === 'response.failed') {
              const failure = Schema.decodeUnknownOption(
                Schema.Struct({
                  response: Schema.Struct({ error: Schema.Struct({ code: Schema.String }) }),
                }),
              )(event)
              return Effect.fail(
                Http.providerFailure(
                  Option.isSome(failure) ? failure.value.response.error.code : undefined,
                  'openai-codex',
                ),
              )
            }
            if (event.type === 'response.completed' || event.type === 'response.done') {
              const terminal = Schema.decodeUnknownOption(
                Schema.Struct({
                  response: Schema.Struct({
                    status: Schema.optionalKey(Schema.String),
                    error: Schema.optionalKey(Schema.NullOr(Schema.Unknown)),
                    incomplete_details: Schema.optionalKey(Schema.NullOr(Schema.Unknown)),
                  }),
                }),
              )(event)
              if (
                Option.isNone(terminal) ||
                (terminal.value.response.status !== undefined &&
                  terminal.value.response.status !== 'completed') ||
                terminal.value.response.error != null ||
                terminal.value.response.incomplete_details != null
              )
                return Effect.fail(Http.malformed())
            }
            return Schema.decodeUnknownEffect(OpenAiSchema.ResponseStreamEvent)(
              event.type === 'response.done' ? { ...event, type: 'response.completed' } : event,
            ).pipe(Effect.mapError(Http.malformed))
          }),
          Stream.map((event) => {
            if (event.type === 'response.completed' || event.type === 'response.incomplete')
              terminal = true
            return event
          }),
          Stream.takeUntil(
            (event) => event.type === 'response.completed' || event.type === 'response.incomplete',
          ),
          Stream.concat(
            Stream.fromEffect(
              Effect.suspend(() => (terminal ? Effect.void : Effect.fail(Http.malformed()))),
            ).pipe(Stream.drain),
          ),
        )
      })
      return [Http.safeResponse(response), stream] as const
    })
  const client = OpenAiClient.OpenAiClient.of({
    client: HttpClient.make(() => Effect.die('Codex low-level HTTP is not exposed')),
    createEmbedding: () => Effect.fail(Http.invalid('Codex embeddings are unsupported')),
    createResponseStream: streamRequest,
    createResponse: (input) =>
      Effect.gen(function* () {
        const [response, stream] = yield* streamRequest(input)
        const terminal = yield* stream.pipe(
          Stream.filter(
            (event) => event.type === 'response.completed' || event.type === 'response.incomplete',
          ),
          Stream.runLast,
        )
        if (Option.isNone(terminal)) return yield* Http.malformed()
        const event = yield* Schema.decodeUnknownEffect(
          Schema.Struct({ response: OpenAiSchema.Response }),
        )(terminal.value).pipe(Effect.mapError(Http.malformed))
        return [event.response, response] as const
      }).pipe(Effect.scoped),
  })
  return yield* Native.make({
    model: options.model,
    config: { store: false, useItemReferences: false },
  }).pipe(Effect.provideService(OpenAiClient.OpenAiClient, client))
})

export const layer = (options: Options) => Layer.effect(LanguageModel.LanguageModel, make(options))
