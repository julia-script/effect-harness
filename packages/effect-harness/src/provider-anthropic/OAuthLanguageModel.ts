/** Claude OAuth Messages transport using native Effect AI translation. */
import * as Native from '@effect/ai-anthropic/AnthropicLanguageModel'
import * as AnthropicClient from '@effect/ai-anthropic/AnthropicClient'
import * as Generated from '@effect/ai-anthropic/Generated'
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
import type * as Credential from '../provider-auth/Credential.js'
import { toAiError } from '../provider-auth/AuthError.js'
import * as Resolver from '../provider-auth/Resolver.js'
import * as OAuth from './OAuth.js'
import * as ThinkingPolicy from './ThinkingPolicy.js'
import * as ToolResult from './ToolResult.js'
import * as Http from '../internal/SubscriptionHttp.js'

export const Effort = Schema.Literals(['low', 'medium', 'high', 'xhigh', 'max'])
export const Settings = Schema.Struct({
  maxTokens: Schema.optionalKey(Schema.Int.check(Schema.isGreaterThan(0))),
  thinking: Schema.optionalKey(
    Schema.Union([
      Schema.Struct({ type: Schema.Literal('disabled') }),
      Schema.Struct({
        type: Schema.Literal('enabled'),
        budget_tokens: Schema.Int.check(Schema.isGreaterThanOrEqualTo(1024)),
      }),
      Schema.Struct({ type: Schema.Literal('adaptive') }),
    ]),
  ),
  effort: Schema.optionalKey(Effort),
  thinkingDisplay: Schema.optionalKey(Schema.Literals(['summarized', 'omitted'])),
  temperature: Schema.optionalKey(
    Schema.Finite.check(Schema.isBetween({ minimum: 0, maximum: 1 })),
  ),
  cache: Schema.optionalKey(Schema.Literals(['none', 'short', 'long'])),
  userId: Schema.optionalKey(Schema.String),
})
export type Settings = typeof Settings.Type
export class Config extends Context.Service<Config, Settings>()(
  'effect-harness/provider-anthropic/OAuthConfig',
) {}
export interface Options extends OAuth.Endpoints {
  readonly model: string
  readonly binding: Credential.Binding
  readonly apiUrl?: string
  readonly apiVersion?: string
  readonly config?: Settings
  readonly minimumValidityMs?: number
  readonly refreshTimeoutMs?: number
  readonly automaticRefresh?: boolean
  readonly transport?: 'sse'
  /** Replay compatibility override; omitted values use the known model defaults. */
  readonly thinkingPolicy?: ThinkingPolicy.Policy
  readonly thinkingModes?: ReadonlyArray<'disabled' | 'enabled' | 'adaptive'>
  readonly efforts?: ReadonlyArray<NonNullable<Settings['effort']>>
  readonly supportsLongCache?: boolean
  readonly supportsTemperature?: boolean
}

const canonicalNames = new Map(
  [
    'Read',
    'Write',
    'Edit',
    'Bash',
    'Grep',
    'Glob',
    'AskUserQuestion',
    'EnterPlanMode',
    'ExitPlanMode',
    'KillShell',
    'NotebookEdit',
    'Skill',
    'Task',
    'TaskOutput',
    'TodoWrite',
    'WebFetch',
    'WebSearch',
  ].map((name) => [name.toLowerCase(), name]),
)
const wireName = (name: string) => canonicalNames.get(name.toLowerCase()) ?? name
/** Remove protocol metadata only at a known block boundary, never inside application JSON. */
const withoutCacheControl = <A extends object>(block: A): A => {
  const result = { ...block }
  Reflect.deleteProperty(result, 'cache_control')
  return result
}
const events = Schema.Union([
  Generated.BetaMessageStartEvent,
  Generated.BetaMessageDeltaEvent,
  Generated.BetaMessageStopEvent,
  Generated.BetaContentBlockStartEvent,
  Generated.BetaContentBlockDeltaEvent,
  Generated.BetaContentBlockStopEvent,
  Schema.Struct({
    type: Schema.Literal('error'),
    error: Schema.Struct({ type: Schema.String, message: Schema.optionalKey(Schema.String) }),
  }),
  Schema.Struct({ type: Schema.Literal('ping') }),
])

export const make = Effect.fnUntraced(function* (options: Options) {
  if (
    options.binding.provider !== 'anthropic' ||
    (options.transport !== undefined && options.transport !== 'sse')
  )
    return yield* Http.invalid('Anthropic OAuth requires an anthropic binding and SSE transport')
  const thinkingPolicy = options.thinkingPolicy ?? ThinkingPolicy.forModel(options.model)
  const http = yield* HttpClient.HttpClient
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
  const prepare = (
    input: Parameters<AnthropicClient.Service['createMessage']>[0],
    stream: boolean,
  ) =>
    Effect.gen(function* () {
      const nativeConfig = yield* Effect.serviceOption(Native.Config)
      if (Option.isSome(nativeConfig) && Object.keys(nativeConfig.value).length > 0)
        return yield* Http.invalid(
          'Use OAuth Config; native request overrides are unsupported for OAuth',
        )
      const dynamic = yield* Effect.serviceOption(Config)
      const config = yield* Schema.decodeEffect(Settings, { onExcessProperty: 'error' })(
        Option.getOrElse(dynamic, () => options.config ?? {}),
      ).pipe(Effect.mapError(() => Http.invalid('Unsupported Anthropic OAuth settings')))
      const thinkingBinding = yield* ThinkingPolicy.configure(thinkingPolicy, config.thinking?.type)
      const maximum = config.maxTokens ?? 4096
      if (config.temperature !== undefined && options.supportsTemperature !== true)
        return yield* Http.invalid('Temperature is not declared supported for this model')
      if (config.thinking !== undefined && !options.thinkingModes?.includes(config.thinking.type))
        return yield* Http.invalid('Thinking mode is not declared supported for this model')
      if (config.thinking?.type === 'enabled' && config.thinking.budget_tokens >= maximum)
        return yield* Http.invalid('Thinking budget must be smaller than maxTokens')
      if (
        config.temperature !== undefined &&
        config.thinking !== undefined &&
        config.thinking.type !== 'disabled'
      )
        return yield* Http.invalid('Temperature cannot be combined with thinking')
      if (
        config.effort !== undefined &&
        (config.thinking?.type !== 'adaptive' || !options.efforts?.includes(config.effort))
      )
        return yield* Http.invalid('Effort requires a supported adaptive thinking model')
      if (
        config.thinkingDisplay !== undefined &&
        (config.thinking === undefined || config.thinking.type === 'disabled')
      )
        return yield* Http.invalid('Thinking display requires thinking')
      if (config.cache === 'long' && options.supportsLongCache !== true)
        return yield* Http.invalid('Long cache retention is not declared supported')
      const allowed = new Set(['model', 'messages', 'system', 'tools', 'tool_choice', 'max_tokens'])
      for (const [key, value] of Object.entries(input.payload))
        if (value !== undefined && !allowed.has(key))
          return yield* Http.invalid(`Unsupported Anthropic OAuth request option: ${key}`)
      if (input.payload.model !== options.model)
        return yield* Http.invalid('Anthropic OAuth model is fixed')
      const translated = yield* ToolResult.request(input)
      const names = new Map<string, string>()
      const register = (name: string) => {
        const wire = wireName(name)
        const previous = names.get(wire)
        if (previous !== undefined && previous !== name)
          return Effect.fail(Http.invalid('OAuth tool names collide after normalization'))
        names.set(wire, name)
        return Effect.succeed(wire)
      }
      const tools = yield* Effect.forEach(translated.payload.tools ?? [], (tool) =>
        Effect.gen(function* () {
          if (tool.type !== undefined && tool.type !== 'custom')
            return yield* Http.invalid('OAuth supports application-defined tools only')
          const name = yield* register(tool.name)
          return { ...tool, name }
        }),
      )
      const messages = yield* Effect.forEach(translated.payload.messages, (message) =>
        Effect.gen(function* () {
          if (typeof message.content === 'string') return message
          const content = yield* Effect.forEach(message.content, (block) =>
            Effect.gen(function* () {
              if (block.type !== 'tool_use') return block
              return { ...block, name: yield* register(block.name) }
            }),
          )
          return { ...message, content }
        }),
      )
      const choice = translated.payload.tool_choice
      const toolChoice =
        choice?.type === 'tool' ? { ...choice, name: wireName(choice.name) } : choice
      const system = translated.payload.system
      const systemBlocks =
        typeof system === 'string' ? [{ type: 'text' as const, text: system }] : (system ?? [])
      const cacheControl = {
        type: 'ephemeral' as const,
        ...(config.cache === 'long' ? { ttl: '1h' as const } : {}),
      }
      const payload = {
        ...translated.payload,
        model: options.model,
        max_tokens: maximum,
        stream,
        messages,
        tools,
        tool_choice: toolChoice,
        system: [
          { type: 'text', text: "You are Claude Code, Anthropic's official CLI for Claude." },
          ...systemBlocks,
        ],
        ...(config.thinking === undefined
          ? {}
          : {
              thinking: {
                ...config.thinking,
                ...thinkingBinding,
                ...(config.thinking.type === 'disabled'
                  ? {}
                  : { display: config.thinkingDisplay ?? 'summarized' }),
              },
            }),
        ...(config.effort === undefined ? {} : { output_config: { effort: config.effort } }),
        ...(config.temperature === undefined ? {} : { temperature: config.temperature }),
        ...(config.userId === undefined ? {} : { metadata: { user_id: config.userId } }),
        ...(config.cache === 'short' || config.cache === 'long'
          ? { cache_control: cacheControl }
          : {}),
      }
      // Visit protocol block boundaries; tool schemas and tool-use inputs remain opaque.
      const body =
        config.cache === 'none'
          ? {
              ...withoutCacheControl(payload),
              system: payload.system.map(withoutCacheControl),
              tools: payload.tools.map(withoutCacheControl),
              messages: payload.messages.map((message) => ({
                ...message,
                content:
                  typeof message.content === 'string'
                    ? message.content
                    : message.content.map((block) => {
                        const clean = withoutCacheControl(block)
                        if (clean.type === 'tool_result' && Array.isArray(clean.content))
                          return { ...clean, content: clean.content.map(withoutCacheControl) }
                        return clean
                      }),
              })),
            }
          : payload
      const credential = yield* auth.resolve.pipe(Effect.mapError(toAiError))
      const betas = new Set([
        'claude-code-20250219',
        'oauth-2025-04-20',
        ...(thinkingPolicy === 'server-drop' ? [ThinkingPolicy.beta] : []),
        ...(translated.params?.['anthropic-beta']?.split(',').filter(Boolean) ?? []),
      ])
      const request = HttpClientRequest.post(
        `${(options.apiUrl ?? 'https://api.anthropic.com').replace(/\/$/, '')}/v1/messages?beta=true`,
      ).pipe(
        HttpClientRequest.setHeaders({
          authorization: `Bearer ${Redacted.value(credential.accessToken)}`,
          'anthropic-version': options.apiVersion ?? '2023-06-01',
          'anthropic-beta': [...betas].join(','),
          'user-agent': 'claude-cli/2.1.280',
          'x-app': 'cli',
          'anthropic-dangerous-direct-browser-access': 'true',
          accept: stream ? 'text/event-stream' : 'application/json',
        }),
        HttpClientRequest.bodyJsonUnsafe(body),
      )
      const response = yield* Http.execute(http, request)
      yield* Http.checkStatus(response, 'anthropic')
      return { response, originalName: (name: string) => names.get(name) ?? name }
    })
  const unused = yield* AnthropicClient.make({}).pipe(
    Effect.provideService(
      HttpClient.HttpClient,
      HttpClient.make(() => Effect.die('OAuth exposes only LanguageModel')),
    ),
  )
  const client = AnthropicClient.AnthropicClient.of({
    ...unused,
    createMessage: (input) =>
      Effect.gen(function* () {
        const { response, originalName } = yield* prepare(input, false)
        const message = yield* response.json.pipe(
          Effect.flatMap(Schema.decodeUnknownEffect(Generated.BetaMessage)),
          Effect.mapError(Http.malformed),
        )
        return [
          {
            ...message,
            content: message.content.map((block) =>
              block.type === 'tool_use' ? { ...block, name: originalName(block.name) } : block,
            ),
          },
          Http.safeResponse(response),
        ] satisfies [Generated.BetaMessage, typeof response]
      }).pipe(Effect.scoped),
    createMessageStream: (input) =>
      Effect.gen(function* () {
        const { response, originalName } = yield* prepare(input, true)
        const stream = Stream.suspend(() => {
          let terminal = false
          return response.stream.pipe(
            Stream.decodeText(),
            Stream.pipeThroughChannel(Sse.decodeDataSchema(events)),
            Stream.map((event) => event.data),
            Stream.mapError(Http.malformed),
            Stream.filter((event) => event.type !== 'ping'),
            Stream.mapEffect((event) => {
              if (event.type === 'error')
                return Effect.fail(
                  Http.providerFailure(event.error.type, 'anthropic', event.error.message),
                )
              if (event.type === 'message_stop') terminal = true
              if (event.type === 'content_block_start' && event.content_block.type === 'tool_use')
                return Effect.succeed({
                  ...event,
                  content_block: {
                    ...event.content_block,
                    name: originalName(event.content_block.name),
                  },
                })
              return Effect.succeed(event)
            }),
            Stream.takeUntil((event) => event.type === 'message_stop'),
            Stream.concat(
              Stream.fromEffect(
                Effect.suspend(() => (terminal ? Effect.void : Effect.fail(Http.malformed()))),
              ).pipe(Stream.drain),
            ),
          )
        })
        return [Http.safeResponse(response), stream]
      }),
  })
  return yield* Native.make({ model: options.model }).pipe(
    Effect.provideService(AnthropicClient.AnthropicClient, client),
  )
})
export const layer = (options: Options) => Layer.effect(LanguageModel.LanguageModel, make(options))
