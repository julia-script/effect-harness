import * as AnthropicClient from '@effect/ai-anthropic/AnthropicClient'
import * as AnthropicLanguageModel from '@effect/ai-anthropic/AnthropicLanguageModel'
import * as Prompt from './Prompt.ts'
import * as Generated from '@effect/ai-anthropic/Generated'
import type { AuthError } from '@effect-harness/auth/Credential'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Predicate from 'effect/Predicate'
import * as Redacted from 'effect/Redacted'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as AiError from 'effect/ai/AiError'
import * as Headers from 'effect/http/Headers'
import * as HttpClient from 'effect/http/HttpClient'
import * as HttpClientError from 'effect/http/HttpClientError'
import * as HttpClientRequest from 'effect/http/HttpClientRequest'
import { OAuth, tokenUrl } from './OAuth.ts'

// Transport identity and aliases adapted from Pi commit 636703a0 (MIT); see NOTICE.
export const identity = "You are Claude Code, Anthropic's official CLI for Claude."
export const cliVersion = '2.1.280'
export const betas = ['claude-code-20250219', 'oauth-2025-04-20'] as const
const canonicalTools = [
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
]
const canonicalByLower = new Map(canonicalTools.map((name) => [name.toLowerCase(), name]))
const alias = (name: string) => canonicalByLower.get(name.toLowerCase()) ?? name
const invalid = (description: string) =>
  new AiError.AiError({
    module: 'AnthropicAccount',
    method: 'request',
    reason: new AiError.InvalidRequestError({ description }),
  })
const authError = (error: AuthError) => {
  let reason: AiError.AiErrorReason
  if (error.reason._tag === 'AuthNetworkError')
    reason = new AiError.NetworkError({
      reason: 'TransportError',
      request: { method: 'POST', url: tokenUrl, urlParams: [], hash: undefined, headers: {} },
      description: error.message,
    })
  else if (error.isRetryable && error.status === 429)
    reason = new AiError.RateLimitError({ retryAfter: error.retryAfter })
  else if (error.isRetryable)
    reason = new AiError.InternalProviderError({ description: error.message })
  else
    reason = new AiError.AuthenticationError({
      kind: error.reason._tag === 'AuthPermissionError' ? 'InsufficientPermissions' : 'Unknown',
      description: error.message,
    })
  return new AiError.AiError({ module: 'AnthropicAccount', method: 'credential', reason })
}
const mergeBetas = (value?: string) =>
  [
    ...new Set([
      ...betas,
      ...(value ?? '')
        .split(',')
        .map((item) => item.trim())
        .filter(Boolean),
    ]),
  ].join(',')

/** Rewrites protocol content nodes only. User text, images and arbitrary JSON tool inputs are untouched. */
const rewriteBlock = (block: unknown, rename: (name: string) => string): unknown => {
  if (!Predicate.isReadonlyObject(block)) return block
  if (block.type === 'tool_use' && typeof block.name === 'string')
    return { ...block, name: rename(block.name) }
  if (block.type === 'tool_reference') {
    if (typeof block.tool_name === 'string') return { ...block, tool_name: rename(block.tool_name) }
    if (typeof block.name === 'string') return { ...block, name: rename(block.name) }
  }
  if (block.type === 'tool_result' && Array.isArray(block.content))
    return { ...block, content: block.content.map((part) => rewriteBlock(part, rename)) }
  return block
}
const prepare = Effect.fnUntraced(function* (
  payload: typeof Generated.BetaCreateMessageParams.Encoded,
) {
  const aliases = new Map<string, string>()
  const insensitive = new Set<string>()
  const forward = new Map<string, string>()
  const wireName = (tool: { readonly name: string; readonly type?: string | null | undefined }) =>
    tool.type === undefined || tool.type === null || tool.type === 'custom'
      ? alias(tool.name)
      : tool.name
  const tools = payload.tools?.map((tool) => {
    if (!('name' in tool)) return tool
    const canonical = wireName(tool)
    aliases.set(canonical.toLowerCase(), tool.name)
    forward.set(tool.name.toLowerCase(), canonical)
    return { ...tool, name: canonical }
  })
  for (const tool of payload.tools ?? []) {
    if (!('name' in tool)) continue
    const name = wireName(tool).toLowerCase()
    if (insensitive.has(name))
      return yield* invalid('Tool names collide after account canonicalization')
    insensitive.add(name)
  }
  const rename = (name: string) => forward.get(name.toLowerCase()) ?? alias(name)
  const system =
    typeof payload.system === 'string'
      ? [{ type: 'text', text: payload.system }]
      : (payload.system ?? [])
  const preamble = {
    type: 'text',
    text: identity,
    ...(payload.cache_control === undefined || payload.cache_control === null
      ? {}
      : { cache_control: payload.cache_control }),
  }
  const transformed = yield* Schema.decodeUnknownEffect(Generated.BetaCreateMessageParams)({
    ...payload,
    system: [preamble, ...system],
    messages: payload.messages.map((message) => ({
      ...message,
      content:
        typeof message.content === 'string'
          ? message.content
          : message.content.map((block) => rewriteBlock(block, rename)),
    })),
    ...(tools === undefined ? {} : { tools }),
    ...(payload.tool_choice?.type === 'tool'
      ? { tool_choice: { ...payload.tool_choice, name: rename(payload.tool_choice.name) } }
      : {}),
  }).pipe(Effect.mapError(() => invalid('Unsupported account Messages payload or tool alias')))
  return {
    payload: transformed,
    reverse: (name: string) => aliases.get(name.toLowerCase()) ?? name,
  }
})

export interface ClientOptions {
  readonly account: string
  readonly apiUrl?: string | undefined
  readonly apiVersion?: string | undefined
  readonly transformClient?: AnthropicClient.Options['transformClient']
  readonly interleavedThinking?: boolean | undefined
}
/** Supplies the standard native client with refreshed bearer auth and Pi account wire adaptation. */
export const layerClient = (options: ClientOptions) =>
  Layer.effect(AnthropicClient.AnthropicClient)(
    Effect.gen(function* () {
      if (options.account.length === 0)
        return yield* invalid('Supply a nonempty account storage key')
      const auth = yield* OAuth
      const http = yield* HttpClient.HttpClient
      const identityRequest = (
        request: HttpClientRequest.HttpClientRequest,
        token: Redacted.Redacted<string>,
      ) =>
        request.pipe(
          HttpClientRequest.removeHeader('x-api-key'),
          HttpClientRequest.bearerToken(token),
          HttpClientRequest.setHeader('accept', 'application/json'),
          HttpClientRequest.setHeader('anthropic-dangerous-direct-browser-access', 'true'),
          HttpClientRequest.setHeader('user-agent', `claude-cli/${cliVersion}`),
          HttpClientRequest.setHeader('x-app', 'cli'),
          HttpClientRequest.setHeader(
            'anthropic-beta',
            mergeBetas(request.headers['anthropic-beta']),
          ),
        )
      const identityClient = (client: HttpClient.HttpClient, token: Redacted.Redacted<string>) =>
        client.pipe(HttpClient.mapRequest((request) => identityRequest(request, token)))
      const make = (token: Redacted.Redacted<string>, client = http) =>
        AnthropicClient.make({
          apiUrl: options.apiUrl,
          apiVersion: options.apiVersion,
          transformClient: (value) =>
            identityClient(options.transformClient?.(value) ?? value, token),
        }).pipe(Effect.provideService(HttpClient.HttpClient, client))
      const fresh = auth.accessToken(options.account).pipe(
        Effect.mapError(authError),
        Effect.flatMap((token) => make(token)),
      )
      // Generated raw endpoints remain authenticated. Message generation uses the adapted methods below.
      const dynamicClient = (client: HttpClient.HttpClient) =>
        client.pipe(
          HttpClient.mapRequestEffect((request) =>
            auth.accessToken(options.account).pipe(
              Effect.mapError(
                (error) =>
                  new HttpClientError.HttpClientError({
                    reason: new HttpClientError.TransportError({
                      request,
                      description: error.message,
                      cause: error,
                    }),
                  }),
              ),
              Effect.map((token) => identityRequest(request, token)),
            ),
          ),
        )
      const base = yield* AnthropicClient.make({
        apiUrl: options.apiUrl,
        apiVersion: options.apiVersion,
        transformClient: (value) => dynamicClient(options.transformClient?.(value) ?? value),
      }).pipe(Effect.provideService(HttpClient.HttpClient, http))
      const withThinkingBeta = (
        request: Parameters<AnthropicClient.Service['createMessage']>[0],
      ) => {
        if (request.payload.thinking?.type !== 'enabled' || options.interleavedThinking === false)
          return request.params
        return {
          ...request.params,
          'anthropic-beta': [request.params?.['anthropic-beta'], 'interleaved-thinking-2025-05-14']
            .filter((value) => value !== undefined && value.length > 0)
            .join(','),
        }
      }
      const redacted = Effect.updateService(Headers.CurrentRedactedNames, (names) => [
        ...new Set([...names, 'authorization', 'x-api-key']),
      ])
      return AnthropicClient.AnthropicClient.of({
        client: base.client,
        streamRequest: (schema) => (request) =>
          base
            .streamRequest(schema)(request)
            .pipe(
              Stream.updateService(Headers.CurrentRedactedNames, (names) => [
                ...new Set([...names, 'authorization', 'x-api-key']),
              ]),
            ),
        createMessage: Effect.fnUntraced(function* (request) {
          const prepared = yield* prepare(request.payload)
          const client = yield* fresh
          const [body, response] = yield* client.createMessage({
            ...request,
            payload: prepared.payload,
            params: withThinkingBeta(request),
          })
          const result: [typeof body, typeof response] = [
            {
              ...body,
              content: body.content.map((block) =>
                block.type === 'tool_use'
                  ? { ...block, name: prepared.reverse(block.name) }
                  : block,
              ),
            },
            response,
          ]
          return result
        }, redacted),
        createMessageStream: Effect.fnUntraced(function* (request) {
          const prepared = yield* prepare(request.payload)
          const client = yield* fresh
          const [response, stream] = yield* client.createMessageStream({
            ...request,
            payload: prepared.payload,
            params: withThinkingBeta(request),
          })
          const adapted = stream.pipe(
            Stream.map((event) => {
              if (event.type === 'content_block_start' && event.content_block.type === 'tool_use')
                return {
                  ...event,
                  content_block: {
                    ...event.content_block,
                    name: prepared.reverse(event.content_block.name),
                  },
                }
              if (event.type === 'message_start')
                return {
                  ...event,
                  message: {
                    ...event.message,
                    content: event.message.content.map((block) =>
                      block.type === 'tool_use'
                        ? { ...block, name: prepared.reverse(block.name) }
                        : block,
                    ),
                  },
                }
              return event
            }),
          )
          const result: [typeof response, typeof stream] = [response, adapted]
          return result
        }, redacted),
      })
    }),
  )

/** Native direct Messages LanguageModel, preserving structured Prompt history. */
export const layer = (
  options: ClientOptions & {
    readonly model: string
    readonly config?: Omit<typeof AnthropicLanguageModel.Config.Service, 'model'>
  },
) =>
  Prompt.layer({ model: options.model, config: options.config }).pipe(
    Layer.provideMerge(layerClient(options)),
  )
