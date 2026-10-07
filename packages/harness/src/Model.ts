import * as SchemaField from './SchemaField.ts'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import type * as LanguageModel from 'effect/ai/LanguageModel'
import type * as Prompt from 'effect/ai/Prompt'
import * as Agent from './Agent.ts'
import { ModelError, ModelNoModel, ModelUnsupported } from './Error.ts'
import type * as Usage from './Usage.ts'
import * as AiError from 'effect/ai/AiError'
import type * as Response from 'effect/ai/Response'
import * as Serialization from './Serialization.ts'

export const RequestOptions = Schema.Struct({
  thinking: Schema.String,
  options: Schema.Record(Schema.String, Schema.Json),
  sessionId: SchemaField.optional(Schema.String),
  maxTokens: SchemaField.optional(Schema.Finite),
  cache: SchemaField.optional(Schema.Literals(['none', 'short', 'long'])),
})
export type RequestOptions = typeof RequestOptions.Type
/** A native LanguageModel with provider capability translation. Layer construction captures required provider/client services. */
export interface Descriptor {
  readonly ref: Agent.ModelRef
  readonly model: LanguageModel.LanguageModel
  readonly deferred?: DeferredCapability | undefined
  readonly contextWindow: number
  readonly maxOutputTokens: number
  readonly configure: (options: RequestOptions) => Effect.Effect<Context.Context<never>, ModelError>
  /** Provider transcript normalization runs after request hooks, preserving native message data. */
  readonly normalizePrompt?: ((prompt: Prompt.Prompt) => Prompt.Prompt) | undefined
  readonly estimate?: ((message: Prompt.Message) => number) | undefined
  readonly usage?:
    | ((usage: Response.Usage, metadata: Response.ProviderMetadata) => Usage.Usage)
    | undefined
  readonly classify?:
    | ((error: unknown) => { readonly retryable: boolean; readonly overflow: boolean })
    | undefined
}
export class Catalog extends Context.Service<
  Catalog,
  {
    readonly resolve: (ref: Agent.ModelRef) => Effect.Effect<Descriptor, ModelError>
  }
>()('@effect-harness/harness/Model/Catalog') {}
export function layer(descriptors: ReadonlyArray<Descriptor>): Layer.Layer<Catalog> {
  const entries = new Map(descriptors.map((descriptor) => [key(descriptor.ref), descriptor]))
  return Layer.succeed(
    Catalog,
    Catalog.of({
      resolve: (ref) => {
        const descriptor = entries.get(key(ref))
        return descriptor === undefined
          ? Effect.fail(
              new ModelError({
                reason: new ModelNoModel({
                  message: `Model ${ref.provider}/${ref.modelId} is not available`,
                }),
              }),
            )
          : Effect.succeed(descriptor)
      },
    }),
  )
}
const key = (ref: Agent.ModelRef): string => JSON.stringify([ref.provider, ref.modelId])
/** Default capability translator for models with no provider-specific options. Unknown options fail explicitly. */
export const noOptions = (
  options: RequestOptions,
): Effect.Effect<Context.Context<never>, ModelError> =>
  options.thinking !== 'off' ||
  Object.keys(options.options).length > 0 ||
  options.sessionId !== undefined ||
  options.maxTokens !== undefined ||
  options.cache !== undefined
    ? Effect.fail(
        new ModelError({
          reason: new ModelUnsupported({
            message: 'This model adapter does not support request options',
          }),
        }),
      )
    : Effect.succeed(Context.empty())

export const DeferredDecision = Schema.Struct({
  handle: Schema.Json,
  pollAfterMs: SchemaField.optional(Schema.Finite),
})
export type DeferredDecision = typeof DeferredDecision.Type
export interface DeferredCapability {
  readonly inspect: (parts: ReadonlyArray<Response.AnyPart>) => DeferredDecision | undefined
  readonly fetch: (
    handle: Schema.Json,
    options: RequestOptions,
  ) => import('effect/Stream').Stream<
    import('./Executor.ts').Part,
    ModelError | import('effect/ai/AiError').AiError
  >
  readonly cancel: (
    handle: Schema.Json,
    options: RequestOptions,
  ) => Effect.Effect<void, ModelError | import('effect/ai/AiError').AiError>
}
/** Root persists this absolute deadline; the harness does not create a timer or poll loop. */
export const pollAt = (now: number, previous?: number, delay = 5000): number =>
  Math.max(now + delay, previous === undefined ? -Infinity : previous + 1)

// Error patterns adapted from pi-ai (MIT), pinned 636703a0; see the package NOTICE.
const overflowPatterns = [
  /prompt (?:is )?too long/i, // Anthropic and z.ai token overflow
  /prompt exceeds max length/i, // z.ai CN endpoint token overflow
  /request_too_large/i, // Anthropic request byte-size overflow (HTTP 413)
  /input is too long for requested model/i, // Amazon Bedrock
  /exceeds the context window/i, // OpenAI (Completions & Responses API)
  /exceeds (?:the )?(?:model'?s )?maximum context length(?: of [\d,]+ tokens?|\s*\([\d,]+\))/i, // OpenAI-compatible proxies (LiteLLM)
  /input token count.*exceeds the maximum/i, // Google (Gemini)
  /maximum prompt length is \d+/i, // xAI (Grok)
  /reduce the length of the messages/i, // Groq
  /maximum context length is \d+ tokens/i, // OpenRouter (most backends)
  /exceeds (?:the )?maximum allowed input length of [\d,]+ tokens?/i, // OpenRouter/Poolside
  /input \(\d+ tokens\) is longer than the model'?s context length \(\d+ tokens\)/i, // Together AI
  /exceeds the limit of \d+/i, // GitHub Copilot
  /exceeds the available context size/i, // llama.cpp server
  /greater than the context length/i, // LM Studio
  /context window exceeds limit/i, // MiniMax
  /exceeded model token limit/i, // Kimi For Coding
  /too large for model with \d+ maximum context length/i, // Mistral
  /prompt has [\d,]+ tokens?, but the configured context size is [\d,]+ tokens?/i, // DS4 server
  /model_context_window_exceeded/i, // z.ai non-standard finish_reason surfaced as error text
  /prompt too long; exceeded (?:max )?context length/i, // Ollama explicit overflow error
  /range of input length should be/i, // DashScope / Qwen Token Plan
  /context[_ ]length[_ ]exceeded/i, // Generic fallback
  /too many tokens/i, // Generic fallback
  /token limit exceeded/i, // Generic fallback
]
const nonOverflowPatterns = [
  /^(Throttling error|Service unavailable):/i, // AWS Bedrock non-overflow errors (human-readable prefixes from formatBedrockError)
  /rate limit/i, // Generic rate limiting
  /too many requests/i, // Generic HTTP 429 style
]
const nonRetryable = new RegExp(
  [
    // OpenCode Go/free-tier limits returned as 429 JSON error types by OpenCode's
    // Zen API. These are subscription/account limits, not transient throttles.
    'GoUsageLimitError',
    'FreeUsageLimitError',

    // OpenCode Go subscription-limit text asks users to enable available-balance
    // usage after rolling/weekly/monthly limits are reached.
    'Monthly usage limit reached',
    'available balance',

    // Generic quota/budget/billing exhaustion. `insufficient_quota` is OpenAI's
    // quota/billing error code; the other strings cover common gateway wording.
    'insufficient_quota',
    'out of budget',
    'quota exceeded',
    'billing',

    // Sign in with ChatGPT: the subscription's shared usage limit, which resets
    // after hours rather than seconds.
    'subscription_sharing_usage_limit_exceeded',
  ].join('|'),
  'i',
)
const foreignTransient = new RegExp(
  [
    // Generic provider load, HTTP status, and server-side transient failures.
    'overloaded',
    'server_busy',
    'servers are currently busy',
    'currently experiencing high demand',
    'model is at capacity',
    'rate.?limit',
    'too many requests',
    '429',
    '500',
    '502',
    '503',
    '504',
    '520',
    '524',
    'service.?unavailable',
    'server.?error',
    'internal.?error',

    // Wrapper/provider text for transient upstream failures, including OpenRouter
    // "Provider returned error" responses (#2264).
    'provider.?returned.?error',
    'exceeded request buffer limit while retrying upstream',

    // Network, proxy, and fetch transport failures. This includes OpenAI Codex
    // raw-fetch failures such as "upstream connect", "connection refused", and
    // "reset before headers" (#733), plus OpenRouter connection drops (#3317).
    'network.?error',
    'connection.?error',
    'connection.?refused',
    'connection.?lost',
    'other side closed',
    'fetch failed',
    'getaddrinfo',
    'ENOTFOUND',
    'EAI_AGAIN',
    'upstream.?connect',
    'reset before headers',
    'socket hang up',
    'socket connection was closed',
    'timed? out',
    'timeout',
    'terminated',

    // WebSocket transports can report close/error text instead of HTTP/fetch text.
    'websocket.?closed',
    'websocket.?error',

    // Premature stream endings from SDKs and transports. Anthropic can throw
    // "stream ended without ..." and "Anthropic stream ended before message_stop"
    // (#4433); Bedrock/Smithy can throw an HTTP/2 no-response error (#3594).
    'ended without',
    'stream ended before message_stop',
    'stream ended before a terminal response event',
    'http2 request did not get a response',
    // Node ERR_HTTP2_STREAM_CANCEL: the HTTP/2 session died before the request was
    // sent, e.g. after the Bedrock SDK's 5-minute session timeout (#10379).
    'pending stream has been canceled',

    // Provider-requested retry delay cap failures should flow through the outer
    // retry policy so callers can surface/abort the backoff (#1123).
    'retry delay',

    // Explicit retry guidance emitted mid-stream by OpenAI Responses and Bedrock
    // stream exceptions (#6019).
    'you can retry your request',
    'try your request again',
    'please retry your request',

    // gRPC based providers (e.g. NVIDIA NIM)
    'ResourceExhausted',

    // Sign in with ChatGPT: usage or user data temporarily unavailable. Usage
    // failures can arrive mid-stream without an HTTP 503 in the message.
    'subscription_sharing_usage_unavailable',
    'subscription_sharing_user_unavailable',
  ].join('|'),
  'i',
)

export function errorText(error: unknown): string {
  return Serialization.errorText(error)
}
/** Converts SDK invalid-request diagnostics and otherwise unclassified foreign sentinels once at the model boundary. */
export function providerError(error: unknown, provider?: string): AiError.AiError {
  if (error instanceof ModelError)
    return new AiError.AiError({
      module: 'Harness',
      method: 'model',
      reason: new AiError.InvalidRequestError({
        description: error.message,
        metadata: { harness: { reason: error.reason._tag } },
      }),
    })
  const native = AiError.isAiError(error) ? error : undefined
  if (
    native !== undefined &&
    native.reason._tag !== 'UnknownError' &&
    native.reason._tag !== 'InvalidRequestError'
  )
    return native
  let text = errorText(error)
  if (native !== undefined && 'description' in native.reason)
    text = native.reason.description ?? native.message
  const metadata =
    native !== undefined && 'metadata' in native.reason
      ? native.reason.metadata
      : { [provider ?? 'foreign']: { message: text } }
  const http = native !== undefined && 'http' in native.reason ? native.reason.http : undefined
  const overflow =
    !nonOverflowPatterns.some((pattern) => pattern.test(text)) &&
    (overflowPatterns.some((pattern) => pattern.test(text)) ||
      (provider === 'cerebras' && /^4(?:00|13)\s*(?:status code)?\s*\(no body\)/i.test(text)))
  let reason: AiError.AiErrorReason
  if (overflow)
    reason = new AiError.InvalidRequestError({
      description: text,
      parameter: 'context_window',
      metadata,
      http,
    })
  else if (native !== undefined && native.reason._tag === 'InvalidRequestError') return native
  else if (nonRetryable.test(text)) reason = new AiError.QuotaExhaustedError({ metadata })
  else if (/rate.?limit|too many requests|\b429\b|ResourceExhausted/i.test(text))
    reason = new AiError.RateLimitError({ metadata })
  else if (foreignTransient.test(text))
    reason = new AiError.InternalProviderError({ description: text, metadata })
  else if (native !== undefined) return native
  else reason = new AiError.UnknownError({ description: text, metadata })
  return new AiError.AiError({
    module: native?.module ?? provider ?? 'ForeignProvider',
    method: native?.method ?? 'response',
    reason,
  })
}

export function classify(
  error: unknown,
  provider?: string,
): { readonly retryable: boolean; readonly overflow: boolean } {
  const typed = providerError(error, provider)
  return {
    overflow:
      typed.reason._tag === 'InvalidRequestError' && typed.reason.parameter === 'context_window',
    retryable: typed.isRetryable,
  }
}
