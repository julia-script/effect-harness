/**
 * Native model catalogs, deferred capabilities and semantic provider failures.
 */
import * as Record from 'effect/Record'

import * as Option from 'effect/Option'
import * as DateTime from 'effect/DateTime'
import { dual } from 'effect/Function'
import * as Duration from 'effect/Duration'
import * as Time from './Time.ts'
import * as SchemaField from './SchemaField.ts'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import type * as LanguageModel from 'effect/ai/LanguageModel'
import type * as Prompt from 'effect/ai/Prompt'
import type * as Agent from './Agent.ts'
import { ModelError, ModelNoModelError, ModelUnsupportedError } from './ModelError.ts'
import type * as Usage from './Usage.ts'
import * as AiError from 'effect/ai/AiError'
import type * as Response from 'effect/ai/Response'
import * as Serialization from './Serialization.ts'

/**
 * Schema for thinking, token, cache and provider-specific options for a model request.
 *
 * @category schemas
 */
export const RequestOptions = Schema.Struct({
  thinking: Schema.String,
  options: Schema.Record(Schema.String, Schema.Json),
  sessionId: SchemaField.optional(Schema.String),
  maxTokens: SchemaField.optional(Schema.Finite),
  cache: SchemaField.optional(Schema.Literals(['none', 'short', 'long'])),
})
/**
 * Thinking, token, cache and provider-specific options for a model request.
 *
 * @category models
 */
export type RequestOptions = typeof RequestOptions.Type
/**
 * Native LanguageModel with declared limits and provider-specific request behavior.
 *
 * **Details**
 *
 * configure produces request services. Optional normalizePrompt prepares native prompts;
 * usage interprets measurements, estimate can provide tokenization, and classify selects
 * retry/overflow behavior. Deferred capability is opt-in.
 *
 * **Gotchas**
 *
 * Capabilities and prices are caller declarations; catalogue membership is not proof of
 * current remote entitlement or availability.
 *
 * @category models
 */
export interface Descriptor {
  readonly ref: Agent.ModelRef
  readonly model: LanguageModel.LanguageModel
  readonly deferred?: DeferredCapability | undefined
  readonly contextWindow: number
  readonly maxOutputTokens: number
  /**
   * Validates request options and builds the provider-specific services used for that request.
   */
  readonly configure: (options: RequestOptions) => Effect.Effect<Context.Context<never>, ModelError>
  /** Provider transcript normalization runs after request hooks, preserving native message data. */
  readonly normalizePrompt?: ((prompt: Prompt.Prompt) => Prompt.Prompt) | undefined
  /**
   * Optional provider-specific token estimate used in context selection.
   */
  readonly estimate?: ((message: Prompt.Message) => number) | undefined
  /**
   * Maps native response measurements to known harness token and cost fields.
   */
  readonly usage?:
    | ((usage: Response.Usage, metadata: Response.ProviderMetadata) => Usage.Usage)
    | undefined
  /**
   * Classifies provider failures for retry or context-overflow handling.
   */
  readonly classify?:
    | ((error: unknown) => { readonly retryable: boolean; readonly overflow: boolean })
    | undefined
}
/**
 * Service resolving a provider/model reference to a native model descriptor.
 *
 * **Details**
 *
 * Resolution uses the selected provider and model ID. Unknown references fail with
 * ModelNoModel; callers decide which descriptors are available.
 *
 * @category services
 */
export class Catalog extends Context.Service<
  Catalog,
  {
    /**
     * Resolves a declared provider/model pair; an unknown reference fails with ModelNoModel.
     */
    readonly resolve: (ref: Agent.ModelRef) => Effect.Effect<Descriptor, ModelError>
  }
>()('effect-harness/Model/Catalog') {}
/**
 * Provides a catalogue from already constructed model descriptors.
 *
 * **Details**
 *
 * References are keyed by provider/model ID. If multiple descriptors use the same reference,
 * the last descriptor is retained; unknown references fail with ModelNoModel.
 *
 * @category layers
 */
// effect-nit-allow B-no-layer-arguments: Explicit registration-contract exception: descriptors contain heterogeneous already-owned LanguageModel instances keyed by provider/model, plus their configure/deferred capabilities. DemoModel and TranscriptPrompt/Executor consumers retain caller-owned provider scopes and exact instances; replacing this array with one ambient LanguageModel would collapse provider identities. This is an additional policy exception, not one of the catalogue's layer-transform/runtime-edge/test-harness exceptions.
export function layer(descriptors: ReadonlyArray<Descriptor>): Layer.Layer<Catalog> {
  const entries = new Map(descriptors.map((descriptor) => [key(descriptor.ref), descriptor]))
  return Layer.succeed(
    Catalog,
    Catalog.of({
      resolve: (ref) => {
        const descriptor = Option.fromUndefinedOr(entries.get(key(ref)))
        return Effect.fromOption(descriptor).pipe(
          Effect.mapError(
            () =>
              new ModelError({
                reason: new ModelNoModelError({
                  message: `Model ${ref.provider}/${ref.modelId} is not available`,
                }),
              }),
          ),
        )
      },
    }),
  )
}
const key = (ref: Agent.ModelRef): string => JSON.stringify([ref.provider, ref.modelId])
/**
 * Validates request options for models with no provider-specific configuration.
 *
 * **Details**
 *
 * Unknown options fail explicitly.
 *
 * @category combinators
 */
export const noOptions = (
  options: RequestOptions,
): Effect.Effect<Context.Context<never>, ModelError> =>
  options.thinking !== 'off' ||
  !Record.isEmptyReadonlyRecord(options.options) ||
  options.sessionId !== undefined ||
  options.maxTokens !== undefined ||
  options.cache !== undefined
    ? Effect.fail(
        new ModelError({
          reason: new ModelUnsupportedError({
            message: 'This model adapter does not support request options',
          }),
        }),
      )
    : Effect.succeed(Context.empty())

/**
 * Schema for persistable deferred request handle and optional delay before its next poll.
 *
 * @category schemas
 */
export const DeferredDecision = Schema.Struct({
  handle: Schema.Json,
  pollAfterMs: SchemaField.optional(Time.DurationFromMillis),
})
/**
 * Persistable deferred request handle and optional delay before its next poll.
 *
 * @category models
 */
export type DeferredDecision = typeof DeferredDecision.Type
/**
 * Provider operations for starting and polling a deferred request.
 *
 * @category models
 */
export interface DeferredCapability {
  readonly inspect: (parts: ReadonlyArray<Response.AnyPart>) => Option.Option<DeferredDecision>
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
/**
 * Optional previous deadline and delay for absolute polling-time calculation.
 *
 * **Details**
 *
 * The root persists the returned deadline; the harness does not create a timer or poll loop.
 *
 * @category combinators
 */
export interface PollOptions {
  readonly previous?: DateTime.Utc | undefined
  readonly delay?: Duration.Input | undefined
}
const pollAtImpl = (
  self: DateTime.Utc,
  thatOrOptions?: DateTime.Utc | PollOptions,
  delay: Duration.Input = importDefaultPollDelay,
): DateTime.Utc => {
  const options =
    thatOrOptions !== undefined && !DateTime.isDateTime(thatOrOptions) ? thatOrOptions : undefined
  let previous: DateTime.Utc | undefined
  if (thatOrOptions !== undefined) {
    previous = DateTime.isDateTime(thatOrOptions) ? thatOrOptions : thatOrOptions.previous
  }
  const next = DateTime.addDuration(self, options?.delay ?? delay)
  return previous === undefined
    ? next
    : DateTime.max(next, DateTime.addDuration(previous, '1 millis'))
}
/** Calculates an absolute polling deadline, preserving the prior deadline floor.
 * @category combinators
 */
export const pollAt: {
  (options: PollOptions): (self: DateTime.Utc) => DateTime.Utc
  (self: DateTime.Utc, that?: DateTime.Utc, delay?: Duration.Input): DateTime.Utc
} = dual((args) => DateTime.isDateTime(args[0]), pollAtImpl)

const importDefaultPollDelay = Duration.millis(5000)

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

/**
 * Returns a guarded display description of an arbitrary caught value.
 *
 * @category combinators
 */
export function errorText(self: unknown): string {
  return Serialization.errorText(self)
}
/**
 * Converts SDK invalid-request diagnostics and otherwise unclassified foreign sentinels once at the model boundary.
 *
 * @category combinators
 */
export function providerError(self: unknown, provider?: string): AiError.AiError {
  if (self instanceof ModelError)
    return new AiError.AiError({
      module: 'Harness',
      method: 'model',
      reason: new AiError.InvalidRequestError({
        description: self.message,
        metadata: { harness: { reason: self.reason._tag } },
      }),
    })
  const native = AiError.isAiError(self) ? self : undefined
  if (
    native !== undefined &&
    native.reason._tag !== 'UnknownError' &&
    native.reason._tag !== 'InvalidRequestError'
  )
    return native
  let text = errorText(self)
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

/**
 * Classifies retry and context-overflow behavior using semantic native AI errors.
 *
 * @category combinators
 */
export function classify(
  self: unknown,
  provider?: string,
): { readonly retryable: boolean; readonly overflow: boolean } {
  const typed = providerError(self, provider)
  return {
    overflow:
      typed.reason._tag === 'InvalidRequestError' && typed.reason.parameter === 'context_window',
    retryable: typed.isRetryable,
  }
}

/**
 * Checks whether a value satisfies the decoded `RequestOptions` schema.
 *
 * **Details**
 *
 * Does not decode, transform or coerce input. Use the schema decoder at an external data
 * boundary.
 *
 * @category guards
 */
export const isRequestOptions: (u: unknown) => u is RequestOptions = Schema.is(RequestOptions)

/**
 * Checks whether a value satisfies the decoded `DeferredDecision` schema.
 *
 * **Details**
 *
 * Does not decode, transform or coerce input. Use the schema decoder at an external data
 * boundary.
 *
 * @category guards
 */
export const isDeferredDecision: (u: unknown) => u is DeferredDecision = Schema.is(DeferredDecision)
