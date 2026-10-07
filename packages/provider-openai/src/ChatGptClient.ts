/**
 * Authenticated ChatGPT Responses clients with semantic terminal-event validation.
 */
import * as Config from 'effect/Config'
import * as OpenAiClient from '@effect/ai-openai/OpenAiClient'
import * as OpenAiSchema from '@effect/ai-openai/OpenAiSchema'
import type { AuthError } from '@effect-harness/auth/Credential'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Redacted from 'effect/Redacted'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as AiError from 'effect/ai/AiError'
import * as HttpClient from 'effect/http/HttpClient'
import * as HttpClientError from 'effect/http/HttpClientError'
import * as HttpClientRequest from 'effect/http/HttpClientRequest'
import { ChatGpt, issuer, resource } from './ChatGpt.ts'
import * as ToolResult from './ToolResult.ts'

const authenticationError = (error: AuthError) => {
  let reason: AiError.AiErrorReason
  const metadata = {
    openai: {
      authReason: error.reason._tag,
      authCode: error.code,
      message: error.message,
      status: error.status ?? null,
    },
  }
  if (error.reason._tag === 'AuthNetworkError')
    reason = new AiError.NetworkError({
      reason: 'TransportError',
      request: {
        method: 'POST',
        url: `${issuer}/api/accounts/oauth/token`,
        urlParams: [],
        hash: undefined,
        headers: {},
      },
      description: error.message,
    })
  else if (error.isRetryable && error.status === 429)
    reason = new AiError.RateLimitError({ metadata, retryAfter: error.retryAfter })
  else if (error.isRetryable)
    reason = new AiError.InternalProviderError({ description: error.message, metadata })
  else
    reason = new AiError.AuthenticationError({
      kind: error.reason._tag === 'AuthPermissionError' ? 'InsufficientPermissions' : 'Unknown',
      description: error.message,
      metadata,
    })
  return new AiError.AiError({ module: 'ChatGpt', method: 'credential', reason })
}

// The SDK schema excludes known event types from UnknownResponseStreamEvent's fallback.
const isFailedEvent = (
  event: OpenAiSchema.ResponseStreamEvent,
): event is Extract<
  OpenAiSchema.ResponseStreamEvent,
  { readonly type: 'error' | 'response.failed' | 'response.incomplete' }
> =>
  event.type === 'error' || event.type === 'response.failed' || event.type === 'response.incomplete'

/** Terminal provider events are protocol boundaries: retain their codes and diagnostics in semantic reasons. */
const terminalError = (
  event: Extract<
    OpenAiSchema.ResponseStreamEvent,
    { readonly type: 'error' | 'response.failed' | 'response.incomplete' }
  >,
) => {
  const code = event.type === 'error' ? event.code : event.response.error?.code
  const message = event.type === 'error' ? event.message : event.response.error?.message
  const detail =
    event.type === 'response.incomplete' ? event.response.incomplete_details?.reason : undefined
  const status = event.type === 'error' ? event.status : undefined
  const description =
    message ?? `Inference ${event.type}${detail === undefined ? '' : `: ${detail}`}`
  const metadata = {
    openai: {
      event: event.type,
      code: code ?? null,
      message: message ?? null,
      incompleteReason: detail ?? null,
      status: status ?? null,
    },
  }
  let reason: AiError.AiErrorReason
  if (
    code === 'insufficient_quota' ||
    code === 'insufficient_quota_error' ||
    code === 'billing_insufficient_balance' ||
    code === 'subscription_sharing_usage_limit_exceeded'
  )
    reason = new AiError.QuotaExhaustedError({ metadata })
  else if (code === 'context_length_exceeded')
    reason = new AiError.InvalidRequestError({ description, parameter: 'context_window', metadata })
  else if (
    code === 'invalid_api_key' ||
    code === 'invalid_api_key_error' ||
    code === 'authentication_error'
  )
    reason = new AiError.AuthenticationError({ kind: 'InvalidKey', description, metadata })
  else if (code === 'invalid_request_error')
    reason = new AiError.InvalidRequestError({ description, metadata })
  else if (detail === 'content_filter' || code === 'content_policy_violation')
    reason = new AiError.ContentPolicyError({ description, metadata })
  else if (detail === 'max_output_tokens')
    reason = new AiError.InvalidRequestError({
      description,
      parameter: 'max_output_tokens',
      metadata,
    })
  else if (code === 'rate_limit_exceeded' || code === 'rate_limit_error' || status === 429)
    reason = new AiError.RateLimitError({ metadata })
  else if (
    code === 'server_error' ||
    code === 'server_busy' ||
    code === 'service_unavailable_error' ||
    code === 'subscription_sharing_usage_unavailable' ||
    code === 'subscription_sharing_user_unavailable'
  )
    reason = new AiError.InternalProviderError({ description, metadata })
  else if (status !== undefined)
    reason = AiError.reasonFromHttpStatus({ status, description, metadata })
  else reason = new AiError.UnknownError({ description, metadata })
  return new AiError.AiError({ module: 'ChatGpt', method: 'response', reason })
}
const incomplete = () =>
  new AiError.AiError({
    module: 'ChatGpt',
    method: 'response',
    reason: new AiError.InvalidOutputError({
      description: 'Inference ended without a completed response',
    }),
  })

/** Validates terminal success even when native providers would expose incomplete/error finish parts. */
const completedStream = (
  stream: Stream.Stream<OpenAiSchema.ResponseStreamEvent, AiError.AiError>,
) =>
  Stream.suspend(() => {
    let completed = false
    return stream.pipe(
      Stream.mapEffect((event) => {
        if (isFailedEvent(event)) return Effect.fail(terminalError(event))
        if (event.type === 'response.completed') completed = true
        return Effect.succeed(event)
      }),
      Stream.concat(
        Stream.fromEffect(
          Effect.suspend(() => (completed ? Effect.void : Effect.fail(incomplete()))),
        ).pipe(Stream.drain),
      ),
    )
  })

/**
 * Provides the native OpenAI client authenticated by an authorized ChatGPT account.
 *
 * **Details**
 *
 * Refreshes credentials for requests and uses public Responses with remote storage disabled.
 * Checks terminal response completion.
 *
 * **Gotchas**
 *
 * The account must have granted direct-inference permission. This client does not resume
 * private application sessions.
 *
 * @category layers
 */
export const layer = (options: {
  readonly account: string
}): Layer.Layer<OpenAiClient.OpenAiClient, never, ChatGpt | HttpClient.HttpClient> =>
  Layer.effect(OpenAiClient.OpenAiClient)(
    Effect.gen(function* () {
      const auth = yield* ChatGpt
      const httpClient = yield* HttpClient.HttpClient
      const accountClient = httpClient.pipe(
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
            Effect.map((token) =>
              request.pipe(
                HttpClientRequest.prependUrl(resource),
                HttpClientRequest.bearerToken(Redacted.value(token)),
                HttpClientRequest.acceptJson,
              ),
            ),
          ),
        ),
        HttpClient.filterStatusOk,
      )
      const fresh = auth.accessToken(options.account).pipe(
        Effect.mapError(authenticationError),
        Effect.flatMap((apiKey) =>
          OpenAiClient.make({ apiKey, apiUrl: resource }).pipe(
            Effect.provideService(HttpClient.HttpClient, httpClient),
          ),
        ),
      )
      const createResponseStream: OpenAiClient.Service['createResponseStream'] = Effect.fnUntraced(
        function* (payload) {
          const client = yield* fresh
          const [response, stream] = yield* client
            .createResponseStream({ ...payload, store: false, previous_response_id: undefined })
            .pipe(
              Effect.updateContext((services: Context.Context<never>) =>
                Context.omit(OpenAiClient.OpenAiSocket)(services),
              ),
            )
          return [response, completedStream(stream)] as const
        },
      )
      return ToolResult.client(
        OpenAiClient.OpenAiClient.of({
          client: accountClient,
          createResponseStream,
          createResponse: Effect.fnUntraced(function* (payload) {
            const [response, stream] = yield* createResponseStream(payload)
            const body = yield* Stream.runFoldEffect(
              stream,
              () => Option.none<OpenAiSchema.Response>(),
              (last, event) =>
                event.type === 'response.completed'
                  ? Schema.decodeUnknownEffect(OpenAiSchema.Response)(event.response).pipe(
                      Effect.asSome,
                      Effect.mapError(
                        (error) =>
                          new AiError.AiError({
                            module: 'ChatGpt',
                            method: 'response',
                            reason: AiError.InvalidOutputError.fromSchemaError(error),
                          }),
                      ),
                    )
                  : Effect.succeed(last),
            )
            return yield* Option.match(body, {
              onNone: () => Effect.fail(incomplete()),
              onSome: (body) => Effect.succeed([body, response] as const),
            })
          }),
          createEmbedding: () =>
            Effect.fail(
              new AiError.AiError({
                module: 'ChatGpt',
                method: 'embedding',
                reason: new AiError.InvalidRequestError({
                  description: 'ChatGPT plan credentials support Responses inference',
                }),
              }),
            ),
        }),
      )
    }),
  )

/**
 * Resolves all layer options through the caller's ConfigProvider.
 *
 * @category layers
 */
export const layerConfig = (
  config: Config.Wrap<NonNullable<Parameters<typeof layer>[0]>>,
): Layer.Layer<OpenAiClient.OpenAiClient, Config.ConfigError, ChatGpt | HttpClient.HttpClient> =>
  Layer.unwrap(
    Effect.gen(function* () {
      return layer(yield* Config.unwrap(config))
    }),
  )
