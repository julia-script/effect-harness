/** Sanitized provider transport boundaries. No invisible inference retries. */
import * as AiError from 'effect/ai/AiError'
import * as Effect from 'effect/Effect'
import * as Cause from 'effect/Cause'
import * as HttpClient from 'effect/http/HttpClient'
import * as Scope from 'effect/Scope'
import * as Duration from 'effect/Duration'
import * as Headers from 'effect/http/Headers'
import * as HttpClientRequest from 'effect/http/HttpClientRequest'
import type * as HttpClientResponse from 'effect/http/HttpClientResponse'
import * as Schema from 'effect/Schema'

export const invalid = (description: string) =>
  new AiError.AiError({
    module: 'SubscriptionProvider',
    method: 'request',
    reason: new AiError.InvalidRequestError({ description }),
  })
export const malformed = () =>
  new AiError.AiError({
    module: 'SubscriptionProvider',
    method: 'response',
    reason: new AiError.InvalidOutputError({
      description: 'Invalid or truncated provider response',
    }),
  })
export const transport = () =>
  new AiError.AiError({
    module: 'SubscriptionProvider',
    method: 'request',
    reason: new AiError.InternalProviderError({ description: 'Provider transport failed' }),
  })

/** Even defective transport middleware must not persist a credential-bearing request/cause. */
export const execute = (
  client: HttpClient.HttpClient,
  request: HttpClientRequest.HttpClientRequest,
) =>
  Effect.gen(function* () {
    const scope = yield* Effect.serviceOption(Scope.Scope)
    return yield* scope._tag === 'Some'
      ? HttpClient.withScope(client)
          .execute(request)
          .pipe(Effect.provideService(Scope.Scope, scope.value))
      : client.execute(request)
  }).pipe(
    Effect.catchCause((cause) =>
      Cause.hasInterrupts(cause) ? Effect.interrupt : Effect.fail(transport()),
    ),
  )

export type Provider = 'openai-codex' | 'anthropic'
const overflow = () =>
  new AiError.AiError({
    module: 'SubscriptionProvider',
    method: 'request',
    reason: new AiError.InvalidRequestError({
      description: 'Provider input context exceeded',
      metadata: { subscription: { failure: 'input-context-overflow' } },
    }),
  })
const discriminator = Schema.Struct({
  subscription: Schema.Struct({ failure: Schema.Literal('input-context-overflow') }),
})
export const isOverflow = (error: AiError.AiError): boolean =>
  error.module === 'SubscriptionProvider' &&
  error.reason._tag === 'InvalidRequestError' &&
  Schema.is(discriminator)(error.reason.metadata)
const inputOverflow = (
  provider: Provider | undefined,
  code: string | undefined,
  message?: string,
): boolean =>
  (provider === 'openai-codex' && code === 'context_length_exceeded') ||
  (provider === 'anthropic' &&
    code === 'invalid_request_error' &&
    message !== undefined &&
    /^prompt is too long(?:: \d+ tokens > \d+ maximum)?\.?$/.test(message))

/** Terminal provider codes are classified without retaining a provider-supplied message. */
export const providerFailure = (
  code: string | undefined,
  provider?: Provider,
  message?: string,
) => {
  if (inputOverflow(provider, code, message)) return overflow()
  let reason: AiError.AiError['reason']
  if (code === 'usage_limit_reached' || code === 'insufficient_quota')
    reason = new AiError.QuotaExhaustedError({})
  else if (code === 'authentication_error' || code === 'invalid_api_key')
    reason = new AiError.AuthenticationError({ kind: 'Unknown' })
  else if (code === 'rate_limit_error' || code === 'rate_limit_exceeded')
    reason = new AiError.RateLimitError({})
  else if (code === 'invalid_request_error' || code === 'permission_error')
    reason = new AiError.InvalidRequestError({ description: 'Provider rejected the request' })
  else
    reason = new AiError.InternalProviderError({
      description: 'Provider reported a terminal failure',
    })
  return new AiError.AiError({ module: 'SubscriptionProvider', method: 'response', reason })
}

/** Only routing/status metadata crosses into native responses; auth headers/bodies never do. */
export const safeResponse = (
  response: HttpClientResponse.HttpClientResponse,
): HttpClientResponse.HttpClientResponse =>
  Object.create(response, {
    request: {
      value: HttpClientRequest.make(response.request.method)(response.request.url),
      enumerable: true,
    },
    headers: { value: Headers.empty, enumerable: true },
  })

const errorCode = Schema.Struct({
  error: Schema.Struct({
    code: Schema.optionalKey(Schema.String),
    type: Schema.optionalKey(Schema.String),
    message: Schema.optionalKey(Schema.String),
  }),
})
export const checkStatus = (response: HttpClientResponse.HttpClientResponse, provider?: Provider) =>
  Effect.gen(function* () {
    if (response.status >= 200 && response.status < 300) return
    let reason: AiError.AiError['reason']
    if (response.status === 401 || response.status === 403)
      reason = new AiError.AuthenticationError({ kind: 'Unknown' })
    else if (response.status === 429) {
      const body = yield* response.json.pipe(
        Effect.flatMap(Schema.decodeUnknownEffect(errorCode)),
        Effect.option,
      )
      const code =
        body._tag === 'Some' ? (body.value.error.code ?? body.value.error.type) : undefined
      if (code === 'usage_limit_reached' || code === 'insufficient_quota')
        reason = new AiError.QuotaExhaustedError({})
      else {
        const retry = Number(response.headers['retry-after'])
        reason = new AiError.RateLimitError(
          Number.isFinite(retry) && retry >= 0 ? { retryAfter: Duration.seconds(retry) } : {},
        )
      }
    } else if (response.status === 400) {
      const body = yield* response.json.pipe(
        Effect.flatMap(Schema.decodeUnknownEffect(errorCode)),
        Effect.option,
      )
      if (
        body._tag === 'Some' &&
        inputOverflow(
          provider,
          body.value.error.code ?? body.value.error.type,
          body.value.error.message,
        )
      )
        return yield* overflow()
      reason = new AiError.InvalidRequestError({ description: 'Provider HTTP 400' })
    } else if (response.status >= 500)
      reason = new AiError.InternalProviderError({
        description: `Provider HTTP ${response.status}`,
      })
    else
      reason = new AiError.InvalidRequestError({ description: `Provider HTTP ${response.status}` })
    return yield* new AiError.AiError({ module: 'SubscriptionProvider', method: 'request', reason })
  })
