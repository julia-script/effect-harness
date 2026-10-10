/** Native Responses background work, using the caller's exact bound HTTP transport. */
import * as OpenAiClient from '@effect/ai-openai/OpenAiClient'
import { OpenAiConfig } from '@effect/ai-openai/OpenAiConfig'
import * as NativeModel from '@effect/ai-openai/OpenAiLanguageModel'
import * as OpenAiSchema from '@effect/ai-openai/OpenAiSchema'
import * as Cause from 'effect/Cause'
import * as Clock from 'effect/Clock'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Duration from 'effect/Duration'
import * as Number from 'effect/Number'
import * as Option from 'effect/Option'
import * as Result from 'effect/Result'
import * as Schema from 'effect/Schema'
import * as AiError from 'effect/ai/AiError'
import type * as Tool from 'effect/ai/Tool'
import * as HttpBody from 'effect/http/HttpBody'
import type * as HttpClientError from 'effect/http/HttpClientError'
import * as HttpClientRequest from 'effect/http/HttpClientRequest'
import * as HttpClientResponse from 'effect/http/HttpClientResponse'
import * as Model from '../Model.js'
import { ModelError, ModelInvalidResponseError, ModelUnsupportedError } from '../ModelError.js'
import * as ToolResult from './ToolResult.js'

/** The native response schema omits background status; add only those protocol fields. */
export const ResponseSchema = Schema.Struct({
  ...OpenAiSchema.Response.fields,
  status: Schema.Literals([
    'queued',
    'in_progress',
    'completed',
    'incomplete',
    'failed',
    'cancelled',
  ]),
  expires_at: Schema.optionalKey(Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))),
})

/** Credentials and clients are intentionally absent from recoverable handles. */
export const HandleSchema = Schema.Struct({
  version: Schema.Literal(1),
  provider: Schema.Literal('openai'),
  modelId: Schema.NonEmptyString,
  responseId: Schema.NonEmptyString,
  ...Model.DeferredIdentitySchema.fields,
  expiresAt: Schema.optionalKey(Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))),
})
export type Handle = typeof HandleSchema.Type
type WireResponse = readonly [typeof ResponseSchema.Type, HttpClientResponse.HttpClientResponse]

const invalid = (message: string, cause?: unknown) =>
  new ModelError({
    reason: new ModelInvalidResponseError({ message, ...(cause === undefined ? {} : { cause }) }),
  })
const unsupported = (message: string) =>
  new ModelError({ reason: new ModelUnsupportedError({ message }) })
const ErrorBody = Schema.Struct({
  error: Schema.Struct({
    message: Schema.String,
    code: Schema.optionalKey(Schema.NullOr(Schema.String)),
    type: Schema.optionalKey(Schema.NullOr(Schema.String)),
  }),
})
const httpError = Effect.fnUntraced(function* (error: HttpClientError.HttpClientError) {
  const reason = error.reason
  if (reason._tag === 'StatusCodeError') {
    const body = yield* Effect.option(reason.response.text)
    const parsed = Option.flatMap(
      body,
      Schema.decodeUnknownOption(Schema.fromJsonString(ErrorBody)),
    )
    const remote = Option.getOrUndefined(parsed)?.error
    let aiReason = AiError.reasonFromHttpStatus({
      status: reason.response.status,
      description: remote?.message ?? Option.getOrUndefined(body) ?? reason.description,
      ...(remote === undefined
        ? {}
        : { metadata: { errorCode: remote.code ?? null, errorType: remote.type ?? null } }),
    })
    if (remote?.code === 'insufficient_quota' || remote?.type === 'insufficient_quota')
      aiReason = new AiError.QuotaExhaustedError({ metadata: { errorCode: remote.code ?? null } })
    if (aiReason._tag === 'RateLimitError') {
      const delay = Number.parse(reason.response.headers['retry-after'] ?? '')
      if (Option.isSome(delay) && globalThis.Number.isFinite(delay.value) && delay.value >= 0)
        aiReason = new AiError.RateLimitError({ retryAfter: Duration.seconds(delay.value) })
    }
    return yield* AiError.make({
      module: 'OpenAiDeferredResponse',
      method: 'request',
      reason: aiReason,
    })
  }
  if (
    reason._tag === 'TransportError' ||
    reason._tag === 'EncodeError' ||
    reason._tag === 'InvalidUrlError'
  ) {
    return yield* AiError.make({
      module: 'OpenAiDeferredResponse',
      method: 'request',
      reason: AiError.NetworkError.fromRequestError(reason),
    })
  }
  return yield* invalid('Could not read deferred response body', error)
})

/**
 * Constructs an optional descriptor; it never replaces the native LanguageModel or client.
 *
 * start uses native prompt/tool/schema/media conversion with background=true, store=true and
 * stream=false. Explicit conflicting options fail before HTTP. poll retrieves the original id;
 * cancel uses POST /responses/:id/cancel. All operations retain authentication, API URL, project,
 * organization, caller transport transforms and scoped OpenAiConfig transforms. Polling never
 * creates another remote response. Reapply the saved configuration and offered declarations to
 * poll for native completion conversion; requestId and attempt must match the handle.
 *
 * The descriptor owns no loop or resources. Keep its client Layer scope alive. The runtime must
 * persist handles/deadlines, wait, enforce expiry/abort fencing and bound remote cancellation.
 * Native incomplete responses are completed results with their native finish reason.
 */
export const make = Effect.fnUntraced(function* (
  options: Parameters<typeof NativeModel.make>[0] & { readonly pollIntervalMs?: number },
): Effect.fn.Return<Model.Deferred, ModelError, OpenAiClient.OpenAiClient> {
  const native = yield* OpenAiClient.OpenAiClient
  const interval = options.pollIntervalMs ?? 1000
  yield* Schema.decodeEffect(Schema.Int.check(Schema.isGreaterThan(0)))(interval).pipe(
    Effect.mapError((cause) => unsupported(`Invalid deferred poll interval: ${cause.message}`)),
  )
  const send = Effect.fnUntraced(function* (
    request: HttpClientRequest.HttpClientRequest,
  ): Effect.fn.Return<WireResponse, Model.DeferredError> {
    const config = yield* OpenAiConfig.getOrUndefined
    const client =
      config?.transformClient === undefined ? native.client : config.transformClient(native.client)
    const response = yield* client
      .execute(request)
      .pipe(Effect.catchTag('HttpClientError', httpError))
    const body = yield* HttpClientResponse.schemaBodyJson(ResponseSchema)(response).pipe(
      Effect.catchTag('HttpClientError', httpError),
      Effect.mapError((cause) =>
        Schema.isSchemaError(cause) ? invalid('Invalid deferred Responses payload', cause) : cause,
      ),
    )
    return [body, response]
  })
  const nextDeadline = (previous = -1) =>
    Effect.map(Clock.currentTimeMillis, (now) => Math.max(now + interval, previous + 1))
  const check = Effect.fnUntraced(function* <Tools extends Record<string, Tool.Any>>(
    handle: Handle,
    request?: Model.DeferredRequest<Tools>,
  ) {
    if (handle.modelId !== options.model)
      return yield* unsupported('Deferred handle belongs to a different model')
    if (
      request !== undefined &&
      (handle.requestId !== request.requestId || handle.attempt !== request.attempt)
    )
      return yield* unsupported('Deferred handle belongs to a different request attempt')
    if (handle.expiresAt !== undefined && (yield* Clock.currentTimeMillis) >= handle.expiresAt) {
      return yield* new Model.DeferredTerminalError({
        reason: 'expired',
        responseId: handle.responseId,
        message: 'Deferred response handle expired',
      })
    }
  })
  const terminal = (body: typeof ResponseSchema.Type) =>
    body.status === 'failed' || body.status === 'cancelled'
      ? Effect.fail(
          new Model.DeferredTerminalError({
            reason: body.status,
            responseId: body.id,
            message: body.error?.message ?? `Deferred response ${body.status}`,
          }),
        )
      : Effect.void

  // A pending response has no native GenerateTextResponse representation. This private token
  // exits only the local native conversion call; its single identity-checked defect is caught
  // before leaving this adapter. All unrelated defects, errors and interruptions are preserved.
  const convert = Effect.fnUntraced(function* <Request extends Model.DeferredRequest<any>>(
    request: Request,
    receive: (
      payload: typeof OpenAiSchema.CreateResponse.Encoded,
    ) => Effect.Effect<
      readonly [OpenAiSchema.Response, HttpClientResponse.HttpClientResponse],
      Model.DeferredError
    >,
  ): Effect.fn.Return<Model.Complete, Model.DeferredError, Model.DeferredRequestServices<Request>> {
    const failureToken = {}
    let failure: Model.DeferredError | undefined
    const client = OpenAiClient.OpenAiClient.of({
      client: native.client,
      createResponse: (payload) =>
        receive(payload).pipe(
          Effect.catch((error) => {
            failure = error
            return Effect.die(failureToken)
          }),
        ),
      createResponseStream: native.createResponseStream.bind(native),
      createEmbedding: native.createEmbedding.bind(native),
    })
    const languageModel = yield* NativeModel.make(options).pipe(
      Effect.provideService(OpenAiClient.OpenAiClient, ToolResult.client(client, options.config)),
    )
    const generation: {
      readonly prompt: Request['prompt']
      readonly toolkit: Request['toolkit']
      readonly toolChoice: Request['toolChoice']
      readonly disableToolCallResolution: true
    } = {
      prompt: request.prompt,
      toolkit: request.toolkit,
      toolChoice: request.toolChoice,
      disableToolCallResolution: true as const,
    }
    return yield* languageModel.generateText(generation).pipe(
      Effect.map((response): Model.Complete => ({ _tag: 'Complete', response })),
      Effect.catchCause((cause) => {
        const defect = Cause.findDefect(cause)
        return cause.reasons.length === 1 &&
          Result.isSuccess(defect) &&
          defect.success === failureToken &&
          failure !== undefined
          ? Effect.fail(failure)
          : Effect.failCause(cause)
      }),
    )
  })
  return yield* Model.makeDeferred<typeof HandleSchema, never>({
    handle: HandleSchema,
    // The exact native client is already captured above; retain no ambient default Config.
    capture: Effect.succeed(Context.empty()),
    start: <Request extends Model.DeferredRequest<any>>(request: Request) =>
      Effect.suspend(() => {
        const token = {}
        let pending: Model.DeferredHandle<Handle> | undefined
        return convert(
          request,
          Effect.fnUntraced(function* (payload) {
            if (payload.background === false || payload.store === false || payload.stream === true)
              return yield* unsupported(
                'Deferred responses require background=true, store=true and stream=false',
              )
            const [body, response] = yield* send(
              HttpClientRequest.post('/responses', {
                body: HttpBody.jsonUnsafe({
                  ...payload,
                  background: true,
                  store: true,
                  stream: false,
                }),
              }),
            )
            yield* terminal(body)
            if (body.status === 'queued' || body.status === 'in_progress') {
              const handle: Handle = {
                version: 1,
                provider: 'openai',
                modelId: options.model,
                responseId: body.id,
                requestId: request.requestId,
                attempt: request.attempt,
                ...(body.expires_at === undefined ? {} : { expiresAt: body.expires_at * 1000 }),
              }
              pending = {
                _tag: 'Deferred',
                handle,
                nextPollAt: yield* nextDeadline(),
                ...(handle.expiresAt === undefined ? {} : { expiresAt: handle.expiresAt }),
              }
              return yield* Effect.die(token)
            }
            return [body, response]
          }),
        ).pipe(
          Effect.catchCause((cause) => {
            const defect = Cause.findDefect(cause)
            return cause.reasons.length === 1 &&
              Result.isSuccess(defect) &&
              defect.success === token &&
              pending !== undefined
              ? Effect.succeed(pending)
              : Effect.failCause(cause)
          }),
        )
      }),
    poll: Effect.fnUntraced(function* <Request extends Model.DeferredRequest<any>>(
      handle: Handle,
      previous: number,
      request: Request,
    ): Effect.fn.Return<
      Model.Complete | Model.Pending,
      Model.DeferredError,
      Model.DeferredRequestServices<Request>
    > {
      yield* check(handle, request)
      const [body, response] = yield* send(
        HttpClientRequest.get(`/responses/${encodeURIComponent(handle.responseId)}`),
      )
      if (body.id !== handle.responseId)
        return yield* invalid('Retrieved response id differs from deferred handle')
      yield* terminal(body)
      if (body.status === 'queued' || body.status === 'in_progress')
        return { _tag: 'Pending', nextPollAt: yield* nextDeadline(previous) }
      return yield* convert(request, () => Effect.succeed([body, response]))
    }),
    cancel: Effect.fnUntraced(function* (handle) {
      // Expired handles may still be cancelled remotely; expiry only prohibits polling.
      if (handle.modelId !== options.model)
        return yield* unsupported('Deferred handle belongs to a different model')
      const [body] = yield* send(
        HttpClientRequest.post(`/responses/${encodeURIComponent(handle.responseId)}/cancel`),
      )
      if (body.id !== handle.responseId)
        return yield* invalid('Cancelled response id differs from deferred handle')
      if (body.status === 'failed') return yield* terminal(body)
      if (body.status === 'queued' || body.status === 'in_progress')
        return yield* invalid('Remote response remains pending after cancellation')
    }),
  })
})
