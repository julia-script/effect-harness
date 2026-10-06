import * as OpenAiClient from '@effect/ai-openai/OpenAiClient'
import * as OpenAiLanguageModel from '@effect/ai-openai/OpenAiLanguageModel'
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
import * as NativeLanguageModel from 'effect/ai/LanguageModel'
import * as HttpClient from 'effect/http/HttpClient'
import * as HttpClientError from 'effect/http/HttpClientError'
import * as HttpClientRequest from 'effect/http/HttpClientRequest'
import { ChatGpt, resource } from './ChatGpt.ts'
import * as ToolResult from './ToolResult.ts'

/** Constructs the native model with canonical tool-media translation at its captured client boundary. */
export const make = Effect.fnUntraced(function* (
  options: Parameters<typeof OpenAiLanguageModel.make>[0],
) {
  const native = yield* OpenAiClient.OpenAiClient
  return yield* OpenAiLanguageModel.make(options).pipe(
    Effect.provideService(OpenAiClient.OpenAiClient, ToolResult.client(native, options.config)),
  )
})

export const layerApiKey = (options: {
  readonly apiKey: Redacted.Redacted<string>
  readonly model: string
  readonly config?: Omit<typeof OpenAiLanguageModel.Config.Service, 'model'> | undefined
  readonly apiUrl?: string | undefined
}) =>
  Layer.effect(
    NativeLanguageModel.LanguageModel,
    make({ model: options.model, config: options.config }),
  ).pipe(Layer.provide(OpenAiClient.layer({ apiKey: options.apiKey, apiUrl: options.apiUrl })))

const authenticationError = (error: AuthError) =>
  new AiError.AiError({
    module: 'ChatGpt',
    method: 'credential',
    reason: new AiError.AuthenticationError({
      kind: 'Unknown',
      description: error.message,
    }),
  })
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
        if (
          event.type === 'response.failed' ||
          event.type === 'response.incomplete' ||
          event.type === 'error'
        )
          return Effect.fail(incomplete())
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

/** Standard OpenAiClient service. Both generated and streamed text use the public streaming Responses endpoint. */
export const layerChatGptClient = (options: { readonly account: string }) =>
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
                      Effect.mapError(() => incomplete()),
                    )
                  : Effect.succeed(last),
            )
            if (Option.isNone(body)) return yield* incomplete()
            return [body.value, response] as const
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

export const layerChatGpt = (options: {
  readonly account: string
  readonly model: string
  readonly config?: Omit<typeof OpenAiLanguageModel.Config.Service, 'model'> | undefined
}) =>
  Layer.effect(
    NativeLanguageModel.LanguageModel,
    make({
      model: options.model,
      config: { ...options.config, store: false },
    }),
  ).pipe(Layer.provide(layerChatGptClient({ account: options.account })))
