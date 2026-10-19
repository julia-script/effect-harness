/**
 * A `Provider` over an effect/ai `LanguageModel`. Each request gets its `LanguageModel` from
 * `languageModel`, so a credential read there is fresh for every request. Tools are declared from
 * the transcript with tool call resolution off: a response that calls tools ends with `toolUse`,
 * and the loop runs the calls.
 */
import { Clock, Effect, Stream } from 'effect'
import type { LanguageModel } from 'effect/ai'
import type { Scope } from 'effect'
import { fromAiError, events } from './internal/languageModel/response.ts'
import { prompt, toolkit } from './internal/languageModel/prompt.ts'
import type { Definition, Info, Provider, Request } from './Model.ts'
import { define } from './Model.ts'
import type { ModelError } from './ModelError.ts'

export interface Options<R = never> {
  /** The provider name requests route by, such as `anthropic`. */
  readonly name: string
  /** The API its responses name, such as `anthropic-messages`. */
  readonly api: string
  readonly models: ReadonlyArray<Definition>
  /**
   * Where the system prompt goes. Default `prompt`, as the prompt's first message; `separate`
   * leaves it out, for an API that takes it as a request field the `languageModel` sets.
   */
  readonly system?: 'prompt' | 'separate'
  /**
   * The `LanguageModel` for one request to `model`, configured from the request's options: read
   * the credential here. Its scope closes when the response ends.
   */
  readonly languageModel: (
    model: Info,
    request: Request,
  ) => Effect.Effect<LanguageModel.LanguageModel, ModelError, R | Scope.Scope>
}

/** A provider that needs what `languageModel` needs. */
export const make = <R = never>(options: Options<R>): Provider<Exclude<R, Scope.Scope>> => ({
  name: options.name,
  models: options.models.map((definition) => define(options.name, options.api, definition)),
  stream: (request, model) =>
    Stream.unwrap(
      Effect.gen(function* () {
        const service = yield* options.languageModel(model, request)
        const timestamp = yield* Clock.currentTimeMillis
        const parts = service
          .streamText({
            prompt: prompt(request.messages, options.system !== 'separate'),
            toolkit: toolkit(request.messages),
            disableToolCallResolution: true,
            toolCallValidation: 'deferred',
            ...(request.options?.toolChoice === undefined
              ? {}
              : { toolChoice: request.options.toolChoice }),
          })
          .pipe(Stream.mapError(fromAiError))
        return events(parts, model, timestamp)
      }),
    ),
})
