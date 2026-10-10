import * as Predicate from 'effect/Predicate'
/**
 * Canonical tool-media translation at the captured native client boundary.
 */
import { dual, constant } from 'effect/Function'
import * as OpenAiClient from '@effect/ai-openai/OpenAiClient'
import * as OpenAiLanguageModel from '@effect/ai-openai/OpenAiLanguageModel'
import * as OpenAiSchema from '@effect/ai-openai/OpenAiSchema'
import * as ToolResult from 'effect-harness/ToolResult'
import * as Effect from 'effect/Effect'
import * as Option from 'effect/Option'
import * as Arr from 'effect/Array'
import * as Schema from 'effect/Schema'
import * as AiError from 'effect/ai/AiError'
import type * as Prompt from 'effect/ai/Prompt'
import * as Base64 from 'effect/encoding/Base64'

const invalid = (description: string) =>
  new AiError.AiError({
    module: 'OpenAiToolResult',
    method: 'translate',
    reason: new AiError.InvalidRequestError({ description }),
  })
const decodeEnvelope = Schema.decodeUnknownOption(
  Schema.fromJsonString(Schema.toCodecJson(ToolResult.Envelope)),
)
const decode = (value: string) => Effect.sync(constant(decodeEnvelope(value)))
const base64 = (data: string | Uint8Array) =>
  typeof data === 'string' ? data.replace(/^data:[^;]+;base64,/, '') : Base64.encode(data)

/** Keeps canonical tool content inside the native function output, including mixed media and part options. */
const isPrefixList = (
  u: ReadonlyArray<string> | { readonly prefixes?: ReadonlyArray<string> | undefined },
): u is ReadonlyArray<string> => Arr.isArray(u)
const contentImpl = (
  self: ReadonlyArray<Prompt.UserMessagePart>,
  options: ReadonlyArray<string> | { readonly prefixes?: ReadonlyArray<string> | undefined } = [],
): Effect.Effect<Array<typeof OpenAiSchema.InputContent.Encoded>, AiError.AiError> =>
  Effect.suspend(() => {
    const prefixes = isPrefixList(options) ? options : (options.prefixes ?? [])
    return Effect.forEach(self, (part, index) => {
      let block: unknown
      if (part.type === 'text')
        block = {
          type: 'input_text',
          text: part.text,
          ...(part.options.openai?.promptCacheBreakpoint == null
            ? {}
            : { prompt_cache_breakpoint: part.options.openai.promptCacheBreakpoint }),
        }
      else {
        const data = part.data
        const file = typeof data === 'string' && prefixes.some((prefix) => data.startsWith(prefix))
        if (part.mediaType.startsWith('image/')) {
          const media = part.mediaType === 'image/*' ? 'image/jpeg' : part.mediaType
          let location: string
          if (data instanceof URL) location = data.href
          else if (typeof data === 'string' && /^(data:|https?:\/\/)/i.test(data)) location = data
          else location = `data:${media};base64,${base64(data)}`
          block = {
            type: 'input_image',
            detail: part.options.openai?.imageDetail ?? 'auto',
            ...(file ? { file_id: data } : { image_url: location }),
          }
        } else if (part.mediaType === 'application/pdf') {
          if (file) block = { type: 'input_file', file_id: data }
          else if (data instanceof URL) block = { type: 'input_file', file_url: data.href }
          else if (typeof data === 'string' && /^https?:\/\//i.test(data))
            block = { type: 'input_file', file_url: data }
          else
            block = {
              type: 'input_file',
              filename: part.fileName ?? `part-${index}.pdf`,
              file_data: `data:application/pdf;base64,${base64(data)}`,
            }
        } else return Effect.fail(invalid(`Unsupported tool-result media type: ${part.mediaType}`))
      }
      return Schema.decodeUnknownEffect(OpenAiSchema.InputContent)(block).pipe(
        Effect.mapError(() => invalid('Invalid tool-result media or provider options')),
      )
    })
  })

/** Translates validated markers in function outputs; all other self input items remain unchanged. */
const requestImpl = Effect.fnUntraced(function* (
  self: typeof OpenAiSchema.CreateResponse.Encoded,
  prefixes: ReadonlyArray<string> = [],
): Effect.fn.Return<typeof OpenAiSchema.CreateResponse.Encoded, AiError.AiError> {
  if (self.input == null || typeof self.input === 'string') return self
  const input = yield* Effect.forEach(
    self.input,
    Effect.fnUntraced(function* (item) {
      if (item.type !== 'function_call_output' || typeof item.output !== 'string') return item
      const envelope = yield* decode(item.output)
      return yield* Option.match(envelope, {
        onNone: () => Effect.succeed(item),
        onSome: (envelope) =>
          content(envelope.content, prefixes).pipe(Effect.map((output) => ({ ...item, output }))),
      })
    }),
  )
  return { ...self, input }
})

/** Adapts a captured self client without altering its model/stream/tool generics or embeddings. */
const clientImpl = (
  self: OpenAiClient.Service,
  defaults?: Pick<typeof OpenAiLanguageModel.Config.Service, 'fileIdPrefixes'>,
): OpenAiClient.Service => {
  const translate = Effect.fnUntraced(function* (
    payload: typeof OpenAiSchema.CreateResponse.Encoded,
  ) {
    const dynamic = yield* Effect.serviceOption(OpenAiLanguageModel.Config)
    const config = { ...defaults, ...Option.getOrUndefined(dynamic) }
    return yield* request(payload, config.fileIdPrefixes ?? [])
  })
  return OpenAiClient.OpenAiClient.of({
    ...self,
    client: self.client,
    createEmbedding: self.createEmbedding.bind(self),
    createResponse: Effect.fnUntraced(function* (payload) {
      const translated = yield* translate(payload)
      return yield* self.createResponse(translated)
    }),
    createResponseStream: Effect.fnUntraced(function* (payload) {
      const translated = yield* translate(payload)
      return yield* self.createResponseStream(translated)
    }),
  })
}

/**
 * Translates canonical content into validated native provider parts.
 *
 * @category combinators
 */
export const content: {
  (options?: {
    readonly prefixes?: ReadonlyArray<string> | undefined
  }): (self: ReadonlyArray<Prompt.UserMessagePart>) => ReturnType<typeof contentImpl>
  (
    self: ReadonlyArray<Prompt.UserMessagePart>,
    prefixes?: ReadonlyArray<string>,
  ): ReturnType<typeof contentImpl>
} = dual((args) => Arr.isArray(args[0]), contentImpl)
/**
 * Projects a request through the provider boundary without changing opaque protocol fields.
 *
 * @category combinators
 */
export const request: {
  (
    prefixes?: ReadonlyArray<string>,
  ): (self: typeof OpenAiSchema.CreateResponse.Encoded) => ReturnType<typeof requestImpl>
  (
    self: typeof OpenAiSchema.CreateResponse.Encoded,
    prefixes?: ReadonlyArray<string>,
  ): ReturnType<typeof requestImpl>
} = dual(
  Predicate.mapInput(Predicate.isObject, (args: IArguments) => args[0]),
  requestImpl,
)
/**
 * Adapts the exact captured native client while preserving unrelated capabilities.
 *
 * @category combinators
 */
export const client: {
  (
    defaults?: Pick<typeof OpenAiLanguageModel.Config.Service, 'fileIdPrefixes'>,
  ): (self: OpenAiClient.Service) => OpenAiClient.Service
  (
    self: OpenAiClient.Service,
    defaults?: Pick<typeof OpenAiLanguageModel.Config.Service, 'fileIdPrefixes'>,
  ): OpenAiClient.Service
} = dual(
  Predicate.mapInput(
    Predicate.and(Predicate.isObjectOrArray, Predicate.hasProperty('createResponseStream')),
    (args: IArguments) => args[0],
  ),
  clientImpl,
)
