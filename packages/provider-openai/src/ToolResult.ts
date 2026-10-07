import * as OpenAiClient from '@effect/ai-openai/OpenAiClient'
import * as OpenAiLanguageModel from '@effect/ai-openai/OpenAiLanguageModel'
import * as OpenAiSchema from '@effect/ai-openai/OpenAiSchema'
import * as Canonical from '@effect-harness/harness/ToolResult'
import * as Effect from 'effect/Effect'
import * as Option from 'effect/Option'
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
  Schema.fromJsonString(Schema.toCodecJson(Canonical.Envelope)),
)
const decode = (value: string) => Effect.succeed(decodeEnvelope(value))
const base64 = (data: string | Uint8Array) =>
  typeof data === 'string' ? data.replace(/^data:[^;]+;base64,/, '') : Base64.encode(data)

/** Keeps canonical tool content inside the native function output, including mixed media and part options. */
export const content = Effect.fnUntraced(function* (
  parts: ReadonlyArray<Prompt.UserMessagePart>,
  prefixes: ReadonlyArray<string> = [],
): Effect.fn.Return<Array<typeof OpenAiSchema.InputContent.Encoded>, AiError.AiError> {
  return yield* Effect.forEach(parts, (part, index) => {
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

/** Translates validated markers in function outputs; all other native input items remain unchanged. */
export const request = Effect.fnUntraced(function* (
  payload: typeof OpenAiSchema.CreateResponse.Encoded,
  prefixes: ReadonlyArray<string> = [],
): Effect.fn.Return<typeof OpenAiSchema.CreateResponse.Encoded, AiError.AiError> {
  if (payload.input == null || typeof payload.input === 'string') return payload
  const input = yield* Effect.forEach(
    payload.input,
    Effect.fnUntraced(function* (item) {
      if (item.type !== 'function_call_output' || typeof item.output !== 'string') return item
      const envelope = yield* decode(item.output)
      if (Option.isNone(envelope)) return item
      return { ...item, output: yield* content(envelope.value.content, prefixes) }
    }),
  )
  return { ...payload, input }
})

/** Adapts a captured native client without altering its model/stream/tool generics or embeddings. */
export const client = (
  native: OpenAiClient.Service,
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
    ...native,
    createResponse: Effect.fnUntraced(function* (payload) {
      const translated = yield* translate(payload)
      return yield* native.createResponse(translated)
    }),
    createResponseStream: Effect.fnUntraced(function* (payload) {
      const translated = yield* translate(payload)
      return yield* native.createResponseStream(translated)
    }),
  })
}
