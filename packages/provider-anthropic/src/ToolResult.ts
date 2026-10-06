import * as AnthropicClient from '@effect/ai-anthropic/AnthropicClient'
import * as Generated from '@effect/ai-anthropic/Generated'
import * as Canonical from '@effect-harness/harness/ToolResult'
import * as Effect from 'effect/Effect'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as AiError from 'effect/ai/AiError'
import type * as Prompt from 'effect/ai/Prompt'
import * as Base64 from 'effect/encoding/Base64'

type Block =
  | typeof Generated.BetaRequestTextBlock.Encoded
  | typeof Generated.BetaRequestImageBlock.Encoded
  | typeof Generated.BetaRequestDocumentBlock.Encoded
const invalid = (description: string) =>
  new AiError.AiError({
    module: 'AnthropicToolResult',
    method: 'translate',
    reason: new AiError.InvalidUserInputError({ description }),
  })
const decode = (value: string) =>
  Effect.try({
    try: (): unknown => JSON.parse(value),
    catch: () => undefined,
  }).pipe(Effect.flatMap(Canonical.decode), Effect.option)
const encoded = (data: string | Uint8Array) =>
  typeof data === 'string' ? data.replace(/^data:[^;]+;base64,/, '') : Base64.encode(data)
const url = (data: Prompt.FilePart['data']): string | undefined => {
  if (data instanceof URL) return data.href
  if (typeof data === 'string' && /^https?:\/\//i.test(data)) return data
  return undefined
}

/** Converts only the canonical envelope into schema-validated native tool-result blocks. */
export const content = Effect.fnUntraced(function* (parts: ReadonlyArray<Prompt.UserMessagePart>) {
  return yield* Effect.forEach(parts, (part): Effect.Effect<Block, AiError.AiError> => {
    const cache_control = part.options.anthropic?.cacheControl ?? null
    let block: unknown
    if (part.type === 'text') block = { type: 'text', text: part.text, cache_control }
    else {
      const location = url(part.data)
      const data = part.data instanceof URL ? part.data.href : part.data
      if (part.mediaType.startsWith('image/'))
        block = {
          type: 'image',
          cache_control,
          source:
            location === undefined
              ? {
                  type: 'base64',
                  media_type: part.mediaType === 'image/*' ? 'image/jpeg' : part.mediaType,
                  data: encoded(data),
                }
              : { type: 'url', url: location },
        }
      else if (part.mediaType === 'application/pdf' || part.mediaType === 'text/plain') {
        let source: unknown
        if (location !== undefined) source = { type: 'url', url: location }
        else if (part.mediaType === 'application/pdf')
          source = { type: 'base64', media_type: 'application/pdf', data: encoded(data) }
        else
          source = {
            type: 'text',
            media_type: 'text/plain',
            data: typeof data === 'string' ? data : new TextDecoder().decode(data),
          }
        block = {
          type: 'document',
          cache_control,
          title: part.options.anthropic?.documentTitle ?? part.fileName ?? null,
          ...(part.options.anthropic?.documentContext == null
            ? {}
            : { context: part.options.anthropic.documentContext }),
          ...(part.options.anthropic?.citations?.enabled === true
            ? { citations: { enabled: true } }
            : {}),
          source,
        }
      } else return Effect.fail(invalid(`Unsupported tool-result media type: ${part.mediaType}`))
    }
    return Schema.decodeUnknownEffect(
      Schema.Union([
        Generated.BetaRequestTextBlock,
        Generated.BetaRequestImageBlock,
        Generated.BetaRequestDocumentBlock,
      ]),
    )(block).pipe(Effect.mapError(() => invalid('Invalid tool-result media or provider options')))
  })
})

/** Expands a validated marker without changing message roles, tool IDs, error flags or outer cache options. */
export const request = Effect.fnUntraced(function* (
  options: Parameters<AnthropicClient.Service['createMessage']>[0],
) {
  let documents = false
  const messages = yield* Effect.forEach(options.payload.messages, (message) =>
    Effect.gen(function* () {
      if (typeof message.content === 'string') return message
      const blocks = yield* Effect.forEach(message.content, (block) =>
        Effect.gen(function* () {
          if (block.type !== 'tool_result' || typeof block.content !== 'string') return block
          const envelope = yield* decode(block.content)
          if (Option.isNone(envelope)) return block
          const translated = yield* content(envelope.value.content)
          documents ||= translated.some((item) => item.type === 'document')
          return { ...block, content: translated }
        }),
      )
      return { ...message, content: blocks }
    }),
  )
  const betas = new Set(options.params?.['anthropic-beta']?.split(',').filter(Boolean))
  if (documents) betas.add('pdfs-2024-09-25')
  return {
    ...options,
    payload: { ...options.payload, messages },
    ...(documents ? { params: { ...options.params, 'anthropic-beta': [...betas].join(',') } } : {}),
  }
})

/** Captured native client adapter; unrelated endpoints and generic streaming capabilities remain native. */
export const client = (native: AnthropicClient.Service): AnthropicClient.Service =>
  AnthropicClient.AnthropicClient.of({
    ...native,
    createMessage: (options) => request(options).pipe(Effect.flatMap(native.createMessage)),
    createMessageStream: (options) =>
      request(options).pipe(Effect.flatMap(native.createMessageStream)),
  })
