/**
 * Canonical tool-media translation at the captured native client boundary.
 */
import * as Arr from 'effect/Array'
import * as String from 'effect/String'
import { constant } from 'effect/Function'
import * as AnthropicClient from '@effect/ai-anthropic/AnthropicClient'
import * as Generated from '@effect/ai-anthropic/Generated'
import * as ToolResult from 'effect-harness/ToolResult'
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
const decodeEnvelope = Schema.decodeUnknownOption(
  Schema.fromJsonString(Schema.toCodecJson(ToolResult.Envelope)),
)
const decode = (value: string) => Effect.sync(constant(decodeEnvelope(value)))
const encoded = (data: string | Uint8Array) =>
  typeof data === 'string' ? data.replace(/^data:[^;]+;base64,/, '') : Base64.encode(data)
const url = (data: Prompt.FilePart['data']): Option.Option<string> => {
  if (data instanceof URL) return Option.some(data.href)
  if (typeof data === 'string' && /^https?:\/\//i.test(data)) return Option.some(data)
  return Option.none()
}

/** Converts only the canonical envelope into schema-validated native tool-result blocks. */
const contentImpl = (
  self: ReadonlyArray<Prompt.UserMessagePart>,
): Effect.Effect<Array<Block>, AiError.AiError> =>
  Effect.suspend(() =>
    Effect.forEach(self, (part): Effect.Effect<Block, AiError.AiError> => {
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
            source: Option.match(location, {
              onNone: () => ({
                type: 'base64',
                media_type: part.mediaType === 'image/*' ? 'image/jpeg' : part.mediaType,
                data: encoded(data),
              }),
              onSome: (url) => ({ type: 'url', url }),
            }),
          }
        else if (part.mediaType === 'application/pdf' || part.mediaType === 'text/plain') {
          let source: unknown
          source = Option.match(location, {
            onSome: (url) => ({ type: 'url', url }),
            onNone: () =>
              part.mediaType === 'application/pdf'
                ? { type: 'base64', media_type: 'application/pdf', data: encoded(data) }
                : {
                    type: 'text',
                    media_type: 'text/plain',
                    data: typeof data === 'string' ? data : new TextDecoder().decode(data),
                  },
          })
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
    }),
  )

/** Expands a validated marker without changing message roles, tool IDs, error flags or outer cache options. */
const requestImpl = Effect.fnUntraced(function* (
  self: Parameters<AnthropicClient.Service['createMessage']>[0],
): Effect.fn.Return<Parameters<AnthropicClient.Service['createMessage']>[0], AiError.AiError> {
  let documents = false
  const messages = yield* Effect.forEach(
    self.payload.messages,
    Effect.fnUntraced(function* (message) {
      if (typeof message.content === 'string') return message
      const blocks = yield* Effect.forEach(
        message.content,
        Effect.fnUntraced(function* (block) {
          if (block.type !== 'tool_result' || typeof block.content !== 'string') return block
          const envelope = yield* decode(block.content)
          return yield* Option.match(envelope, {
            onNone: () => Effect.succeed(block),
            onSome: (envelope) =>
              content(envelope.content).pipe(
                Effect.map((translated) => {
                  documents ||= translated.some((item) => item.type === 'document')
                  return { ...block, content: translated }
                }),
              ),
          })
        }),
      )
      return { ...message, content: blocks }
    }),
  )
  const betas = Arr.dedupe(
    Arr.filter(String.split(self.params?.['anthropic-beta'] ?? '', ','), String.isNonEmpty),
  )
  const requestedBetas = documents ? Arr.union(betas, ['pdfs-2024-09-25']) : betas
  return {
    ...self,
    payload: { ...self.payload, messages },
    ...(documents
      ? { params: { ...self.params, 'anthropic-beta': requestedBetas.join(',') } }
      : {}),
  }
})

/** Captured self client adapter; unrelated endpoints and generic streaming capabilities remain self. */
const clientImpl = (self: AnthropicClient.Service): AnthropicClient.Service =>
  AnthropicClient.AnthropicClient.of({
    ...self,
    createMessage: Effect.fnUntraced(function* (options) {
      const translated = yield* request(options)
      return yield* self.createMessage(translated)
    }),
    createMessageStream: Effect.fnUntraced(function* (options) {
      const translated = yield* request(options)
      return yield* self.createMessageStream(translated)
    }),
  })

/**
 * Translates canonical content into validated native provider parts.
 *
 * @category combinators
 */
export const content: (
  self: ReadonlyArray<Prompt.UserMessagePart>,
) => ReturnType<typeof contentImpl> = contentImpl
/**
 * Projects a request through the provider boundary without changing opaque protocol fields.
 *
 * @category combinators
 */
export const request: (
  self: Parameters<AnthropicClient.Service['createMessage']>[0],
) => ReturnType<typeof requestImpl> = requestImpl
/**
 * Adapts the exact captured native client while preserving unrelated capabilities.
 *
 * @category combinators
 */
export const client: (self: AnthropicClient.Service) => AnthropicClient.Service = clientImpl
