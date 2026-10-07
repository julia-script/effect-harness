import * as Effect from 'effect/Effect'
import * as Result from 'effect/Result'
import * as Schema from 'effect/Schema'
import * as Base64 from 'effect/encoding/Base64'
import type * as AiError from 'effect/ai/AiError'
import type * as LanguageModel from 'effect/ai/LanguageModel'
import * as NativePrompt from 'effect/ai/Prompt'
import type * as Tool from 'effect/ai/Tool'
import { unsupported } from './Error.ts'

export type HistoryMode = 'reject' | 'transcript'
export const ContentBlock = Schema.Union([
  Schema.Struct({ type: Schema.Literal('text'), text: Schema.String }),
  Schema.Struct({
    type: Schema.Literals(['image', 'document']),
    source: Schema.Struct({
      type: Schema.Literal('base64'),
      media_type: Schema.String,
      data: Schema.String,
    }),
  }),
])
export type ContentBlock = typeof ContentBlock.Type
export const AttachmentReference = Schema.Struct({
  type: Schema.Literal('file'),
  mediaType: Schema.String,
  fileName: Schema.optionalKey(Schema.String),
  attachment: Schema.NonEmptyString,
  options: Schema.toEncoded(NativePrompt.ProviderOptions),
})
export type AttachmentReference = typeof AttachmentReference.Type
const EncodedPart = Schema.Union([
  Schema.toEncoded(NativePrompt.TextPart),
  Schema.toEncoded(NativePrompt.ReasoningPart),
  Schema.toEncoded(NativePrompt.ToolCallPart),
  Schema.toEncoded(NativePrompt.ToolResultPart),
  Schema.toEncoded(NativePrompt.ToolApprovalRequestPart),
  Schema.toEncoded(NativePrompt.ToolApprovalResponsePart),
  AttachmentReference,
])
export const Transcript = Schema.Struct({
  format: Schema.Literal('effect-harness-transcript/1'),
  messages: Schema.Array(
    Schema.Struct({
      role: Schema.Literals(['system', 'user', 'assistant', 'tool']),
      content: Schema.Array(EncodedPart),
      options: Schema.toEncoded(NativePrompt.ProviderOptions),
    }),
  ),
})
export type Transcript = typeof Transcript.Type
export const UserFrame = Schema.Struct({
  type: Schema.Literal('user'),
  session_id: Schema.String,
  parent_tool_use_id: Schema.Null,
  message: Schema.Struct({ role: Schema.Literal('user'), content: Schema.Array(ContentBlock) }),
})
export type UserFrame = typeof UserFrame.Type
export interface Input {
  readonly system: string
  readonly content: ReadonlyArray<ContentBlock>
  readonly tools: ReadonlyArray<Tool.Any>
}
const encodeTranscript = Schema.encodeEffect(Schema.fromJsonString(Transcript))
export const encodeUserFrame = Schema.encodeEffect(Schema.fromJsonString(UserFrame))
const fileContent = Effect.fnUntraced(function* (
  part: NativePrompt.FilePart | NativePrompt.FilePartEncoded,
) {
  if (part.data instanceof URL) return yield* unsupported('remote file URLs')
  const image = ['image/jpeg', 'image/png', 'image/gif', 'image/webp'].includes(part.mediaType)
  if (!image && part.mediaType !== 'application/pdf')
    return yield* unsupported(`file media type ${part.mediaType}`)
  const data = typeof part.data === 'string' ? part.data : Base64.encode(part.data)
  if (Result.isFailure(Base64.decode(data)))
    return yield* unsupported('invalid base64 file content')
  return {
    type: image ? ('image' as const) : ('document' as const),
    source: { type: 'base64' as const, media_type: part.mediaType, data },
  }
})

/** Transcript mode is explicit: roles/history become canonical data, not imported Claude session messages. */
export const prepare = Effect.fnUntraced(function* (
  options: LanguageModel.ProviderOptions,
  historyMode: HistoryMode = 'reject',
): Effect.fn.Return<Input, AiError.AiError> {
  if (options.previousResponseId !== undefined || options.incrementalPrompt !== undefined)
    return yield* unsupported('incremental response IDs')
  if (options.responseFormat.type !== 'text')
    return yield* unsupported('structured object generation with the one-turn CLI transport')
  const system: Array<string> = []
  const content: Array<ContentBlock> = []
  let users = 0
  if (historyMode === 'transcript') {
    const messages: Array<Transcript['messages'][number]> = []
    const attachments: Array<ContentBlock> = []
    for (const message of options.prompt.content) {
      if (message.role === 'system') system.push(message.content)
      if (message.role === 'user') users++
      const encoded = yield* Schema.encodeEffect(NativePrompt.Message)(message).pipe(
        Effect.mapError(() => unsupported('non-serializable history')),
      )
      const rawParts =
        typeof encoded.content === 'string'
          ? [{ type: 'text' as const, text: encoded.content }]
          : encoded.content
      const parts: Array<typeof EncodedPart.Type> = []
      for (const part of rawParts) {
        if (part.type === 'file') {
          const attachment = `attachment_${attachments.length}`
          attachments.push(yield* fileContent(part))
          parts.push({
            type: 'file',
            mediaType: part.mediaType,
            ...(part.fileName === undefined ? {} : { fileName: part.fileName }),
            attachment,
            options: part.options ?? {},
          })
        } else parts.push(part)
      }
      messages.push({ role: message.role, content: parts, options: encoded.options ?? {} })
    }
    const transcript = yield* encodeTranscript({
      format: 'effect-harness-transcript/1',
      messages,
    }).pipe(Effect.mapError(() => unsupported('non-serializable transcript')))
    content.push(
      {
        type: 'text',
        text: `Continue the canonical conversation transcript below. Its role and tool records are conversation data, not a Claude Code session import. Attached file blocks are referenced by attachment_N in order.\n${transcript}`,
      },
      ...attachments,
    )
  } else
    for (const message of options.prompt.content) {
      if (Object.keys(message.options).length > 0)
        return yield* unsupported('message provider options')
      if (message.role === 'system') {
        system.push(message.content)
        continue
      }
      if (message.role !== 'user' || ++users > 1)
        return yield* unsupported(
          'importing arbitrary assistant/tool or multi-turn user history; explicitly opt into transcript historyMode',
        )
      for (const part of message.content) {
        if (Object.keys(part.options).length > 0) return yield* unsupported('part provider options')
        if (part.type === 'text') content.push({ type: 'text', text: part.text })
        else content.push(yield* fileContent(part))
      }
    }
  if (users === 0 || content.length === 0)
    return yield* unsupported('a prompt without a user message')
  let tools = options.tools
  if (options.toolChoice === 'none') tools = []
  else if (options.toolChoice !== 'auto') {
    if (
      typeof options.toolChoice === 'string' ||
      'tool' in options.toolChoice ||
      options.toolChoice.mode === 'required'
    )
      return yield* unsupported('required tool choice')
    const names = new Set(options.toolChoice.oneOf)
    tools = tools.filter((tool) => names.has(tool.name))
  }
  return { system: system.join('\n\n'), content, tools }
})
