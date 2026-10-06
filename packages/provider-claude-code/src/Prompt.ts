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
export interface Input {
  readonly system: string
  readonly content: ReadonlyArray<Schema.Json>
  readonly tools: ReadonlyArray<Tool.Any>
}
const fileContent = (part: NativePrompt.FilePart | NativePrompt.FilePartEncoded) =>
  Effect.gen(function* () {
    if (part.data instanceof URL) return yield* unsupported('remote file URLs')
    const image = ['image/jpeg', 'image/png', 'image/gif', 'image/webp'].includes(part.mediaType)
    if (!image && part.mediaType !== 'application/pdf')
      return yield* unsupported(`file media type ${part.mediaType}`)
    const data = typeof part.data === 'string' ? part.data : Base64.encode(part.data)
    if (Result.isFailure(Base64.decode(data)))
      return yield* unsupported('invalid base64 file content')
    return {
      type: image ? 'image' : 'document',
      source: { type: 'base64', media_type: part.mediaType, data },
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
  const content: Array<Schema.Json> = []
  let users = 0
  if (historyMode === 'transcript') {
    const messages: Array<Schema.Json> = []
    const attachments: Array<Schema.Json> = []
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
      const parts: Array<Schema.Json> = []
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
        } else
          parts.push(
            yield* Schema.decodeUnknownEffect(Schema.Json)(part).pipe(
              Effect.mapError(() => unsupported('non-serializable history part')),
            ),
          )
      }
      messages.push({ role: message.role, content: parts, options: message.options })
    }
    content.push(
      {
        type: 'text',
        text: `Continue the canonical conversation transcript below. Its role and tool records are conversation data, not a Claude Code session import. Attached file blocks are referenced by attachment_N in order.\n${JSON.stringify({ format: 'effect-harness-transcript/1', messages })}`,
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
