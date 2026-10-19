/** Final Chat Completions translation, shared by ordinary and streaming requests. */
import * as Effect from 'effect/Effect'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Base64 from 'effect/encoding/Base64'
import * as AiError from 'effect/ai/AiError'
import type * as Client from '@effect/ai-openai-compat/OpenAiClient'
import * as ToolResult from '../ToolResult.js'

const invalid = (description: string) =>
  new AiError.AiError({
    module: 'OpenAiCompatible',
    method: 'translate',
    reason: new AiError.InvalidRequestError({ description }),
  })
const decodeEnvelope = Schema.decodeUnknownOption(
  Schema.fromJsonString(Schema.toCodecJson(ToolResult.Envelope)),
)
const Reasoning = Schema.Struct({ effort: Schema.NonEmptyString })
const ImageOptions = Schema.Struct({
  imageDetail: Schema.optionalKey(Schema.Literals(['auto', 'low', 'high'])),
})
const content = Effect.fnUntraced(function* (parts: ToolResult.Content) {
  const result: Client.ChatCompletionContentPart[] = []
  for (const part of parts) {
    if (part.type === 'text') {
      result.push({ type: 'text', text: part.text })
      continue
    }
    if (!part.mediaType.startsWith('image/'))
      return yield* invalid(
        `Unsupported Chat Completions tool-result media type: ${part.mediaType}`,
      )
    const options = yield* Schema.decodeUnknownEffect(ImageOptions)(part.options.openai ?? {}).pipe(
      Effect.mapError(() => invalid('Unsupported tool-result image detail')),
    )
    const detail = options.imageDetail ?? 'auto'
    const mediaType = part.mediaType === 'image/*' ? 'image/jpeg' : part.mediaType
    let url: string
    if (part.data instanceof URL) url = part.data.href
    else if (typeof part.data === 'string' && /^(data:|https?:\/\/)/i.test(part.data))
      url = part.data
    else {
      const data = typeof part.data === 'string' ? part.data : Base64.encode(part.data)
      url = `data:${mediaType};base64,${data}`
    }
    result.push({ type: 'image_url', image_url: { url, detail } })
  }
  return result
})

const request = Effect.fnUntraced(function* (
  payload: Client.CreateResponseRequestJson,
  reasoningFormat: 'openai' | 'openrouter',
) {
  const messages: Client.ChatCompletionRequestMessage[] = []
  let pending: Client.ChatCompletionContentPart[] = []
  const flush = () => {
    if (pending.length === 0) return
    messages.push({ role: 'user', content: pending })
    pending = []
  }
  for (const message of payload.messages) {
    if (message.role !== 'tool') {
      flush()
      messages.push(message)
      continue
    }
    const envelope = decodeEnvelope(message.content)
    if (Option.isNone(envelope)) {
      messages.push(message)
      continue
    }
    const parts = yield* content(envelope.value.content)
    if (parts.some((part) => part.type === 'image_url')) {
      // Chat Completions tool messages accept text only. Finish the entire tool
      // result batch before attaching a user media message, preserving call IDs.
      messages.push({
        ...message,
        content: 'Tool result media follows after this tool-result batch.',
      })
      pending.push(
        { type: 'text', text: `Tool result for call ${message.tool_call_id}:` },
        ...parts,
      )
    } else {
      messages.push({
        ...message,
        content: parts.flatMap((part) => (part.type === 'text' ? [part.text] : [])).join('\n'),
      })
    }
  }
  flush()
  const { reasoning, ...rest } = payload
  if (reasoning === undefined) return { ...rest, messages }
  const selected = yield* Schema.decodeUnknownEffect(Reasoning, { onExcessProperty: 'error' })(
    reasoning,
  ).pipe(Effect.mapError(() => invalid('Unsupported gateway reasoning configuration')))
  if (reasoningFormat === 'openrouter') return { ...rest, messages, reasoning: selected }
  return { ...rest, messages, reasoning_effort: selected.effort }
})

/** Keep the captured client's authentication and response handling intact. */
export const client = (self: Client.Service, format: 'openai' | 'openrouter'): Client.Service => ({
  ...self,
  createResponse: (payload) =>
    request(payload, format).pipe(Effect.flatMap((body) => self.createResponse(body))),
  // Native Omit loses model/messages through its string index signature; the
  // native model supplies the same complete Chat Completions request in both paths.
  createResponseStream: (payload) =>
    request(payload as Client.CreateResponseRequestJson, format).pipe(
      Effect.flatMap((body) => self.createResponseStream(body)),
    ),
})
