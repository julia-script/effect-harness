/**
 * Reading a transcript, ported from pi-ai's `utils/transcript.ts`: the tools and system prompt in
 * effect after every system message, for providers that take them outside the message list, and
 * the transcript as another model can read it after a model switch.
 */
import type {
  AssistantContent,
  AssistantMessage,
  ImageContent,
  Message,
  SystemMessage,
  TextContent,
  ToolCall,
  ToolDefinition,
} from './Message.ts'

/** The text blocks of `content`, joined by `separator`. */
export const contentText = (
  content: string | ReadonlyArray<TextContent | ImageContent>,
  separator = '\n',
): string =>
  typeof content === 'string'
    ? content
    : content.flatMap((block) => (block.type === 'text' ? [block.text] : [])).join(separator)

/** A system message as a whole prompt: its content followed by its sections. */
export const systemText = (message: SystemMessage): string => {
  const parts = [contentText(message.content)]
  for (const text of Object.values(message.sections ?? {})) if (text !== null) parts.push(text)
  return parts.filter((part) => part.length > 0).join('\n\n')
}

const systemMessages = (messages: ReadonlyArray<Message>) =>
  messages.filter((message): message is SystemMessage => message.role === 'system')

/** The tools available after every system message, in the order they were last added. */
export const tools = (messages: ReadonlyArray<Message>): ReadonlyArray<ToolDefinition> => {
  const current = new Map<string, ToolDefinition>()
  for (const message of systemMessages(messages)) {
    for (const tool of message.toolsRemoved ?? []) current.delete(tool.name)
    for (const tool of message.toolsAdded ?? []) current.set(tool.name, tool)
  }
  return [...current.values()]
}

/**
 * Every system message replayed into one: later content appended to the prompt, sections patched
 * by name, and the current tools. `undefined` when the transcript has neither a prompt nor tools.
 */
export const systemMessage = (messages: ReadonlyArray<Message>): SystemMessage | undefined => {
  const content: Array<string> = []
  const sections = new Map<string, string>()
  let timestamp: number | undefined
  for (const message of systemMessages(messages)) {
    timestamp ??= message.timestamp
    const text = contentText(message.content)
    if (text.length > 0) content.push(text)
    for (const [name, value] of Object.entries(message.sections ?? {})) {
      if (value === null) sections.delete(name)
      else sections.set(name, value)
    }
  }
  const current = tools(messages)
  if (timestamp === undefined && current.length === 0) return undefined
  return {
    role: 'system',
    content: content.join('\n\n'),
    ...(sections.size > 0 ? { sections: Object.fromEntries(sections) } : {}),
    ...(current.length > 0 ? { toolsAdded: current } : {}),
    timestamp: timestamp ?? 0,
  }
}

/** The system prompt in effect after every system message. */
export const systemPrompt = (messages: ReadonlyArray<Message>): string => {
  const message = systemMessage(messages)
  return message === undefined ? '' : systemText(message)
}

/**
 * The transcript for providers without system messages mid-conversation: the replayed system
 * message leads, and every other one is dropped.
 */
export const collapse = (messages: ReadonlyArray<Message>): ReadonlyArray<Message> => {
  const head = systemMessage(messages)
  const rest = messages.filter((message) => message.role !== 'system')
  return head === undefined ? rest : [head, ...rest]
}

/** The tool calls of a response, in order. */
export const toolCalls = (message: AssistantMessage): ReadonlyArray<ToolCall> =>
  message.content.filter((content): content is ToolCall => content.type === 'toolCall')

/** The model a transcript is replayed to: `Model.Info`'s fields that decide what it can read. */
export interface Reader {
  readonly provider: string
  readonly api: string
  readonly id: string
  readonly input: ReadonlyArray<'text' | 'image'>
}

/**
 * APIs whose models read each other's reasoning from the same account, or drop what they cannot
 * read themselves: Claude thinking goes back unchanged after a switch, as Anthropic asks.
 */
const sharedReasoning = new Set(['anthropic-messages'])

/** Characters every provider accepts in a tool call ID, and the length they all allow. */
const SAFE_ID = /^[\w-]{1,64}$/u
const UNSAFE_CHARACTERS = /[^\w-]/gu
const MAX_ID_LENGTH = 64

/** IDs of other models' tool calls that some provider would refuse, rewritten to safe ones. */
const safeIds = (
  messages: ReadonlyArray<Message>,
  foreign: (message: AssistantMessage) => boolean,
) => {
  const taken = new Set<string>()
  for (const message of messages)
    if (message.role === 'assistant') for (const call of toolCalls(message)) taken.add(call.id)
  const renamed = new Map<string, string>()
  for (const message of messages) {
    if (message.role !== 'assistant' || !foreign(message)) continue
    for (const { id } of toolCalls(message)) {
      if (SAFE_ID.test(id) || renamed.has(id)) continue
      const base = (id.replace(UNSAFE_CHARACTERS, '_') || 'call').slice(0, MAX_ID_LENGTH - 4)
      let candidate = base
      for (let n = 1; taken.has(candidate); n += 1) candidate = `${base}_${n}`
      taken.add(candidate)
      renamed.set(id, candidate)
    }
  }
  return renamed
}

/** `content` with images replaced by a note, for a model that reads text only. */
const withoutImages = <A extends TextContent | ImageContent>(
  content: ReadonlyArray<A>,
  reader: Reader,
): ReadonlyArray<A | TextContent> =>
  content.map((part) =>
    part.type === 'image'
      ? { type: 'text', text: `[An image was here; ${reader.id} does not read images.]` }
      : part,
  )

/**
 * The transcript as `reader` can take it, after other models wrote some of it. Responses `reader`
 * wrote itself go back unchanged. From any other model, the request keeps visible text, tool calls
 * and tool results, and leaves out what only the writer can use: empty text blocks, provider item
 * IDs, Gemini thought signatures, and reasoning (except Claude thinking sent to Claude on the same
 * account, which the API keeps or drops itself). Tool call IDs another provider would refuse are
 * rewritten, with their results, and images become a note when `reader` reads text only. Stored
 * messages never change; this is applied to each request.
 */
export const replay = (
  messages: ReadonlyArray<Message>,
  reader: Reader,
): ReadonlyArray<Message> => {
  const foreign = (message: AssistantMessage) =>
    message.provider !== reader.provider ||
    message.api !== reader.api ||
    message.model !== reader.id
  const keepsReasoning = (message: AssistantMessage) =>
    message.provider === reader.provider &&
    message.api === reader.api &&
    sharedReasoning.has(reader.api)
  const renamed = safeIds(messages, foreign)
  const images = reader.input.includes('image')
  const rename = (id: string) => renamed.get(id) ?? id
  return messages.map((message): Message => {
    switch (message.role) {
      case 'assistant': {
        if (!foreign(message)) return message
        const reasoning = keepsReasoning(message)
        const content = message.content.flatMap((block): ReadonlyArray<AssistantContent> => {
          if (block.type === 'thinking') return reasoning ? [block] : []
          if (block.type === 'text')
            return block.text.length === 0 ? [] : [{ type: 'text', text: block.text }]
          const { thoughtSignature: _signature, ...call } = block
          return [{ ...call, id: rename(block.id) }]
        })
        return { ...message, content }
      }
      case 'toolResult':
        return {
          ...message,
          toolCallId: rename(message.toolCallId),
          content: images ? message.content : withoutImages(message.content, reader),
        }
      case 'user':
        return images || typeof message.content === 'string'
          ? message
          : { ...message, content: withoutImages(message.content, reader) }
      default:
        return message
    }
  })
}
