/**
 * Reading a transcript, ported from pi-ai's `utils/transcript.ts`: the tools and system prompt in
 * effect after every system message, for providers that take them outside the message list.
 */
import type {
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
