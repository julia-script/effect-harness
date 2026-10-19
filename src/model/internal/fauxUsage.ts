/**
 * The `Faux` provider's usage estimate, ported from pi-ai's faux provider: four characters a
 * token, and with a session ID the common prefix with that session's previous prompt counts as a
 * cache read.
 */
import type {
  AssistantContent,
  AssistantMessage,
  ImageContent,
  Message,
  TextContent,
} from '../Message.ts'
import type { Request } from '../Model.ts'
import { systemText } from '../Transcript.ts'

export const estimateTokens = (text: string) => Math.ceil(text.length / 4)

const contentToText = (content: string | ReadonlyArray<TextContent | ImageContent>) =>
  typeof content === 'string'
    ? content
    : content
        .map((block) =>
          block.type === 'text' ? block.text : `[image:${block.mimeType}:${block.data.length}]`,
        )
        .join('\n')

export const assistantText = (content: ReadonlyArray<AssistantContent>) =>
  content
    .map((block) => {
      if (block.type === 'text') return block.text
      if (block.type === 'thinking') return block.thinking
      return `${block.name}:${JSON.stringify(block.arguments)}`
    })
    .join('\n')

const messageText = (message: Message): string => {
  if (message.role === 'user') return contentToText(message.content)
  if (message.role === 'assistant') return assistantText(message.content)
  if (message.role === 'toolResult')
    return [message.toolName, ...message.content.map((block) => contentToText([block]))].join('\n')
  return [
    systemText(message),
    ...(message.toolsRemoved ?? []).map((tool) => `tool-:${JSON.stringify(tool)}`),
    ...(message.toolsAdded ?? []).map((tool) => `tool+:${JSON.stringify(tool)}`),
  ]
    .filter((part) => part.length > 0)
    .join('\n')
}

/** Length of the prompt that joins `messages` with blank lines. */
const joinedLength = (messages: ReadonlyArray<string>, count = messages.length) => {
  let length = count > 0 ? (count - 1) * 2 : 0
  for (let index = 0; index < count; index++) length += messages[index]!.length
  return length
}

const commonPrefixLength = (a: string, b: string) => {
  const length = Math.min(a.length, b.length)
  let index = 0
  while (index < length && a[index] === b[index]) index++
  return index
}

/** Common prefix of two joined prompts; equal messages compare whole. */
const commonPromptPrefixLength = (
  previous: ReadonlyArray<string>,
  current: ReadonlyArray<string>,
) => {
  let index = 0
  while (index < previous.length && index < current.length && previous[index] === current[index])
    index++
  const rest = (messages: ReadonlyArray<string>) => {
    if (index === messages.length) return ''
    return (index > 0 ? '\n\n' : '') + messages.slice(index).join('\n\n')
  }
  return joinedLength(previous, index) + commonPrefixLength(rest(previous), rest(current))
}

/** `message` with usage estimated from `request`; `prompts` keeps each session's last prompt. */
export const withUsage = (
  message: AssistantMessage,
  request: Request,
  prompts: Map<string, ReadonlyArray<string>>,
): AssistantMessage => {
  const prompt = request.messages.map((each) => `${each.role}:${messageText(each)}`)
  const promptLength = joinedLength(prompt)
  const promptTokens = Math.ceil(promptLength / 4)
  const output = estimateTokens(assistantText(message.content))
  let input = promptTokens
  let cacheRead = 0
  let cacheWrite = 0
  const sessionId = request.options?.sessionId
  if (sessionId !== undefined && request.options?.cacheRetention !== 'none') {
    const previous = prompts.get(sessionId)
    if (previous === undefined) cacheWrite = promptTokens
    else {
      const cached = commonPromptPrefixLength(previous, prompt)
      cacheRead = Math.ceil(cached / 4)
      cacheWrite = Math.ceil((promptLength - cached) / 4)
      input = Math.max(0, promptTokens - cacheRead)
    }
    prompts.set(sessionId, prompt)
  }
  return {
    ...message,
    usage: {
      input,
      output,
      cacheRead,
      cacheWrite,
      totalTokens: input + output + cacheRead + cacheWrite,
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
    },
  }
}
