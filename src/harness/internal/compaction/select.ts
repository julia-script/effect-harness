/**
 * What a compaction summarizes, and when preparation compacts first (spec §8.3, §8.7). Token
 * counts are estimates, ported from pi-ai's `utils/estimate.ts`.
 */
import type { AssistantMessage, Message, Usage } from '../../../model/Message.ts'
import * as Transcript from '../../../model/Transcript.ts'
import type { Compaction } from '../../Settings.ts'
import { orderToolResults } from '../context.ts'
import type { View } from '../context.ts'

const CHARS_PER_TOKEN = 3.5
const IMAGE_CHARS = 4800

/** The context a response's request held, by its usage. */
export const contextTokens = (usage: Usage): number =>
  usage.totalTokens || usage.input + usage.output + usage.cacheRead + usage.cacheWrite

const textTokens = (text: string) => Math.ceil(text.length / CHARS_PER_TOKEN)

const contentChars = (content: Extract<Message, { role: 'user' | 'toolResult' }>['content']) =>
  typeof content === 'string'
    ? content.length
    : content.reduce(
        (chars, block) => chars + (block.type === 'text' ? block.text.length : IMAGE_CHARS),
        0,
      )

const toolsTokens = (tools: ReadonlyArray<unknown> | undefined) =>
  tools === undefined || tools.length === 0 ? 0 : textTokens(JSON.stringify(tools))

/** About how many tokens `message` takes. */
export const estimateTokens = (message: Message): number => {
  if (message.role === 'system')
    return (
      textTokens(Transcript.systemText(message)) +
      toolsTokens(message.toolsAdded) +
      toolsTokens(message.toolsRemoved)
    )
  if (message.role === 'user' || message.role === 'toolResult')
    return Math.ceil(contentChars(message.content) / CHARS_PER_TOKEN)
  let chars = 0
  for (const block of message.content) {
    if (block.type === 'text') chars += block.text.length
    else if (block.type === 'thinking') chars += block.thinking.length
    else chars += block.name.length + JSON.stringify(block.arguments).length
  }
  return Math.ceil(chars / CHARS_PER_TOKEN)
}

/**
 * The size of a request over `view` followed by `extra`: the usage of the newest assistant
 * message after the head marker, whose request held everything before it, plus estimates of the
 * messages after it; without one, estimates of every message.
 */
export const estimateContext = (view: View, extra: ReadonlyArray<Message>): number => {
  let measured: AssistantMessage | undefined
  const after = view.head?.id ?? Number.NEGATIVE_INFINITY
  for (let index = view.entries.length - 1; index >= 0 && measured === undefined; index--) {
    const entry = view.entries[index]
    if (entry === undefined || entry.id <= after) continue
    measured = view.contributions[index]?.findLast(
      (message): message is AssistantMessage =>
        message.role === 'assistant' && contextTokens(message.usage) > 0,
    )
  }
  const from = measured === undefined ? 0 : view.messages.lastIndexOf(measured) + 1
  let tokens = measured === undefined ? 0 : contextTokens(measured.usage)
  for (const message of view.messages.slice(from)) tokens += estimateTokens(message)
  for (const message of extra) tokens += estimateTokens(message)
  return tokens
}

const callIds = (message: AssistantMessage) =>
  new Set(Transcript.toolCalls(message).map((call) => call.id))

/** The calls of the nearest assistant message before entry `index`. */
const precedingCalls = (contributions: View['contributions'], index: number) => {
  for (let before = index - 1; before >= 0; before--) {
    const assistant = contributions[before]?.findLast(
      (message): message is AssistantMessage => message.role === 'assistant',
    )
    if (assistant !== undefined) return callIds(assistant)
  }
  return new Set<string>()
}

/**
 * Whether a summary may keep entry `index` first: its contribution starts with an assistant
 * message, or with a user message no result of the preceding assistant's calls still follows.
 */
const isCandidate = (contributions: View['contributions'], index: number): boolean => {
  const first = contributions[index]?.[0]
  if (first?.role === 'assistant') return true
  if (first?.role !== 'user') return false
  const calls = precedingCalls(contributions, index)
  if (calls.size === 0) return true
  for (let after = index; after < contributions.length; after++) {
    for (const [position, message] of (contributions[after] ?? []).entries()) {
      if (message.role === 'assistant' && (after > index || position > 0)) return true
      if (message.role === 'toolResult' && calls.has(message.toolCallId)) return false
    }
  }
  return true
}

/**
 * The index in `view.entries` of the first entry a summary keeps, or `undefined` when there is
 * nothing to compact. Walks back from the tail until `keepRecentTokens` are kept, then cuts at the
 * first candidate at or after that entry.
 */
export const selectCut = (view: View, keepRecentTokens: number): number | undefined => {
  const { contributions } = view
  const start = view.head === undefined ? 0 : 1
  const candidates: Array<number> = []
  for (let index = start; index < contributions.length; index++)
    if (isCandidate(contributions, index)) candidates.push(index)
  let kept = 0
  let cut: number | undefined
  for (let index = contributions.length - 1; index >= start && cut === undefined; index--) {
    for (const message of contributions[index] ?? []) kept += estimateTokens(message)
    if (kept >= keepRecentTokens)
      cut = candidates.find((candidate) => candidate >= index) ?? candidates.at(-1)
  }
  if (cut === undefined) return cut
  const summarized = contributions.slice(start, cut).some((messages) => messages.length > 0)
  return summarized ? cut : undefined
}

/** The model messages of the entries before `cut`, the head marker first, ordered as context. */
export const summarizedMessages = (view: View, cut: number): ReadonlyArray<Message> =>
  orderToolResults(view.contributions.slice(0, cut).flat())

/**
 * Which threshold compaction preparation starts before its request: `blocking` above
 * `contextWindow - reserveTokens`, `background` above the background threshold, and only when
 * selection finds a cut.
 */
export const threshold = (
  view: View,
  planned: ReadonlyArray<Message>,
  contextWindow: number,
  policy: Compaction,
): 'blocking' | 'background' | undefined => {
  if (!policy.enabled || contextWindow <= 0) return undefined
  const tokens = estimateContext(view, planned)
  const blocking = contextWindow - policy.reserveTokens
  const background = blocking - policy.backgroundTokens
  let over: 'blocking' | 'background' | undefined
  if (tokens > blocking) over = 'blocking'
  else if (policy.backgroundTokens > 0 && tokens > background) over = 'background'
  if (over === undefined || selectCut(view, policy.keepRecentTokens) === undefined) return undefined
  return over
}
