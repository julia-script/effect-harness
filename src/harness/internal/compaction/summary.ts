/**
 * The summarization request and what its response means, ported from pi's `compaction.ts`.
 */
import type { AssistantMessage, Message } from '../../../model/Message.ts'
import * as Transcript from '../../../model/Transcript.ts'

/** The longest tool result text the serialized conversation keeps. */
const TOOL_RESULT_MAX_CHARS = 2000

export const SUMMARY_PREFIX =
  'The conversation history before this point was compacted into the following summary:\n\n<summary>\n'
export const SUMMARY_SUFFIX = '\n</summary>'

export const SYSTEM_PROMPT = `You are a context summarization assistant. Your task is to read a conversation between a user and an AI assistant, then produce a structured summary following the exact format specified.

Do NOT continue the conversation. Do NOT respond to any questions in the conversation. ONLY output the structured summary.`

const PROMPT = `The messages above are a conversation to summarize. Create a structured context checkpoint summary that another LLM will use to continue the work. If the conversation starts with an earlier summary, preserve its information and fold the newer messages into it.

Use this EXACT format:

## Goal
[What is the user trying to accomplish? Can be multiple items if the session covers different tasks.]

## Constraints & Preferences
- [Any constraints, preferences, or requirements mentioned by user]
- [Or "(none)" if none were mentioned]

## Progress
### Done
- [x] [Completed tasks/changes]

### In Progress
- [ ] [Current work]

### Blocked
- [Issues preventing progress, if any]

## Key Decisions
- **[Decision]**: [Brief rationale]

## Next Steps
1. [Ordered list of what should happen next]

## Critical Context
- [Any data, examples, or references needed to continue]
- [Or "(none)" if not applicable]

Keep each section concise. Preserve exact file paths, function names, and error messages.`

const truncate = (text: string, maxChars: number) =>
  text.length <= maxChars
    ? text
    : `${text.slice(0, maxChars)}\n\n[... ${text.length - maxChars} more characters truncated]`

const callText = (call: ReturnType<typeof Transcript.toolCalls>[number]) =>
  `${call.name}(${Object.entries(call.arguments)
    .map(([key, value]) => `${key}=${JSON.stringify(value)}`)
    .join(', ')})`

/** One message as transcript lines. */
const lines = (message: Message): ReadonlyArray<string> => {
  if (message.role === 'user') {
    const text = Transcript.contentText(message.content)
    return text.length > 0 ? [`[User]: ${text}`] : []
  }
  if (message.role === 'toolResult') {
    const text = Transcript.contentText(message.content)
    return text.length > 0 ? [`[Tool result]: ${truncate(text, TOOL_RESULT_MAX_CHARS)}`] : []
  }
  if (message.role === 'system') return []
  const text = message.content.flatMap((block) => (block.type === 'text' ? [block.text] : []))
  const calls = Transcript.toolCalls(message).map((call) => callText(call))
  return [
    ...(text.length > 0 ? [`[Assistant]: ${text.join('\n')}`] : []),
    ...(calls.length > 0 ? [`[Assistant tool calls]: ${calls.join('; ')}`] : []),
  ]
}

/**
 * Messages as plain text, so the summarizer reads a transcript instead of continuing it. System
 * messages and thinking are left out; visible text, tool calls and results record what happened.
 */
export const serialize = (messages: ReadonlyArray<Message>): string =>
  messages.flatMap((message) => lines(message)).join('\n\n')

/** The summarizer's user message: the serialized conversation, the prompt, and any focus. */
export const prompt = (messages: ReadonlyArray<Message>, instructions: string | undefined) => {
  const focus = instructions === undefined ? '' : `\n\nAdditional focus: ${instructions}`
  return `<conversation>\n${serialize(messages)}\n</conversation>\n\n${PROMPT}${focus}`
}

/** The summary of a clean `stop` with text and no tool call; anything else is none. */
export const text = (message: AssistantMessage): string | undefined => {
  if (message.stopReason !== 'stop' || Transcript.toolCalls(message).length > 0) return undefined
  const summary = message.content
    .flatMap((block) => (block.type === 'text' ? [block.text] : []))
    .join('\n')
    .trim()
  return summary.length === 0 ? undefined : summary
}

/** Why a response is not a summary. */
export const failure = (message: AssistantMessage): string => {
  if (message.stopReason === 'error' || message.stopReason === 'aborted')
    return `Summarization failed: ${message.errorMessage ?? message.stopReason}`
  if (message.stopReason === 'length')
    return 'Summarization hit the token limit; the summary is incomplete'
  if (Transcript.toolCalls(message).length > 0) return 'Summarization attempted to call a tool'
  return 'Summarization produced no text'
}
