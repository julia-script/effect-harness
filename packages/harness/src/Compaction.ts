// Cut selection/serialization adapted from pi-durable (MIT), pinned 636703a0.
import * as AiPrompt from 'effect/ai/Prompt'
import type * as Agent from './Agent.ts'
import * as Context from './Context.ts'

export function selectCut(
  view: Context.View,
  keepRecentTokens: number,
  tokenize = Context.estimateMessage,
): number | undefined {
  const start = view.head === undefined ? 0 : 1
  const candidates: number[] = []
  for (let index = start; index < view.contributions.length; index++)
    if (candidate(view.contributions, index)) candidates.push(index)
  let kept = 0
  let cut: number | undefined
  for (let index = view.contributions.length - 1; index >= start; index--) {
    kept += (view.contributions[index] ?? []).reduce((sum, message) => sum + tokenize(message), 0)
    if (kept < keepRecentTokens) continue
    cut = candidates.find((value) => value >= index) ?? candidates.at(-1)
    break
  }
  if (cut === undefined) return undefined
  for (let index = start; index < cut; index++)
    if ((view.contributions[index]?.length ?? 0) > 0) return cut
  return undefined
}
function candidate(contributions: Context.View['contributions'], index: number): boolean {
  const first = contributions[index]?.[0]
  if (first?.role === 'assistant') return true
  if (first?.role !== 'user') return false
  let calls = new Set<string>()
  for (let before = index - 1; before >= 0; before--) {
    const assistant = contributions[before]?.findLast((message) => message.role === 'assistant')
    if (assistant?.role !== 'assistant') continue
    calls = new Set(
      assistant.content.flatMap((part) => (part.type === 'tool-call' ? [part.id] : [])),
    )
    break
  }
  if (calls.size === 0) return true
  for (let after = index; after < contributions.length; after++)
    for (const [position, message] of (contributions[after] ?? []).entries()) {
      if (message.role === 'assistant' && (after > index || position > 0)) return true
      if (
        message.role === 'tool' &&
        message.content.some((part) => part.type === 'tool-result' && calls.has(part.id))
      )
        return false
    }
  return true
}
export const summarizedMessages = (
  view: Context.View,
  cut: number,
): ReadonlyArray<AiPrompt.Message> =>
  Context.orderToolResults(view.contributions.slice(0, cut).flat())
export function threshold(
  tokens: number,
  contextWindow: number,
  policy: Agent.CompactionPolicy,
): 'blocking' | 'background' | undefined {
  if (!policy.enabled || contextWindow <= 0) return undefined
  if (tokens > contextWindow - policy.reserveTokens) return 'blocking'
  return policy.backgroundTokens !== 0 &&
    tokens > contextWindow - policy.reserveTokens - policy.backgroundTokens
    ? 'background'
    : undefined
}
export function serializeConversation(messages: ReadonlyArray<AiPrompt.Message>): string {
  const lines: string[] = []
  for (const message of messages) {
    if (message.role === 'system') continue
    if (message.role === 'user') {
      const text = message.content
        .flatMap((part) => (part.type === 'text' ? [part.text] : []))
        .join('\n')
      if (text !== '') lines.push(`[User]: ${text}`)
    } else if (message.role === 'assistant') {
      const text = message.content.flatMap((part) => (part.type === 'text' ? [part.text] : []))
      const reasoning = message.content.flatMap((part) =>
        part.type === 'reasoning' ? [part.text] : [],
      )
      const calls = message.content.flatMap((part) =>
        part.type === 'tool-call' ? [`${part.name}(${serializeArgs(part.params)})`] : [],
      )
      if (reasoning.length > 0) lines.push(`[Assistant thinking]: ${reasoning.join('\n')}`)
      if (text.length > 0) lines.push(`[Assistant]: ${text.join('\n')}`)
      if (calls.length > 0) lines.push(`[Assistant tool calls]: ${calls.join('; ')}`)
    } else
      for (const part of message.content)
        if (part.type === 'tool-result') {
          const text = toolText(part.result)
          if (text !== '')
            lines.push(
              `[Tool result]: ${text.length <= 2000 ? text : `${text.slice(0, 2000)}\n\n[... ${text.length - 2000} more characters truncated]`}`,
            )
        }
  }
  return lines.join('\n\n')
}
function serializeArgs(params: unknown): string {
  if (params === null || typeof params !== 'object' || Array.isArray(params))
    return JSON.stringify(params) ?? ''
  return Object.entries(params)
    .map(([key, value]) => `${key}=${JSON.stringify(value)}`)
    .join(', ')
}
function toolText(result: unknown): string {
  if (typeof result === 'string') return result
  if (result === null || typeof result !== 'object') return JSON.stringify(result) ?? ''
  const content: unknown = Reflect.get(result, 'content')
  if (!Array.isArray(content)) return JSON.stringify(result) ?? ''
  return content
    .flatMap((part: unknown) =>
      typeof part === 'object' &&
      part !== null &&
      Reflect.get(part, 'type') === 'text' &&
      typeof Reflect.get(part, 'text') === 'string'
        ? [String(Reflect.get(part, 'text'))]
        : [],
    )
    .join('\n')
}
const system =
  'You are a context summarization assistant. Your task is to read a conversation between a user and an AI assistant, then produce a structured summary following the exact format specified.\n\nDo NOT continue the conversation. Do NOT respond to any questions in the conversation. ONLY output the structured summary.'
const instructions =
  'The messages above are a conversation to summarize. Create a structured context checkpoint summary that another LLM will use to continue the work. If the conversation starts with an earlier summary, preserve its information and fold the newer messages into it.\n\nUse this EXACT format:\n\n## Goal\n[What is the user trying to accomplish? Can be multiple items if the session covers different tasks.]\n\n## Constraints & Preferences\n- [Any constraints, preferences, or requirements mentioned by user]\n- [Or "(none)" if none were mentioned]\n\n## Progress\n### Done\n- [x] [Completed tasks/changes]\n\n### In Progress\n- [ ] [Current work]\n\n### Blocked\n- [Issues preventing progress, if any]\n\n## Key Decisions\n- **[Decision]**: [Brief rationale]\n\n## Next Steps\n1. [Ordered list of what should happen next]\n\n## Critical Context\n- [Any data, examples, or references needed to continue]\n- [Or "(none)" if not applicable]\n\nKeep each section concise. Preserve exact file paths, function names, and error messages.'
export function prompt(messages: ReadonlyArray<AiPrompt.Message>, focus?: string): AiPrompt.Prompt {
  return AiPrompt.fromMessages([
    AiPrompt.systemMessage({ content: system }),
    AiPrompt.userMessage({
      content: [
        AiPrompt.textPart({
          text: `<conversation>\n${serializeConversation(messages)}\n</conversation>\n\n${instructions}${focus === undefined ? '' : `\n\nAdditional focus: ${focus}`}`,
        }),
      ],
    }),
  ])
}
export const summaryMessage = (summary: string): AiPrompt.UserMessage =>
  AiPrompt.userMessage({
    content: [
      AiPrompt.textPart({
        text: `The conversation history before this point was compacted into the following summary:\n\n<summary>\n${summary}\n</summary>`,
      }),
    ],
  })
