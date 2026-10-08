/**
 * Conversation cut selection and native summarization prompts.
 */
import { constant } from 'effect/Function'
import { dual } from 'effect/Function'
import * as Arr from 'effect/Array'
// Cut selection/serialization adapted from pi-durable (MIT), pinned 636703a0.
import * as Prompt from 'effect/ai/Prompt'
import * as Result from 'effect/Result'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as ToolResult from './ToolResult.ts'
import type * as Agent from './Agent.ts'
import * as Context from './Context.ts'
import * as Serialization from './Serialization.ts'

function selectCutImpl(
  self: Context.View,
  keepRecentTokens: number,
  tokenize = Context.estimateMessage,
): Option.Option<number> {
  const start = self.head === undefined ? 0 : 1
  const candidates: Array<number> = []
  for (let index = start; index < self.contributions.length; index++)
    if (candidate(self.contributions, index)) candidates.push(index)
  let kept = 0
  let cut = Option.none<number>()
  for (let index = self.contributions.length - 1; index >= start; index--) {
    kept += Option.getOrElse(Arr.get(self.contributions, index), () => []).reduce(
      (sum, message) => sum + tokenize(message),
      0,
    )
    if (kept < keepRecentTokens) continue
    cut = Arr.findFirst(candidates, (value) => value >= index).pipe(
      Option.orElse(() => Arr.last(candidates)),
    )
    break
  }
  return Option.flatMap(cut, (value) => {
    for (let index = start; index < value; index++)
      if (
        Option.getOrElse(
          Option.map(Arr.get(self.contributions, index), (self) => self.length),
          constant(0),
        ) > 0
      )
        return cut
    return Option.none()
  })
}
/**
 * Selects a safe conversation boundary while retaining the requested token budget.
 *
 * @category combinators
 */
export const selectCut: {
  (
    keepRecentTokens: number,
    tokenize?: (message: import('effect/ai/Prompt').Message) => number,
  ): (self: Context.View) => Option.Option<number>
  (
    self: Context.View,
    keepRecentTokens: number,
    tokenize?: (message: import('effect/ai/Prompt').Message) => number,
  ): Option.Option<number>
} = dual(
  (args) => args[0] != null && typeof args[0] === 'object' && 'contributions' in args[0],
  selectCutImpl,
)
function candidate(self: Context.View['contributions'], index: number): boolean {
  const first = Arr.get(self, index).pipe(Option.flatMap(Arr.head))
  const role = Option.map(first, (self) => self.role)
  if (Option.contains(role, 'assistant')) return true
  if (!Option.contains(role, 'user')) return false
  let calls = new Set<string>()
  for (let before = index - 1; before >= 0; before--) {
    const assistant = Arr.get(self, before).pipe(
      Option.flatMap((messages) =>
        Arr.findLast(messages, (message) => message.role === 'assistant'),
      ),
    )
    if (Option.isNone(assistant)) continue
    calls = Option.match(assistant, {
      onNone: () => new Set<string>(),
      onSome: (self) =>
        new Set(
          Arr.filterMap(self.content, (part) =>
            part.type === 'tool-call' ? Result.succeed(part.id) : Result.failVoid,
          ),
        ),
    })
    break
  }
  if (calls.size === 0) return true
  for (let after = index; after < self.length; after++)
    for (const [position, message] of Option.getOrElse(Arr.get(self, after), () => []).entries()) {
      if (message.role === 'assistant' && (after > index || position > 0)) return true
      if (
        message.role === 'tool' &&
        message.content.some((part) => part.type === 'tool-result' && calls.has(part.id))
      )
        return false
    }
  return true
}

const summarizedMessagesImpl = (self: Context.View, cut: number): Array<Prompt.Message> =>
  Context.orderToolResults(Arr.flatten(self.contributions.slice(0, cut)))
/**
 * Returns call-ordered native messages before the selected cut.
 *
 * @category combinators
 */
export const summarizedMessages: {
  (cut: number): (self: Context.View) => Array<Prompt.Message>
  (self: Context.View, cut: number): Array<Prompt.Message>
} = dual(2, summarizedMessagesImpl)
function thresholdImpl(
  self: number,
  contextWindow: number,
  policy: Agent.CompactionPolicy,
): Option.Option<'blocking' | 'background'> {
  if (!policy.enabled || contextWindow <= 0) return Option.none()
  if (self > contextWindow - policy.reserveTokens) return Option.some('blocking')
  return policy.backgroundTokens !== 0 &&
    self > contextWindow - policy.reserveTokens - policy.backgroundTokens
    ? Option.some('background')
    : Option.none()
}
/**
 * Classifies a context estimate against blocking and background compaction thresholds.
 *
 * @category combinators
 */
export const threshold: {
  (
    contextWindow: number,
    policy: Agent.CompactionPolicy,
  ): (self: number) => Option.Option<'blocking' | 'background'>
  (
    self: number,
    contextWindow: number,
    policy: Agent.CompactionPolicy,
  ): Option.Option<'blocking' | 'background'>
} = dual(3, thresholdImpl)
/**
 * Formats a native conversation for the summarization model.
 *
 * @category combinators
 */
export function serializeConversation(self: ReadonlyArray<Prompt.Message>): string {
  const lines: Array<string> = []
  for (const message of self) {
    if (message.role === 'system') continue
    if (message.role === 'user') {
      const text = Arr.filterMap(message.content, (part) =>
        part.type === 'text' ? Result.succeed(part.text) : Result.failVoid,
      ).join('\n')
      if (text !== '') lines.push(`[User]: ${text}`)
    } else if (message.role === 'assistant') {
      const text = Arr.filterMap(message.content, (part) =>
        part.type === 'text' ? Result.succeed(part.text) : Result.failVoid,
      )
      const reasoning = Arr.filterMap(message.content, (part) =>
        part.type === 'reasoning' ? Result.succeed(part.text) : Result.failVoid,
      )
      const calls = Arr.filterMap(message.content, (part) =>
        part.type === 'tool-call'
          ? Result.succeed(`${part.name}(${serializeArgs(part.params)})`)
          : Result.failVoid,
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
const jsonText = (self: unknown): string =>
  Result.getOrElse(Serialization.stringify(self), () => Serialization.unencodable)
function serializeArgs(self: unknown): string {
  return Serialization.textOrMarker(() => {
    if (self === null || typeof self !== 'object' || Arr.isArray(self)) return jsonText(self)
    return Object.entries(self)
      .map(([key, value]) => `${key}=${jsonText(value)}`)
      .join(', ')
  })
}
const envelope = Schema.decodeUnknownOption(Schema.toCodecJson(ToolResult.Envelope))
const Content = Schema.Struct({ content: Schema.Array(Schema.Unknown) })
const content = Schema.decodeUnknownOption(Content)
const textBlock = Schema.decodeUnknownOption(
  Schema.Struct({ type: Schema.Literal('text'), text: Schema.String }),
)
function toolText(self: unknown): string {
  return Serialization.textOrMarker(() => {
    if (typeof self === 'string') return self
    return Option.match(envelope(self), {
      onSome: (known) =>
        Arr.filterMap(known.content, (part) =>
          part.type === 'text' ? Result.succeed(part.text) : Result.failVoid,
        ).join('\n'),
      onNone: () =>
        Option.match(content(self), {
          onNone: () => Serialization.display(self),
          onSome: (generic) =>
            Arr.filterMap(generic.content, (part) =>
              Option.match(textBlock(part), {
                onNone: () => Result.failVoid,
                onSome: (text) => Result.succeed(text.text),
              }),
            ).join('\n'),
        }),
    })
  })
}

const system =
  'You are a context summarization assistant. Your task is to read a conversation between a user and an AI assistant, then produce a structured summary following the exact format specified.\n\nDo NOT continue the conversation. Do NOT respond to any questions in the conversation. ONLY output the structured summary.'
const instructions =
  'The messages above are a conversation to summarize. Create a structured context checkpoint summary that another LLM will use to continue the work. If the conversation starts with an earlier summary, preserve its information and fold the newer messages into it.\n\nUse this EXACT format:\n\n## Goal\n[What is the user trying to accomplish? Can be multiple items if the session covers different tasks.]\n\n## Constraints & Preferences\n- [Any constraints, preferences, or requirements mentioned by user]\n- [Or "(none)" if none were mentioned]\n\n## Progress\n### Done\n- [x] [Completed tasks/changes]\n\n### In Progress\n- [ ] [Current work]\n\n### Blocked\n- [Issues preventing progress, if any]\n\n## Key Decisions\n- **[Decision]**: [Brief rationale]\n\n## Next Steps\n1. [Ordered list of what should happen next]\n\n## Critical Context\n- [Any data, examples, or references needed to continue]\n- [Or "(none)" if not applicable]\n\nKeep each section concise. Preserve exact file paths, function names, and error messages.'
function promptImpl(self: ReadonlyArray<Prompt.Message>, focus?: string): Prompt.Prompt {
  return Prompt.fromMessages([
    Prompt.systemMessage({ content: system }),
    Prompt.userMessage({
      content: [
        Prompt.textPart({
          text: `<conversation>\n${serializeConversation(self)}\n</conversation>\n\n${instructions}${focus === undefined ? '' : `\n\nAdditional focus: ${focus}`}`,
        }),
      ],
    }),
  ])
}
/**
 * Creates the native summarization prompt with optional additional focus.
 *
 * @category combinators
 */
export const prompt: {
  (focus?: string): (self: ReadonlyArray<Prompt.Message>) => Prompt.Prompt
  (self: ReadonlyArray<Prompt.Message>, focus?: string): Prompt.Prompt
} = dual((args) => Array.isArray(args[0]), promptImpl)
/**
 * Creates a native user message containing a completed context summary.
 *
 * @category combinators
 */
export const summaryMessage = (self: string): Prompt.UserMessage =>
  Prompt.userMessage({
    content: [
      Prompt.textPart({
        text: `The conversation history before this point was compacted into the following summary:\n\n<summary>\n${self}\n</summary>`,
      }),
    ],
  })
