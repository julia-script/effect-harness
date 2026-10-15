import * as PromptWire from '../PromptWire.js'
/** Pure compaction preparation. A fingerprint is advisory until revalidated in the placement transaction. */
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as Prompt from 'effect/ai/Prompt'
import * as Compaction from '../Compaction.js'
import { CompactionError } from '../CompactionError.js'
import * as Projection from '../ContextProjection.js'
import * as Record from '../Record.js'
import * as Protocol from './ContextProtocol.js'
import * as Selection from './CompactionSelection.js'
import * as Value from './DocumentValue.js'

export type Failure = CompactionError | Projection.Failure
export interface CanonicalInput {
  readonly entries: ReadonlyArray<Record.Entry>
  readonly protocol: Protocol.Protocol
  /** Full current projection, including edits appended after the selected tail. */
  readonly view: Projection.ContextView
}
const ItemSchema = Schema.Struct({
  entry: Record.Entry,
  contribution: Schema.Array(PromptWire.Message),
  fact: Schema.optionalKey(Projection.FactSchema),
})
export const FingerprintSchema = Schema.Struct({
  version: Schema.Literal(1),
  conversationId: Record.ConversationId,
  protocol: Protocol.ProtocolSchema,
  legacyInterpretation: Projection.LegacyInterpretationSchema,
  head: Schema.optionalKey(Record.Entry),
  firstKept: Record.EntryId,
  tail: Record.EntryId,
  selected: Schema.Array(ItemSchema),
  retained: Schema.Array(ItemSchema),
  /** Original append-only records whose edits affect pinned active entries, in audit order. */
  edits: Schema.Array(Record.Entry),
})
export type Fingerprint = typeof FingerprintSchema.Type
export const TranscriptSchema = Schema.Struct({
  text: Schema.String,
  /** Files are omitted from text, exactly as in the pinned text-only serializer. No file is fetched. */
  omittedMedia: Schema.Array(
    Schema.Struct({
      message: Schema.Int,
      part: Schema.Int,
      mediaType: Schema.String,
      fileName: Schema.optionalKey(Schema.String),
    }),
  ),
})
export type Transcript = typeof TranscriptSchema.Type

const freeze = <A>(value: A): A => {
  if (
    value !== null &&
    typeof value === 'object' &&
    !ArrayBuffer.isView(value) &&
    !(value instanceof URL) &&
    !Object.isFrozen(value)
  ) {
    for (const child of Object.values(value)) freeze(child)
    Object.freeze(value)
  }
  return value
}
const invalid = (message: string) => new CompactionError({ reason: 'invalid', message })
const stale = (message: string) => new CompactionError({ reason: 'stale', message })

/** Unknown native tool payloads must be JSON values, without coercion, accessors or cycles. */
const jsonSafe = (value: unknown, ancestors = new Set<object>()): boolean => {
  if (value === null || typeof value === 'string' || typeof value === 'boolean') return true
  if (typeof value === 'number') return Number.isFinite(value)
  if (typeof value !== 'object' || ancestors.has(value)) return false
  if (
    !Array.isArray(value) &&
    Object.getPrototypeOf(value) !== Object.prototype &&
    Object.getPrototypeOf(value) !== null
  )
    return false
  ancestors.add(value)
  const keys = Reflect.ownKeys(value)
  let valid = true
  if (Array.isArray(value)) {
    if (Object.keys(value).length !== value.length) valid = false
    for (let index = 0; index < value.length; index++)
      if (!Object.hasOwn(value, index)) valid = false
  }
  for (const key of keys) {
    if (Array.isArray(value) && key === 'length') continue
    const descriptor = Object.getOwnPropertyDescriptor(value, key)
    if (
      typeof key !== 'string' ||
      descriptor === undefined ||
      !('value' in descriptor) ||
      !descriptor.enumerable ||
      !jsonSafe(descriptor.value, ancestors)
    ) {
      valid = false
      break
    }
  }
  ancestors.delete(value)
  return valid
}
const requireJson = Effect.fnUntraced(function* (
  value: unknown,
): Effect.fn.Return<Schema.Json, CompactionError | Schema.SchemaError> {
  const safe = yield* Effect.try({
    try: () => jsonSafe(value),
    catch: () => invalid('Tool payload cannot be inspected as JSON'),
  })
  if (!safe) return yield* invalid('Tool parameters and results must be JSON values')
  return yield* Schema.decodeUnknownEffect(Schema.Json)(value)
})

const checkMessages = Effect.fnUntraced(function* (
  messages: ReadonlyArray<Prompt.Message>,
  canonical: boolean,
) {
  for (const message of messages) {
    if (message.role === 'system') continue
    for (const part of message.content) {
      if (part.type === 'tool-call') yield* requireJson(part.params)
      if (part.type === 'tool-result') yield* requireJson(part.result)
      if (canonical && part.type === 'file' && typeof part.data !== 'string')
        return yield* invalid('Canonical entry media must use its persisted JSON representation')
    }
  }
})

const sameView = Schema.toEquivalence(Projection.ContextViewSchema)
const samePrompt = Schema.toEquivalence(Prompt.Prompt)
const sameFingerprint = Schema.toEquivalence(FingerprintSchema)
const canonical = Effect.fnUntraced(function* (
  input: CanonicalInput,
): Effect.fn.Return<CanonicalInput, Failure> {
  // Validate before JSON copying: URL serialization must not silently become a string branch.
  yield* checkMessages(input.view.prompt.content, true)
  for (const messages of input.view.contributions) yield* checkMessages(messages, true)
  const protocol = freeze(yield* Value.copy(Protocol.ProtocolSchema, input.protocol))
  const entries = freeze(yield* Value.copy(Schema.Array(Record.Entry), input.entries))
  const view = yield* Value.copy(Schema.toCodecJson(Projection.ContextViewSchema), input.view)
  const expected = yield* Protocol.project(view.bounds.conversationId, entries, protocol, {
    legacyMode: view.bounds.legacyInterpretation._tag,
  })
  if (!sameView(view, expected))
    return yield* invalid('Supplied projection is not the full current canonical view')
  return { entries, protocol, view: expected }
})

/** Reject a call/result or approval pair crossing either selected cut or pinned tail. */
const crosses = (view: Projection.ContextView, boundary: number): boolean => {
  const calls = new Map<string, number>()
  const approvals = new Map<string, number>()
  for (const [index, messages] of view.contributions.entries()) {
    for (const message of messages) {
      if (message.role === 'system') continue
      if (message.role === 'assistant') calls.clear()
      for (const part of message.content) {
        if (part.type === 'tool-call') calls.set(JSON.stringify([part.id, part.name]), index)
        if (part.type === 'tool-result') {
          const key = JSON.stringify([part.id, part.name])
          const from = calls.get(key)
          if (from !== undefined && from < boundary && index >= boundary) return true
          calls.delete(key)
        }
        if (part.type === 'tool-approval-request') approvals.set(part.approvalId, index)
        if (part.type === 'tool-approval-response') {
          const from = approvals.get(part.approvalId)
          if (from !== undefined && from < boundary && index >= boundary) return true
          approvals.delete(part.approvalId)
        }
      }
    }
  }
  return false
}

const fingerprint = Effect.fnUntraced(function* (
  input: CanonicalInput,
  firstKept: Record.EntryId,
  tail: Record.EntryId,
): Effect.fn.Return<Fingerprint, Failure> {
  const { view } = input
  const through = view.activeEntries.filter((entry) => entry.id <= tail)
  const cut = through.findIndex((entry) => entry.id === firstKept)
  if (cut < 1 || !through.some((entry) => entry.id === tail))
    return yield* stale('Pinned first-kept entry or tail is absent')
  if (
    !Selection.candidates(view).includes(cut) ||
    crosses(view, cut) ||
    crosses(view, through.length)
  )
    return yield* stale('Pinned range crosses a tool group or has no safe cut')
  const items = through.map((entry, index) => {
    const fact = view.positionalFacts.find((item) => item.entryId === entry.id)?.fact
    return {
      entry,
      contribution: view.contributions[index]!,
      ...(fact === undefined ? {} : { fact }),
    }
  })
  const ids = new Set(through.map((entry) => entry.id))
  const start = view.bounds.head?.head
  const edits = input.entries.filter(
    (entry) =>
      (start === undefined || entry.id >= start) &&
      entry.edits?.some((edit) => ids.has(edit.target)),
  )
  return freeze(
    yield* Value.copy(Schema.toCodecJson(FingerprintSchema), {
      version: 1,
      conversationId: view.bounds.conversationId,
      protocol: input.protocol,
      legacyInterpretation: view.bounds.legacyInterpretation,
      ...(view.bounds.head === undefined ? {} : { head: view.bounds.head }),
      firstKept,
      tail,
      selected: items.slice(0, cut),
      retained: items.slice(cut),
      edits,
    }),
  )
})

/** Capture a detached range, without storage reads, policy decisions or execution services. */
export const capture = Effect.fnUntraced(function* (
  inputSelection: Compaction.Selected,
  input: CanonicalInput,
): Effect.fn.Return<Fingerprint, Failure> {
  yield* checkMessages(inputSelection.source.content, true)
  yield* checkMessages(inputSelection.retained.content, true)
  const selection = yield* Value.copy(Schema.toCodecJson(Compaction.SelectedSchema), inputSelection)
  const current = yield* canonical(input)
  const { view } = current
  const cut = view.activeEntries.findIndex((entry) => entry.id === selection.firstKept)
  if (
    selection.conversationId !== view.bounds.conversationId ||
    selection.tail !== view.bounds.tail ||
    !Schema.toEquivalence(Projection.LegacyInterpretationSchema)(
      selection.legacyInterpretation,
      view.bounds.legacyInterpretation,
    ) ||
    cut < 1 ||
    !samePrompt(
      selection.source,
      Projection.projectMessages(view.contributions.slice(0, cut).flat()),
    ) ||
    !samePrompt(
      selection.retained,
      Projection.projectMessages(view.contributions.slice(cut).flat()),
    )
  )
    return yield* invalid('Selection does not describe the supplied canonical range')
  return yield* fingerprint(current, selection.firstKept, selection.tail)
})

/**
 * Reapply full current history before restricting to pinned IDs. Ordinary unchanged appended
 * context is allowed. This is advisory; the engine must call it again inside the SAME placement
 * transaction, including after queued admission. No mutable entry revision or storage CAS exists.
 */
export const validate = Effect.fnUntraced(function* (
  inputFingerprint: Fingerprint,
  input: CanonicalInput,
): Effect.fn.Return<Fingerprint, Failure> {
  for (const item of [...inputFingerprint.selected, ...inputFingerprint.retained])
    yield* checkMessages(item.contribution, true)
  const pinned = freeze(yield* Value.copy(Schema.toCodecJson(FingerprintSchema), inputFingerprint))
  const current = yield* canonical(input)
  const observed = yield* fingerprint(current, pinned.firstKept, pinned.tail)
  if (!sameFingerprint(pinned, observed))
    return yield* stale('Pinned canonical range, edits, protocol or head marker changed')
  return pinned
})

/**
 * Visible transcript with 2000-character tool-result truncation; reasoning is excluded.
 * JSON object params keep named arguments, other JSON params use their exact text.
 * Files are deliberately omitted and disclosed. Native unknown tool payloads are validated first.
 */
export const serialize = Effect.fnUntraced(function* (
  input: Prompt.Prompt,
): Effect.fn.Return<Transcript, CompactionError | Schema.SchemaError> {
  const prompt = yield* Schema.decodeEffect(Prompt.Prompt)(input)
  yield* checkMessages(prompt.content, false)
  const parts: Array<string> = []
  const omittedMedia: Array<Transcript['omittedMedia'][number]> = []
  for (const [messageIndex, message] of prompt.content.entries()) {
    if (message.role === 'system') continue
    for (const [partIndex, part] of message.content.entries()) {
      if (part.type === 'file')
        omittedMedia.push({
          message: messageIndex,
          part: partIndex,
          mediaType: part.mediaType,
          ...(part.fileName === undefined ? {} : { fileName: part.fileName }),
        })
    }
    if (message.role === 'user') {
      const text = message.content
        .flatMap((part) => (part.type === 'text' ? [part.text] : []))
        .join('\n')
      if (text.length > 0) parts.push(`[User]: ${text}`)
    } else if (message.role === 'assistant') {
      const text = message.content.flatMap((part) => (part.type === 'text' ? [part.text] : []))
      const calls: Array<string> = []
      for (const part of message.content) {
        if (part.type === 'tool-call') {
          const params = yield* requireJson(part.params)
          const args =
            params !== null && typeof params === 'object' && !Array.isArray(params)
              ? Object.entries(params)
                  .map(([key, value]) => `${key}=${JSON.stringify(value)}`)
                  .join(', ')
              : JSON.stringify(params)
          calls.push(`${part.name}(${args})`)
        }
        // Native provider-executed results may reside within the assistant message.
      }
      if (text.length > 0) parts.push(`[Assistant]: ${text.join('\n')}`)
      if (calls.length > 0) parts.push(`[Assistant tool calls]: ${calls.join('; ')}`)
    }
    if (message.role === 'assistant' || message.role === 'tool') {
      for (const part of message.content) {
        if (part.type === 'tool-result') {
          const result = yield* requireJson(part.result)
          const text = typeof result === 'string' ? result : JSON.stringify(result)
          if (text.length > 0)
            parts.push(
              `[Tool result]: ${text.length <= 2000 ? text : `${text.slice(0, 2000)}\n\n[... ${text.length - 2000} more characters truncated]`}`,
            )
        }
      }
    }
  }
  return freeze({ text: parts.join('\n\n'), omittedMedia })
})

const SUMMARIZATION_SYSTEM_PROMPT =
  'You are a context summarization assistant. Your task is to read a conversation between a user and an AI assistant, then produce a structured summary following the exact format specified.\n\nDo NOT continue the conversation. Do NOT respond to any questions in the conversation. ONLY output the structured summary.'

const SUMMARIZATION_PROMPT =
  'The messages above are a conversation to summarize. Create a structured context checkpoint summary that another LLM will use to continue the work. If the conversation starts with an earlier summary, preserve its information and fold the newer messages into it.\n\nUse this EXACT format:\n\n## Goal\n[What is the user trying to accomplish? Can be multiple items if the session covers different tasks.]\n\n## Constraints & Preferences\n- [Any constraints, preferences, or requirements mentioned by user]\n- [Or "(none)" if none were mentioned]\n\n## Progress\n### Done\n- [x] [Completed tasks/changes]\n\n### In Progress\n- [ ] [Current work]\n\n### Blocked\n- [Issues preventing progress, if any]\n\n## Key Decisions\n- **[Decision]**: [Brief rationale]\n\n## Next Steps\n1. [Ordered list of what should happen next]\n\n## Critical Context\n- [Any data, examples, or references needed to continue]\n- [Or "(none)" if not applicable]\n\nKeep each section concise. Preserve exact file paths, function names, and error messages.'

export const PreparedTranscriptSchema = Schema.Struct({
  prompt: Prompt.Prompt,
  transcript: TranscriptSchema,
})
export type PreparedTranscript = typeof PreparedTranscriptSchema.Type

/** Construct the pinned summarizer system/user prompt; budget and provider configuration belong to the engine. */
export const preparePrompt = Effect.fnUntraced(function* (
  source: Prompt.Prompt,
  inputInstructions?: string,
): Effect.fn.Return<PreparedTranscript, CompactionError | Schema.SchemaError> {
  const transcript = yield* serialize(source)
  const instructions = yield* Schema.decodeEffect(Schema.UndefinedOr(Schema.String))(
    inputInstructions,
  )
  const focus = instructions === undefined ? '' : `\n\nAdditional focus: ${instructions}`
  const prompt = Prompt.fromMessages([
    Prompt.systemMessage({ content: SUMMARIZATION_SYSTEM_PROMPT }),
    Prompt.userMessage({
      content: [
        Prompt.textPart({
          text: `<conversation>\n${transcript.text}\n</conversation>\n\n${SUMMARIZATION_PROMPT}${focus}`,
        }),
      ],
    }),
  ])
  return freeze({ prompt, transcript })
})
