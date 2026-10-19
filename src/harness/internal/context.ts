/**
 * Model context of one conversation, ported from pi's `harness/context.ts`.
 *
 * H = the newest head marker at or before the tail; the range runs from `H.head`, or transcript
 * start, through the tail. Per target, the newest edit in the range wins. Context entries are H
 * followed by the range's non-head entries.
 */
import { Effect, Option, Schema } from 'effect'
import { Message } from '../../model/Message.ts'
import type { AssistantMessage, ToolCall, ToolResultMessage } from '../../model/Message.ts'
import type { Tx } from '../../session/Session.ts'
import { TransactionError } from '../../session/SessionError.ts'
import type { ContextEdit, EntryRecord } from '../../storage/schema/Entry.ts'
import type { ConversationId, EntryId } from '../../storage/schema/Ids.ts'
import type { ModelMessage } from '../../storage/schema/Json.ts'
import type { Cursor } from '../../storage/schema/Scan.ts'
import type { ContextView } from '../ViewState.ts'

export const SYSTEM_KIND = 'pi.system'

const SCAN_PAGE_SIZE = 256
const EXCLUDED: ReadonlySet<AssistantMessage['stopReason']> = new Set([
  'aborted',
  'error',
  'deferred',
  'pending',
])
const MISSING_RESULT_TEXT = 'Tool result unavailable: history ends before this call completed.'

type HeadMarker = EntryRecord & { readonly head: EntryId }

/** What the model sees of a conversation, and the active entries it came from. */
export interface View {
  readonly head: HeadMarker | undefined
  /** Active entries, oldest first. */
  readonly entries: ReadonlyArray<EntryRecord>
  /** Each active entry's messages after its edit, before tool result ordering. */
  readonly contributions: ReadonlyArray<ReadonlyArray<Message>>
  readonly messages: ReadonlyArray<Message>
}

const decodeMessage = Schema.decodeUnknownOption(Message)

/** The messages of `model` that decode; a message of another shape contributes nothing. */
export const decodeAll = (model: ReadonlyArray<ModelMessage>): ReadonlyArray<Message> =>
  model.flatMap((message) => Option.toArray(decodeMessage(message)))

/** Every entry of `query`, oldest first. */
const scan = (
  tx: Tx,
  conversationId: ConversationId,
  range: { readonly minEntryId?: EntryId; readonly maxEntryId: EntryId },
) =>
  Effect.gen(function* () {
    const entries: Array<EntryRecord> = []
    let cursor: Cursor | undefined
    do {
      const query = { conversationId, ...range, order: 'ascending' as const }
      const page = yield* tx.scanEntries(query, SCAN_PAGE_SIZE, cursor)
      entries.push(...page.items)
      cursor = page.next
    } while (cursor !== undefined)
    return entries
  })

/** The newest head marker at or before `tail`. */
const headAt = (tx: Tx, conversationId: ConversationId, tail: EntryId) =>
  Effect.gen(function* () {
    const latest = yield* tx.latestHeadMarker(conversationId)
    if (latest === undefined || latest.id <= tail) return latest
    let found: HeadMarker | undefined
    let cursor: Cursor | undefined
    do {
      const page = yield* tx.scanEntries({ conversationId, maxEntryId: tail }, 64, cursor)
      const entry = page.items.find((item) => item.head !== undefined)
      if (entry?.head !== undefined) found = { ...entry, head: entry.head }
      cursor = found === undefined ? page.next : undefined
    } while (cursor !== undefined)
    return found
  })

/**
 * Read the context of a conversation in a commit, before its first table write: now, or cut off at
 * the entry `at`.
 */
export const read = (tx: Tx, conversationId: ConversationId, at?: EntryId) =>
  Effect.gen(function* () {
    const tail = at ?? (yield* tx.scanEntries({ conversationId }, 1)).items[0]?.id
    if (tail === undefined) {
      const empty: View = { head: undefined, entries: [], contributions: [], messages: [] }
      return empty
    }
    const head = yield* headAt(tx, conversationId, tail)
    const range =
      head === undefined ? { maxEntryId: tail } : { minEntryId: head.head, maxEntryId: tail }
    return derive(head, yield* scan(tx, conversationId, range))
  })

/**
 * The public context of a conversation, read in a commit before its first table write: now, or
 * cut off at `at`, which must be visible from the conversation.
 */
export const readVisible = (tx: Tx, conversationId: ConversationId, at?: EntryId) =>
  Effect.gen(function* () {
    if (at !== undefined) {
      const found = yield* tx.scanEntries({ conversationId, minEntryId: at, maxEntryId: at }, 1)
      if (found.items.length === 0)
        return yield* new TransactionError({
          message: `Entry ${at} is not visible from conversation ${conversationId}`,
        })
    }
    const { head, ...rest } = yield* read(tx, conversationId, at)
    const view: ContextView = head === undefined ? rest : { head, ...rest }
    return view
  })

/** Derive the view from the entries of the range. */
export const derive = (head: HeadMarker | undefined, range: ReadonlyArray<EntryRecord>): View => {
  const edits = new Map<EntryId, ContextEdit>()
  // Edits of every entry in the range count, also older head markers the active entries drop.
  for (const entry of range) for (const edit of entry.edits ?? []) edits.set(edit.target, edit)
  const entries =
    head === undefined ? range : [head, ...range.filter((entry) => entry.head === undefined)]
  const own = new Map(entries.map((entry) => [entry.id, decodeAll(entry.model ?? [])]))
  const contributions = entries.map((entry) => contribute(edits.get(entry.id), own.get(entry.id)))
  const baseline = findBaseline(head, entries, own)
  const messages = leadWithSystem(orderToolResults(contributions.flat()), baseline)
  return { head, entries, contributions, messages }
}

/** One active entry's messages after its edit, without excluded stop reasons. */
const contribute = (
  edit: ContextEdit | undefined,
  own: ReadonlyArray<Message> = [],
): ReadonlyArray<Message> => {
  if (edit?.action === 'omit') return []
  const messages = edit?.action === 'replace' ? decodeAll(edit.messages) : own
  return messages.filter(
    (message) => message.role !== 'assistant' || !EXCLUDED.has(message.stopReason),
  )
}

/** A baseline's system message, `null` when there cannot be one, `undefined` when none came. */
type Baseline = Message | null | undefined

const isSystem = (message: { readonly role?: unknown }) => message.role === 'system'

/**
 * The system message of the first `pi.system` entry after the head marker, or of the transcript
 * without one, when generation wrote it with every section and tool in effect: after a head
 * marker it always does; without one, unless an earlier entry carries a system message.
 */
const findBaseline = (
  head: HeadMarker | undefined,
  entries: ReadonlyArray<EntryRecord>,
  own: ReadonlyMap<EntryId, ReadonlyArray<Message>>,
): Baseline => {
  for (const entry of entries) {
    if (head !== undefined && entry.id <= head.id) continue
    if (entry.kind === SYSTEM_KIND)
      return own.get(entry.id)?.find((message) => isSystem(message)) ?? null
    const carries =
      (entry.model?.some((message) => isSystem(message)) ?? false) ||
      (entry.edits?.some(
        (edit) => edit.action === 'replace' && edit.messages.some((message) => isSystem(message)),
      ) ??
        false)
    if (head === undefined && carries) return null
  }
  return undefined
}

/**
 * Move the first system message to the front when only user messages precede it, or when it is
 * the baseline's: a run's input commits before its system prompt, and providers treat only a
 * leading system message as the initial prompt and tool set.
 */
const leadWithSystem = (
  messages: ReadonlyArray<Message>,
  baseline: Baseline,
): ReadonlyArray<Message> => {
  const index = messages.findIndex((message) => message.role === 'system')
  const system = messages[index]
  if (index <= 0 || system === undefined) return messages
  if (system !== baseline && messages.slice(0, index).some((message) => message.role !== 'user'))
    return messages
  return [system, ...messages.slice(0, index), ...messages.slice(index + 1)]
}

const missingResult = (call: ToolCall, timestamp: number): ToolResultMessage => ({
  role: 'toolResult',
  toolCallId: call.id,
  toolName: call.name,
  content: [{ type: 'text', text: MISSING_RESULT_TEXT }],
  isError: true,
  details: { reason: 'missing_result' },
  timestamp,
})

/** The first result of each call among the messages after `index`, up to the next assistant. */
const resultsAfter = (messages: ReadonlyArray<Message>, index: number) => {
  const results = new Map<string, Message>()
  for (const candidate of messages.slice(index + 1)) {
    if (candidate.role === 'assistant') break
    if (candidate.role === 'toolResult' && !results.has(candidate.toolCallId))
      results.set(candidate.toolCallId, candidate)
  }
  return results
}

/**
 * Place each assistant's tool results directly after it in call order, taken from the messages
 * before the next assistant; a missing result is synthesized and unmatched results are dropped.
 */
export const orderToolResults = (messages: ReadonlyArray<Message>): ReadonlyArray<Message> =>
  messages.flatMap((message, index) => {
    if (message.role === 'toolResult') return []
    if (message.role !== 'assistant') return [message]
    const calls = message.content.filter((block): block is ToolCall => block.type === 'toolCall')
    if (calls.length === 0) return [message]
    const results = resultsAfter(messages, index)
    return [
      message,
      ...calls.map((call) => results.get(call.id) ?? missingResult(call, message.timestamp)),
    ]
  })
