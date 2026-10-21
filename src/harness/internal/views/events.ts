/**
 * Agent events from the view, ported from pi's `harness/events.ts` (spec §9.4): every event one
 * commit causes, from the view before and after it, its operations, and its publication.
 */
import { Schema } from 'effect'
import { AssistantMessage } from '../../../model/Message.ts'
import type { CommitPublication } from '../../../session/Session.ts'
import type { Op } from '../../../storage/schema/Document.ts'
import type { EntryRecord } from '../../../storage/schema/Entry.ts'
import type { ConversationId, TaskId } from '../../../storage/schema/Ids.ts'
import { JsonObject } from '../../../storage/schema/Json.ts'
import type { SubmissionRecord } from '../../../storage/schema/Submission.ts'
import type { TaskRecord } from '../../../storage/schema/Task.ts'
import type { AgentEvent, QueuedItem, Snapshot } from '../../../protocol/AgentEvent.ts'
import { State as AgentState } from '../../../protocol/AgentState.ts'
import { State as InboxState } from '../../Inbox.ts'
import { LiveState, UsageState } from '../../../protocol/ViewState.ts'
import type { ConversationView, ToolSlot } from '../../../protocol/ViewState.ts'
import { messageChanges } from './messages.ts'

const isLive = Schema.is(LiveState)
const isInbox = Schema.is(InboxState)
const isAgent = Schema.is(AgentState)
const isUsage = Schema.is(UsageState)
const isPartial = Schema.is(AssistantMessage)
const isObject = Schema.is(JsonObject)

/** The typed documents of a view; a missing or unreadable one is absent. */
export interface Parts {
  readonly live: LiveState
  readonly inbox: InboxState | undefined
  readonly agent: AgentState | undefined
  readonly usage: UsageState | undefined
  readonly partial: AssistantMessage | undefined
  readonly docs: ConversationView['docs']
}

export const parts = (view: ConversationView): Parts => {
  const { docs } = view
  const live = docs['$live']
  const typed = isLive(live) ? live : {}
  const partial = typed.generation?.message
  return {
    live: typed,
    inbox: isInbox(docs['$inbox']) ? docs['$inbox'] : undefined,
    agent: isAgent(docs['$agent']) ? docs['$agent'] : undefined,
    usage: isUsage(docs['$usage']) ? docs['$usage'] : undefined,
    partial: isPartial(partial) ? partial : undefined,
    docs,
  }
}

export const EMPTY_USAGE: UsageState = { models: {}, tools: {} }

export const queued = (inbox: InboxState | undefined): ReadonlyArray<QueuedItem> =>
  (inbox?.items ?? []).map(({ id, mode }) => ({ id, mode }))

export const snapshotOf = (view: ConversationView): Snapshot => {
  const { live, inbox, agent, usage } = parts(view)
  return {
    type: 'snapshot',
    entries: view.entries,
    ...(live.run === undefined ? {} : { run: { inputs: live.run.inputs } }),
    ...(live.generation === undefined ? {} : { generation: live.generation }),
    tools: live.tools ?? [],
    compactions: live.compactions ?? [],
    ...(live.compactionFailure === undefined ? {} : { compactionFailure: live.compactionFailure }),
    inbox: queued(inbox),
    agent: agent ?? {},
    usage: usage ?? EMPTY_USAGE,
  }
}

/** What one publication wrote to a conversation. */
export interface Written {
  readonly entries: ReadonlyArray<EntryRecord>
  readonly tasks: ReadonlyMap<TaskId, TaskRecord>
  readonly submissions: ReadonlyArray<SubmissionRecord>
}

export const writtenTo = (id: ConversationId, publication: CommitPublication): Written => {
  const entries: Array<EntryRecord> = []
  const tasks = new Map<TaskId, TaskRecord>()
  const submissions: Array<SubmissionRecord> = []
  for (const change of publication.changes) {
    if (change.type === 'entry' && change.value.conversationId === id) entries.push(change.value)
    if (change.type === 'task' && change.value.conversationId === id)
      tasks.set(change.value.id, change.value)
    if (change.type === 'submission' && change.value.conversationId === id)
      submissions.push(change.value)
  }
  // Submissions are published in the order the commit first touched them.
  return { entries, tasks, submissions: submissions.toSorted((a, b) => a.id - b.id) }
}

/** One commit's translation: the views around it, its operations, and what it wrote. */
export interface Step {
  readonly was: Parts
  readonly now: Parts
  readonly ops: ReadonlyArray<Op>
  readonly written: Written
}

const callOf = (slot: ToolSlot) => ({
  toolCallId: slot.callId,
  toolName: slot.name,
  ...(slot.taskId === undefined ? {} : { taskId: slot.taskId }),
})

/** The arguments a tool call runs with, from its tool task's intent checkpoint. */
const argumentsOf = (step: Step, slot: ToolSlot): JsonObject => {
  const record = slot.taskId === undefined ? undefined : step.written.tasks.get(slot.taskId)
  const checkpoint = record?.state.status === 'terminal' ? undefined : record?.state
  const value = checkpoint !== undefined && 'checkpoint' in checkpoint ? checkpoint.checkpoint : {}
  const args = isObject(value) ? value['arguments'] : undefined
  return isObject(args) ? args : {}
}

/** Tool starts, the in-flight message, then retry and deferred state. */
export const progress = (step: Step): ReadonlyArray<AgentEvent> => {
  const { was, now } = step
  const events: Array<AgentEvent> = []
  const before = new Map((was.live.tools ?? []).map((slot) => [slot.callId, slot]))
  for (const slot of now.live.tools ?? []) {
    if (slot.status !== 'running' || before.get(slot.callId)?.status === 'running') continue
    events.push({ type: 'tool_execution_start', ...callOf(slot), args: argumentsOf(step, slot) })
  }
  if (now.partial !== undefined && was.partial === undefined) {
    events.push({ type: 'message_start', message: now.partial })
  } else if (now.partial !== undefined && now.partial !== was.partial) {
    const changes = messageChanges(step.ops, now.partial)
    events.push({ type: 'message_update', usage: now.partial.usage, changes })
  }
  const generation = now.live.generation
  const previous = was.live.generation
  if (generation?.retry !== undefined && previous?.retry === undefined) {
    const { at, error } = generation.retry
    events.push({ type: 'auto_retry_start', attempt: generation.attempt, at, errorMessage: error })
  }
  if (previous?.retry !== undefined && generation?.retry === undefined)
    events.push({ type: 'auto_retry_end', attempt: previous.attempt })
  if (
    generation?.deferred !== undefined &&
    generation.deferred.pollAt !== previous?.deferred?.pollAt
  )
    events.push({ type: 'deferred_poll', pollAt: generation.deferred.pollAt })
  return events
}

type ToolEnd = Extract<AgentEvent, { readonly type: 'tool_execution_end' }>

const resultOf = (entries: ReadonlyArray<EntryRecord>, callId: string) =>
  entries.find((entry) => {
    const message = entry.model?.[0]
    return message?.['role'] === 'toolResult' && message['toolCallId'] === callId
  })

/**
 * Tools that end in this commit: a slot that becomes done, one created done (a call not offered),
 * or an unfinished one that vanishes because its run ended.
 */
export const toolEnds = (step: Step): ReadonlyArray<ToolEnd> => {
  const { entries } = step.written
  const ends: Array<ToolEnd> = []
  const end = (slot: ToolSlot, entry: EntryRecord | undefined) => {
    ends.push({
      type: 'tool_execution_end',
      ...callOf(slot),
      ...(entry === undefined ? {} : { entry }),
    })
  }
  const slots = step.now.live.tools ?? []
  const before = step.was.live.tools ?? []
  for (const previous of before) {
    if (previous.status === 'done') continue
    const slot = slots.find((each) => each.callId === previous.callId)
    if (slot?.status === 'done')
      end(
        slot,
        entries.find((entry) => entry.id === slot.entry),
      )
    else if (slot === undefined) end(previous, resultOf(entries, previous.callId))
  }
  for (const slot of slots) {
    if (slot.status === 'done' && !before.some((each) => each.callId === slot.callId))
      end(
        slot,
        entries.find((entry) => entry.id === slot.entry),
      )
  }
  return ends
}

/** Entries in append order; a tool's end comes right before its result's message. */
export const appended = (step: Step, ends: ReadonlyArray<ToolEnd>): ReadonlyArray<AgentEvent> => {
  const events: Array<AgentEvent> = []
  let assistant = false
  for (const entry of step.written.entries) {
    events.push(...ends.filter((end) => end.entry === entry))
    const message = entry.model?.[0]
    if (message === undefined) {
      events.push({ type: 'entry_appended', entry })
      continue
    }
    const isAssistant = message['role'] === 'assistant'
    // A streamed answer started with its first partial.
    const streamed = isAssistant && step.was.partial !== undefined && !assistant
    if (isAssistant) assistant = true
    if (!streamed) events.push({ type: 'message_start', message })
    events.push({ type: 'message_end', entry })
  }
  // Ends without a result entry: a faulted or orphaned tool, or one whose run ended.
  events.push(...ends.filter((end) => end.entry === undefined))
  return events
}
