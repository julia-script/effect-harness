// Semantic commit ordering adapted from pi-durable (MIT), pinned 636703a0.
import * as Agent from '@effect-harness/harness/Agent'
import type * as Invocation from '@effect-harness/harness/Invocation'
import * as Totals from '@effect-harness/harness/Usage'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Scope from 'effect/Scope'
import * as Prompt from 'effect/ai/Prompt'
import * as Schema from 'effect/Schema'
import * as Inbox from './Inbox.ts'
import * as Record from './Record.ts'
import { rejected, type StorageError } from './StorageError.ts'
import * as View from './View.ts'

export type QueuedItem = { readonly id: Record.SubmissionId; readonly mode: Inbox.Item['mode'] }
export type MessageChange =
  | {
      readonly type: 'text_start' | 'thinking_start' | 'toolcall_start'
      readonly contentIndex: number
      readonly block: Prompt.AssistantMessagePart
    }
  | {
      readonly type: 'text_delta' | 'thinking_delta'
      readonly contentIndex: number
      readonly delta: string
    }
  | {
      readonly type: 'toolcall_delta'
      readonly contentIndex: number
      readonly path: View.Path
      readonly delta: string
    }
  | {
      readonly type: 'block'
      readonly contentIndex: number
      readonly block: Prompt.AssistantMessagePart
    }
  | { readonly type: 'message'; readonly message: Prompt.AssistantMessage }
export interface Snapshot {
  readonly type: 'snapshot'
  readonly entries: ReadonlyArray<Record.Entry>
  readonly run?: { readonly inputs: ReadonlyArray<Record.SubmissionId> }
  readonly generation?: Omit<NonNullable<Inbox.LiveState['generation']>, 'message'> & {
    readonly message?: Prompt.AssistantMessage
    readonly usage?: Totals.Usage
  }
  readonly tools: ReadonlyArray<typeof Inbox.ToolSlot.Type>
  readonly compactions: NonNullable<Inbox.LiveState['compactions']>
  readonly inbox: ReadonlyArray<QueuedItem>
  readonly agent: Agent.State
  readonly usage: Totals.State
}
export type AgentEvent =
  | Snapshot
  | { readonly type: 'run_start' | 'run_end'; readonly inputs: ReadonlyArray<Record.SubmissionId> }
  | { readonly type: 'turn_start' | 'turn_end' }
  | { readonly type: 'message_start'; readonly message: Prompt.Message }
  | {
      readonly type: 'message_update'
      readonly usage: Totals.Usage
      readonly changes: ReadonlyArray<MessageChange>
    }
  | { readonly type: 'message_end' | 'entry_appended'; readonly entry: Record.Entry }
  | {
      readonly type: 'tool_execution_start'
      readonly toolCallId: string
      readonly toolName: string
      readonly args: Record.Json
    }
  | {
      readonly type: 'tool_execution_update'
      readonly toolCallId: string
      readonly toolName: string
      readonly output?:
        | { readonly trimStart?: number; readonly append?: string }
        | { readonly set: string }
      readonly details?: Schema.Json
      readonly diagnostics?: ReadonlyArray<Invocation.Diagnostic>
    }
  | {
      readonly type: 'tool_execution_end'
      readonly toolCallId: string
      readonly toolName: string
      readonly entry?: Record.Entry
    }
  | { readonly type: 'inbox_update'; readonly items: ReadonlyArray<QueuedItem> }
  | { readonly type: 'submission'; readonly record: Record.Submission }
  | {
      readonly type: 'auto_retry_start'
      readonly attempt: number
      readonly at: number
      readonly errorMessage: string
    }
  | { readonly type: 'auto_retry_end'; readonly attempt: number }
  | { readonly type: 'deferred_poll'; readonly pollAt: number }
  | { readonly type: 'agent_changed'; readonly agent: Agent.State }
  | { readonly type: 'usage_changed'; readonly usage: Totals.State }
  | {
      readonly type: 'task_failed'
      readonly taskId: Record.TaskId
      readonly kind: string
      readonly message: string
    }
  | {
      readonly type: 'compaction_start'
      readonly taskId: Record.TaskId
      readonly reason: NonNullable<Inbox.LiveState['compactions']>[number]['reason']
      readonly blocking: boolean
    }
  | {
      readonly type: 'compaction_end'
      readonly taskId: Record.TaskId
      readonly reason: NonNullable<Inbox.LiveState['compactions']>[number]['reason']
    }
export type Batch = ReadonlyArray<AgentEvent>
export interface Watch extends View.ProjectionWatch<Batch> {
  readonly snapshot: Snapshot
}
export interface Service {
  readonly watch: (id: Record.ConversationId) => Effect.Effect<Watch, StorageError, Scope.Scope>
}
/** Ordered semantic batches derived exclusively from committed conversation mounts. */
export class Event extends Context.Service<Event, Service>()('@effect-harness/durable/Event') {}

const decode = <S extends Schema.Constraint>(schema: S, value: unknown) =>
  Schema.decodeUnknownEffect(schema)(value).pipe(
    Effect.mapError((cause) => rejected('Invalid committed event payload', 'corrupt', cause)),
  )
const messageCodec = Schema.toCodecJson(Prompt.Message)
const assistantCodec = Schema.toCodecJson(Prompt.AssistantMessage)
const liveJson = (view: View.Value) => view.docs['harness.live']
const partialJson = (view: View.Value) => object(liveJson(view)?.generation)?.message
const usageJson = (view: View.Value) => object(liveJson(view)?.generation)?.usage
const toolJson = (view: View.Value) => {
  const values = liveJson(view)?.tools
  const result = new Map<string, Record.JsonObject>()
  if (Array.isArray(values))
    for (const value of values) {
      const slot = object(value)
      if (slot !== undefined && typeof slot.callId === 'string') result.set(slot.callId, slot)
    }
  return result
}
const queued = (inbox: typeof Inbox.State.Type | undefined): ReadonlyArray<QueuedItem> =>
  (inbox?.items ?? []).map(({ id, mode }) => ({ id, mode }))
const parts = Effect.fnUntraced(function* (view: View.Value) {
  const live = yield* decode(Inbox.LiveState, view.docs['harness.live'] ?? {})
  const inbox =
    view.docs['harness.inbox'] === undefined
      ? undefined
      : yield* decode(Inbox.State, view.docs['harness.inbox'])
  const agent =
    view.docs['harness.agent'] === undefined
      ? undefined
      : yield* decode(Agent.State, view.docs['harness.agent'])
  const usage =
    view.docs['harness.usage'] === undefined
      ? undefined
      : yield* decode(Totals.State, view.docs['harness.usage'])
  const partial =
    live.generation?.message === undefined
      ? undefined
      : yield* decode(assistantCodec, live.generation.message)
  const partialUsage = view.docs['harness.live']?.generation
  const currentUsage =
    isObject(partialUsage) && partialUsage.usage !== undefined
      ? yield* decode(Totals.Usage, partialUsage.usage)
      : undefined
  return { live, inbox, agent, usage, partial, currentUsage }
})
export const snapshot = Effect.fnUntraced(function* (
  view: View.Value,
): Effect.fn.Return<Snapshot, StorageError> {
  const { live, inbox, agent, usage, partial, currentUsage } = yield* parts(view)
  const generation = live.generation
  return {
    type: 'snapshot',
    entries: view.entries,
    ...(live.run === undefined ? {} : { run: { inputs: live.run.inputs } }),
    ...(generation === undefined
      ? {}
      : {
          generation: {
            attempt: generation.attempt,
            ...(generation.retry === undefined ? {} : { retry: generation.retry }),
            ...(generation.deferred === undefined ? {} : { deferred: generation.deferred }),
            ...(partial === undefined ? {} : { message: partial }),
            ...(currentUsage === undefined ? {} : { usage: currentUsage }),
          },
        }),
    tools: live.tools ?? [],
    compactions: live.compactions ?? [],
    inbox: queued(inbox),
    agent: agent ?? {},
    usage: usage ?? Totals.empty(),
  }
})
const same = (left: unknown, right: unknown) => JSON.stringify(left) === JSON.stringify(right)
const isObject = (value: Record.Json | undefined): value is Record.JsonObject =>
  value !== null && typeof value === 'object' && !Array.isArray(value)
const object = (value: Record.Json | undefined): Record.JsonObject | undefined =>
  isObject(value) ? value : undefined
const generationKind = (kind: string) =>
  kind === '@effect-harness/durable/Generation/v1' ||
  kind === 'harness.generation' ||
  kind === 'pi.generation'
const startsWith = (path: View.Path, prefix: View.Path) =>
  prefix.length <= path.length && prefix.every((segment, index) => path[index] === segment)
const partialPath = ['docs', 'harness.live', 'generation', 'message']

/** Compact changes within a committed native assistant partial; ancestor replacements use one complete message. */
export function messageChanges(
  ops: ReadonlyArray<View.Op>,
  before: Prompt.AssistantMessage,
  message: Prompt.AssistantMessage,
): ReadonlyArray<MessageChange> {
  const changes: MessageChange[] = []
  const whole = new Set<number>()
  for (const op of ops) {
    if (op[0] === 'replace') return [{ type: 'message', message }]
    const path = op[1]
    if (!startsWith(path, partialPath)) {
      if (startsWith(partialPath, path)) return [{ type: 'message', message }]
      continue
    }
    const rest = path.slice(partialPath.length)
    if (rest[0] === 'usage' || rest[0] === 'options') continue
    if (rest[0] !== 'content') return [{ type: 'message', message }]
    if (rest.length === 1) {
      if (message.content.length < before.content.length) return [{ type: 'message', message }]
      for (let index = 0; index < message.content.length; index++) {
        const block = message.content[index]
        if (block === undefined || same(block, before.content[index])) continue
        let type: 'text_start' | 'thinking_start' | 'toolcall_start' = 'toolcall_start'
        if (block.type === 'text') type = 'text_start'
        else if (block.type === 'reasoning') type = 'thinking_start'
        changes.push(
          index >= before.content.length &&
            (block.type === 'text' || block.type === 'reasoning' || block.type === 'tool-call')
            ? { type, contentIndex: index, block }
            : { type: 'block', contentIndex: index, block },
        )
        whole.add(index)
      }
      continue
    }
    const index = rest[1]
    if (typeof index !== 'number' || whole.has(index)) continue
    const block = message.content[index]
    const previous = before.content[index]
    if (block === undefined) return [{ type: 'message', message }]
    if (
      rest.length === 3 &&
      rest[2] === 'text' &&
      previous !== undefined &&
      'text' in previous &&
      'text' in block &&
      block.text.startsWith(previous.text)
    ) {
      changes.push({
        type: block.type === 'reasoning' ? 'thinking_delta' : 'text_delta',
        contentIndex: index,
        delta: block.text.slice(previous.text.length),
      })
    } else if (
      rest[2] === 'params' &&
      op[0] === 'set' &&
      typeof op[2] === 'string' &&
      previous?.type === 'tool-call'
    ) {
      let previousValue: unknown = previous.params
      for (const segment of rest.slice(3))
        previousValue =
          previousValue !== null &&
          typeof previousValue === 'object' &&
          Object.hasOwn(previousValue, segment)
            ? Reflect.get(previousValue, segment)
            : undefined
      if (typeof previousValue === 'string' && op[2].startsWith(previousValue))
        changes.push({
          type: 'toolcall_delta',
          contentIndex: index,
          path: rest.slice(3),
          delta: op[2].slice(previousValue.length),
        })
      else {
        whole.add(index)
        changes.push({ type: 'block', contentIndex: index, block })
      }
    } else {
      whole.add(index)
      changes.push({ type: 'block', contentIndex: index, block })
    }
  }
  return changes.length === 0 && !same(before.content, message.content)
    ? [{ type: 'message', message }]
    : changes
}
/** A retained output window is a front trim followed by an append when its overlap is known. */
export function outputChange(
  before: string | undefined,
  after: string | undefined,
): Extract<AgentEvent, { type: 'tool_execution_update' }>['output'] {
  if (before === after) return undefined
  if (before === undefined || after === undefined) return { set: after ?? '' }
  const prefix = new Uint32Array(after.length)
  for (let index = 1; index < after.length; index++) {
    let matched = prefix[index - 1] ?? 0
    while (matched > 0 && after[index] !== after[matched]) matched = prefix[matched - 1] ?? 0
    if (after[index] === after[matched]) matched++
    prefix[index] = matched
  }
  let overlap = 0
  for (let index = 0; index < before.length; index++) {
    while (overlap > 0 && before[index] !== after[overlap]) overlap = prefix[overlap - 1] ?? 0
    if (before[index] === after[overlap]) overlap++
  }
  if (overlap > 0) {
    const trimStart = before.length - overlap
    return {
      ...(trimStart === 0 ? {} : { trimStart }),
      ...(overlap === after.length ? {} : { append: after.slice(overlap) }),
    }
  }
  return { set: after }
}

/** Translate one domain commit in progress/end/submission/state/start order; held generations end their turn once. */
export const translate = Effect.fnUntraced(function* (
  id: Record.ConversationId,
  change: View.Change,
  held: Set<Record.TaskId>,
): Effect.fn.Return<Batch, StorageError> {
  const frame = change.publication
  if (frame === undefined) return [yield* snapshot(change.value)]
  const entries: Record.Entry[] = []
  const tasks = new Map<Record.TaskId, Record.Task>()
  const submissions = new Map<Record.SubmissionId, Record.Submission>()
  for (const write of frame.writes) {
    if (write.type === 'entry' && write.value.conversationId === id) entries.push(write.value)
    if (write.type === 'task' && write.value.conversationId === id)
      tasks.set(write.value.id, write.value)
    if (write.type === 'submission' && write.value.conversationId === id)
      submissions.set(write.value.id, write.value)
  }
  entries.sort((a, b) => a.id - b.id)
  const touchedPartial =
    !change.rebased ||
    frame.documents.some(
      (publication) =>
        publication.record.kind === 'harness.live' &&
        publication.ops.some(
          (op) =>
            op[0] === 'replace' ||
            startsWith(['generation', 'message'], op[1]) ||
            startsWith(op[1], ['generation', 'message']),
        ),
    )
  const touchedUsage =
    !change.rebased ||
    frame.documents.some(
      (publication) =>
        publication.record.kind === 'harness.live' &&
        publication.ops.some(
          (op) =>
            op[0] === 'replace' ||
            startsWith(['generation', 'usage'], op[1]) ||
            startsWith(op[1], ['generation', 'usage']),
        ),
    )
  const was = yield* parts(change.before)
  const now = yield* parts(change.value)
  const events: AgentEvent[] = []
  const previousSlots = new Map((was.live.tools ?? []).map((slot) => [slot.callId, slot]))
  const rawBefore = toolJson(change.before)
  const rawNow = toolJson(change.value)
  const slots = now.live.tools ?? []
  for (const slot of slots) {
    if (slot.status !== 'running' || previousSlots.get(slot.callId)?.status === 'running') continue
    const task = slot.taskId === undefined ? undefined : tasks.get(slot.taskId)
    const args = object(task?.input)?.arguments ?? object(task?.state.checkpoint)?.arguments ?? {}
    events.push({
      type: 'tool_execution_start',
      toolCallId: slot.callId,
      toolName: slot.name,
      args,
    })
  }
  if (touchedPartial && now.partial !== undefined && was.partial === undefined)
    events.push({ type: 'message_start', message: now.partial })
  else if (
    now.partial !== undefined &&
    was.partial !== undefined &&
    ((touchedPartial && partialJson(change.value) !== partialJson(change.before)) ||
      (touchedUsage && usageJson(change.value) !== usageJson(change.before)))
  ) {
    events.push({
      type: 'message_update',
      usage: now.currentUsage ?? Totals.zero(),
      changes: messageChanges(change.ops, was.partial, now.partial),
    })
  }
  for (const slot of slots) {
    const previous = previousSlots.get(slot.callId)
    if (slot.status !== 'running' || previous?.status !== 'running') continue
    const output = outputChange(previous.output, slot.output)
    const detailsChanged = rawBefore.get(slot.callId)?.details !== rawNow.get(slot.callId)?.details
    const diagnosticsChanged =
      rawBefore.get(slot.callId)?.diagnostics !== rawNow.get(slot.callId)?.diagnostics
    if (output === undefined && !detailsChanged && !diagnosticsChanged) continue
    events.push({
      type: 'tool_execution_update',
      toolCallId: slot.callId,
      toolName: slot.name,
      ...(output === undefined ? {} : { output }),
      ...(detailsChanged ? { details: slot.details ?? null } : {}),
      ...(diagnosticsChanged ? { diagnostics: slot.diagnostics ?? [] } : {}),
    })
  }
  const generation = now.live.generation
  const generationBefore = was.live.generation
  if (generation?.retry !== undefined && generationBefore?.retry === undefined)
    events.push({
      type: 'auto_retry_start',
      attempt: generation.attempt,
      at: generation.retry.at,
      errorMessage: generation.retry.error,
    })
  if (generationBefore?.retry !== undefined && generation?.retry === undefined)
    events.push({ type: 'auto_retry_end', attempt: generationBefore.attempt })
  if (
    generation?.deferred !== undefined &&
    generation.deferred.pollAt !== generationBefore?.deferred?.pollAt
  )
    events.push({ type: 'deferred_poll', pollAt: generation.deferred.pollAt })
  const decodedEntries = yield* Effect.forEach(
    entries,
    Effect.fnUntraced(function* (entry) {
      return {
        entry,
        message:
          entry.model?.[0] === undefined ? undefined : yield* decode(messageCodec, entry.model[0]),
      }
    }),
  )
  const ends: Array<Extract<AgentEvent, { type: 'tool_execution_end' }>> = []
  const end = (callId: string, name: string, entryId?: Record.EntryId) => {
    const entry = entries.find((item) => item.id === entryId)
    ends.push({
      type: 'tool_execution_end',
      toolCallId: callId,
      toolName: name,
      ...(entry === undefined ? {} : { entry }),
    })
  }
  for (const previous of previousSlots.values()) {
    if (previous.status === 'done') continue
    const slot = slots.find((item) => item.callId === previous.callId)
    if (slot?.status === 'done') end(previous.callId, previous.name, slot.entry)
    else if (slot === undefined) {
      const result = decodedEntries.find(
        ({ message }) =>
          message?.role === 'tool' &&
          message.content.some(
            (part) => part.type === 'tool-result' && part.id === previous.callId,
          ),
      )
      end(previous.callId, previous.name, result?.entry.id)
    }
  }
  for (const slot of slots)
    if (slot.status === 'done' && !previousSlots.has(slot.callId))
      end(slot.callId, slot.name, slot.entry)
  let assistantAppended = false
  for (const { entry, message } of decodedEntries) {
    events.push(...ends.filter((item) => item.entry === entry))
    if (message === undefined) {
      events.push({ type: 'entry_appended', entry })
      continue
    }
    const streamed = message.role === 'assistant' && was.partial !== undefined && !assistantAppended
    if (message.role === 'assistant') assistantAppended = true
    if (!streamed) events.push({ type: 'message_start', message })
    events.push({ type: 'message_end', entry })
  }
  events.push(...ends.filter((item) => item.entry === undefined))
  const compactionsBefore = was.live.compactions ?? []
  const compactions = now.live.compactions ?? []
  for (const { taskId, reason } of compactionsBefore)
    if (!compactions.some((item) => item.taskId === taskId))
      events.push({ type: 'compaction_end', taskId, reason })
  let turnEnded = false
  for (const task of tasks.values()) {
    if (generationKind(task.kind) && task.state.status === 'completing' && !held.has(task.id)) {
      held.add(task.id)
      turnEnded = true
    }
    if (task.state.status !== 'terminal') continue
    if (generationKind(task.kind) && !held.delete(task.id)) turnEnded = true
    const outcome = object(task.state.outcome)
    const status = outcome?.status
    if (status === 'faulted' || status === 'orphaned') {
      const error = object(outcome?.error)
      const detail = outcome?.detail ?? error?.message ?? outcome?.reason
      events.push({
        type: 'task_failed',
        taskId: task.id,
        kind: task.kind,
        message: typeof detail === 'string' ? detail : 'Task failed',
      })
    }
  }
  if (turnEnded) events.push({ type: 'turn_end' })
  const run = now.live.run
  const runBefore = was.live.run
  const runChanged = run?.inputs[0] !== runBefore?.inputs[0]
  if (runBefore !== undefined && runChanged)
    events.push({ type: 'run_end', inputs: runBefore.inputs })
  for (const record of [...submissions.values()].sort((a, b) => a.id - b.id))
    events.push({ type: 'submission', record })
  if (change.value.docs['harness.inbox'] !== change.before.docs['harness.inbox'])
    events.push({ type: 'inbox_update', items: queued(now.inbox) })
  if (change.value.docs['harness.agent'] !== change.before.docs['harness.agent'])
    events.push({ type: 'agent_changed', agent: now.agent ?? {} })
  if (change.value.docs['harness.usage'] !== change.before.docs['harness.usage'])
    events.push({ type: 'usage_changed', usage: now.usage ?? Totals.empty() })
  for (const { taskId, reason, blocking } of compactions)
    if (!compactionsBefore.some((item) => item.taskId === taskId))
      events.push({ type: 'compaction_start', taskId, reason, blocking })
  if (run !== undefined && runChanged) events.push({ type: 'run_start', inputs: run.inputs })
  if (
    run !== undefined &&
    run.taskId !== runBefore?.taskId &&
    generationKind(tasks.get(run.taskId)?.kind ?? '')
  )
    events.push({ type: 'turn_start' })
  return events
})
export const make = (views: View.Service): Service => ({
  watch: Effect.fnUntraced(function* (id) {
    const held = new Set<Record.TaskId>()
    let initial: Snapshot | undefined
    const seedHeld = (tasks: ReadonlyArray<Record.Task>) => {
      held.clear()
      for (const task of tasks)
        if (generationKind(task.kind) && task.state.status === 'completing') held.add(task.id)
    }
    const subscription = yield* views.observe<Batch>(id, {
      initial: Effect.fnUntraced(function* (value, tasks) {
        seedHeld(tasks)
        initial = yield* snapshot(value)
        return [initial]
      }),
      project: (change) =>
        translate(id, change, held).pipe(
          Effect.map((batch) => (batch.length === 0 ? undefined : batch)),
        ),
      reset: Effect.fnUntraced(function* (value, _seq, tasks) {
        seedHeld(tasks)
        return [yield* snapshot(value)]
      }),
    })
    if (initial === undefined)
      return yield* rejected('Event snapshot was not initialized', 'corrupt')
    return {
      get value() {
        return subscription.value
      },
      snapshot: initial,
      changes: subscription.changes,
      closed: subscription.closed,
      stop: subscription.stop,
      listen: subscription.listen,
    }
  }),
})
export const layer = Layer.effect(Event)(
  Effect.gen(function* () {
    return make(yield* View.View)
  }),
)
