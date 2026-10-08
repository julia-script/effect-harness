/**
 * Committed agent event projections and bounded observation streams.
 */
import * as Order from 'effect/Order'
import * as Arr from 'effect/Array'
import * as Predicate from 'effect/Predicate'
import { dual } from 'effect/Function'
import { tagged } from './internal/legacyTag.ts'
import * as Time from 'effect-harness/Time'
import * as DateTime from 'effect/DateTime'
import * as Outcome from './workflow/Outcome.ts'
import * as Ownership from './Ownership.ts'
import { ToolCall } from './workflow/ToolCall.ts'
import * as Option from 'effect/Option'
// Semantic commit ordering adapted from pi-durable (MIT), pinned 636703a0.
import * as Agent from 'effect-harness/Agent'
import * as Invocation from 'effect-harness/Invocation'
import * as Usage from 'effect-harness/Usage'
import * as Context from 'effect/Context'
import * as Ref from 'effect/Ref'
import * as HashSet from 'effect/HashSet'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import type * as Scope from 'effect/Scope'
import * as Prompt from 'effect/ai/Prompt'
import * as Schema from 'effect/Schema'
import * as Inbox from './Inbox.ts'
import * as Record from './Record.ts'
import { rejected, type StorageError, Corrupt } from './StorageError.ts'
import * as View from './View.ts'

/**
 * Schema for submission metadata included in an event snapshot.
 *
 * @category schemas
 */
export const QueuedItem = Schema.Struct({
  id: Record.SubmissionId,
  mode: Schema.Literals(['steer', 'followUp', 'write']),
})
/**
 * Submission metadata included in an event snapshot.
 *
 * @category models
 */
export type QueuedItem = Event.QueuedItem
/**
 * Schema for incremental change to committed model-visible message content.
 *
 * @category schemas
 */
export const MessageChange = Schema.Union([
  tagged('text_start', {
    type: Schema.tag('text_start'),
    contentIndex: Schema.Int,
    block: Prompt.AssistantMessagePart,
  }),
  tagged('thinking_start', {
    type: Schema.tag('thinking_start'),
    contentIndex: Schema.Int,
    block: Prompt.AssistantMessagePart,
  }),
  tagged('toolcall_start', {
    type: Schema.tag('toolcall_start'),
    contentIndex: Schema.Int,
    block: Prompt.AssistantMessagePart,
  }),
  tagged('block', {
    type: Schema.tag('block'),
    contentIndex: Schema.Int,
    block: Prompt.AssistantMessagePart,
  }),
  tagged('text_delta', {
    type: Schema.tag('text_delta'),
    contentIndex: Schema.Int,
    delta: Schema.String,
  }),
  tagged('thinking_delta', {
    type: Schema.tag('thinking_delta'),
    contentIndex: Schema.Int,
    delta: Schema.String,
  }),
  tagged('toolcall_delta', {
    type: Schema.tag('toolcall_delta'),
    contentIndex: Schema.Int,
    path: View.Path,
    delta: Schema.String,
  }),
  tagged('message', { type: Schema.tag('message'), message: Prompt.AssistantMessage }),
])
/**
 * Incremental change to committed model-visible message content.
 *
 * @category models
 */
export type MessageChange = Event.MessageChange
/**
 * Schema for committed conversation execution, queue, usage and entry snapshot.
 *
 * @category schemas
 */
export const Snapshot = tagged('snapshot', {
  type: Schema.tag('snapshot'),
  entries: Schema.Array(Record.Entry),
  run: Schema.optionalKey(Schema.Struct({ inputs: Schema.Array(Record.SubmissionId) })),
  generation: Schema.optionalKey(
    Schema.Struct({
      attempt: Inbox.LiveDomain.fields.generation.schema.fields.attempt,
      model: Inbox.LiveDomain.fields.generation.schema.fields.model,
      retry: Inbox.LiveDomain.fields.generation.schema.fields.retry,
      deferred: Inbox.LiveDomain.fields.generation.schema.fields.deferred,
      message: Schema.optionalKey(Prompt.AssistantMessage),
      usage: Schema.optionalKey(Usage.Usage),
    }),
  ),
  tools: Schema.Array(Inbox.ToolSlot),
  compactions: Inbox.LiveDomain.fields.compactions.schema,
  inbox: Schema.Array(QueuedItem),
  agent: Agent.State,
  usage: Usage.State,
})
/**
 * Committed conversation execution, queue, usage and entry snapshot.
 *
 * @category models
 */
export type Snapshot = Event.Snapshot
const toolIdentity = { toolCallId: Schema.String, toolName: Schema.String }
const compactionIdentity = {
  taskId: Record.TaskId,
  reason: Schema.Literals(['manual', 'threshold', 'overflow', 'background']),
}
/**
 * Schema for semantic event derived from a committed conversation update.
 *
 * @category schemas
 */
export const AgentEvent = Schema.Union([
  Snapshot,
  tagged('run_start', {
    type: Schema.tag('run_start'),
    inputs: Schema.Array(Record.SubmissionId),
  }),
  tagged('run_end', {
    type: Schema.tag('run_end'),
    inputs: Schema.Array(Record.SubmissionId),
  }),
  tagged('turn_start', { type: Schema.tag('turn_start') }),
  tagged('turn_end', { type: Schema.tag('turn_end') }),
  tagged('message_start', { type: Schema.tag('message_start'), message: Prompt.Message }),
  tagged('message_update', {
    type: Schema.tag('message_update'),
    usage: Usage.Usage,
    changes: Schema.Array(MessageChange),
  }),
  tagged('message_end', { type: Schema.tag('message_end'), entry: Record.Entry }),
  tagged('entry_appended', { type: Schema.tag('entry_appended'), entry: Record.Entry }),
  tagged('tool_execution_start', {
    type: Schema.tag('tool_execution_start'),
    ...toolIdentity,
    args: Schema.Json,
  }),
  tagged('tool_execution_update', {
    type: Schema.tag('tool_execution_update'),
    ...toolIdentity,
    output: Schema.optionalKey(
      Schema.Union([
        Schema.Struct({
          trimStart: Schema.optionalKey(Schema.Finite),
          append: Schema.optionalKey(Schema.String),
        }),
        Schema.Struct({ set: Schema.String }),
      ]),
    ),
    details: Schema.optionalKey(Schema.Json),
    diagnostics: Schema.optionalKey(Schema.Array(Invocation.Diagnostic)),
  }),
  tagged('tool_execution_end', {
    type: Schema.tag('tool_execution_end'),
    ...toolIdentity,
    entry: Schema.optionalKey(Record.Entry),
  }),
  tagged('inbox_update', { type: Schema.tag('inbox_update'), items: Schema.Array(QueuedItem) }),
  tagged('submission', { type: Schema.tag('submission'), record: Record.Submission }),
  tagged('auto_retry_start', {
    type: Schema.tag('auto_retry_start'),
    attempt: Schema.Int,
    at: Time.EpochMillis,
    errorMessage: Schema.String,
  }),
  tagged('auto_retry_end', { type: Schema.tag('auto_retry_end'), attempt: Schema.Int }),
  tagged('deferred_poll', { type: Schema.tag('deferred_poll'), pollAt: Time.EpochMillis }),
  tagged('agent_changed', { type: Schema.tag('agent_changed'), agent: Agent.State }),
  tagged('usage_changed', { type: Schema.tag('usage_changed'), usage: Usage.State }),
  tagged('task_failed', {
    type: Schema.tag('task_failed'),
    taskId: Record.TaskId,
    kind: Schema.String,
    message: Schema.String,
  }),
  tagged('compaction_start', {
    type: Schema.tag('compaction_start'),
    ...compactionIdentity,
    blocking: Schema.Boolean,
  }),
  tagged('compaction_end', { type: Schema.tag('compaction_end'), ...compactionIdentity }),
])
/**
 * Semantic event derived from a committed conversation update.
 *
 * @category models
 */
export type AgentEvent = typeof AgentEvent.Type
/**
 * Schema for ordered semantic events emitted for one committed update.
 *
 * @category schemas
 */
export const Batch = Schema.Array(AgentEvent)
/**
 * Ordered semantic events emitted for one committed update.
 *
 * @category models
 */
export type Batch = Event.Batch
/**
 * JSON codec for an ordered batch of committed semantic events.
 *
 * @category schemas
 */
export const BatchJson = Schema.toCodecJson(Batch)
/**
 * Scoped initial semantic snapshot and subsequent event batches.
 *
 * @category models
 */
export type Watch = Event.Watch
/**
 * Semantic observation operations built on committed View projections.
 *
 * @category models
 */
export type Service = Event.Service
/**
 * Service deriving semantic conversation events from committed views.
 *
 * **Details**
 *
 * An initial snapshot is separate from later ordered batches. Events cover model content,
 * tools, queue, usage and execution boundaries.
 *
 * **Gotchas**
 *
 * These are committed domain events, not provider transport deltas. task_failed reports
 * faulted or orphaned tasks rather than every expected failed tool receipt.
 *
 * @category services
 */
export class Event extends Context.Service<Event, Service>()('@effect-harness/durable/Event') {}

const decode = <S extends Schema.Constraint>(schema: S, value: unknown) =>
  Schema.decodeUnknownEffect(schema)(value).pipe(
    Effect.mapError((cause) => rejected('Invalid committed event payload', Corrupt, cause)),
  )
const messageCodec = Schema.toCodecJson(Prompt.Message)
const assistantCodec = Schema.toCodecJson(Prompt.AssistantMessage)
const queued = (inbox: Inbox.State | undefined): ReadonlyArray<QueuedItem> =>
  (inbox?.items ?? []).map(({ id, mode }) => ({ id, mode }))
const parts = Effect.fnUntraced(function* (view: View.Value) {
  const live = Inbox.domain(view.docs['harness.live'] ?? {})
  const inbox = view.docs['harness.inbox']
  const agent = view.docs['harness.agent']
  const usage = view.docs['harness.usage']
  const partial =
    live.generation?.message === undefined
      ? undefined
      : yield* decode(assistantCodec, live.generation.message)
  const currentUsage = live.generation?.usage
  return { live, inbox, agent, usage, partial, currentUsage }
})
/**
 * Projects a committed conversation view into an agent snapshot.
 *
 * @category combinators
 */
export const snapshot = Effect.fnUntraced(function* (
  self: View.Value,
): Effect.fn.Return<Snapshot, StorageError> {
  const { live, inbox, agent, usage, partial, currentUsage } = yield* parts(self)
  const generation = live.generation
  return {
    _tag: 'snapshot',
    type: 'snapshot',
    entries: self.entries,
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
    usage: usage ?? Usage.empty(),
  }
})
const samePart = Schema.toEquivalence(Prompt.AssistantMessagePart)
const sameParts = Schema.toEquivalence(Schema.Array(Prompt.AssistantMessagePart))
const generationKind = (kind: string) =>
  kind === '@effect-harness/durable/Generation/v1' ||
  kind === 'harness.generation' ||
  kind === 'pi.generation'
const startsWith = (path: View.Path, prefix: View.Path) =>
  prefix.length <= path.length && prefix.every((segment, index) => path[index] === segment)
const partialPath = ['docs', 'harness.live', 'generation', 'message']

/** Compact changes within a committed native assistant partial; ancestor replacements use one complete message. */
function messageChangesImpl(
  self: ReadonlyArray<View.Op>,
  before: Prompt.AssistantMessage,
  message: Prompt.AssistantMessage,
): Array<MessageChange> {
  // Coalesce paths against the complete committed values. Keep the first-touch
  // order, but decide every block fallback before publishing any narrow delta.
  const touched = new Map<number, Map<string, View.Path>>()
  const whole = new Set<number>()
  const starts = new Set<number>()
  for (const op of self) {
    if (op[0] === 'replace') return [{ _tag: 'message', type: 'message', message }]
    const path = op[1]
    if (!startsWith(path, partialPath)) {
      if (startsWith(partialPath, path)) return [{ _tag: 'message', type: 'message', message }]
      continue
    }
    const rest = path.slice(partialPath.length)
    if (rest[0] === 'usage' || rest[0] === 'options') continue
    if (rest[0] !== 'content') return [{ _tag: 'message', type: 'message', message }]
    if (rest.length === 1) {
      if (message.content.length < before.content.length)
        return [{ _tag: 'message', type: 'message', message }]
      for (let index = 0; index < message.content.length; index++) {
        const block = message.content[index]
        const previous = before.content[index]
        if (block === undefined || (previous !== undefined && samePart(block, previous))) continue
        if (!touched.has(index)) touched.set(index, new Map())
        whole.add(index)
        if (index >= before.content.length) starts.add(index)
      }
      continue
    }
    const index = rest[1]
    if (!Predicate.isNumber(index)) continue
    if (message.content[index] === undefined) return [{ _tag: 'message', type: 'message', message }]
    let paths = touched.get(index)
    if (paths === undefined) {
      paths = new Map()
      touched.set(index, paths)
    }
    const tail = rest.slice(2)
    paths.set(JSON.stringify(tail), tail)
    if (op[0] !== 'set') whole.add(index)
  }
  const changes: Array<MessageChange> = []
  const at = (value: unknown, path: View.Path): unknown => {
    for (const segment of path)
      value =
        Predicate.isObjectOrArray(value) && Object.hasOwn(value, segment)
          ? Reflect.get(value, segment)
          : undefined
    return value
  }
  for (const [index, paths] of touched) {
    const block = message.content[index]
    const previous = before.content[index]
    if (block === undefined) return [{ _tag: 'message', type: 'message', message }]
    const deltas: Array<MessageChange> = []
    for (const path of paths.values()) {
      if (
        path.length === 1 &&
        path[0] === 'text' &&
        (block.type === 'text' || block.type === 'reasoning') &&
        previous?.type === block.type &&
        Predicate.hasProperty(previous, 'text') &&
        block.text.startsWith(previous.text)
      ) {
        deltas.push(
          block.type === 'reasoning'
            ? {
                _tag: 'thinking_delta',
                type: 'thinking_delta',
                contentIndex: index,
                delta: block.text.slice(previous.text.length),
              }
            : {
                _tag: 'text_delta',
                type: 'text_delta',
                contentIndex: index,
                delta: block.text.slice(previous.text.length),
              },
        )
      } else if (
        path[0] === 'params' &&
        block.type === 'tool-call' &&
        previous?.type === 'tool-call'
      ) {
        const paramPath = path.slice(1)
        const previousValue = at(previous.params, paramPath)
        const finalValue = at(block.params, paramPath)
        if (
          typeof previousValue === 'string' &&
          typeof finalValue === 'string' &&
          finalValue.startsWith(previousValue)
        )
          deltas.push({
            _tag: 'toolcall_delta',
            type: 'toolcall_delta',
            contentIndex: index,
            path: paramPath,
            delta: finalValue.slice(previousValue.length),
          })
        else whole.add(index)
      } else whole.add(index)
    }
    if (whole.has(index)) {
      let type: 'text_start' | 'thinking_start' | 'toolcall_start' = 'toolcall_start'
      if (block.type === 'text') type = 'text_start'
      else if (block.type === 'reasoning') type = 'thinking_start'
      if (
        !starts.has(index) ||
        (block.type !== 'text' && block.type !== 'reasoning' && block.type !== 'tool-call')
      )
        changes.push({ _tag: 'block', type: 'block', contentIndex: index, block })
      else if (type === 'text_start')
        changes.push({ _tag: 'text_start', type: 'text_start', contentIndex: index, block })
      else if (type === 'thinking_start')
        changes.push({ _tag: 'thinking_start', type: 'thinking_start', contentIndex: index, block })
      else
        changes.push({ _tag: 'toolcall_start', type: 'toolcall_start', contentIndex: index, block })
    } else changes.push(...deltas)
  }
  return Arr.isArrayEmpty(changes) && !sameParts(before.content, message.content)
    ? [{ _tag: 'message', type: 'message', message }]
    : changes
}
/** A retained output window is a front trim followed by an append when its overlap is known. */
function outputChangeImpl(
  self: string | undefined,
  that: string | undefined,
): Option.Option<NonNullable<Extract<AgentEvent, { type: 'tool_execution_update' }>['output']>> {
  if (self === that) return Option.none()
  if (self === undefined || that === undefined) return Option.some({ set: that ?? '' })
  const prefix = new Uint32Array(that.length)
  for (let index = 1; index < that.length; index++) {
    let matched = prefix[index - 1] ?? 0
    while (matched > 0 && that[index] !== that[matched]) matched = prefix[matched - 1] ?? 0
    if (that[index] === that[matched]) matched++
    prefix[index] = matched
  }
  let overlap = 0
  for (let index = 0; index < self.length; index++) {
    while (overlap > 0 && self[index] !== that[overlap]) overlap = prefix[overlap - 1] ?? 0
    if (self[index] === that[overlap]) overlap++
  }
  if (overlap > 0) {
    const trimStart = self.length - overlap
    return Option.some({
      ...(trimStart === 0 ? {} : { trimStart }),
      ...(overlap === that.length ? {} : { append: that.slice(overlap) }),
    })
  }
  return Option.some({ set: that })
}

/**
 * Translates one domain commit in progress/end/submission/state/start order; held generations end their turn once.
 *
 * @category combinators
 */
export const translate = Effect.fnUntraced(function* (
  id: Record.ConversationId,
  change: View.Change,
  held: Ref.Ref<HashSet.HashSet<Record.TaskId>>,
): Effect.fn.Return<Batch, StorageError> {
  const frame = change.publication
  if (frame === undefined) return [yield* snapshot(change.value)]
  const entries: Array<Record.Entry> = []
  const tasks = new Map<Record.TaskId, Record.Task>()
  const submissions = new Map<Record.SubmissionId, Record.Submission>()
  for (const write of frame.writes) {
    if (write.type === 'entry' && write.value.conversationId === id) entries.push(write.value)
    if (write.type === 'task' && write.value.conversationId === id)
      tasks.set(write.value.id, write.value)
    if (write.type === 'submission' && write.value.conversationId === id)
      submissions.set(write.value.id, write.value)
  }
  // effect-review-allow P1-order-equivalence-params: entries is a newly allocated local buffer; its ordering is updated before event publication.
  entries.sort(Order.mapInput(Order.Number, (item: Record.Entry) => item.id))
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
  const events: Array<AgentEvent> = []
  const previousSlots = new Map((was.live.tools ?? []).map((slot) => [slot.callId, slot]))
  const slots = now.live.tools ?? []
  for (const slot of slots) {
    if (slot.status !== 'running' || previousSlots.get(slot.callId)?.status === 'running') continue
    const task = slot.taskId === undefined ? undefined : tasks.get(slot.taskId)
    const binding = Schema.decodeUnknownOption(Ownership.Binding)(task?.input)
    const payload =
      Option.isSome(binding) && binding.value.workflow === ToolCall._tag
        ? Schema.decodeUnknownOption(ToolCall.payloadSchema)(binding.value.payload)
        : Option.none()
    const checkpoint = Schema.decodeUnknownOption(Outcome.ToolCheckpoint)(task?.state.checkpoint)
    let args: Record.Json = {}
    if (Option.isSome(payload))
      args = Option.isSome(checkpoint) ? checkpoint.value.arguments : payload.value.arguments
    events.push({
      _tag: 'tool_execution_start',
      type: 'tool_execution_start',
      toolCallId: slot.callId,
      toolName: slot.name,
      args,
    })
  }
  if (touchedPartial && now.partial !== undefined && was.partial === undefined)
    events.push({ _tag: 'message_start', type: 'message_start', message: now.partial })
  else if (
    now.partial !== undefined &&
    was.partial !== undefined &&
    ((touchedPartial && now.live.generation?.message !== was.live.generation?.message) ||
      (touchedUsage && now.live.generation?.usage !== was.live.generation?.usage))
  ) {
    events.push({
      _tag: 'message_update',
      type: 'message_update',
      usage: now.currentUsage ?? Usage.zero(),
      changes: messageChanges(change.ops, was.partial, now.partial),
    })
  }
  for (const slot of slots) {
    const previous = previousSlots.get(slot.callId)
    if (slot.status !== 'running' || previous?.status !== 'running') continue
    const output = outputChange(previous.output, slot.output)
    const detailsChanged = previous.details !== slot.details
    const diagnosticsChanged = previous.diagnostics !== slot.diagnostics
    if (Option.isNone(output) && !detailsChanged && !diagnosticsChanged) continue
    events.push({
      _tag: 'tool_execution_update',
      type: 'tool_execution_update',
      toolCallId: slot.callId,
      toolName: slot.name,
      ...Option.match(output, { onNone: () => ({}), onSome: (output) => ({ output }) }),
      ...(detailsChanged ? { details: slot.details ?? null } : {}),
      ...(diagnosticsChanged ? { diagnostics: slot.diagnostics ?? [] } : {}),
    })
  }
  const generation = now.live.generation
  const generationBefore = was.live.generation
  if (generation?.retry !== undefined && generationBefore?.retry === undefined)
    events.push({
      _tag: 'auto_retry_start',
      type: 'auto_retry_start',
      attempt: generation.attempt,
      at: generation.retry.at,
      errorMessage: generation.retry.error,
    })
  if (generationBefore?.retry !== undefined && generation?.retry === undefined)
    events.push({
      _tag: 'auto_retry_end',
      type: 'auto_retry_end',
      attempt: generationBefore.attempt,
    })
  if (
    generation?.deferred !== undefined &&
    (generationBefore?.deferred === undefined ||
      !DateTime.Equivalence(generation.deferred.pollAt, generationBefore.deferred.pollAt))
  )
    events.push({
      _tag: 'deferred_poll',
      type: 'deferred_poll',
      pollAt: generation.deferred.pollAt,
    })
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
    const entry = Arr.findFirst(entries, (item) => item.id === entryId)
    ends.push({
      _tag: 'tool_execution_end',
      type: 'tool_execution_end',
      toolCallId: callId,
      toolName: name,
      ...(Option.isNone(entry) ? {} : { entry: entry.value }),
    })
  }
  for (const previous of previousSlots.values()) {
    if (previous.status === 'done') continue
    const slot = Arr.findFirst(slots, (item) => item.callId === previous.callId)
    if (Option.isSome(slot) && slot.value.status === 'done')
      end(previous.callId, previous.name, slot.value.entry)
    else if (Option.isNone(slot)) {
      const result = Arr.findFirst(
        decodedEntries,
        ({ message }) =>
          message?.role === 'tool' &&
          message.content.some(
            (part) => part.type === 'tool-result' && part.id === previous.callId,
          ),
      )
      end(
        previous.callId,
        previous.name,
        Option.getOrUndefined(Option.map(result, (found) => found.entry.id)),
      )
    }
  }
  for (const slot of slots)
    if (slot.status === 'done' && !previousSlots.has(slot.callId))
      end(slot.callId, slot.name, slot.entry)
  let assistantAppended = false
  for (const { entry, message } of decodedEntries) {
    events.push(...Arr.filter(ends, (item) => item.entry === entry))
    if (message === undefined) {
      events.push({ _tag: 'entry_appended', type: 'entry_appended', entry })
      continue
    }
    const streamed = message.role === 'assistant' && was.partial !== undefined && !assistantAppended
    if (message.role === 'assistant') assistantAppended = true
    if (!streamed) events.push({ _tag: 'message_start', type: 'message_start', message })
    events.push({ _tag: 'message_end', type: 'message_end', entry })
  }
  events.push(...Arr.filter(ends, (item) => item.entry === undefined))
  const compactionsBefore = was.live.compactions ?? []
  const compactions = now.live.compactions ?? []
  for (const { taskId, reason } of compactionsBefore)
    if (!compactions.some((item) => item.taskId === taskId))
      events.push({ _tag: 'compaction_end', type: 'compaction_end', taskId, reason })
  let turnEnded = false
  for (const task of tasks.values()) {
    if (
      generationKind(task.kind) &&
      task.state.status === 'completing' &&
      !HashSet.has(yield* Ref.get(held), task.id)
    ) {
      yield* Ref.update(held, HashSet.add(task.id))
      turnEnded = true
    }
    if (task.state.status !== 'terminal') continue
    if (generationKind(task.kind)) {
      if (!HashSet.has(yield* Ref.get(held), task.id)) turnEnded = true
      yield* Ref.update(held, HashSet.remove(task.id))
    }
    const outcome = Outcome.classifyTask(task)
    const status = outcome?.directStatus
    if (status === 'faulted' || status === 'orphaned') {
      events.push({
        _tag: 'task_failed',
        type: 'task_failed',
        taskId: task.id,
        kind: task.kind,
        message: outcome?.message ?? 'Task failed',
      })
    }
  }
  if (turnEnded) events.push({ _tag: 'turn_end', type: 'turn_end' })
  const run = now.live.run
  const runBefore = was.live.run
  const runChanged = run?.inputs[0] !== runBefore?.inputs[0]
  if (runBefore !== undefined && runChanged)
    events.push({ _tag: 'run_end', type: 'run_end', inputs: runBefore.inputs })
  for (const record of Arr.sortWith([...submissions.values()], (item) => item.id, Order.Number))
    events.push({ _tag: 'submission', type: 'submission', record })
  if (change.value.docs['harness.inbox'] !== change.before.docs['harness.inbox'])
    events.push({ _tag: 'inbox_update', type: 'inbox_update', items: queued(now.inbox) })
  if (change.value.docs['harness.agent'] !== change.before.docs['harness.agent'])
    events.push({ _tag: 'agent_changed', type: 'agent_changed', agent: now.agent ?? {} })
  if (change.value.docs['harness.usage'] !== change.before.docs['harness.usage'])
    events.push({ _tag: 'usage_changed', type: 'usage_changed', usage: now.usage ?? Usage.empty() })
  for (const { taskId, reason, blocking } of compactions)
    if (!compactionsBefore.some((item) => item.taskId === taskId))
      events.push({ _tag: 'compaction_start', type: 'compaction_start', taskId, reason, blocking })
  if (run !== undefined && runChanged)
    events.push({ _tag: 'run_start', type: 'run_start', inputs: run.inputs })
  if (
    run !== undefined &&
    run.taskId !== runBefore?.taskId &&
    generationKind(tasks.get(run.taskId)?.kind ?? '')
  )
    events.push({ _tag: 'turn_start', type: 'turn_start' })
  return events
})
/**
 * Scoped agent-event service acquisition.
 *
 * @category constructors
 */
export const make: Effect.Effect<Service, never, View.View> = Effect.gen(function* () {
  const views = yield* View.View
  return Event.of({
    watch: Effect.fnUntraced(function* (id) {
      const held = yield* Ref.make(HashSet.empty<Record.TaskId>())
      let initial: Snapshot | undefined
      const seedHeld = (tasks: ReadonlyArray<Record.Task>) =>
        Ref.set(
          held,
          HashSet.fromIterable(
            Arr.filter(
              tasks,
              (task) => generationKind(task.kind) && task.state.status === 'completing',
            ).map((task) => task.id),
          ),
        )
      const subscription = yield* views.observe<Batch>(
        id,
        View.makeProjection<Batch>({
          initial: Effect.fnUntraced(function* (value, tasks) {
            yield* seedHeld(tasks)
            initial = yield* snapshot(value)
            return [initial]
          }),
          project: (change) =>
            translate(id, change, held).pipe(
              Effect.map((batch) => (Arr.isReadonlyArrayEmpty(batch) ? undefined : batch)),
            ),
          reset: Effect.fnUntraced(function* (value, _seq, tasks) {
            yield* seedHeld(tasks)
            return [yield* snapshot(value)]
          }),
        }),
      )
      if (initial === undefined)
        return yield* rejected('Event snapshot was not initialized', Corrupt)
      return Object.assign(
        View.makeProjectionWatch({
          get value() {
            return subscription.value
          },
          changes: subscription.changes,
          closed: subscription.closed,
          stop: subscription.stop,
          listen: subscription.listen,
        }),
        { snapshot: initial },
      )
    }),
  })
})
/**
 * Provides semantic conversation observation from View.
 *
 * **Details**
 *
 * Uses the same committed structural stream and subscription lifetime as View projections.
 *
 * @category layers
 */
export const layer: Layer.Layer<Event, never, View.View> = Layer.effect(Event, make)

/**
 * Coalesces draft operations into final assistant-message changes.
 *
 * @category combinators
 */
export const messageChanges: {
  (
    before: Prompt.AssistantMessage,
    message: Prompt.AssistantMessage,
  ): (self: ReadonlyArray<View.Op>) => Array<MessageChange>
  (
    self: ReadonlyArray<View.Op>,
    before: Prompt.AssistantMessage,
    message: Prompt.AssistantMessage,
  ): Array<MessageChange>
} = dual(3, messageChangesImpl)

/**
 * Returns the final output change between two assistant messages.
 *
 * @category combinators
 */
export const outputChange: {
  (
    that: string | undefined,
  ): (
    self: string | undefined,
  ) => Option.Option<NonNullable<Extract<AgentEvent, { type: 'tool_execution_update' }>['output']>>
  (
    self: string | undefined,
    that: string | undefined,
  ): Option.Option<NonNullable<Extract<AgentEvent, { type: 'tool_execution_update' }>['output']>>
} = dual(2, outputChangeImpl)

/**
 * Type-level contracts for `Event`.
 *
 * @category utility types
 */
export declare namespace Event {
  /**
   * Submission metadata included in an event snapshot.
   *
   * @category models
   */
  export interface QueuedItem {
    readonly id: Record.SubmissionId
    readonly mode: Inbox.Item['mode']
  }
  /**
   * Incremental change to committed model-visible message content.
   *
   * @category models
   */
  export type MessageChange = typeof MessageChange.Type
  /**
   * Committed conversation execution, queue, usage and entry snapshot.
   *
   * @category models
   */
  export type Snapshot = typeof Snapshot.Type
  /**
   * Ordered semantic events emitted for one committed update.
   *
   * @category models
   */
  export type Batch = typeof Batch.Type
  /**
   * Scoped initial semantic snapshot and subsequent event batches.
   *
   * @category models
   */
  export interface Watch extends View.ProjectionWatch<Batch> {
    readonly snapshot: Snapshot
  }
  /**
   * Semantic observation operations built on committed View projections.
   *
   * @category models
   */
  export interface Service {
    /**
     * Acquires an initial semantic snapshot and scoped stream of ordered committed event
     * batches.
     */
    readonly watch: (id: Record.ConversationId) => Effect.Effect<Watch, StorageError, Scope.Scope>
  }
}
