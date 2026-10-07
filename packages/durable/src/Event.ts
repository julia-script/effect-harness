import * as Time from '@effect-harness/harness/Time'
import * as DateTime from 'effect/DateTime'
import * as Outcome from './workflow/Outcome.ts'
import * as Ownership from './Ownership.ts'
import { ToolCall } from './workflow/ToolCall.ts'
import * as Option from 'effect/Option'
// Semantic commit ordering adapted from pi-durable (MIT), pinned 636703a0.
import * as Agent from '@effect-harness/harness/Agent'
import * as Invocation from '@effect-harness/harness/Invocation'
import * as Totals from '@effect-harness/harness/Usage'
import * as Context from 'effect/Context'
import * as Ref from 'effect/Ref'
import * as HashSet from 'effect/HashSet'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Scope from 'effect/Scope'
import * as Prompt from 'effect/ai/Prompt'
import * as Schema from 'effect/Schema'
import * as Inbox from './Inbox.ts'
import * as Record from './Record.ts'
import { rejected, type StorageError, Corrupt } from './StorageError.ts'
import * as View from './View.ts'

export const QueuedItem = Schema.Struct({
  id: Record.SubmissionId,
  mode: Schema.Literals(['steer', 'followUp', 'write']),
})
export type QueuedItem = typeof QueuedItem.Type
export const MessageChange = Schema.Union([
  Schema.Struct({
    type: Schema.Literals(['text_start', 'thinking_start', 'toolcall_start', 'block']),
    contentIndex: Schema.Int,
    block: Prompt.AssistantMessagePart,
  }),
  Schema.Struct({
    type: Schema.Literals(['text_delta', 'thinking_delta']),
    contentIndex: Schema.Int,
    delta: Schema.String,
  }),
  Schema.Struct({
    type: Schema.Literal('toolcall_delta'),
    contentIndex: Schema.Int,
    path: View.Path,
    delta: Schema.String,
  }),
  Schema.Struct({ type: Schema.Literal('message'), message: Prompt.AssistantMessage }),
])
export type MessageChange = typeof MessageChange.Type
export const Snapshot = Schema.Struct({
  type: Schema.Literal('snapshot'),
  entries: Schema.Array(Record.Entry),
  run: Schema.optionalKey(Schema.Struct({ inputs: Schema.Array(Record.SubmissionId) })),
  generation: Schema.optionalKey(
    Schema.Struct({
      attempt: Inbox.LiveDomain.fields.generation.schema.fields.attempt,
      model: Inbox.LiveDomain.fields.generation.schema.fields.model,
      retry: Inbox.LiveDomain.fields.generation.schema.fields.retry,
      deferred: Inbox.LiveDomain.fields.generation.schema.fields.deferred,
      message: Schema.optionalKey(Prompt.AssistantMessage),
      usage: Schema.optionalKey(Totals.Usage),
    }),
  ),
  tools: Schema.Array(Inbox.ToolSlot),
  compactions: Inbox.LiveDomain.fields.compactions.schema,
  inbox: Schema.Array(QueuedItem),
  agent: Agent.State,
  usage: Totals.State,
})
export type Snapshot = typeof Snapshot.Type
const toolIdentity = { toolCallId: Schema.String, toolName: Schema.String }
const compactionIdentity = {
  taskId: Record.TaskId,
  reason: Schema.Literals(['manual', 'threshold', 'overflow', 'background']),
}
export const AgentEvent = Schema.Union([
  Snapshot,
  Schema.Struct({
    type: Schema.Literals(['run_start', 'run_end']),
    inputs: Schema.Array(Record.SubmissionId),
  }),
  Schema.Struct({ type: Schema.Literals(['turn_start', 'turn_end']) }),
  Schema.Struct({ type: Schema.Literal('message_start'), message: Prompt.Message }),
  Schema.Struct({
    type: Schema.Literal('message_update'),
    usage: Totals.Usage,
    changes: Schema.Array(MessageChange),
  }),
  Schema.Struct({ type: Schema.Literals(['message_end', 'entry_appended']), entry: Record.Entry }),
  Schema.Struct({
    type: Schema.Literal('tool_execution_start'),
    ...toolIdentity,
    args: Schema.Json,
  }),
  Schema.Struct({
    type: Schema.Literal('tool_execution_update'),
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
  Schema.Struct({
    type: Schema.Literal('tool_execution_end'),
    ...toolIdentity,
    entry: Schema.optionalKey(Record.Entry),
  }),
  Schema.Struct({ type: Schema.Literal('inbox_update'), items: Schema.Array(QueuedItem) }),
  Schema.Struct({ type: Schema.Literal('submission'), record: Record.Submission }),
  Schema.Struct({
    type: Schema.Literal('auto_retry_start'),
    attempt: Schema.Int,
    at: Time.EpochMillis,
    errorMessage: Schema.String,
  }),
  Schema.Struct({ type: Schema.Literal('auto_retry_end'), attempt: Schema.Int }),
  Schema.Struct({ type: Schema.Literal('deferred_poll'), pollAt: Time.EpochMillis }),
  Schema.Struct({ type: Schema.Literal('agent_changed'), agent: Agent.State }),
  Schema.Struct({ type: Schema.Literal('usage_changed'), usage: Totals.State }),
  Schema.Struct({
    type: Schema.Literal('task_failed'),
    taskId: Record.TaskId,
    kind: Schema.String,
    message: Schema.String,
  }),
  Schema.Struct({
    type: Schema.Literal('compaction_start'),
    ...compactionIdentity,
    blocking: Schema.Boolean,
  }),
  Schema.Struct({ type: Schema.Literal('compaction_end'), ...compactionIdentity }),
])
export type AgentEvent = typeof AgentEvent.Type
export const Batch = Schema.Array(AgentEvent)
export type Batch = typeof Batch.Type
export const BatchJson = Schema.toCodecJson(Batch)
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
    Effect.mapError((cause) => rejected('Invalid committed event payload', Corrupt, cause)),
  )
const messageCodec = Schema.toCodecJson(Prompt.Message)
const assistantCodec = Schema.toCodecJson(Prompt.AssistantMessage)
const queued = (inbox: typeof Inbox.State.Type | undefined): ReadonlyArray<QueuedItem> =>
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
export function messageChanges(
  ops: ReadonlyArray<View.Op>,
  before: Prompt.AssistantMessage,
  message: Prompt.AssistantMessage,
): ReadonlyArray<MessageChange> {
  // Coalesce paths against the complete committed values. Keep the first-touch
  // order, but decide every block fallback before publishing any narrow delta.
  const touched = new Map<number, Map<string, View.Path>>()
  const whole = new Set<number>()
  const starts = new Set<number>()
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
        const previous = before.content[index]
        if (block === undefined || (previous !== undefined && samePart(block, previous))) continue
        if (!touched.has(index)) touched.set(index, new Map())
        whole.add(index)
        if (index >= before.content.length) starts.add(index)
      }
      continue
    }
    const index = rest[1]
    if (typeof index !== 'number') continue
    if (message.content[index] === undefined) return [{ type: 'message', message }]
    let paths = touched.get(index)
    if (paths === undefined) {
      paths = new Map()
      touched.set(index, paths)
    }
    const tail = rest.slice(2)
    paths.set(JSON.stringify(tail), tail)
    if (op[0] !== 'set') whole.add(index)
  }
  const changes: MessageChange[] = []
  const at = (value: unknown, path: View.Path): unknown => {
    for (const segment of path)
      value =
        value !== null && typeof value === 'object' && Object.hasOwn(value, segment)
          ? Reflect.get(value, segment)
          : undefined
    return value
  }
  for (const [index, paths] of touched) {
    const block = message.content[index]
    const previous = before.content[index]
    if (block === undefined) return [{ type: 'message', message }]
    const deltas: MessageChange[] = []
    for (const path of paths.values()) {
      if (
        path.length === 1 &&
        path[0] === 'text' &&
        (block.type === 'text' || block.type === 'reasoning') &&
        previous?.type === block.type &&
        'text' in previous &&
        block.text.startsWith(previous.text)
      ) {
        deltas.push({
          type: block.type === 'reasoning' ? 'thinking_delta' : 'text_delta',
          contentIndex: index,
          delta: block.text.slice(previous.text.length),
        })
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
      changes.push(
        starts.has(index) &&
          (block.type === 'text' || block.type === 'reasoning' || block.type === 'tool-call')
          ? { type, contentIndex: index, block }
          : { type: 'block', contentIndex: index, block },
      )
    } else changes.push(...deltas)
  }
  return changes.length === 0 && !sameParts(before.content, message.content)
    ? [{ type: 'message', message }]
    : changes
}
/** A retained output window is a front trim followed by an append when its overlap is known. */
export function outputChange(
  before: string | undefined,
  after: string | undefined,
): Option.Option<NonNullable<Extract<AgentEvent, { type: 'tool_execution_update' }>['output']>> {
  if (before === after) return Option.none()
  if (before === undefined || after === undefined) return Option.some({ set: after ?? '' })
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
    return Option.some({
      ...(trimStart === 0 ? {} : { trimStart }),
      ...(overlap === after.length ? {} : { append: after.slice(overlap) }),
    })
  }
  return Option.some({ set: after })
}

/** Translate one domain commit in progress/end/submission/state/start order; held generations end their turn once. */
export const translate = Effect.fnUntraced(function* (
  id: Record.ConversationId,
  change: View.Change,
  held: Ref.Ref<HashSet.HashSet<Record.TaskId>>,
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
    ((touchedPartial && now.live.generation?.message !== was.live.generation?.message) ||
      (touchedUsage && now.live.generation?.usage !== was.live.generation?.usage))
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
    const detailsChanged = previous.details !== slot.details
    const diagnosticsChanged = previous.diagnostics !== slot.diagnostics
    if (Option.isNone(output) && !detailsChanged && !diagnosticsChanged) continue
    events.push({
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
      type: 'auto_retry_start',
      attempt: generation.attempt,
      at: generation.retry.at,
      errorMessage: generation.retry.error,
    })
  if (generationBefore?.retry !== undefined && generation?.retry === undefined)
    events.push({ type: 'auto_retry_end', attempt: generationBefore.attempt })
  if (
    generation?.deferred !== undefined &&
    (generationBefore?.deferred === undefined ||
      !DateTime.Equivalence(generation.deferred.pollAt, generationBefore.deferred.pollAt))
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
        type: 'task_failed',
        taskId: task.id,
        kind: task.kind,
        message: outcome?.message ?? 'Task failed',
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
            tasks
              .filter((task) => generationKind(task.kind) && task.state.status === 'completing')
              .map((task) => task.id),
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
              Effect.map((batch) => (batch.length === 0 ? undefined : batch)),
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
export const layer: Layer.Layer<Event, never, View.View> = Layer.effect(Event, make)
