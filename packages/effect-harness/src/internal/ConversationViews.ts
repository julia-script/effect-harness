/** Complete backing facts are captured once on the native line, then advanced from adopted batches. */
import * as Effect from 'effect/Effect'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as Struct from 'effect/Struct'
import * as Record from '../Record.js'
import type * as Session from '../Session.js'
import * as View from '../ConversationView.js'
import * as Agent from '../Agent.js'
import * as Live from '../LiveState.js'
import * as Progress from '../ToolProgress.js'
import * as Compaction from '../CompactionStatus.js'
import * as RunControl from '../RunControl.js'
import * as Usage from '../Usage.js'
import * as ToolResult from '../ToolResult.js'
import * as Subagent from '../Subagent.js'
import { ObservationError } from '../ObservationError.js'
import type * as Runtime from './SessionRuntime.js'
import * as Graph from './TaskGraph.js'
import * as AgentState from './AgentState.js'
import * as Protocol from './ContextProtocol.js'
import * as Projection from '../ContextProjection.js'
import * as Round from './ToolRound.js'
import * as Run from './RunState.js'
import * as Nested from './NestedInvocation.js'
import * as NestedState from './NestedObservationState.js'
import * as Supervisor from './SubagentTask.js'
import * as Value from './DocumentValue.js'

export interface Facts {
  readonly id: Record.ConversationId
  readonly conversations: Map<Record.ConversationId, Record.Conversation>
  readonly tasks: Map<Record.TaskId, Record.Task>
  readonly entries: Map<Record.EntryId, Record.Entry>
  readonly submissions: Map<Record.SubmissionId, Record.Submission>
  readonly documents: Map<Record.DocumentId, Record.StoredDocument>
  readonly ancestry: Map<Record.ConversationId, number>
}
const conversationKinds = new Set([
  AgentState.document.definition.kind,
  Live.document.definition.kind,
  Progress.document.definition.kind,
  Compaction.document.definition.kind,
  RunControl.inbox.definition.kind,
  RunControl.document.definition.kind,
  RunControl.contextProtocol.definition.kind,
  Round.document.definition.kind,
  'harness.usage.receipt',
  NestedState.document.definition.kind,
])
const taskKinds = new Set([
  Nested.calls.definition.kind,
  Nested.results.definition.kind,
  'harness.subagent.slots',
])
const acceptsDocument = (facts: Facts, doc: Record.StoredDocument) =>
  (doc.record.scope._tag === 'conversation' &&
    doc.record.scope.conversationId === facts.id &&
    conversationKinds.has(doc.record.kind)) ||
  (doc.record.scope._tag === 'task' &&
    facts.tasks.has(doc.record.scope.taskId) &&
    taskKinds.has(doc.record.kind))

export const hydrate = Effect.fnUntraced(function* (
  runtime: Runtime.State,
  id: Record.ConversationId,
) {
  const storage = runtime.storage
  const conversation = yield* storage.conversation(id)
  if (Option.isNone(conversation))
    return yield* new ObservationError({
      reason: 'notFound',
      operation: 'view.snapshot',
      message: 'The conversation does not exist',
    })
  const tasks = yield* storage.scanTasks({ conversationId: id }).pipe(Stream.runCollect)
  const owned = yield* storage
    .scanConversations({ ownerConversationId: id })
    .pipe(Stream.runCollect)
  const entries = yield* storage
    .scanEntries({ conversationId: id, order: 'ascending' })
    .pipe(Stream.runCollect)
  const submissions = yield* storage.scanSubmissions({ conversationId: id }).pipe(Stream.runCollect)
  const ancestry = new Map<Record.ConversationId, number>()
  let at: Record.Conversation = conversation.value
  let cap = Number.MAX_SAFE_INTEGER
  while (true) {
    if (ancestry.has(at.id))
      return yield* new ObservationError({
        reason: 'projection',
        operation: 'view.hydrate',
        message: 'Conversation ancestry is cyclic',
      })
    ancestry.set(at.id, cap)
    if (at.parent === undefined) break
    cap = Math.min(cap, at.parent.at)
    const parent = yield* storage.conversation(at.parent.conversationId)
    if (Option.isNone(parent))
      return yield* new ObservationError({
        reason: 'projection',
        operation: 'view.hydrate',
        message: 'Conversation ancestry is absent',
      })
    at = parent.value
  }
  const facts: Facts = {
    id,
    conversations: new Map([conversation.value, ...owned].map((c) => [c.id, c])),
    tasks: new Map(tasks.map((t) => [t.id, t])),
    entries: new Map(entries.map((e) => [e.id, e])),
    submissions: new Map(submissions.map((s) => [s.id, s])),
    documents: new Map(),
    ancestry,
  }
  const scopes: Record.Scope[] = [
    { _tag: 'conversation', conversationId: id },
    ...tasks
      .filter((t) => t.state.status !== 'terminal')
      .map((t): Record.Scope => ({ _tag: 'task', taskId: t.id })),
  ]
  for (const scope of scopes) {
    const members = yield* storage.scanDocuments({ scope }).pipe(Stream.runCollect)
    for (const member of members) {
      if (!(scope._tag === 'conversation' ? conversationKinds : taskKinds).has(member.kind))
        continue
      const value = yield* storage.document(member.id)
      if (Option.isNone(value))
        return yield* new ObservationError({
          reason: 'projection',
          operation: 'view.hydrate',
          message: 'A mounted document has no current value',
        })
      facts.documents.set(member.id, value.value)
    }
  }
  return facts
})

export const advance = (
  facts: Facts,
  commit: Session.Commit,
  documents: ReadonlyMap<Record.DocumentId, Record.StoredDocument | null>,
): void => {
  // Ownership edges are interpreted only after every task replacement in this batch.
  for (const write of commit.changes)
    if (write._tag === 'task' && write.value.conversationId === facts.id)
      facts.tasks.set(write.value.id, write.value)
  for (const write of commit.changes) {
    if (
      write._tag === 'conversation' &&
      (write.value.id === facts.id || write.value.owner?.conversationId === facts.id)
    )
      facts.conversations.set(write.value.id, write.value)
    else if (
      write._tag === 'entry' &&
      write.value.id <= (facts.ancestry.get(write.value.conversationId) ?? -1)
    )
      facts.entries.set(write.value.id, write.value)
    else if (write._tag === 'submission' && write.value.conversationId === facts.id)
      facts.submissions.set(write.value.id, write.value)
  }
  for (const [id, value] of documents) {
    if (value === null) facts.documents.delete(id)
    else if (acceptsDocument(facts, value)) facts.documents.set(id, value)
  }
}
const decode = <S extends Schema.ConstraintDecoder<unknown, never>>(
  schema: S,
  doc: Record.StoredDocument,
) => {
  if (doc.version !== 1)
    return Effect.fail(
      new ObservationError({
        reason: 'projection',
        operation: 'view.document',
        message: 'Unsupported built-in document version',
      }),
    )
  return Schema.decodeEffect(schema)(doc.value)
}
export const project = Effect.fnUntraced(function* (facts: Facts) {
  const docs = [...facts.documents.values()].sort((a, b) => a.record.id - b.record.id)
  const mounted: View.Value['documents'][number][] = []
  let agent: Agent.State = {}
  let live: Live.Snapshot = {}
  let run: RunControl.State = {}
  let inbox: typeof RunControl.InboxSchema.Type = { items: [] }
  let compactions: typeof Compaction.SnapshotSchema.Type = { items: [] }
  let protocol: Protocol.Protocol | undefined
  const progress: View.Value['progress'][number][] = []
  const rounds: View.Value['rounds'][number][] = []
  const usage: Usage.Receipt[] = []
  const nestedReceipts: (typeof View.ReceiptSchema.Type)[] = []
  for (const doc of docs) {
    const base = { record: doc.record, version: doc.version }
    switch (doc.record.kind) {
      case 'harness.agent':
        agent = yield* decode(Agent.StateSchema, doc)
        mounted.push({ _tag: 'Agent', ...base, value: agent })
        break
      case 'harness.model.live':
        live = yield* decode(Live.SnapshotSchema, doc)
        mounted.push({ _tag: 'Live', ...base, value: live })
        break
      case 'harness.runControl':
        run = yield* decode(RunControl.StateSchema, doc)
        mounted.push({ _tag: 'Run', ...base, value: run })
        break
      case 'harness.inbox':
        inbox = yield* decode(RunControl.InboxSchema, doc)
        mounted.push({ _tag: 'Inbox', ...base, value: inbox })
        break
      case 'harness.compactionStatus':
        compactions = yield* decode(Compaction.SnapshotSchema, doc)
        mounted.push({ _tag: 'Compaction', ...base, value: compactions })
        break
      case 'harness.contextProtocol':
        protocol = yield* decode(Protocol.ProtocolSchema, doc)
        break
      case 'harness.tool.progress': {
        const value = yield* decode(Progress.Snapshot, doc)
        progress.push({ key: doc.record.key ?? '', value })
        mounted.push({ _tag: 'Progress', ...base, value })
        break
      }
      case 'harness.usage.receipt': {
        const saved = yield* decode(
          Schema.Struct({ receipt: Schema.optionalKey(Usage.ReceiptSchema) }),
          doc,
        )
        if (saved.receipt !== undefined) usage.push(saved.receipt)
        break
      }
      case 'harness.tool.round': {
        const saved = yield* decode(Round.document.definition.schema, doc)
        if (saved.round !== undefined)
          rounds.push({
            key: doc.record.key ?? '',
            ownerTaskId: saved.round.ownerTaskId,
            assistant: saved.round.assistant,
            mode: saved.round.mode,
            applied: saved.round.applied,
            slots: saved.round.slots.map((slot) => ({
              callId: slot.call.id,
              name: slot.call.name,
              ...(slot.taskId === undefined ? {} : { taskId: slot.taskId }),
              ...(slot.entryId === undefined ? {} : { entryId: slot.entryId }),
              ...(slot.receipt === undefined ? {} : { status: slot.receipt.status }),
              ...(slot.result === undefined
                ? {}
                : {
                    result: ToolResult.nested(
                      slot.result,
                      slot.result.structuredOutput !== undefined,
                    ),
                  }),
            })),
          })
        break
      }
      case 'harness.tool.nested.receipt': {
        const saved = yield* decode(NestedState.document.definition.schema, doc)
        if (saved.receipt !== undefined) {
          if (doc.record.key !== NestedState.key(saved.receipt))
            return yield* new ObservationError({
              reason: 'projection',
              operation: 'view.nested',
              message: 'Retained nested identity differs from its member key',
            })
          if (
            !nestedReceipts.some(
              (r) => r.taskId === saved.receipt!.taskId && r.attempt === saved.receipt!.attempt,
            )
          )
            nestedReceipts.push(Struct.omit(saved.receipt, ['legacyValue']))
        }
        break
      }
      case 'harness.tool.nested.result': {
        const saved = yield* decode(Nested.results.definition.schema, doc)
        if (
          saved.receipt !== undefined &&
          !nestedReceipts.some(
            (r) => r.taskId === saved.receipt!.taskId && r.attempt === saved.receipt!.attempt,
          )
        )
          nestedReceipts.push(Struct.omit(saved.receipt, ['legacyValue']))
        break
      }
      default:
        break
    }
  }
  const entries = [...facts.entries.values()].sort((a, b) => a.id - b.id)
  if (run.active !== undefined) {
    const task = facts.tasks.get(run.active.taskId)
    if (task === undefined || task.conversationId !== facts.id || task.kind !== 'harness.run')
      return yield* new ObservationError({
        reason: 'projection',
        operation: 'view.run',
        message: 'Run membership does not identify its actual native task',
      })
    if (
      task.abortRequested ||
      task.state.status === 'completing' ||
      task.state.status === 'terminal'
    )
      run = {}
  }
  const context =
    protocol === undefined
      ? yield* Projection.project(facts.id, entries)
      : yield* Protocol.project(facts.id, entries, protocol)
  const terminal: View.Value['terminal'][number][] = []
  const generations: View.Value['generations'][number][] = []
  const nested: View.Value['nested'][number][] = []
  const subagents: View.Value['subagents'][number][] = []
  for (const task of [...facts.tasks.values()].sort((a, b) => a.id - b.id)) {
    if (task.state.status === 'terminal' && Schema.is(Record.TaskOutcome)(task.state.outcome)) {
      const outcome = task.state.outcome
      terminal.push({
        taskId: task.id,
        status: outcome.status,
        ...('reason' in outcome && typeof outcome.reason === 'string'
          ? { reason: outcome.reason }
          : {}),
        ...('error' in outcome &&
        typeof outcome.error === 'object' &&
        outcome.error !== null &&
        'message' in outcome.error &&
        typeof outcome.error.message === 'string'
          ? { error: { message: outcome.error.message } }
          : {}),
      })
    }
    if (task.kind === 'harness.run' && task.version === 1 && task.state.checkpoint !== undefined) {
      const state = yield* Run.decode(task.state.checkpoint)
      generations.push({
        taskId: task.id,
        phase: state.phase,
        ...(state._tag === 'queued'
          ? {}
          : {
              ...(state.attempt === undefined ? {} : { attempt: state.attempt }),
              ...(state.ordinal === undefined ? {} : { ordinal: state.ordinal }),
              ...(state.preparation === undefined
                ? {}
                : {
                    requestId: state.preparation.requestId,
                    selection: {
                      agent: state.preparation.agent,
                      settings: state.preparation.settings,
                      modelBinding: state.preparation.model.binding,
                      configurationBinding: state.preparation.configurationBinding,
                      registryGeneration: state.preparation.registryGeneration,
                    },
                  }),
            }),
        ...(state._tag === 'retry' ? { until: state.until } : {}),
        ...(state._tag === 'deferred' ? { pollAt: state.pollAt, expiresAt: state.expiresAt } : {}),
        ...(state._tag === 'deferred-start' ? { expiresAt: state.expiresAt } : {}),
        ...(state._tag === 'compaction' ? { child: state.child } : {}),
        ...(state._tag === 'toolRound' ? { roundKey: state.roundKey } : {}),
      })
    }
    if (task.kind === 'harness.tool.nested' && task.version === 1) {
      const input = yield* Schema.decodeEffect(Schema.toCodecJson(Nested.InputSchema))(task.input)
      const receipts = nestedReceipts.filter((r) => r.taskId === task.id)
      let availability: View.Value['nested'][number]['availability'] =
        task.state.status === 'terminal' ? 'unavailable' : 'pending'
      if (receipts.length > 0) availability = 'available'
      nested.push({
        taskId: task.id,
        parentTaskId: input.parentTaskId,
        key: input.key,
        callId: input.logicalCallId,
        name: input.call.name,
        availability,
        receipts,
      })
    }
    if (task.kind === Supervisor.kind && task.version === 1) {
      const input = yield* Schema.decodeEffect(Schema.toCodecJson(Supervisor.InputSchema))(
        task.input,
      )
      let attachment: Subagent.Attachment | undefined
      if (task.state.checkpoint !== undefined) {
        const checkpoint = yield* Schema.decodeEffect(
          Schema.toCodecJson(Supervisor.CheckpointSchema),
        )(task.state.checkpoint)
        if (checkpoint.phase === 'observe')
          attachment = {
            conversationId: checkpoint.conversationId,
            submissionId: checkpoint.submissionId,
          }
      } else if (
        Schema.is(Record.TaskOutcome)(task.state.outcome) &&
        'result' in task.state.outcome &&
        Schema.is(Subagent.ReceiptSchema)(task.state.outcome.result)
      )
        attachment = {
          conversationId: task.state.outcome.result.conversationId,
          submissionId: task.state.outcome.result.submissionId,
        }
      subagents.push({
        handle: {
          originTaskId: input.originTaskId,
          originConversationId: input.originConversationId,
          taskId: task.id,
          key: input.key,
          mode: input.mode,
        },
        status: task.state.status,
        ...(attachment === undefined ? {} : { attachment }),
      })
    }
  }
  return yield* Value.copy(View.ValueSchema, {
    conversation: facts.conversations.get(facts.id)!,
    context,
    entries,
    graph: Graph.project(facts.tasks, facts.conversations),
    terminal,
    submissions: [...facts.submissions.values()].sort((a, b) => a.id - b.id),
    inbox,
    run,
    agent,
    live,
    generations,
    rounds,
    progress,
    compactions,
    nested,
    subagents,
    usage: Usage.aggregate(
      entries.filter((e) => e.conversationId === facts.id),
      usage,
    ),
    documents: mounted,
  })
})
