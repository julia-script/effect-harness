/**
 * Committed task inspection and structurally shared ownership graph projections.
 */
import * as MutableHashMap from 'effect/MutableHashMap'
import * as Rec from 'effect/Record'
import * as Order from 'effect/Order'
import * as Array from 'effect/Array'
import type { StorageError } from './StorageError.ts'
import { cursor as journalCursor } from './storage/internal/state.ts'
import * as Outcome from './workflow/Outcome.ts'
import * as Effect from 'effect/Effect'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as Ownership from './Ownership.ts'
import * as Record from './Record.ts'
import type * as Session from './Session.ts'
import type * as Store from './Store.ts'

/**
 * Schema for persisted task lifecycle classification without running its handler.
 *
 * @category schemas
 */
export const TaskState = Schema.Union([
  Schema.TaggedStruct('ready', {}),
  Schema.TaggedStruct('running', {}),
  Schema.TaggedStruct('waiting', { on: Schema.Array(Record.TaskId) }),
  Schema.TaggedStruct('completing', { outcome: Schema.Json }),
  Schema.TaggedStruct('blocked', {
    reason: Schema.Literals(['missing_workflow', 'invalid_binding']),
  }),
])
/**
 * Persisted task lifecycle classification without running its handler.
 *
 * @category models
 */
export type TaskState = typeof TaskState.Type
/**
 * Schema for task record paired with committed lifecycle diagnostics.
 *
 * @category schemas
 */
export const TaskInspection = Schema.Struct({ record: Record.Task, state: TaskState })
/**
 * Task record paired with committed lifecycle diagnostics.
 *
 * @category models
 */
export type TaskInspection = typeof TaskInspection.Type
/**
 * Schema for committed session inspection of conversations, tasks and submissions.
 *
 * @category schemas
 */
export const Value = Schema.Struct({
  tasks: Schema.Array(TaskInspection),
  submissions: Schema.Array(Record.Submission),
})
/**
 * Committed session inspection of conversations, tasks and submissions.
 *
 * @category models
 */
export type Value = typeof Value.Type

/**
 * Inspects committed facts and declaration metadata without invoking handlers, codecs, migrations, or the engine.
 *
 * @category combinators
 */
// effect-nit-allow B-no-service-arguments: get is a public combinator over the supplied Session self capability; its facts, journal and owning lifetime must remain those of the selected instance even when ambient services differ.
export const get = Effect.fnUntraced(function* (
  session: Session.Session.Service,
): Effect.fn.Return<Value, StorageError, Ownership.Declarations> {
  const state = yield* session.committed
  const declarations = yield* Ownership.Declarations
  const tasks = Array.sortWith(
    Array.filter(state.tasks, (task) => task.state.status !== 'terminal'),
    (item: Record.Task) => item.id,
    Order.Number,
  )
  const inspect = (task: Record.Task): TaskState => {
    if (task.state.status === 'completing')
      return { _tag: 'completing', outcome: task.state.outcome ?? null }
    const binding = Schema.decodeUnknownOption(Ownership.Binding)(task.input)
    if (Option.isNone(binding)) return { _tag: 'blocked', reason: 'invalid_binding' }
    if (Option.isNone(declarations.get(binding.value.workflow)))
      return { _tag: 'blocked', reason: 'missing_workflow' }
    if (task.state.status === 'running') return { _tag: 'running' }
    if (task.state.status === 'waiting') {
      const on = Array.filter(
        task.state.on ?? [],
        (id) =>
          !Option.exists(
            Array.findFirst(state.tasks, (child) => child.id === id),
            (child) => child.state.status === 'terminal',
          ),
      )
      if (Array.isReadonlyArrayNonEmpty(on)) return { _tag: 'waiting', on }
    }
    return { _tag: 'ready' }
  }
  return {
    tasks: tasks.map((record) => ({ record, state: inspect(record) })),
    submissions: Array.sortWith(
      Array.filter(
        state.submissions,
        (item) => item.status === 'queued' || item.status === 'placed',
      ),
      (item: Record.Submission) => item.id,
      Order.Number,
    ),
  } satisfies Value
})

/**
 * Schema for conversation or task represented in an ownership graph.
 *
 * @category schemas
 */
export const GraphNode = Schema.Struct({
  id: Record.Task.fields.id,
  kind: Record.Task.fields.kind,
  conversationId: Record.Task.fields.conversationId,
  owner: Record.Task.fields.owner,
  background: Record.Task.fields.background,
  abortRequested: Record.Task.fields.abortRequested,
  state: Schema.Struct({
    status: Schema.Literals(['pending', 'running', 'waiting', 'completing']),
    on: Record.Task.fields.state.fields.on,
    policy: Record.Task.fields.state.fields.policy,
    outcome: Schema.optionalKey(Schema.String),
  }),
  conversations: Schema.Array(Record.ConversationId),
})
/**
 * Conversation or task represented in an ownership graph.
 *
 * @category models
 */
export type GraphNode = typeof GraphNode.Type
/**
 * Schema for committed ownership graph for diagnostics or visualization.
 *
 * @category schemas
 */
export const Graph = Schema.Struct({ tasks: Schema.Record(Schema.String, GraphNode) })
/**
 * Committed ownership graph for diagnostics or visualization.
 *
 * @category models
 */
export type Graph = typeof Graph.Type
/**
 * Returns a live ownership graph without checkpoint or result payloads.
 *
 * @category combinators
 */
export function graph(state: Ownership.Graph): Graph {
  const tasks: Record<string, GraphNode> = Object.create(null)
  for (const task of Array.sortWith(state.tasks, (item: Record.Task) => item.id, Order.Number)) {
    if (task.state.status === 'terminal') continue
    const status = Outcome.classifyTaskOrUndefined(task)?.status
    tasks[String(task.id)] = {
      id: task.id,
      kind: task.kind,
      conversationId: task.conversationId,
      ...(task.owner === undefined ? {} : { owner: task.owner }),
      background: task.background,
      abortRequested: task.abortRequested,
      state: {
        status: task.state.status,
        ...(task.state.status === 'waiting'
          ? { on: task.state.on ?? [], policy: task.state.policy ?? 'allSettled' }
          : {}),
        ...(task.state.status === 'completing' && status !== undefined ? { outcome: status } : {}),
      },
      conversations: Array.sort(
        Array.filter(
          state.conversations,
          (conversation) => conversation.owner?.taskId === task.id,
        ).map((conversation) => conversation.id),
        Order.Number,
      ),
    }
  }
  return { tasks }
}

/**
 * Schema for incremental change to an ownership graph.
 *
 * @category schemas
 */
export const GraphOp = Schema.Union([
  Schema.Tuple([Schema.Literal('replace'), Graph]),
  Schema.Tuple([
    Schema.Literal('set'),
    Schema.Tuple([Schema.Literal('tasks'), Schema.String]),
    GraphNode,
  ]),
  Schema.Tuple([Schema.Literal('delete'), Schema.Tuple([Schema.Literal('tasks'), Schema.String])]),
])
/**
 * Incremental change to an ownership graph.
 *
 * @category models
 */
export type GraphOp = typeof GraphOp.Type
/**
 * Schema for ordered graph operations for a committed session update.
 *
 * @category schemas
 */
export const GraphChange = Schema.Struct({
  seq: Record.JournalCursor,
  before: Graph,
  value: Graph,
  ops: Schema.Array(GraphOp),
  reset: Schema.Boolean,
})
/**
 * Ordered graph operations for a committed session update.
 *
 * @category models
 */
export type GraphChange = typeof GraphChange.Type
const changed = (
  before: Graph,
  candidate: Graph,
): { readonly value: Graph; readonly ops: ReadonlyArray<GraphOp> } => {
  const tasks = { ...before.tasks }
  const ops: Array<GraphOp> = []
  for (const id of Rec.keys(before.tasks))
    if (!Object.hasOwn(candidate.tasks, id)) {
      delete tasks[id]
      ops.push(['delete', ['tasks', id]])
    }
  for (const [id, node] of Rec.toEntries(candidate.tasks))
    if (
      !Object.hasOwn(before.tasks, id) ||
      !Schema.toEquivalence(GraphNode)(before.tasks[id]!, node)
    ) {
      Rec.assignProperty(tasks, id, node)
      ops.push(['set', ['tasks', id], node])
    }
  return { value: Array.isReadonlyArrayEmpty(ops) ? before : { tasks }, ops }
}
/**
 * Streams exact committed ownership-graph changes.
 *
 * **Details**
 *
 * Preserves unchanged branches and emits a replacement when bounded journal retention overflows.
 *
 * @category combinators
 */
// effect-nit-allow B-no-service-arguments: changes is a public combinator over the supplied Store self capability; its facts, journal and owning lifetime must remain those of the selected instance even when ambient services differ.
export const changes = (
  store: Store.Store.Service,
): Stream.Stream<GraphChange, import('./StorageError.ts').StorageError> =>
  Stream.unwrap(
    Effect.gen(function* () {
      const initial = yield* store.committed
      let tasks = MutableHashMap.fromIterable(initial.tasks.map((task) => [task.id, task]))
      let conversations = MutableHashMap.fromIterable(
        initial.conversations.map((conversation) => [conversation.id, conversation]),
      )
      let value = graph(initial)
      const baseline: GraphChange = {
        seq: yield* journalCursor(initial.nextSeq),
        before: value,
        value,
        ops: [['replace', value]],
        reset: true,
      }
      const next = Stream.unfold(
        { after: baseline.seq, pending: [] as Array<GraphChange>, delivered: value },
        Effect.fnUntraced(function* (cursor) {
          let after = cursor.after
          const pending = [...cursor.pending]
          while (true) {
            const journal = yield* store
              .journal(after)
              .pipe(Effect.catchReason('StorageError', 'ClosedError', () => Effect.void))
            if (journal === undefined) return undefined
            if (journal.reset) {
              tasks = MutableHashMap.fromIterable(
                journal.state.tasks.map((task) => [task.id, task]),
              )
              conversations = MutableHashMap.fromIterable(
                journal.state.conversations.map((conversation) => [conversation.id, conversation]),
              )
              const before = cursor.delivered
              value = graph(journal.state)
              pending.splice(0, pending.length, {
                seq: yield* journalCursor(journal.state.nextSeq),
                before,
                value,
                ops: [['replace', value]],
                reset: true,
              })
            } else
              for (const frame of journal.frames) {
                let relevant = false
                for (const write of frame.writes) {
                  if (write._tag === 'task') {
                    MutableHashMap.set(tasks, write.value.id, write.value)
                    relevant = true
                  } else if (write._tag === 'conversation') {
                    MutableHashMap.set(conversations, write.value.id, write.value)
                    relevant = true
                  }
                }
                if (!relevant) continue
                const before = value
                const delta = changed(
                  before,
                  graph({
                    tasks: [...MutableHashMap.values(tasks)],
                    conversations: [...MutableHashMap.values(conversations)],
                  }),
                )
                value = delta.value
                if (Array.isReadonlyArrayNonEmpty(delta.ops))
                  pending.push({ seq: frame.seq, before, value, ops: delta.ops, reset: false })
              }
            after = yield* journalCursor(journal.state.nextSeq)
            if (pending.length > 100)
              pending.splice(0, pending.length, {
                seq: after,
                before: cursor.delivered,
                value,
                ops: [['replace', value]],
                reset: true,
              })
            const head = pending.shift()
            if (head !== undefined) {
              return [head, { after, pending, delivered: head.value }] as const
            }
            yield* Effect.sleep('20 millis')
          }
        }),
      )
      return Stream.concat(Stream.succeed(baseline), next)
    }),
  )

/** Checks the decoded TaskState contract without decoding or coercing input.
 * @category guards
 */
export const isTaskState: (u: unknown) => u is TaskState = Schema.is(Schema.toType(TaskState))

/** Checks the decoded TaskInspection contract without decoding or coercing input.
 * @category guards
 */
export const isTaskInspection: (u: unknown) => u is TaskInspection = Schema.is(
  Schema.toType(TaskInspection),
)

/** Checks the decoded Value contract without decoding or coercing input.
 * @category guards
 */
export const isValue: (u: unknown) => u is Value = Schema.is(Schema.toType(Value))

/** Checks the decoded GraphNode contract without decoding or coercing input.
 * @category guards
 */
export const isGraphNode: (u: unknown) => u is GraphNode = Schema.is(Schema.toType(GraphNode))

/** Checks the decoded Graph contract without decoding or coercing input.
 * @category guards
 */
export const isGraph: (u: unknown) => u is Graph = Schema.is(Schema.toType(Graph))

/** Checks the decoded GraphOp contract without decoding or coercing input.
 * @category guards
 */
export const isGraphOp: (u: unknown) => u is GraphOp = Schema.is(Schema.toType(GraphOp))

/** Checks the decoded GraphChange contract without decoding or coercing input.
 * @category guards
 */
export const isGraphChange: (u: unknown) => u is GraphChange = Schema.is(Schema.toType(GraphChange))
