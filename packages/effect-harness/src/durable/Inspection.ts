/**
 * Committed task inspection and structurally shared ownership graph projections.
 */
import * as records from 'effect/Record'
// effect-review-allow P9-namespace-alias-equals-module: durable/Record supplies domain schemas; effect/Record supplies safe dictionary operations.
import * as Order from 'effect/Order'
import * as Arr from 'effect/Array'
import { tagged } from './internal/legacyTag.ts'
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
  tagged('ready', { kind: Schema.tag('ready') }),
  tagged('running', { kind: Schema.tag('running') }),
  tagged('waiting', { kind: Schema.tag('waiting'), on: Schema.Array(Record.TaskId) }),
  tagged('completing', { kind: Schema.tag('completing'), outcome: Schema.Json }),
  tagged('blocked', {
    kind: Schema.tag('blocked'),
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
export const get = Effect.fnUntraced(function* (
  session: Session.Service,
): Effect.fn.Return<Value, StorageError, Ownership.Declarations> {
  const state = yield* session.committed
  const declarations = yield* Ownership.Declarations
  const tasks = Arr.sortWith(
    Arr.filter(state.tasks, (task) => task.state.status !== 'terminal'),
    (item: Record.Task) => item.id,
    Order.Number,
  )
  const inspect = (task: Record.Task): TaskState => {
    if (task.state.status === 'completing')
      return { _tag: 'completing', kind: 'completing', outcome: task.state.outcome ?? null }
    const binding = Schema.decodeUnknownOption(Ownership.Binding)(task.input)
    if (Option.isNone(binding))
      return { _tag: 'blocked', kind: 'blocked', reason: 'invalid_binding' }
    if (Option.isNone(declarations.get(binding.value.workflow)))
      return { _tag: 'blocked', kind: 'blocked', reason: 'missing_workflow' }
    if (task.state.status === 'running') return { _tag: 'running', kind: 'running' }
    if (task.state.status === 'waiting') {
      const on = Arr.filter(
        task.state.on ?? [],
        (id) =>
          !Option.exists(
            Arr.findFirst(state.tasks, (child) => child.id === id),
            (child) => child.state.status === 'terminal',
          ),
      )
      if (Arr.isReadonlyArrayNonEmpty(on)) return { _tag: 'waiting', kind: 'waiting', on }
    }
    return { _tag: 'ready', kind: 'ready' }
  }
  return {
    tasks: tasks.map((record) => ({ record, state: inspect(record) })),
    submissions: Arr.sortWith(
      Arr.filter(state.submissions, (item) => item.status === 'queued' || item.status === 'placed'),
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
  id: Record.TaskId,
  kind: Schema.String,
  conversationId: Record.ConversationId,
  owner: Schema.optionalKey(Record.TaskId),
  background: Schema.Boolean,
  abortRequested: Schema.Boolean,
  state: Schema.Struct({
    status: Schema.Literals(['pending', 'running', 'waiting', 'completing']),
    on: Schema.optionalKey(Schema.Array(Record.TaskId)),
    policy: Schema.optionalKey(Schema.Literals(['failFast', 'allSettled'])),
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
 * Live-only ownership graph deliberately excludes arbitrary checkpoint/result payloads.
 *
 * @category combinators
 */
export function graph(state: Ownership.Graph): Graph {
  const tasks: Record<string, GraphNode> = Object.create(null)
  for (const task of Arr.sortWith(state.tasks, (item: Record.Task) => item.id, Order.Number)) {
    if (task.state.status === 'terminal') continue
    const status = Outcome.classifyTask(task)?.status
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
      conversations: Arr.sort(
        Arr.filter(
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
  for (const id of records.keys(before.tasks))
    if (!Object.hasOwn(candidate.tasks, id)) {
      delete tasks[id]
      ops.push(['delete', ['tasks', id]])
    }
  for (const [id, node] of records.toEntries(candidate.tasks))
    if (
      !Object.hasOwn(before.tasks, id) ||
      !Schema.toEquivalence(GraphNode)(before.tasks[id]!, node)
    ) {
      records.assignProperty(tasks, id, node)
      ops.push(['set', ['tasks', id], node])
    }
  return { value: Arr.isReadonlyArrayEmpty(ops) ? before : { tasks }, ops }
}
/**
 * Exact committed graph frames and structural branch sharing, replaced only on bounded journal overflow.
 *
 * @category combinators
 */
export const changes = (
  store: Store.Service,
): Stream.Stream<GraphChange, import('./StorageError.ts').StorageError> =>
  Stream.unwrap(
    Effect.gen(function* () {
      const initial = yield* store.committed
      let tasks = new Map(initial.tasks.map((task) => [task.id, task]))
      let conversations = new Map(
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
            const journal = yield* store.journal(after).pipe(
              Effect.catchIf(
                (error) => error.reason._tag === 'Closed',
                () => Effect.void,
              ),
            )
            if (journal === undefined) return undefined
            if (journal.reset) {
              tasks = new Map(journal.state.tasks.map((task) => [task.id, task]))
              conversations = new Map(
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
                  if (write.type === 'task') {
                    tasks.set(write.value.id, write.value)
                    relevant = true
                  } else if (write.type === 'conversation') {
                    conversations.set(write.value.id, write.value)
                    relevant = true
                  }
                }
                if (!relevant) continue
                const before = value
                const delta = changed(
                  before,
                  graph({ tasks: [...tasks.values()], conversations: [...conversations.values()] }),
                )
                value = delta.value
                if (Arr.isReadonlyArrayNonEmpty(delta.ops))
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

/**
 * Type-level contracts for `Graph`.
 *
 * @category utility types
 */
export declare namespace Graph {
  /**
   * Task or conversation node in the committed ownership graph.
   *
   * @category models
   */
  export type Node = GraphNode
  /**
   * Type alias for `GraphOp`.
   *
   * @category models
   */
  export type Op = GraphOp
  /**
   * Type alias for `GraphChange`.
   *
   * @category models
   */
  export type Change = GraphChange
}

/**
 * Type-level contracts for `TaskInspection`.
 *
 * @category utility types
 */
export declare namespace TaskInspection {
  /**
   * Persisted lifecycle classification of a domain task.
   *
   * @category models
   */
  export type State = TaskState
}
