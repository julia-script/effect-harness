import type { StorageError } from './StorageError.ts'
import { cursor as journalCursor } from './storage/State.ts'
import * as Outcome from './workflow/Outcome.ts'
import * as Effect from 'effect/Effect'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as Ownership from './Ownership.ts'
import * as Record from './Record.ts'
import type * as Session from './Session.ts'
import type * as Store from './Store.ts'

export const TaskState = Schema.Union([
  Schema.Struct({ kind: Schema.Literals(['ready', 'running']) }),
  Schema.Struct({ kind: Schema.Literal('waiting'), on: Schema.Array(Record.TaskId) }),
  Schema.Struct({ kind: Schema.Literal('completing'), outcome: Schema.Json }),
  Schema.Struct({
    kind: Schema.Literal('blocked'),
    reason: Schema.Literals(['missing_workflow', 'invalid_binding']),
  }),
])
export type TaskState = typeof TaskState.Type
export const TaskInspection = Schema.Struct({ record: Record.Task, state: TaskState })
export type TaskInspection = typeof TaskInspection.Type
export const Value = Schema.Struct({
  tasks: Schema.Array(TaskInspection),
  submissions: Schema.Array(Record.Submission),
})
export type Value = typeof Value.Type

/** Inspect committed facts and declaration metadata without invoking handlers, codecs, migrations, or the engine. */
export const get = Effect.fnUntraced(function* (
  session: Session.Service,
): Effect.fn.Return<Value, StorageError, Ownership.Declarations> {
  const state = yield* session.committed
  const declarations = yield* Ownership.Declarations
  const tasks = state.tasks
    .filter((task) => task.state.status !== 'terminal')
    .toSorted((a, b) => a.id - b.id)
  const inspect = (task: Record.Task): TaskState => {
    if (task.state.status === 'completing')
      return { kind: 'completing', outcome: task.state.outcome ?? null }
    const binding = Schema.decodeUnknownOption(Ownership.Binding)(task.input)
    if (Option.isNone(binding)) return { kind: 'blocked', reason: 'invalid_binding' }
    if (Option.isNone(declarations.get(binding.value.workflow)))
      return { kind: 'blocked', reason: 'missing_workflow' }
    if (task.state.status === 'running') return { kind: 'running' }
    if (task.state.status === 'waiting') {
      const on = (task.state.on ?? []).filter(
        (id) => state.tasks.find((child) => child.id === id)?.state.status !== 'terminal',
      )
      if (on.length > 0) return { kind: 'waiting', on }
    }
    return { kind: 'ready' }
  }
  return {
    tasks: tasks.map((record) => ({ record, state: inspect(record) })),
    submissions: state.submissions
      .filter((item) => item.status === 'queued' || item.status === 'placed')
      .toSorted((a, b) => a.id - b.id),
  } satisfies Value
})

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
export type GraphNode = typeof GraphNode.Type
export const Graph = Schema.Struct({ tasks: Schema.Record(Schema.String, GraphNode) })
export type Graph = typeof Graph.Type
/** Live-only ownership graph deliberately excludes arbitrary checkpoint/result payloads. */
export function graph(state: Ownership.Graph): Graph {
  const tasks: Record<string, GraphNode> = {}
  for (const task of state.tasks.toSorted((a, b) => a.id - b.id)) {
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
      conversations: state.conversations
        .filter((conversation) => conversation.owner?.taskId === task.id)
        .map((conversation) => conversation.id)
        .toSorted((a, b) => a - b),
    }
  }
  return { tasks }
}

export const GraphOp = Schema.Union([
  Schema.Tuple([Schema.Literal('replace'), Graph]),
  Schema.Tuple([
    Schema.Literal('set'),
    Schema.Tuple([Schema.Literal('tasks'), Schema.String]),
    GraphNode,
  ]),
  Schema.Tuple([Schema.Literal('delete'), Schema.Tuple([Schema.Literal('tasks'), Schema.String])]),
])
export type GraphOp = typeof GraphOp.Type
export const GraphChange = Schema.Struct({
  seq: Record.JournalCursor,
  before: Graph,
  value: Graph,
  ops: Schema.Array(GraphOp),
  reset: Schema.Boolean,
})
export type GraphChange = typeof GraphChange.Type
const changed = (
  before: Graph,
  candidate: Graph,
): { readonly value: Graph; readonly ops: ReadonlyArray<GraphOp> } => {
  const tasks = { ...before.tasks }
  const ops: GraphOp[] = []
  for (const id of Object.keys(before.tasks))
    if (!Object.hasOwn(candidate.tasks, id)) {
      delete tasks[id]
      ops.push(['delete', ['tasks', id]])
    }
  for (const [id, node] of Object.entries(candidate.tasks))
    if (
      before.tasks[id] === undefined ||
      !Schema.toEquivalence(GraphNode)(before.tasks[id], node)
    ) {
      tasks[id] = node
      ops.push(['set', ['tasks', id], node])
    }
  return { value: ops.length === 0 ? before : { tasks }, ops }
}
/** Exact committed graph frames and structural branch sharing, replaced only on bounded journal overflow. */
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
        { after: baseline.seq, pending: [] as GraphChange[], delivered: value },
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
                if (delta.ops.length > 0)
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
