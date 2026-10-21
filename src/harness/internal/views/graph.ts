/**
 * The task graph mount, ported from pi's `harness/task-graph.ts`: every live task, built in a
 * commit and advanced from every later commit's publication.
 */
import { Effect, Equal, Predicate } from 'effect'
import type { CommitPublication, Tx } from '../../../session/Session.ts'
import type { Op } from '../../../storage/schema/Document.ts'
import type { ConversationId } from '../../../storage/schema/Ids.ts'
import type { Cursor, Page } from '../../../storage/schema/Scan.ts'
import type { TaskRecord } from '../../../storage/schema/Task.ts'
import type { TaskGraph, TaskGraphNode, TaskGraphState } from '../../../protocol/ViewState.ts'

const LIVE_STATUSES = ['pending', 'running', 'waiting', 'completing'] as const
const SCAN_PAGE_SIZE = 256

/** Every item of a paged scan. */
const scanAll = Effect.fnUntraced(function* <A, E>(
  page: (cursor: Cursor | undefined) => Effect.Effect<Page<A>, E>,
) {
  const items: Array<A> = []
  let cursor: Cursor | undefined
  do {
    const next = yield* page(cursor)
    items.push(...next.items)
    cursor = next.next
  } while (cursor !== undefined)
  return items
})

const phaseOf = (checkpoint: unknown): string =>
  Predicate.hasProperty(checkpoint, 'phase') && Predicate.isString(checkpoint.phase)
    ? checkpoint.phase
    : ''

const stateOf = (record: TaskRecord): TaskGraphState => {
  const { state } = record
  if (state.status === 'waiting') {
    const { on, policy } = state
    return { status: 'waiting', phase: phaseOf(state.checkpoint), on, policy }
  }
  if (state.status === 'pending' || state.status === 'running') {
    return { status: state.status, phase: phaseOf(state.checkpoint) }
  }
  // Terminal records never get here: they leave the graph.
  return { status: 'completing', outcome: state.outcome.status }
}

const nodeOf = (
  record: TaskRecord,
  conversations: ReadonlyArray<ConversationId>,
): TaskGraphNode => ({
  id: record.id,
  kind: record.kind,
  conversationId: record.conversationId,
  ...(record.owner === undefined ? {} : { owner: record.owner }),
  background: record.background,
  abortRequested: record.abortRequested,
  state: stateOf(record),
  conversations,
})

const byId = <A extends { readonly id: number }>(a: A, b: A) => a.id - b.id

/** Build the graph in a commit, before its first table write. */
export const build = Effect.fnUntraced(function* (tx: Tx) {
  const records: Array<TaskRecord> = []
  for (const status of LIVE_STATUSES) {
    records.push(...(yield* scanAll((cursor) => tx.scanTasks({ status }, SCAN_PAGE_SIZE, cursor))))
  }
  const tasks: Record<string, TaskGraphNode> = {}
  for (const record of records.toSorted(byId)) {
    const owned = yield* scanAll((cursor) =>
      tx.scanConversations({ ownerTaskId: record.id }, SCAN_PAGE_SIZE, cursor),
    )
    tasks[String(record.id)] = nodeOf(
      record,
      owned.toSorted(byId).map(({ id }) => id),
    )
  }
  const graph: TaskGraph = { tasks }
  return graph
})

/** The graph after one publication's task changes, and the operations that apply them. */
const advanceTasks = (graph: TaskGraph, publication: CommitPublication) => {
  const ops: Array<Op> = []
  const tasks: Record<string, TaskGraphNode> = { ...graph.tasks }
  for (const change of publication.changes) {
    if (change.type !== 'task') continue
    const key = String(change.value.id)
    const previous = tasks[key]
    if (change.value.state.status === 'terminal') {
      if (previous === undefined) continue
      ops.push(['d', ['tasks', key]])
      delete tasks[key]
      continue
    }
    const next = nodeOf(change.value, previous?.conversations ?? [])
    if (previous !== undefined && Equal.equals(next, previous)) continue
    ops.push(['s', ['tasks', key], next])
    tasks[key] = next
  }
  return { tasks, ops }
}

/**
 * Advance `graph` by one publication: the next graph and the operations that turn `graph` into
 * it, none when nothing changed.
 */
export const advance = (graph: TaskGraph, publication: CommitPublication) => {
  const { tasks, ops } = advanceTasks(graph, publication)
  // After the tasks, so a conversation created with its owner in one commit finds its node.
  // Change order within a publication is unspecified, so each owner's list is sorted again.
  const created = new Map<string, Array<ConversationId>>()
  for (const change of publication.changes) {
    if (change.type !== 'conversation' || change.value.owner === undefined) continue
    const key = String(change.value.owner.taskId)
    if (tasks[key] !== undefined) created.set(key, [...(created.get(key) ?? []), change.value.id])
  }
  for (const [key, ids] of created) {
    const node = tasks[key]
    if (node === undefined) continue
    const conversations = [...node.conversations, ...ids].toSorted((a, b) => a - b)
    ops.push(['s', ['tasks', key, 'conversations'], conversations])
    tasks[key] = { ...node, conversations }
  }
  const next: TaskGraph = ops.length === 0 ? graph : { tasks }
  return { graph: next, ops }
}
