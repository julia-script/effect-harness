/**
 * The ownership index (spec §5.5). Tasks and conversations form one tree: a task's parent is its
 * owner task, or its conversation; a conversation's parent is its owner task, if any.
 *
 * `below` lists the tree downward around live work, so what lies below a node is a walk down from
 * it. The index is bounded by live work: an owner edge and an ended task's ownership fields are
 * kept only while something is listed below them, so only the chains above live tasks are known,
 * whole. Work that appears later below a dropped node finds it unknown and loads its chain from
 * Storage again (`chains.ts`).
 */
import { cast } from 'effect/Function'
import type { ConversationId, TaskId } from '../../storage/schema/Ids.ts'
import type { TaskRecord } from '../../storage/schema/Task.ts'
import type { State } from './state.ts'

/** A node of the downward index: a task, or a conversation as `c<id>`. */
export type Node = TaskId | `c${number}`

/** The immutable ownership fields of a task. */
export interface TaskNode {
  readonly conversationId: ConversationId
  readonly owner?: TaskId | undefined
  readonly background: boolean
}

/** Where a walk up continues: an owner task, or a conversation. */
export type Up = { readonly task: TaskId } | { readonly conversation: ConversationId }

/** The index's maps; see the module documentation. */
export interface Tree {
  /** Owner task of each known conversation, `null` when ownerless. */
  readonly edges: Map<ConversationId, TaskId | null>
  /** Ownership fields of the known terminal tasks, which walks pass through. */
  readonly settled: Map<TaskId, TaskNode>
  /** Each node's child nodes that are live tasks or have nodes listed below them. */
  readonly below: Map<Node, Set<Node>>
  /** Ownerless conversations with work below them. */
  readonly roots: Set<ConversationId>
  /** Known tasks whose chain may not be loaded; every other known task's chain is loaded. */
  readonly unloaded: Set<TaskId>
  /** Ended tasks and conversations found unneeded, dropped by the next sweep unless needed again. */
  readonly dropQueue: Set<Node>
}

export const makeTree = (): Tree => ({
  edges: new Map(),
  settled: new Map(),
  below: new Map(),
  roots: new Set(),
  unloaded: new Set(),
  dropQueue: new Set(),
})

export const conversationNode = (id: ConversationId): Node => `c${id}`

const isConversation = (node: Node): node is `c${number}` => typeof node === 'string'

/** The conversation of a conversation node. */
const conversationOf = (node: `c${number}`): ConversationId =>
  // The node was made from a ConversationId by `conversationNode`.
  cast(Number(node.slice(1)))

/** The node a task hangs from: its owner task, or its conversation. */
export const parentNode = (node: TaskNode): Node =>
  node.owner ?? conversationNode(node.conversationId)

export const parentOf = (node: TaskNode): Up =>
  node.owner === undefined ? { conversation: node.conversationId } : { task: node.owner }

export const nodeOf = (record: TaskRecord): TaskNode => ({
  conversationId: record.conversationId,
  owner: record.owner,
  background: record.background,
})

/** Whether the record holds or ended with an outcome other than `completed`. */
export const failedOutcome = (record: TaskRecord): boolean => {
  const { state } = record
  return (
    (state.status === 'completing' || state.status === 'terminal') &&
    state.outcome.status !== 'completed'
  )
}

/** A live owner's durable cancellation intent: its abort mark, or a held failed outcome. */
export const cancellationIntent = (record: TaskRecord): boolean =>
  record.state.status !== 'terminal' && (record.abortRequested || failedOutcome(record))

export const addTo = <K, V>(map: Map<K, Set<V>>, key: K, value: V): void => {
  const values = map.get(key)
  if (values === undefined) map.set(key, new Set([value]))
  else values.add(value)
}

export const deleteFrom = <K, V>(map: Map<K, Set<V>>, key: K, value: V): void => {
  const values = map.get(key)
  if (values === undefined) return
  values.delete(value)
  if (values.size === 0) map.delete(key)
}

/** Request a sweep, which runs on the line after the operation underway. */
export const scheduleSweep = (state: State): void => {
  if (!state.closing) state.sweepWake.openUnsafe()
}

/** Queue an ended task or a conversation for the sweep when nothing is listed below it. */
export const dropIfUnneeded = (state: State, node: Node): void => {
  if (!isConversation(node) && state.live.has(node)) return
  if (state.tree.below.has(node)) return
  state.tree.dropQueue.add(node)
  scheduleSweep(state)
}

/** The parent of a listed node: a node, `null` for an ownerless conversation, or unknown. */
const parentInIndex = (state: State, node: Node): Node | null | undefined => {
  if (isConversation(node)) return state.tree.edges.get(conversationOf(node))
  const known = state.live.get(node) ?? state.tree.settled.get(node)
  return known === undefined ? undefined : parentNode(known)
}

/** List or unlist an ownerless conversation; the walk up ends there. */
const listRoot = (state: State, node: Node, needed: boolean): void => {
  if (!isConversation(node)) return
  if (needed) state.tree.roots.add(conversationOf(node))
  else state.tree.roots.delete(conversationOf(node))
}

/** List or unlist `node` under `parent`; whether its parent's own listing may have changed. */
const relistUnder = (state: State, parent: Node, node: Node, needed: boolean): boolean => {
  const { below } = state.tree
  if ((below.get(parent)?.has(node) === true) === needed) return false
  if (needed) {
    const first = !below.has(parent)
    addTo(below, parent, node)
    return first
  }
  deleteFrom(below, parent, node)
  if (below.has(parent)) return false
  dropIfUnneeded(state, parent)
  return true
}

/**
 * List `start` under its parent while it is a live task or has nodes listed below it, and unlist it
 * otherwise; then its parent, as far up as that changes. A loop, as ownership chains can be deeper
 * than the call stack.
 */
export const relist = (state: State, start: Node): void => {
  for (let node: Node | undefined = start; node !== undefined;) {
    const needed: boolean =
      (!isConversation(node) && state.live.has(node)) || state.tree.below.has(node)
    const parent = parentInIndex(state, node)
    if (parent === undefined) return
    if (parent === null) {
      listRoot(state, node, needed)
      return
    }
    node = relistUnder(state, parent, node, needed) ? parent : undefined
  }
}

export const setEdge = (state: State, conversationId: ConversationId, owner: TaskId | null) => {
  state.tree.edges.set(conversationId, owner)
  // The conversation now hangs from its owner, or is a root; with nothing below, it awaits the sweep.
  relist(state, conversationNode(conversationId))
  dropIfUnneeded(state, conversationNode(conversationId))
}

/** Know a terminal task a chain load read, so walks pass through it; its chain is not loaded yet. */
export const settle = (state: State, id: TaskId, node: TaskNode): void => {
  state.tree.settled.set(id, node)
  state.tree.unloaded.add(id)
  relist(state, id)
  dropIfUnneeded(state, id)
}

/**
 * Drop the queued nodes that are still unneeded. Runs once a publication is tracked, and as a job of
 * its own on the line, never inside an operation: work a commit creates below a chain its own
 * callback loaded keeps that chain.
 */
export const sweep = (state: State): void => {
  const { tree } = state
  for (const node of tree.dropQueue) {
    if (tree.below.has(node)) continue
    if (isConversation(node)) {
      tree.edges.delete(conversationOf(node))
      tree.roots.delete(conversationOf(node))
    } else if (!state.live.has(node)) {
      tree.settled.delete(node)
      tree.unloaded.delete(node)
    }
  }
  tree.dropQueue.clear()
}

/** Whether the chain above `node` is loaded, without walking. */
export const known = (state: State, node: Node): boolean => {
  if (!isConversation(node)) {
    const found = state.live.has(node) || state.tree.settled.has(node)
    return found && !state.tree.unloaded.has(node)
  }
  const owner = state.tree.edges.get(conversationOf(node))
  return owner === null || (owner !== undefined && known(state, owner))
}

/** The task of a task node. */
export const asTask = (node: Node): TaskId | undefined => (isConversation(node) ? undefined : node)

/** Whether a node is a conversation node, and which. */
export const asConversation = (node: Node): ConversationId | undefined =>
  isConversation(node) ? conversationOf(node) : undefined

/** Sizes of the ownership indexes, for tests that bound the Scheduler's memory. */
export const indexSizes = (state: State) => {
  let below = 0
  for (const children of state.tree.below.values()) below += children.size
  return {
    live: state.live.size,
    settled: state.tree.settled.size,
    edges: state.tree.edges.size,
    below,
    roots: state.tree.roots.size,
    unloaded: state.tree.unloaded.size,
    dropQueue: state.tree.dropQueue.size,
  }
}
