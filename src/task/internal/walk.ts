/**
 * Walks over the ownership index: up from a node to its root, and down from a node through the
 * work listed below it. Walks take an `Overlay` of a commit's staged records, which replace
 * committed ones, so a decision inside a commit sees what that commit creates.
 */
import { Effect } from 'effect'
import type { Tx } from '../../session/Session.ts'
import type { ConversationId, TaskId } from '../../storage/schema/Ids.ts'
import type { TaskRecord } from '../../storage/schema/Task.ts'
import type { State } from './state.ts'
import { addTo, asConversation, asTask, cancellationIntent, parentNode, parentOf } from './tree.ts'
import type { Node, TaskNode, Up } from './tree.ts'

/** Candidate records a commit staged; they override committed records in walks. */
export interface Overlay {
  readonly tasks: Map<TaskId, TaskRecord>
  readonly edges: ReadonlyMap<ConversationId, TaskId | null>
  /** Links to the staged tasks `below` does not list, by parent node; built at first use. */
  links?: ReadonlyMap<Node, ReadonlySet<Node>>
}

/** The overlay of what a transaction staged so far. */
export const overlayOf = (tx: Tx) =>
  Effect.gen(function* () {
    const tasks = new Map((yield* tx.stagedTasks).map((record) => [record.id, record]))
    const conversations = yield* tx.stagedConversations
    const edges = new Map(
      conversations.map((record) => [record.id, record.owner?.taskId ?? null] as const),
    )
    const overlay: Overlay = { tasks, edges }
    return overlay
  })

/** Owner task of a conversation, `null` when ownerless, `undefined` while not loaded. */
export const edgeOf = (state: State, id: ConversationId, overlay?: Overlay) =>
  overlay?.edges.has(id) === true ? overlay.edges.get(id) : state.tree.edges.get(id)

const nodeAt = (state: State, id: TaskId, overlay?: Overlay): TaskNode | undefined =>
  overlay?.tasks.get(id) ?? state.live.get(id) ?? state.tree.settled.get(id)

/** One step of a walk up: an owner task, a conversation, or an edge that is not loaded yet. */
export type Step =
  | { readonly task: TaskId; readonly node: TaskNode }
  | { readonly conversation: ConversationId }
  | { readonly unknown: true }

/** Walk up from `start`, ending at an ownerless root or an edge not loaded yet. */
export function* above(state: State, start: Up, overlay?: Overlay): Generator<Step> {
  for (let at: Up | undefined = start; at !== undefined;) {
    if ('task' in at) {
      const node = nodeAt(state, at.task, overlay)
      if (node === undefined) {
        yield { unknown: true }
        return
      }
      yield { task: at.task, node }
      at = parentOf(node)
    } else {
      yield { conversation: at.conversation }
      const edge = edgeOf(state, at.conversation, overlay)
      if (edge === undefined) {
        yield { unknown: true }
        return
      }
      at = edge === null ? undefined : { task: edge }
    }
  }
}

/** The node above `node` in the overlay's view, for linking staged tasks. */
const aboveNode = (state: State, node: Node, overlay: Overlay): Node | null | undefined => {
  const task = asTask(node)
  if (task === undefined) {
    const conversation = asConversation(node)
    return conversation === undefined ? undefined : edgeOf(state, conversation, overlay)
  }
  const fields = state.tree.settled.get(task)
  return fields === undefined ? undefined : parentNode(fields)
}

/**
 * Links walks down need for the commit's new tasks: each one under its parent node, then each node
 * above it that `below` does not list yet, until one it lists, a live task, or the chain's end.
 */
const linksOf = (state: State, overlay: Overlay): ReadonlyMap<Node, ReadonlySet<Node>> => {
  if (overlay.links !== undefined) return overlay.links
  const links = new Map<Node, Set<Node>>()
  for (const record of overlay.tasks.values()) {
    if (state.live.has(record.id) || state.tree.settled.has(record.id)) continue
    let child: Node = record.id
    let parent: Node | null | undefined = parentNode(record)
    while (parent !== undefined && parent !== null && links.get(parent)?.has(child) !== true) {
      addTo(links, parent, child)
      const task = asTask(parent)
      if (task !== undefined && (state.live.has(task) || overlay.tasks.has(task))) break
      const next = aboveNode(state, parent, overlay)
      if (next !== undefined && next !== null && state.tree.below.get(next)?.has(parent) === true) {
        break
      }
      child = parent
      parent = next
    }
  }
  overlay.links = links
  return links
}

export interface Reached {
  readonly id: TaskId
  readonly node: TaskNode
  readonly live: TaskRecord | undefined
}

export interface WalkOptions {
  readonly overlay?: Overlay | undefined
  readonly crossBackground?: boolean
  readonly enter?: (id: TaskId) => boolean
}

/**
 * The tasks below `start` a walk down reaches: those `below` lists and, under an overlay, the
 * commit's new tasks. Background tasks are reached but not entered, unless `crossBackground`, and
 * neither are the tasks `enter` rejects. A loop, as chains can be deeper than the call stack.
 */
export function* walkDown(
  state: State,
  start: Node,
  options: WalkOptions = {},
): Generator<Reached> {
  const { overlay, crossBackground = false, enter } = options
  const links = overlay === undefined ? undefined : linksOf(state, overlay)
  const pending: Array<Node> = [start]
  for (let at = pending.pop(); at !== undefined; at = pending.pop()) {
    for (const children of [state.tree.below.get(at), links?.get(at)]) {
      for (const child of children ?? []) {
        const id = asTask(child)
        if (id === undefined) {
          pending.push(child)
          continue
        }
        const record = overlay?.tasks.get(id) ?? state.live.get(id)
        const node = record ?? state.tree.settled.get(id)
        if (node === undefined) continue
        const live = record !== undefined && record.state.status !== 'terminal' ? record : undefined
        yield { id, node, live }
        if ((crossBackground || !node.background) && (enter === undefined || enter(id))) {
          pending.push(id)
        }
      }
    }
  }
}

/**
 * The live ordinary owned work of `owner` (spec §5.5): live non-background tasks below it that a
 * walk down reaches without entering a background task; with `first`, only the first. The walk
 * does not enter tasks in `done`, which have none. Owner chains must be loaded.
 */
export const ownedLive = (
  state: State,
  owner: TaskId,
  options: {
    readonly overlay?: Overlay
    readonly first?: boolean
    readonly done?: ReadonlySet<TaskId>
  } = {},
): ReadonlyArray<TaskId> => {
  const { overlay, first = false, done } = options
  const found: Array<TaskId> = []
  const enter = done === undefined ? undefined : (id: TaskId) => !done.has(id)
  for (const { id, node, live } of walkDown(state, owner, {
    overlay,
    ...(enter === undefined ? {} : { enter }),
  })) {
    if (live === undefined || node.background) continue
    found.push(id)
    if (first) break
  }
  return found
}

export const hasOwnedLive = (
  state: State,
  owner: TaskId,
  overlay?: Overlay,
  done?: ReadonlySet<TaskId>,
) =>
  ownedLive(state, owner, {
    first: true,
    ...(overlay === undefined ? {} : { overlay }),
    ...(done === undefined ? {} : { done }),
  }).length > 0

/** Where ordinary traversal starts: one conversation, or every ownerless conversation. */
export type Scope = { readonly conversation: ConversationId } | { readonly roots: true }

/**
 * Whether ordinary traversal from `scope` reaches `start`: walking up reaches the scope's
 * conversation, or an ownerless one for `roots`, without crossing a background owner, unless
 * `crossBackground`. `undefined` while an edge is not loaded.
 */
export const inScope = (state: State, start: Up, scope: Scope, crossBackground = false) => {
  let reached: boolean | undefined = 'roots' in scope
  for (const step of above(state, start)) {
    if ('unknown' in step) {
      reached = undefined
      break
    }
    if ('conversation' in step) {
      if ('conversation' in scope && step.conversation === scope.conversation) {
        reached = true
        break
      }
    } else if (step.node.background && !crossBackground) {
      reached = false
      break
    }
  }
  return reached
}

/**
 * The nearest live owner above `start` whose cancellation intent reaches it: walking up finds an
 * owner with intent before a background owner without it. Terminal owners never cascade.
 */
export const cancellingOwner = (state: State, start: Up): TaskRecord | undefined => {
  // Nothing cancels, the common case: no walk.
  if (state.intent.size === 0) return undefined
  for (const step of above(state, start)) {
    if ('unknown' in step) return undefined
    if (!('task' in step)) continue
    const live = state.live.get(step.task)
    if (live !== undefined && cancellationIntent(live)) return live
    if (step.node.background) return undefined
  }
  return undefined
}

export const belowCancelled = (state: State, start: Up): boolean =>
  cancellingOwner(state, start) !== undefined

/**
 * The nearest live task above ended ordinary work `node`, in the overlay's view: the one that work
 * held, if any. Work at or below a background task holds nothing above it.
 */
export const nearestLiveAbove = (state: State, node: TaskNode, overlay?: Overlay) => {
  let found: TaskRecord | undefined
  for (let current: TaskNode | undefined = node; current !== undefined && !current.background;) {
    const owner: TaskId | null | undefined =
      current.owner ?? edgeOf(state, current.conversationId, overlay)
    if (owner === undefined || owner === null) break
    const record: TaskRecord | undefined = overlay?.tasks.get(owner) ?? state.live.get(owner)
    if (record !== undefined && record.state.status !== 'terminal') found = record
    current = found === undefined ? (record ?? state.tree.settled.get(owner)) : undefined
  }
  return found
}

/** The nearest live task above ended work may be free now: one holding, or an aborting one. */
export const release = (state: State, node: TaskNode): void => {
  const holder = nearestLiveAbove(state, node)
  if (holder?.state.status === 'completing') state.finalizeChecks.add(holder.id)
  else if (holder?.abortRequested === true) state.runnable.add(holder.id)
}
