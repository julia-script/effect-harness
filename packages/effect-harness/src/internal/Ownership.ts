/** Ownership traversal for one embedded harness, including owned conversations. */
import type * as Record from '../Record.ts'

export interface Graph {
  readonly tasks: ReadonlyMap<Record.TaskId, Record.Task>
  readonly conversations: ReadonlyMap<Record.ConversationId, Record.Conversation>
}

/** Walk owner tasks, crossing the owner edge of nested conversations. */
export function* ancestors(graph: Graph, task: Record.Task): Generator<Record.Task> {
  let owner = task.owner ?? graph.conversations.get(task.conversationId)?.owner?.taskId
  const seen = new Set<Record.TaskId>([task.id])
  while (owner !== undefined && !seen.has(owner)) {
    seen.add(owner)
    const current = graph.tasks.get(owner)
    if (current === undefined) return
    yield current
    owner = current.owner ?? graph.conversations.get(current.conversationId)?.owner?.taskId
  }
}

/** Ordinary descendants hold each ancestor through the first background boundary. */
export const ownedLive = (
  graph: Graph,
): ReadonlyMap<Record.TaskId, ReadonlyArray<Record.TaskId>> => {
  const owned = new Map<Record.TaskId, Array<Record.TaskId>>()
  for (const task of graph.tasks.values()) {
    if (task.state.status === 'terminal' || task.background) continue
    for (const owner of ancestors(graph, task)) {
      const children = owned.get(owner.id) ?? []
      children.push(task.id)
      owned.set(owner.id, children)
      if (owner.background) break
    }
  }
  return owned
}

/** A live cancelled ancestor cancels ordinary descendants, stopping at background boundaries. */
export const belowCancelled = (graph: Graph, task: Record.Task): boolean => {
  if (task.background) return false
  for (const owner of ancestors(graph, task)) {
    if (owner.state.status !== 'terminal' && (owner.abortRequested || failed(owner))) return true
    if (owner.background) return false
  }
  return false
}

/** Completing failures already carry cancellation intent while their children drain. */
export const failed = (task: Record.Task): boolean => {
  if (task.state.status !== 'terminal' && task.state.status !== 'completing') return false
  const outcome = task.state.outcome
  return (
    typeof outcome === 'object' &&
    outcome !== null &&
    !Array.isArray(outcome) &&
    'status' in outcome &&
    outcome.status !== 'completed'
  )
}

/** Ordinary traversal reaches a conversation through same-conversation and owned-conversation edges. */
export const inConversation = (
  graph: Graph,
  task: Record.Task,
  id: Record.ConversationId,
  background = false,
): boolean => {
  if (task.background && !background) return false
  if (task.conversationId === id) return true
  for (const owner of ancestors(graph, task)) {
    if (owner.background && !background) return false
    if (owner.conversationId === id) return true
  }
  return false
}
