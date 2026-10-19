/** Transaction-local ownership view, including terminal edges used by detached descendants. */
import * as Effect from 'effect/Effect'
import * as Stream from 'effect/Stream'
import * as Schema from 'effect/Schema'
import * as Record from '../Record.js'
import * as TaskPolicy from '../TaskPolicy.js'
import * as Transaction from '../Transaction.js'
import { TaskError } from '../TaskError.js'

export interface Tree {
  readonly tasks: ReadonlyMap<Record.TaskId, Record.Task>
  readonly conversations: ReadonlyMap<Record.ConversationId, Record.Conversation>
}
export const load = Effect.fnUntraced(function* (tx: Transaction.Transaction) {
  const tasks = yield* Transaction.scanTasks(tx).pipe(Stream.runCollect)
  const conversations = yield* Transaction.scanConversations(tx).pipe(Stream.runCollect)
  return {
    tasks: new Map(tasks.map((item) => [item.id, item])),
    conversations: new Map(conversations.map((item) => [item.id, item])),
  } satisfies Tree
})

/** Owner tasks nearest first. Terminal records retain their edges. Cycles terminate traversal. */
export const ancestors = (tree: Tree, task: Record.Task): ReadonlyArray<Record.Task> => {
  const owners: Array<Record.Task> = []
  const seen = new Set<Record.TaskId>([task.id])
  let at: Record.Task | undefined = task
  while (at !== undefined) {
    const id: Record.TaskId | undefined =
      at.owner ?? tree.conversations.get(at.conversationId)?.owner?.taskId
    if (id === undefined) break
    if (seen.has(id)) break
    seen.add(id)
    at = tree.tasks.get(id)
    if (at !== undefined) owners.push(at)
  }
  return owners
}

export const owned = (tree: Tree, id: Record.TaskId, includeBackground = false) =>
  [...tree.tasks.values()].filter((candidate) => {
    if (candidate.id === id || (!includeBackground && candidate.background)) return false
    for (const owner of ancestors(tree, candidate)) {
      if (owner.id === id) return true
      if (!includeBackground && owner.background) return false
    }
    return false
  })

export const liveOwned = (tree: Tree, id: Record.TaskId, includeBackground = false) =>
  owned(tree, id, includeBackground).filter((candidate) => candidate.state.status !== 'terminal')

/** Conversation scopes follow direct task owners before inspecting their conversation edge. */
export const inConversation = (
  tree: Tree,
  record: Record.Task,
  conversationId: Record.ConversationId,
  includeBackground = false,
): boolean => {
  if (record.background && !includeBackground) return false
  let at: Record.Task | undefined = record
  const seen = new Set<Record.TaskId>()
  while (at !== undefined && !seen.has(at.id)) {
    seen.add(at.id)
    if (at.owner !== undefined) {
      at = tree.tasks.get(at.owner)
      if (at?.background && !includeBackground) return false
      continue
    }
    if (at.conversationId === conversationId) return true
    const owner = tree.conversations.get(at.conversationId)?.owner?.taskId
    at = owner === undefined ? undefined : tree.tasks.get(owner)
    if (at?.background && !includeBackground) return false
  }
  return false
}

export const failedOutcome = (record: Record.Task): boolean => {
  const outcome = record.state.outcome
  return (
    (record.state.status === 'terminal' || record.state.status === 'completing') &&
    Schema.is(Record.TaskOutcome)(outcome) &&
    outcome.status !== 'completed'
  )
}
export const cancellingOwner = (tree: Tree, record: Record.Task): Record.Task | undefined => {
  if (record.background) return undefined
  for (const owner of ancestors(tree, record)) {
    if (owner.state.status !== 'terminal' && (owner.abortRequested || failedOutcome(owner)))
      return owner
    if (owner.background) return undefined
  }
  return undefined
}

export const mark = (record: Record.Task, restart = false): Record.Task => {
  if (record.state.status === 'terminal') return record
  if (!restart) return TaskPolicy.requestAbort(record)
  if (record.abortRequested) return record
  return { ...record, abortRequested: true, abortIntent: { _tag: 'restart' } }
}

const reject = (message: string) =>
  new TaskError({ reason: 'invalidCheckpoint', operation: 'task.wait', message })

/** Wait edges and implicit ownership drain edges both participate in deadlock admission. */
export const validateWait = Effect.fnUntraced(function* (
  tx: Transaction.Transaction,
  current: Record.Task,
  next: { readonly on: ReadonlyArray<Record.TaskId>; readonly policy: 'allSettled' | 'failFast' },
) {
  const tree = yield* load(tx)
  const owners = new Set(ancestors(tree, current).map((owner) => owner.id))
  const reaches = (start: Record.TaskId): boolean => {
    const pending = [start]
    const visited = new Set<Record.TaskId>()
    while (pending.length > 0) {
      const id = pending.pop()!
      if (id === current.id) return true
      if (visited.has(id)) continue
      visited.add(id)
      const member = tree.tasks.get(id)
      if (member === undefined || member.state.status === 'terminal') continue
      pending.push(...(member.state.on ?? []))
      pending.push(...liveOwned(tree, id).map((child) => child.id))
    }
    return false
  }
  for (const id of next.on) {
    if (id === current.id || owners.has(id)) return yield* reject('Cannot wait on self or ancestor')
    const member = tree.tasks.get(id)
    if (member === undefined) return yield* reject('Wait references an absent task')
    if (next.policy === 'failFast' && member.owner !== current.id)
      return yield* reject('failFast requires directly owned tasks')
    if (reaches(id)) return yield* reject('Wait would create a dependency cycle')
  }
})
