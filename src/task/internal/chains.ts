/**
 * Loading owner chains into the ownership index from committed Storage. Loads run on the Session
 * line, also inside a commit callback after its writes, so they read Storage directly.
 */
import { Effect } from 'effect'
import type { Tx } from '../../session/Session.ts'
import type { ConversationRecord } from '../../storage/schema/Conversation.ts'
import type { ConversationId, TaskId } from '../../storage/schema/Ids.ts'
import type { Cursor } from '../../storage/schema/Scan.ts'
import type { TaskOutcome, TaskRecord } from '../../storage/schema/Task.ts'
import type { State } from './state.ts'
import { nodeOf, parentOf, setEdge, settle } from './tree.ts'
import type { TaskNode, Up } from './tree.ts'
import { edgeOf } from './walk.ts'
import type { Overlay } from './walk.ts'

const SCAN_PAGE_SIZE = 256

/** Request a reconcile pass, which applies what committed records imply. */
export const scheduleReconcile = (state: State): void => {
  if (!state.closing) state.reconcileWake.openUnsafe()
}

/** One task of a walk up: its node, the end of the chain, or a stop at a loaded chain. */
const taskStep = Effect.fnUntraced(function* (state: State, id: TaskId, overlay?: Overlay) {
  const found: TaskNode | undefined = state.live.get(id) ?? state.tree.settled.get(id)
  // Created in this commit: the load from its own record covers what is above it.
  if (found === undefined && overlay?.tasks.has(id) === true) return 'stop' as const
  if (found !== undefined && !state.tree.unloaded.has(id)) return 'stop' as const
  if (found !== undefined) return found
  const record = yield* state.storage.task(id)
  if (record === undefined) return 'missing' as const
  const node = nodeOf(record)
  if (record.state.status === 'terminal') settle(state, id, node)
  // A loaded node can reveal a cancelling owner: the next reconcile derives marks again.
  scheduleReconcile(state)
  return node
})

/**
 * Load owner edges and task nodes from `start` up to its ownerless root. A known task outside
 * `unloaded` ends the walk, as its chain is loaded; the unloaded tasks passed are loaded after.
 */
export const loadChain = Effect.fnUntraced(function* (state: State, start: Up, overlay?: Overlay) {
  const passed: Array<TaskId> = []
  for (let at: Up | undefined = start; at !== undefined;) {
    if ('task' in at) {
      const step = yield* taskStep(state, at.task, overlay)
      if (step === 'missing') return
      if (step === 'stop') break
      passed.push(at.task)
      at = parentOf(step)
      continue
    }
    let edge: TaskId | null | undefined = edgeOf(state, at.conversation, overlay)
    if (edge === undefined) {
      const record: ConversationRecord | undefined = yield* state.storage.conversation(
        at.conversation,
      )
      edge = record?.owner?.taskId ?? null
      setEdge(state, at.conversation, edge)
      scheduleReconcile(state)
    }
    at = edge === null ? undefined : { task: edge }
  }
  for (const id of passed) state.tree.unloaded.delete(id)
})

/** Load the owner chains of every task in `unloaded`. */
export const loadScopes = Effect.fnUntraced(function* (state: State) {
  // A copy: loading removes what it passes.
  for (const id of Array.from(state.tree.unloaded)) {
    // An earlier load in this loop may have passed it.
    if (state.tree.unloaded.has(id)) yield* loadChain(state, { task: id })
  }
})

/**
 * Load the owner chains of every conversation with queued inputs, and return those conversations.
 * Inputs queued below an owner with cancellation intent are withdrawn with it.
 */
export const loadQueued = Effect.fnUntraced(function* (state: State) {
  const conversations = new Set<ConversationId>()
  let cursor: Cursor | undefined
  do {
    const page = yield* state.storage.scanSubmissions({ status: 'queued' }, SCAN_PAGE_SIZE, cursor)
    for (const submission of page.items) {
      if (submission.type === 'input') conversations.add(submission.conversationId)
    }
    cursor = page.next
  } while (cursor !== undefined)
  for (const id of conversations) yield* loadChain(state, { conversation: id })
  return [...conversations]
})

/** Withdraw the queued inputs of each conversation `withdraws` selects, through the host hook. */
export const withdrawInputs = Effect.fnUntraced(function* (
  state: State,
  tx: Tx,
  conversations: ReadonlyArray<ConversationId>,
  withdraws: (conversationId: ConversationId) => boolean,
) {
  const hook = state.options.withdrawInputs
  if (hook === undefined) return
  for (const id of conversations) if (withdraws(id)) yield* hook(tx, id)
})

/**
 * Stage the host's cleanup for an outcome only the scheduler writes, `faulted` or `orphaned`, in
 * the commit that makes it terminal.
 */
export const settleOutcome = (state: State, tx: Tx, record: TaskRecord, outcome: TaskOutcome) => {
  const hook = state.options.settleOutcome
  return hook !== undefined && (outcome.status === 'faulted' || outcome.status === 'orphaned')
    ? hook(tx, record, outcome)
    : Effect.void
}
