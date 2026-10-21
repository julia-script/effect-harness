/**
 * Idle waits and conversation aborts: both reach the live tasks that ordinary traversal from a
 * conversation, or from every ownerless conversation, finds.
 */
import { Deferred, Effect, Exit, Option } from 'effect'
import type { ConversationId, TaskId } from '../../storage/schema/Ids.ts'
import type { SchedulerClosed } from '../TaskError.ts'
import { loadQueued, loadScopes, scheduleReconcile, withdrawInputs } from './chains.ts'
import { closedError, readsFirst, withAbortMark } from './state.ts'
import type { State } from './state.ts'
import { conversationNode, parentOf } from './tree.ts'
import { inScope, walkDown } from './walk.ts'
import type { Scope } from './walk.ts'
import { waitForTask } from './wait.ts'

type Idle = Deferred.Deferred<void, SchedulerClosed>

/**
 * Whether ordinary traversal from the conversation, or from every ownerless conversation, reaches
 * no live non-background task.
 */
export const isIdle = (state: State, conversationId: ConversationId | undefined): boolean => {
  const scope: Scope =
    conversationId === undefined ? { roots: true } : { conversation: conversationId }
  // Tasks whose chain is not loaded are not listed below their scope yet; one counts as inside
  // unless its walk up decides otherwise.
  for (const id of state.tree.unloaded) {
    const record = state.live.get(id)
    if (record === undefined || record.background) continue
    if (inScope(state, parentOf(record), scope) !== false) return false
  }
  const starts = conversationId === undefined ? state.tree.roots : [conversationId]
  for (const start of starts) {
    for (const { node, live } of walkDown(state, conversationNode(start))) {
      if (live !== undefined && !node.background) return false
    }
  }
  return true
}

/** Complete the idle waits whose scope is idle now. */
export const settleIdle = (state: State) =>
  Effect.suspend(() => {
    const done: Array<Deferred.Deferred<void, SchedulerClosed>> = []
    for (const [conversationId, wait] of state.idleWaiters) {
      if (!isIdle(state, conversationId)) continue
      state.idleWaiters.delete(conversationId)
      done.push(wait)
    }
    return Effect.forEach(done, (wait) => Deferred.done(wait, Exit.void), { discard: true })
  })

/**
 * Wait until ordinary traversal from the conversation, or from every ownerless conversation,
 * reaches no live non-background task. Checked and registered on the line.
 */
export const waitForIdle = (state: State, conversationId: ConversationId | undefined) =>
  Effect.flatMap(
    state.session.commit(() =>
      Effect.suspend(() => {
        if (state.closing) return Effect.fail(closedError())
        if (isIdle(state, conversationId)) return Effect.succeedNone
        // Loading unloaded chains may decide it; the reconcile pass settles idle waits.
        scheduleReconcile(state)
        const wait: Idle = state.idleWaiters.get(conversationId) ?? Deferred.makeUnsafe()
        state.idleWaiters.set(conversationId, wait)
        return Effect.succeedSome(wait)
      }),
    ),
    Option.match({ onNone: () => Effect.void, onSome: Deferred.await }),
  )

/**
 * Mark, in one commit, every live non-background task ordinary traversal from the conversation
 * reaches, and withdraw the queued inputs of every conversation it reaches; then wait until the conversation is idle. With `background`, traversal crosses
 * background boundaries, and the wait also covers every task it reached.
 */
export const abortConversation = Effect.fnUntraced(function* (
  state: State,
  conversationId: ConversationId,
  background: boolean,
) {
  const reached = yield* readsFirst(
    state.session.commit((tx) =>
      Effect.gen(function* () {
        yield* loadScopes(state)
        const queued = yield* loadQueued(state)
        const found: Array<TaskId> = []
        const walk = walkDown(state, conversationNode(conversationId), {
          crossBackground: background,
        })
        for (const { node, live } of walk) {
          if (live === undefined || (node.background && !background)) continue
          found.push(live.id)
          if (!live.abortRequested || live.abortReason !== undefined) {
            yield* tx.setTask(withAbortMark(live))
          }
        }
        const scope = { conversation: conversationId }
        yield* withdrawInputs(
          state,
          tx,
          queued,
          (id) => inScope(state, { conversation: id }, scope, background) === true,
        )
        return found
      }),
    ),
  )
  if (background) yield* Effect.forEach(reached, (id) => waitForTask(state, id), { discard: true })
  yield* waitForIdle(state, conversationId)
})
