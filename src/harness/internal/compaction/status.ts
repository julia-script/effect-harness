/**
 * Compaction tasks and their status in `$live` (spec §8.7).
 */
import { Effect } from 'effect'
import type { Draft } from '../../../delta/Tracker.ts'
import type { TaskKind, Tx } from '../../../session/Session.ts'
import type { ConversationId, TaskId } from '../../../storage/schema/Ids.ts'
import * as Live from '../../Live.ts'

export const COMPACTION_KIND = '$compaction'

/** What starts a compaction: why, and what the summary should focus on. */
export interface Input {
  readonly reason: Live.CompactionReason
  readonly instructions?: string
}

export const compactionTask: TaskKind<{ reason: Live.CompactionReason; instructions?: string }> = {
  name: COMPACTION_KIND,
  version: 1,
  initial: () => ({ phase: 'select' }),
}

/** The status of compaction `taskId`, if listed. */
export const statusOf = (live: Draft<Live.State>, taskId: TaskId) =>
  live.compactions?.find((status) => status.taskId === taskId)

/** Remove the status of compaction `taskId`, and the list once empty. */
export const removeStatus = (live: Draft<Live.State>, taskId: TaskId): void => {
  const statuses = live.compactions
  if (statuses === undefined) return
  const index = statuses.findIndex((status) => status.taskId === taskId)
  if (index >= 0) statuses.splice(index, 1)
  if (statuses.length === 0) delete live.compactions
}

/** Remove the status of compaction `taskId` and record that it failed with `error`. */
export const failStatus = (live: Draft<Live.State>, taskId: TaskId, error: string): void => {
  const status = statusOf(live, taskId)
  if (status !== undefined) live.compactionFailure = { taskId, reason: status.reason, error }
  removeStatus(live, taskId)
}

/**
 * Create a compaction task with its status in this commit. `owner` is the generation that waits
 * for it, a blocking compaction; without one it is conversation-owned, and in the background
 * unless it is manual.
 */
export const createCompaction = Effect.fnUntraced(function* (
  tx: Tx,
  conversationId: ConversationId,
  input: Input,
  owner?: TaskId,
) {
  const options =
    owner === undefined
      ? {
          ownership: { kind: 'conversation' } as const,
          conversationId,
          background: input.reason !== 'manual',
        }
      : { ownership: { kind: 'task', taskId: owner } as const, conversationId }
  const taskId = yield* tx.createTask(compactionTask, { ...input }, options)
  const live = yield* tx.doc(Live.document, conversationId)
  live.compactions ??= []
  live.compactions.push({ taskId, reason: input.reason, blocking: owner !== undefined, attempt: 1 })
  return taskId
})
