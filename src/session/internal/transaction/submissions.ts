import { Effect } from 'effect'
import type { SubmissionRecord } from '../../../storage/schema/Submission.ts'
import type { TransactionError } from '../../SessionError.ts'
import { rejected } from './state.ts'
import type { SubmissionChange, TxState } from './state.ts'

const identity = (current: SubmissionRecord) => ({
  id: current.id,
  conversationId: current.conversationId,
  ...(current.requestId === undefined ? {} : { requestId: current.requestId }),
})

/**
 * The record after one change. Placement turns a queued input `placed` and a queued write `done`;
 * only a placed input can be answered; a settled record stays as it is.
 */
const applyChange = (
  current: SubmissionRecord,
  change: SubmissionChange,
): Effect.Effect<SubmissionRecord, TransactionError> => {
  if (current.status === 'done' || current.status === 'unanswered') return Effect.succeed(current)
  const base = identity(current)
  if (change.status === 'placed') {
    if (current.status !== 'queued') return rejected(`Submission ${current.id} is not queued`)
    return Effect.succeed(
      current.type === 'input'
        ? { ...base, type: 'input', status: 'placed', entry: change.entry }
        : { ...base, type: 'write', status: 'done', entry: change.entry },
    )
  }
  if (change.status === 'done') {
    if (current.type !== 'input' || current.status !== 'placed') {
      return rejected(`Submission ${current.id} is not a placed input`)
    }
    const answer = change.answer
    return Effect.succeed({ ...base, type: 'input', status: 'done', entry: current.entry, answer })
  }
  const settled = {
    ...base,
    status: 'unanswered' as const,
    reason: change.reason,
    ...(change.detail === undefined ? {} : { detail: change.detail }),
  }
  if (current.type === 'write') return Effect.succeed({ ...settled, type: 'write' })
  // A placed input keeps its entry.
  const entry = current.status === 'placed' ? { entry: current.entry } : {}
  return Effect.succeed({ ...settled, ...entry, type: 'input' })
}

/** Resolve every staged change against the latest candidate, falling back to committed state. */
export const resolveSubmissions = (state: TxState) => {
  const { guard, storage } = state.host
  return Effect.forEach(
    state.submissionChanges,
    ({ id, change }) => {
      const staged = state.submissions.get(id)
      const current = staged === undefined ? guard(storage.submission(id)) : Effect.succeed(staged)
      return current.pipe(
        Effect.flatMap((record) =>
          record === undefined
            ? rejected(`Submission ${id} does not exist`)
            : applyChange(record, change),
        ),
        Effect.tap((next) =>
          Effect.sync(() => {
            state.submissions.set(id, next)
          }),
        ),
      )
    },
    { discard: true },
  )
}
