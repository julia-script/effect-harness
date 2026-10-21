/**
 * Submission handles and their waits. A wait is checked and registered on the Session line, and
 * completed by the commit listener, also on the line, so no settling commit falls between them.
 */
import { Deferred, Effect, Exit } from 'effect'
import type { CommitPublication, Service as SessionService } from '../../session/Session.ts'
import { SessionClosed } from '../../session/SessionError.ts'
import { readsFirst } from '../../session/internal/readsFirst.ts'
import type { CommitError } from '../../session/SessionError.ts'
import type { ConversationId, SubmissionId } from '../../storage/schema/Ids.ts'
import type { SubmissionRecord } from '../../storage/schema/Submission.ts'
import { SubmissionNotFound } from '../HarnessError.ts'
import type { ConversationBusy, InvalidSubmission } from '../HarnessError.ts'
import type { Settings } from '../Settings.ts'
import type { AbortResult, Draft, Settled, Submission } from '../Submission.ts'
import { admit } from './admission.ts'
import { removeItem } from './inbox.ts'

type Wait = Deferred.Deferred<Settled, SessionClosed>

export interface Submissions {
  readonly submit: (
    conversationId: ConversationId,
    draft: Draft,
  ) => Effect.Effect<Submission, ConversationBusy | InvalidSubmission | CommitError>
  readonly get: (id: SubmissionId) => Effect.Effect<Submission | undefined, CommitError>
  readonly abort: (
    id: SubmissionId,
    conversationId?: ConversationId,
  ) => Effect.Effect<AbortResult | 'not_found', CommitError>
}

/** Pending waits by submission; `closed` once the Harness closed. */
interface Waits {
  readonly session: SessionService
  readonly pending: Map<SubmissionId, Wait>
  closed: boolean
}

const isSettled = (record: SubmissionRecord): record is Settled =>
  record.status === 'done' || record.status === 'unanswered'

const notFound = (id: SubmissionId) =>
  new SubmissionNotFound({ message: `Submission ${id} does not exist`, submissionId: id })

const closedError = () => new SessionClosed({ message: 'Harness is closed' })

/** The committed record of an existing submission. */
const read = (session: SessionService, id: SubmissionId) =>
  readsFirst(session.commit((tx) => tx.submission(id)))

const status = (session: SessionService, id: SubmissionId) =>
  read(session, id).pipe(
    Effect.filterOrFail(
      (record): record is SubmissionRecord => record !== undefined,
      () => notFound(id),
    ),
  )

/** The settled record, or the wait registered for it. */
const register = (waits: Waits, id: SubmissionId, record: SubmissionRecord | undefined) =>
  Effect.suspend(
    (): Effect.Effect<
      { readonly settled: Settled } | { readonly wait: Wait },
      SubmissionNotFound | SessionClosed
    > => {
      if (record === undefined) return Effect.fail(notFound(id))
      if (isSettled(record)) return Effect.succeed({ settled: record })
      // Close fails registered waits at once, and may have begun during the read.
      if (waits.closed) return Effect.fail(closedError())
      const registered = waits.pending.get(id) ?? Deferred.makeUnsafe<Settled, SessionClosed>()
      waits.pending.set(id, registered)
      return Effect.succeed({ wait: registered })
    },
  )

const wait = (waits: Waits, id: SubmissionId) =>
  Effect.flatMap(
    readsFirst(
      waits.session.commit((tx) =>
        Effect.flatMap(tx.submission(id), (record) => register(waits, id, record)),
      ),
    ),
    (found) => ('wait' in found ? Deferred.await(found.wait) : Effect.succeed(found.settled)),
  )

/** Withdraw a queued submission and remove its inbox item, in one commit. */
const abort = (session: SessionService, id: SubmissionId, conversationId?: ConversationId) =>
  readsFirst(
    session.commit((tx) =>
      Effect.gen(function* () {
        const record = yield* tx.submission(id)
        if (
          record === undefined ||
          (conversationId !== undefined && record.conversationId !== conversationId)
        ) {
          return 'not_found' as const
        }
        if (record.status === 'queued') {
          yield* tx.settleSubmission(id, { status: 'unanswered', reason: 'aborted' })
          yield* removeItem(tx, record.conversationId, id)
          return 'aborted' as const
        }
        return record.status === 'placed' ? ('already_placed' as const) : ('settled' as const)
      }),
    ),
  )

const handle = (waits: Waits, id: SubmissionId): Submission => ({
  id,
  status: status(waits.session, id),
  wait: wait(waits, id),
  abort: abort(waits.session, id).pipe(
    Effect.filterOrFail(
      (result): result is AbortResult => result !== 'not_found',
      () => notFound(id),
    ),
  ),
})

/** Complete the waits of the submissions a commit settled. */
const observe = (waits: Waits) => (publication: CommitPublication) =>
  Effect.forEach(
    publication.changes,
    (change) => {
      if (change.type !== 'submission' || !isSettled(change.value)) return Effect.void
      const pending = waits.pending.get(change.value.id)
      if (pending === undefined) return Effect.void
      waits.pending.delete(change.value.id)
      return Deferred.succeed(pending, change.value)
    },
    { discard: true },
  )

/** Fail every pending wait; later waits fail too. */
const close = (waits: Waits) =>
  Effect.suspend(() => {
    waits.closed = true
    const pending = [...waits.pending.values()]
    waits.pending.clear()
    const failed = Exit.fail(closedError())
    return Effect.forEach(pending, (each) => Deferred.done(each, failed), { discard: true })
  })

/** Open the submissions of one Harness; closing the scope fails every pending wait. */
export const make = Effect.fnUntraced(function* (
  session: SessionService,
  settings: Effect.Effect<Settings>,
) {
  const waits: Waits = { session, pending: new Map(), closed: false }
  yield* session.observeCommits(observe(waits))
  yield* Effect.addFinalizer(() => close(waits))
  const submissions: Submissions = {
    // Settings are read on the line, when the boundary is decided.
    submit: (conversationId, draft) =>
      Effect.map(
        readsFirst(
          session.commit((tx) =>
            Effect.flatMap(settings, (resolved) => admit(tx, conversationId, draft, resolved)),
          ),
        ),
        (id) => handle(waits, id),
      ),
    get: (id) =>
      Effect.map(read(session, id), (record) =>
        record === undefined ? undefined : handle(waits, record.id),
      ),
    abort: (id, conversationId) => abort(session, id, conversationId),
  }
  return submissions
})
