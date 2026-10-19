import { Effect } from 'effect'
import * as Session from '../../session/Session.ts'
import type { CommitError } from '../../session/SessionError.ts'
import { SubmissionId } from '../../storage/schema/Ids.ts'
import type { ConversationId } from '../../storage/schema/Ids.ts'
import type { Cursor } from '../../storage/schema/Scan.ts'
import type { SubmissionRecord } from '../../storage/schema/Submission.ts'
import { Storage } from '../../storage/Storage.ts'
import * as Scheduler from '../../task/Scheduler.ts'
import * as Agent from '../Agent.ts'
import { Extensions } from '../Extensions.ts'
import type { Options, Service } from '../Harness.ts'
import * as Inbox from '../Inbox.ts'
import * as Live from '../Live.ts'
import * as Settings from '../Settings.ts'
import * as Usage from '../Usage.ts'
import { conversation, create } from './conversation.ts'
import type { Host } from './conversation.ts'
import { withdrawQueuedInputs } from './inbox.ts'
import { settleOutcome } from './run.ts'
import * as Submissions from './submissions.ts'
import { readsFirst } from './submissions.ts'
import * as Views from './views/views.ts'

const SCAN_PAGE_SIZE = 256

/** The Session hook: the built-in documents of every new conversation, then the host's. */
const sessionOptions = (options: Options): Session.Options => ({
  ...(options.documentRetention === undefined
    ? {}
    : { documentRetention: options.documentRetention }),
  conversationCreated: (tx, record) =>
    Effect.gen(function* () {
      yield* tx.doc(Live.document, record.id)
      yield* tx.doc(Inbox.document, record.id)
      yield* Agent.created(tx, record)
      if (options.conversationCreated !== undefined) yield* options.conversationCreated(tx, record)
    }),
})

const schedulerOptions: Scheduler.Options = {
  settleOutcome,
  withdrawInputs: withdrawQueuedInputs,
}

/** Every conversation's ID, read on the line. */
const conversationIds = (session: Session.Service) =>
  readsFirst(
    session.commit((tx) =>
      Effect.gen(function* () {
        const ids: Array<ConversationId> = []
        let cursor: Cursor | undefined
        do {
          const page = yield* tx.scanConversations({}, SCAN_PAGE_SIZE, cursor)
          ids.push(...page.items.map(({ id }) => id))
          cursor = page.next
        } while (cursor !== undefined)
        return ids
      }),
    ),
  )

/** Every conversation's `$usage`, summed. */
const usage = (session: Session.Service) =>
  Effect.gen(function* () {
    const states: Array<Usage.State> = []
    for (const id of yield* conversationIds(session)) {
      const state = yield* session.snapshot(Usage.document, id)
      if (state !== undefined) states.push(state)
    }
    return Usage.sum(states)
  })

/** Every unsettled submission in ID order, read from Storage on the line. */
const unsettled = (storage: Storage['Service'], session: Session.Service) =>
  session.commit(() =>
    Effect.gen(function* () {
      const records: Array<SubmissionRecord> = []
      for (const status of ['queued', 'placed'] as const) {
        let cursor: Cursor | undefined
        do {
          const page = yield* storage.scanSubmissions({ status }, SCAN_PAGE_SIZE, cursor)
          records.push(...page.items)
          cursor = page.next
        } while (cursor !== undefined)
      }
      return records.toSorted((a, b) => SubmissionId.make(a.id) - SubmissionId.make(b.id))
    }),
  )

export const make = Effect.fnUntraced(function* (options: Options) {
  const storage = yield* Storage
  const session = yield* Session.makeWith(sessionOptions(options))
  const scheduler: Scheduler.Service = yield* Scheduler.makeWith(schedulerOptions).pipe(
    Effect.provideService(Session.Session, session),
  )
  const submissions = yield* Submissions.make(session, yield* Settings.Current)
  const extensions = yield* Extensions
  const host: Host = {
    session,
    scheduler,
    submissions,
    extensions,
    settings: yield* Settings.Current,
    views: yield* Views.make(session),
  }
  const service: Service = {
    session,
    scheduler,
    root: (rootOptions) => create(host, { kind: 'root' }, rootOptions?.init),
    conversation: (id) =>
      Effect.map(readsFirst(session.commit((tx) => tx.conversation(id))), (record) =>
        record === undefined ? undefined : conversation(host, record.id),
      ),
    createConversation: (createOptions) =>
      create(host, { kind: 'independent', ownership: createOptions.ownership }, createOptions.init),
    submission: submissions.get,
    abortSubmission: submissions.abort,
    task: (id) => readsFirst(session.commit((tx) => tx.task(id))),
    usage: usage(session),
    taskGraph: host.views.taskGraph,
    inspect: Effect.gen(function* () {
      const { scheduling, tasks } = yield* scheduler.inspect
      const pending = yield* unsettled(storage, session)
      return { scheduling, tasks, submissions: pending }
    }),
  }
  return service
}) satisfies (options: Options) => Effect.Effect<Service, CommitError, unknown>
