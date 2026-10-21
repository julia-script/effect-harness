/**
 * Conversation handles and conversation creation.
 */
import { Clock, Effect } from 'effect'
import type { Service as SessionService, Tx } from '../../session/Session.ts'
import type { TxDocumentError, TxReadError } from '../../session/SessionError.ts'
import type { ConversationRecord } from '../../storage/schema/Conversation.ts'
import { ROOT_CONVERSATION_ID } from '../../storage/schema/Ids.ts'
import type { ConversationId, EntryId } from '../../storage/schema/Ids.ts'
import type { Service as SchedulerService } from '../../task/Scheduler.ts'
import * as Agent from '../Agent.ts'
import type { Conversation, CreateOptions, Init } from '../Conversation.ts'
import type { Service as Extensions } from '../Extensions.ts'
import type { Settings } from '../Settings.ts'
import { current } from './agent.ts'
import { createCompaction } from './compaction/status.ts'
import { readVisible } from './context.ts'
import { RESET_KIND } from './inbox.ts'
import type { Submissions } from './submissions.ts'
import { readsFirst } from '../../session/internal/readsFirst.ts'
import type { Views } from './views/views.ts'

export interface Host {
  readonly session: SessionService
  readonly scheduler: SchedulerService
  readonly submissions: Submissions
  readonly extensions: Extensions
  readonly settings: Effect.Effect<Settings>
  readonly views: Views
}

/** Where a conversation is created. */
export type Target =
  | { readonly kind: 'root' }
  | { readonly kind: 'independent'; readonly ownership: CreateOptions['ownership'] }
  | {
      readonly kind: 'fork'
      readonly parentId: ConversationId
      readonly at: EntryId
      readonly ownership: CreateOptions['ownership']
    }

interface Staged {
  readonly record: ConversationRecord
  readonly created: boolean
}

/** The record a target creates, or the root when it exists already. */
const stage = (tx: Tx, target: Target): Effect.Effect<Staged, TxReadError | TxDocumentError> => {
  if (target.kind === 'root') {
    return Effect.flatMap(tx.conversation(ROOT_CONVERSATION_ID), (found) =>
      found === undefined
        ? Effect.map(tx.createRootConversation, (record): Staged => ({ record, created: true }))
        : Effect.succeed<Staged>({ record: found, created: false }),
    )
  }
  const created =
    target.kind === 'fork'
      ? tx.forkConversation(target.parentId, target.at, { ownership: target.ownership })
      : tx.createConversation({ ownership: target.ownership })
  return Effect.map(created, (record) => ({ record, created: true }))
}

/** Create a conversation and run `init` in one commit; the creation hooks run in it too. */
export const create = <E>(host: Host, target: Target, init: Init<E> | undefined) =>
  Effect.map(
    readsFirst(
      host.session.commit((tx) =>
        Effect.gen(function* () {
          const { record, created } = yield* stage(tx, target)
          if (created && init !== undefined) yield* init(tx, record.id)
          return record.id
        }),
      ),
    ),
    (id) => conversation(host, id),
  )

/** The stateless handle of an existing conversation. */
export const conversation = (host: Host, id: ConversationId): Conversation => ({
  id,
  submit: (draft) => host.submissions.submit(id, draft),
  // A write without a request ID is never busy or invalid.
  reset: (handoff) =>
    Effect.flatMap(Clock.currentTimeMillis, (timestamp) =>
      host.submissions.submit(id, {
        type: 'write',
        entry: {
          kind: RESET_KIND,
          head: 'self',
          ...(handoff === undefined
            ? {}
            : { model: [{ role: 'user', content: handoff, timestamp }] }),
        },
      }),
    ).pipe(Effect.catchTags({ ConversationBusy: Effect.die, InvalidSubmission: Effect.die })),
  compact: (instructions) =>
    host.session.commit((tx) =>
      createCompaction(tx, id, {
        reason: 'manual',
        ...(instructions === undefined ? {} : { instructions }),
      }),
    ),
  configure: (change) => host.session.commit((tx) => Agent.configure(tx, id, change)),
  agent: Effect.map(current(host.session, id, host.extensions, host.settings), Agent.view),
  commit: (change, scope) => host.session.commit(change, { ...scope, conversationId: id }),
  entries: (query, limit, cursor) =>
    readsFirst(
      host.session.commit((tx) => tx.scanEntries({ ...query, conversationId: id }, limit, cursor)),
    ),
  context: (options) => readsFirst(host.session.commit((tx) => readVisible(tx, id, options?.at))),
  view: host.views.view(id),
  events: host.views.events(id),
  fork: (at, options) =>
    create(host, { kind: 'fork', parentId: id, at, ownership: options.ownership }, options.init),
  abort: (options) => host.scheduler.abortConversation(id, options),
  waitForIdle: host.scheduler.waitForIdle(id),
})
