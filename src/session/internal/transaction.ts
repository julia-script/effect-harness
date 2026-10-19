import { Effect } from 'effect'
import { cast } from 'effect/Function'
import type { ConversationRecord } from '../../storage/schema/Conversation.ts'
import type { TransactionScope, Tx } from '../Session.ts'
import type { TxDocumentError } from '../SessionError.ts'
import type { AnyToken } from './documents.ts'
import { assemble } from './transaction/assemble.ts'
import { stageConversation, stageFork, stageRootConversation } from './transaction/conversations.ts'
import { abortChanges, doc, prepareChanges, retireDoc } from './transaction/documents.ts'
import { snapshotOnLine } from './snapshots.ts'
import * as Records from './transaction/records.ts'
import { assertOpen, committedTask, makeState, read, rejected, write } from './transaction/state.ts'
import type { Host, TxState } from './transaction/state.ts'

/**
 * Transaction of one commit callback, ported from pi's `session/transaction.ts`.
 *
 * Conversation and entry writes stage eagerly; tasks and submissions are kept per ID as their
 * latest candidate, and documents as open tracker changes. When the callback ends the transaction
 * seals, and every later operation fails; on success every change is prepared and the staged
 * writes are validated and assembled into one batch. Whenever the batch does not commit, every
 * change is aborted.
 */
export interface Transaction {
  readonly tx: Tx
  readonly state: TxState
}

/** What the transaction staged so far; not a read. */
const staged = <A>(state: TxState, collect: (state: TxState) => A) =>
  Effect.flatMap(assertOpen(state), () => Effect.sync(() => collect(state)))

/**
 * Conversation creation: the record, then the host's writes for it, as one table write. `self` is
 * the transaction the host's hook receives.
 */
const conversationWrites = (
  state: TxState,
  self: () => Tx,
): Pick<Tx, 'createConversation' | 'createRootConversation' | 'forkConversation'> => {
  const hook = state.host.conversationCreated
  const created = (staging: Effect.Effect<ConversationRecord, TxDocumentError>) =>
    write(
      state,
      hook === undefined ? staging : Effect.tap(staging, (record) => hook(self(), record)),
    )
  return {
    createConversation: ({ ownership }) => created(stageConversation(state, ownership)),
    createRootConversation: created(stageRootConversation(state)),
    forkConversation: (conversationId, at, { ownership }) =>
      created(stageFork(state, conversationId, at, ownership)),
  }
}

const makeTx = (state: TxState): Tx => {
  const { guard, storage } = state.host
  const tx: Tx = {
    conversation: (id) => read(state, 'conversation', guard(storage.conversation(id))),
    entry: (id) =>
      read(
        state,
        'entry',
        Effect.map(guard(storage.entry(id)), (stored) => stored?.entry),
      ),
    task: (id) => read(state, 'task', committedTask(state, id)),
    submission: (id) => read(state, 'submission', guard(storage.submission(id))),
    submissionByRequest: (conversationId, requestId) =>
      read(
        state,
        'submissionByRequest',
        guard(storage.submissionByRequest(conversationId, requestId)),
      ),
    scanConversations: (query, limit, cursor) =>
      read(state, 'scanConversations', guard(storage.scanConversations(query, limit, cursor))),
    scanEntries: (query, limit, cursor) =>
      read(state, 'scanEntries', guard(storage.scanEntries(query, limit, cursor))),
    scanTasks: (query, limit, cursor) =>
      read(state, 'scanTasks', guard(storage.scanTasks(query, limit, cursor))),
    latestHeadMarker: (conversationId) =>
      read(state, 'latestHeadMarker', guard(storage.findLatestHeadMarker(conversationId))),
    snapshot: cast((token: AnyToken, ...args: ReadonlyArray<unknown>) =>
      read(state, 'snapshot', snapshotOnLine(state.host.cache, token, args)),
    ),
    ...conversationWrites(state, () => tx),
    appendEntry: (conversationId, draft) =>
      write(state, Records.appendEntry(state, conversationId, draft)),
    createTask: (kind, input, options) =>
      write(state, Records.createTask(state, kind, input, options)),
    createSubmission: (create) => write(state, Records.createSubmission(state, create)),
    settleSubmission: (id, settlement) => Records.settleSubmission(state, id, settlement),
    placeSubmission: (id, entry) => Records.placeSubmission(state, id, entry),
    setTask: (record) => Records.setTask(state, record),
    stagedTasks: staged(state, Records.stagedTasks),
    stagedConversations: staged(state, Records.stagedConversations),
    // Overload resolution ends here: past it, tokens and drafts are untyped JSON.
    doc: cast((token: AnyToken, ...args: ReadonlyArray<unknown>) => doc(state, token, args)),
    retireDoc: cast((token: AnyToken, ...args: ReadonlyArray<unknown>) =>
      retireDoc(state, token, args),
    ),
  }
  return tx
}

export const makeTransaction = (host: Host, scope: TransactionScope = {}): Transaction => {
  const state = makeState(host, scope)
  return { tx: makeTx(state), state }
}

/** Seal the transaction; nothing it staged can change any more. */
export const seal = (transaction: Transaction) =>
  Effect.sync(() => {
    transaction.state.sealed = true
  })

/** Abort every document change: the batch will not commit. */
export const discard = (transaction: Transaction) => abortChanges(transaction.state)

/**
 * Seal and discard after the callback failed, then wait for its pending operations, so none
 * outlives the commit's hold on the mutation line.
 */
export const abandon = (transaction: Transaction) =>
  Effect.andThen(
    Effect.andThen(seal(transaction), discard(transaction)),
    transaction.state.drained.await,
  )

/** Seal after the callback succeeded, prepare every change, and assemble the batch. */
export const settle = (transaction: Transaction) => {
  const { state } = transaction
  return Effect.flatMap(seal(transaction), () =>
    state.pending > 0
      ? Effect.andThen(
          abandon(transaction),
          rejected('Session commit callback ended before its pending Tx operations'),
        )
      : Effect.andThen(prepareChanges(state), assemble(state)),
  ).pipe(Effect.onError(() => discard(transaction)))
}
