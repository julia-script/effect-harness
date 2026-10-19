import { Effect, Latch } from 'effect'
import type { Deferred } from 'effect'
import type * as Tracker from '../../../delta/Tracker.ts'
import type { Service as Storage } from '../../../storage/Storage.ts'
import type { StorageError, StorageRequestError } from '../../../storage/StorageError.ts'
import type {
  DocumentAddress,
  DocumentCopySource,
  DocumentCreate,
  DocumentRecord,
} from '../../../storage/schema/Document.ts'
import type {
  ConversationId,
  DocumentId,
  EntryId,
  SubmissionId,
  TaskId,
} from '../../../storage/schema/Ids.ts'
import type { JsonObject } from '../../../storage/schema/Json.ts'
import type { SubmissionRecord } from '../../../storage/schema/Submission.ts'
import type { TaskRecord } from '../../../storage/schema/Task.ts'
import type { StorageWrite } from '../../../storage/schema/Write.ts'
import type { SubmissionSettlement, TransactionScope } from '../../Session.ts'
import { ReadAfterWrite, TransactionError } from '../../SessionError.ts'
import type { DocumentError, SessionFailed } from '../../SessionError.ts'
import type { ConversationCreated } from '../../Session.ts'
import type { Cache, Loaded } from '../cache.ts'
import type { AnyDefinition } from '../documents.ts'

/** A guarded call's errors: its `StorageError` becomes `SessionFailed`. */
export type Guarded<E> = Exclude<E, Extract<E, StorageError>> | SessionFailed

/** Session services a transaction uses while it holds the mutation line. */
export interface Host {
  readonly storage: Storage
  /** Run a Storage call behind the Session's failure latch. */
  readonly guard: <A, E>(effect: Effect.Effect<A, E>) => Effect.Effect<A, Guarded<E>>
  readonly cache: Cache
  /** Stages what every new or forked conversation needs, in its creating transaction. */
  readonly conversationCreated: ConversationCreated | undefined
}

/** Committed and candidate state of one task the transaction touched. */
export interface TxTask {
  /** The committed record, once read. */
  committed?: { readonly record: TaskRecord | undefined }
  write?: { readonly kind: 'create' | 'replace'; readonly record: TaskRecord }
}

/** A staged submission change: a settlement, or the placement of a queued submission. */
export type SubmissionChange =
  | SubmissionSettlement
  | { readonly status: 'placed'; readonly entry: EntryId }

/** One definition-free document copy created with a forked conversation. */
export interface ForkCopy {
  readonly record: Extract<DocumentCreate, { readonly history: 'latest' | 'rewindable' }>
  readonly source: DocumentCopySource
}

/** How a document operation of a transaction fails. */
export type DocumentFailure = DocumentError | TransactionError | SessionFailed | StorageRequestError

/** Where one staged document incarnation comes from. */
export type Target =
  | { readonly kind: 'loaded'; readonly document: Loaded }
  | {
      readonly kind: 'created'
      readonly record: DocumentCreate
      readonly version: number
      readonly tracker: Tracker.Tracker<JsonObject>
    }
  | ({ readonly kind: 'fork-copy' } & ForkCopy)
  | { readonly kind: 'retire-only'; readonly record: DocumentRecord }

/** One document incarnation the transaction acquired, created, copied, or retires. */
export interface DocumentEntry {
  readonly addressId: string
  readonly address: DocumentAddress
  /** Absent for definition-free fork copies. */
  definition?: AnyDefinition
  /** The memoized acquisition; absent for retirement only. */
  acquisition?: Deferred.Deferred<Tracker.Draft<JsonObject>, DocumentFailure>
  /** Set once acquisition or retirement lookup found the incarnation. */
  target?: Target
  change?: Tracker.Change<JsonObject>
  prepared?: Tracker.Prepared<JsonObject>
  retireOnCommit: boolean
}

/** Everything one transaction staged; assembled into one batch when its callback succeeds. */
export interface TxState {
  readonly host: Host
  readonly scope: TransactionScope
  sealed: boolean
  hasTableWrite: boolean
  /** Operations still running; a callback must not end before them. */
  pending: number
  /** Open while no operation is pending. */
  readonly drained: Latch.Latch
  /** Conversation and entry writes, staged eagerly in order. */
  readonly writes: Array<StorageWrite>
  readonly createdConversations: Set<ConversationId>
  /** Every document acquisition or retirement, in staging order. */
  readonly documents: Array<DocumentEntry>
  /** The latest entry at each logical address. */
  readonly latestDocuments: Map<string, DocumentEntry>
  readonly forkSourceDocuments: Set<DocumentId>
  readonly forkSourceConversations: Set<ConversationId>
  readonly tasks: Map<TaskId, TxTask>
  /** Submissions this transaction created, by ID. */
  readonly submissions: Map<SubmissionId, SubmissionRecord>
  /** Settlements and placements in staging order, resolved during assembly. */
  readonly submissionChanges: Array<{
    readonly id: SubmissionId
    readonly change: SubmissionChange
  }>
}

export const makeState = (host: Host, scope: TransactionScope): TxState => ({
  host,
  scope,
  sealed: false,
  hasTableWrite: false,
  pending: 0,
  drained: Latch.makeUnsafe(true),
  writes: [],
  createdConversations: new Set(),
  documents: [],
  latestDocuments: new Map(),
  forkSourceDocuments: new Set(),
  forkSourceConversations: new Set(),
  tasks: new Map(),
  submissions: new Map(),
  submissionChanges: [],
})

export const rejected = (message: string) => Effect.fail(new TransactionError({ message }))

/** Fails once the transaction has settled. */
export const assertOpen = (state: TxState): Effect.Effect<void, TransactionError> =>
  state.sealed ? rejected('Transaction has settled') : Effect.void

/** Count an operation as pending while it runs. */
export const track = <A, E>(state: TxState, effect: Effect.Effect<A, E>) =>
  Effect.suspend(() => {
    if (state.pending++ === 0) state.drained.closeUnsafe()
    return Effect.ensuring(
      effect,
      Effect.sync(() => {
        if (--state.pending === 0) state.drained.openUnsafe()
      }),
    )
  })

/** A table read: refused after the first table write. */
export const read = <A, E>(
  state: TxState,
  method: string,
  effect: Effect.Effect<A, E>,
): Effect.Effect<A, E | TransactionError | ReadAfterWrite> =>
  Effect.suspend((): Effect.Effect<A, E | TransactionError | ReadAfterWrite> => {
    if (state.sealed) return rejected('Transaction has settled')
    if (state.hasTableWrite) {
      const message = `Tx.${method}() cannot read tables after the first table write`
      return Effect.fail(new ReadAfterWrite({ message, method }))
    }
    return track(state, effect)
  })

/** A table write: every later table read is refused. */
export const write = <A, E>(
  state: TxState,
  effect: Effect.Effect<A, E>,
): Effect.Effect<A, E | TransactionError> =>
  Effect.suspend((): Effect.Effect<A, E | TransactionError> => {
    if (state.sealed) return rejected('Transaction has settled')
    state.hasTableWrite = true
    return track(state, effect)
  })

export const taskEntry = (state: TxState, id: TaskId): TxTask => {
  const existing = state.tasks.get(id)
  if (existing !== undefined) return existing
  const created: TxTask = {}
  state.tasks.set(id, created)
  return created
}

/** The committed task record, read from Storage once per transaction. */
export const committedTask = (state: TxState, id: TaskId) =>
  Effect.suspend(() => {
    const task = taskEntry(state, id)
    if (task.committed !== undefined) return Effect.succeed(task.committed.record)
    return Effect.map(state.host.guard(state.host.storage.task(id)), (record) => {
      task.committed = { record }
      return record
    })
  })

/** The latest candidate task record, falling back to committed state; not a caller read. */
export const currentTask = (state: TxState, id: TaskId) =>
  Effect.suspend(() => {
    const candidate = state.tasks.get(id)?.write?.record
    return candidate === undefined ? committedTask(state, id) : Effect.succeed(candidate)
  })

/** Fails unless the conversation was created by this transaction or is committed. */
export const requireConversation = (state: TxState, id: ConversationId) =>
  state.createdConversations.has(id)
    ? Effect.void
    : Effect.flatMap(state.host.guard(state.host.storage.conversation(id)), (record) =>
        record === undefined ? rejected(`Conversation ${id} does not exist`) : Effect.void,
      )
