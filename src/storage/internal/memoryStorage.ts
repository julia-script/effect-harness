import { Effect } from 'effect'
import type { Scope } from 'effect'
import { commit } from './memory/commit.ts'
import { materializeDocument } from './memory/documents.ts'
import * as Reads from './memory/reads.ts'
import { addressKey, ensureOpen, isAliveAt, makeState } from './memory/state.ts'
import type { State } from './memory/state.ts'
import type { DocumentAddress, DocumentPoint, DocumentRecord } from '../schema/Document.ts'
import type { IdSchema, StorageId, Service } from '../Storage.ts'
import { StorageError } from '../StorageError.ts'

/**
 * Detached in-memory reference implementation of `Storage`, ported from pi's
 * `packages/durable/src/storage/memory.ts`.
 *
 * Reads and retained writes are cloned to match the ownership boundary of serialization-backed
 * stores. This is backend conformance, not validation: records are trusted to match their schema.
 */

const mint = <I extends StorageId>(state: State, id: IdSchema<I>) =>
  Effect.flatMap(ensureOpen(state), () => {
    if (!Number.isSafeInteger(state.nextId)) {
      return Effect.fail(new StorageError({ message: 'ID space is exhausted' }))
    }
    const value = id.make(state.nextId)
    state.nextId++
    return Effect.succeed(value)
  })

const findDocument = (
  state: State,
  address: DocumentAddress,
  at: DocumentPoint,
): DocumentRecord | undefined => {
  const index = state.documentAddresses.get(addressKey(address))
  const id =
    at === 'current'
      ? index?.currentId
      : index?.ids.find((candidate) => {
          const record = state.documents.get(candidate)?.record
          return record !== undefined && isAliveAt(record, at)
        })
  return id === undefined ? undefined : structuredClone(state.documents.get(id)?.record)
}

const submissionByRequest = (state: State, conversationId: number, requestId: string) => {
  const id = state.submissionIdsByRequest.get(conversationId)?.get(requestId)
  return id === undefined ? undefined : structuredClone(state.submissions.get(id))
}

/** `materializeDocument` already detached the value; only the record needs a copy. */
const document = (state: State, id: number, at: DocumentPoint) =>
  Effect.map(materializeDocument(state, id, at), (stored) =>
    stored === undefined ? undefined : { ...stored, record: structuredClone(stored.record) },
  )

/** The `Storage` operations over one in-memory state. */
export const service = (state: State): Service => {
  const open = ensureOpen(state)
  return {
    commit: (writes) => commit(state, writes),
    mintId: (id) => mint(state, id),
    conversation: (id) => Effect.map(open, () => structuredClone(state.conversations.get(id))),
    scanConversations: (query, limit, cursor) =>
      Effect.andThen(open, () => Reads.scanConversations(state, query, limit, cursor)),
    entry: (id) => Effect.map(open, () => Reads.storedEntry(state, id)),
    visibleEntry: (conversationId, id) =>
      Effect.andThen(open, () => Reads.visibleEntry(state, conversationId, id)),
    findLatestHeadMarker: (conversationId, atOrBefore) =>
      Effect.andThen(open, () => Reads.findLatestHeadMarker(state, conversationId, atOrBefore)),
    scanEntries: (query, limit, cursor) =>
      Effect.andThen(open, () => Reads.scanEntries(state, query, limit, cursor)),
    task: (id) => Effect.map(open, () => structuredClone(state.tasks.get(id))),
    scanTasks: (query, limit, cursor) =>
      Effect.andThen(open, () => Reads.scanTasks(state, query, limit, cursor)),
    submission: (id) => Effect.map(open, () => structuredClone(state.submissions.get(id))),
    scanSubmissions: (query, limit, cursor) =>
      Effect.andThen(open, () => Reads.scanSubmissions(state, query, limit, cursor)),
    submissionByRequest: (conversationId, requestId) =>
      Effect.map(open, () => submissionByRequest(state, conversationId, requestId)),
    findDocument: (address, at) => Effect.map(open, () => findDocument(state, address, at)),
    document: (id, at) => Effect.andThen(open, () => document(state, id, at)),
    scanDocuments: (query, limit, cursor) =>
      Effect.andThen(open, () => Reads.scanDocuments(state, query, limit, cursor)),
  }
}

/**
 * Build an empty in-memory store. Closing the scope closes the store: every later operation fails
 * with a `StorageError`.
 */
export const makeMemory: Effect.Effect<Service, never, Scope.Scope> = Effect.gen(function* () {
  const state = makeState()
  yield* Effect.addFinalizer(() =>
    Effect.sync(() => {
      state.closed = true
    }),
  )
  return service(state)
})
