import { Effect } from 'effect'
import type { Layer } from 'effect'
import { Storage } from '../index.ts'
import type { Entry, Task, Write } from '../schema/index.ts'
import { Ids } from '../schema/index.ts'

/** One backend-independent check of the `Storage` contract. */
export interface StorageCase {
  readonly name: string
  readonly body: (storage: Storage.Service) => Effect.Effect<unknown>
}

/** A conformance case; any failure it leaves unhandled fails the test as a defect. */
export const storageCase = <E>(
  name: string,
  body: (storage: Storage.Service) => Effect.Effect<unknown, E>,
): StorageCase => ({ name, body: (storage) => Effect.orDie(body(storage)) })

/** Runs a case body against a fresh store of one backend. */
export type RunCase = (body: StorageCase['body']) => Promise<unknown>

/** Run a body against `Storage` from a layer built fresh for this call. */
export const runWithLayer =
  <E>(layer: Layer.Layer<Storage.Storage, E>): RunCase =>
  (body) =>
    // Each test is an entry point that provides its own store.
    // oxlint-disable-next-line effecttsgo/strict-effect-provide
    Effect.runPromise(Storage.Storage.use(body).pipe(Effect.provide(layer)))

export const createRoot = (storage: Storage.Service) =>
  storage
    .commit([{ type: 'conversation', value: { id: Ids.ROOT_CONVERSATION_ID } }])
    .pipe(Effect.as(Ids.ROOT_CONVERSATION_ID))

export const entry = (
  id: Ids.EntryId,
  conversationId: Ids.ConversationId,
  kind = 'message',
  extra: Partial<Entry.EntryRecord> = {},
): Entry.EntryRecord => ({ id, conversationId, kind, ...extra })

export const entryWrite = (...args: Parameters<typeof entry>): Write.StorageWrite => ({
  type: 'entry',
  value: entry(...args),
})

export const pendingTask = (id: Ids.TaskId, conversationId: Ids.ConversationId, phase = 'ready') =>
  ({
    id,
    conversationId,
    kind: 'test.task',
    version: 1,
    input: { value: id },
    state: { status: 'pending', checkpoint: { phase } },
    background: false,
    abortRequested: false,
  }) satisfies Task.TaskRecord

export const ids = (page: { readonly items: ReadonlyArray<{ readonly id: number }> }) =>
  page.items.map(({ id }) => id)

export const conversationId = (value: number) => Ids.ConversationId.make(value)
export const entryId = (value: number) => Ids.EntryId.make(value)
