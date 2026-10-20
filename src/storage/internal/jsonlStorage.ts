import { Effect, FileSystem, Path, Semaphore } from 'effect'
import type { Scope } from 'effect'
import { prepareCommit } from './memory/commit.ts'
import type { PreparedCommit } from './memory/commit.ts'
import { makeState } from './memory/state.ts'
import type { State } from './memory/state.ts'
import { service as memoryService } from './memoryStorage.ts'
import { adoptSidecarState, encodeCommit, planReclamations } from './jsonl/encode.ts'
import type { SidecarState } from './jsonl/encode.ts'
import { append, flush, io, reclaim } from './jsonl/files.ts'
import type { Files } from './jsonl/files.ts'
import { MAIN_FILE } from './jsonl/format.ts'
import { adoptAnalysis, analyze, cleanSidecars, readDisk } from './jsonl/recover.ts'
import { replay } from './jsonl/replay.ts'
import type { JsonlOptions, Service } from '../Storage.ts'
import type { StorageWrite } from '../schema/Write.ts'
import { StorageError } from '../StorageError.ts'

/**
 * Portable JSONL implementation of `Storage`, ported from pi's
 * `packages/durable/src/storage/jsonl/storage.ts`.
 *
 * The in-memory store serves every read; the files are its durable log. A commit is validated and
 * serialized in memory, its sidecar lines are appended, and the `main.jsonl` marker publishes it;
 * only then does memory apply it. A failed append may leave a partial commit on disk, so the store
 * is poisoned: every later operation fails until it is reopened, and reopening recovers exactly
 * the commits whose marker is complete.
 */

interface JsonlState extends SidecarState {
  readonly files: Files
  readonly memory: State
  closed: boolean
  poison: StorageError | undefined
}

const usable = (state: JsonlState): Effect.Effect<void, StorageError> =>
  Effect.suspend(() => {
    if (state.closed) return Effect.fail(new StorageError({ message: 'JsonlStorage is closed' }))
    return state.poison === undefined ? Effect.void : Effect.fail(state.poison)
  })

const poisoned = (state: JsonlState) => (cause: StorageError) => {
  state.poison ??= new StorageError({
    message: 'JSONL storage is poisoned and must be reopened',
    cause,
  })
  return state.poison
}

/** Append every sidecar line, then the marker that publishes them. */
const publish = (state: JsonlState, sidecars: ReadonlyMap<string, string>, marker: string) => {
  const { files } = state
  const appendSidecars = Effect.forEach(
    sidecars,
    ([file, content]) => append(files, file, content),
    {
      discard: true,
    },
  )
  const flushSidecars = files.fsync
    ? Effect.forEach(sidecars.keys(), (file) => flush(files, file), { discard: true })
    : Effect.void
  return appendSidecars.pipe(
    Effect.andThen(flushSidecars),
    Effect.andThen(append(files, MAIN_FILE, marker)),
    Effect.mapError(poisoned(state)),
  )
}

const persist = Effect.fnUntraced(function* (state: JsonlState, prepared: PreparedCommit) {
  const encoded = yield* encodeCommit(prepared.seq, prepared.writes)
  const reclamations = planReclamations(state, prepared.writes, encoded)
  // A commit cut short between its first append and `apply` would desynchronize disk and memory.
  return yield* Effect.uninterruptible(
    Effect.gen(function* () {
      yield* publish(state, encoded.sidecars, encoded.marker)
      const seq = prepared.apply()
      adoptSidecarState(state, prepared.writes)
      yield* reclaim(state.files, reclamations)
      return seq
    }),
  )
})

const makeCommit = (state: JsonlState) => {
  const lock = Semaphore.makeUnsafe(1)
  return (writes: ReadonlyArray<StorageWrite>) =>
    lock.withPermit(
      usable(state).pipe(
        Effect.andThen(prepareCommit(state.memory, writes)),
        Effect.flatMap((prepared) => persist(state, prepared)),
      ),
    )
}

const guard = <A, E>(state: JsonlState, effect: Effect.Effect<A, E>) =>
  Effect.andThen(usable(state), effect)

const service = (state: JsonlState): Service => {
  const memory = memoryService(state.memory)
  return {
    commit: makeCommit(state),
    mintId: (id) => guard(state, memory.mintId(id)),
    conversation: (id) => guard(state, memory.conversation(id)),
    scanConversations: (query, limit, cursor) =>
      guard(state, memory.scanConversations(query, limit, cursor)),
    entry: (id) => guard(state, memory.entry(id)),
    visibleEntry: (conversationId, id) => guard(state, memory.visibleEntry(conversationId, id)),
    findLatestHeadMarker: (conversationId, atOrBefore) =>
      guard(state, memory.findLatestHeadMarker(conversationId, atOrBefore)),
    scanEntries: (query, limit, cursor) => guard(state, memory.scanEntries(query, limit, cursor)),
    task: (id) => guard(state, memory.task(id)),
    scanTasks: (query, limit, cursor) => guard(state, memory.scanTasks(query, limit, cursor)),
    submission: (id) => guard(state, memory.submission(id)),
    scanSubmissions: (query, limit, cursor) =>
      guard(state, memory.scanSubmissions(query, limit, cursor)),
    submissionByRequest: (conversationId, requestId) =>
      guard(state, memory.submissionByRequest(conversationId, requestId)),
    findDocument: (address, at) => guard(state, memory.findDocument(address, at)),
    document: (id, at) => guard(state, memory.document(id, at)),
    scanDocuments: (query, limit, cursor) =>
      guard(state, memory.scanDocuments(query, limit, cursor)),
  }
}

/** Rebuild memory from the files, discarding what no complete marker published. */
const recover = Effect.fnUntraced(function* (state: JsonlState) {
  const disk = yield* readDisk(state.files)
  const analysis = analyze(disk)
  const confirmed = yield* replay(state.memory, disk, analysis)
  const reclamations = yield* cleanSidecars(state.files, disk, analysis, confirmed)
  yield* reclaim(state.files, reclamations)
  adoptAnalysis(state, analysis)
})

/** Open or create a JSONL storage directory. Closing the scope closes the store. */
export const makeJsonl = Effect.fnUntraced(function* (
  directory: string,
  options: JsonlOptions = {},
): Effect.fn.Return<Service, StorageError, FileSystem.FileSystem | Path.Path | Scope.Scope> {
  const fs = yield* FileSystem.FileSystem
  const path = yield* Path.Path
  const absolute = path.resolve(directory)
  yield* fs
    .makeDirectory(absolute, { recursive: true })
    .pipe(Effect.mapError(io('directory creation')))
  const state: JsonlState = {
    files: { fs, path, directory: absolute, fsync: options.fsync ?? false },
    memory: makeState(),
    currentOnlyDocuments: new Set(),
    liveTaskSidecars: new Set(),
    closed: false,
    poison: undefined,
  }
  yield* recover(state)
  yield* Effect.addFinalizer(() =>
    Effect.sync(() => {
      state.closed = true
    }),
  )
  return service(state)
})
