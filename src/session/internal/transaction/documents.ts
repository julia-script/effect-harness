import { Deferred, Effect } from 'effect'
import type { Schema } from 'effect'
import { copyJson, Invalid } from '../../../delta/internal/json.ts'
import * as Tracker from '../../../delta/Tracker.ts'
import { DocumentId } from '../../../storage/schema/Ids.ts'
import type { JsonObject } from '../../../storage/schema/Json.ts'
import { DocumentError } from '../../SessionError.ts'
import {
  checkRecordScope,
  checkRecordVersion,
  documentCreate,
  initialValue,
  materializeValue,
  refused,
  resolveAddress,
} from '../documents.ts'
import type { AnyDefinition, AnyToken } from '../documents.ts'
import { assertOpen, currentTask, rejected, requireConversation, track } from './state.ts'
import type { DocumentEntry, DocumentFailure, Target, TxState } from './state.ts'

/**
 * Documents of one transaction, ported from pi's `session/transaction.ts`.
 *
 * Each acquisition opens a change on a tracker: the cached one of a committed incarnation, or a
 * new one for an incarnation this transaction creates or copies from a fork source. Acquisition is
 * memoized per incarnation, so concurrent calls share one draft. Retirement marks the latest
 * incarnation at the address; acquiring the address again creates a new incarnation.
 */

type Draft = Tracker.Draft<JsonObject>

const begin = (entry: DocumentEntry, tracker: Tracker.Tracker<JsonObject>) =>
  Effect.map(tracker.beginChange, (change) => {
    entry.change = change
    return change.state
  })

/** A new incarnation needs a live owner: an existing conversation, or a task not yet terminal. */
const requireOwner = (state: TxState, entry: DocumentEntry) => {
  const scope = entry.address.scope
  if (scope.kind === 'conversation') return requireConversation(state, scope.conversationId)
  if (scope.kind !== 'task') return Effect.void
  return Effect.flatMap(currentTask(state, scope.taskId), (task) => {
    if (task === undefined) return rejected(`Task ${scope.taskId} does not exist`)
    return task.state.status === 'terminal'
      ? rejected(`Task ${scope.taskId} is terminal`)
      : Effect.void
  })
}

const acquire = Effect.fnUntraced(function* (
  state: TxState,
  entry: DocumentEntry,
  definition: AnyDefinition,
  seed: Schema.Json | undefined,
  skipLoad: boolean,
) {
  const { cache, guard, storage } = state.host
  const loaded = skipLoad
    ? undefined
    : yield* cache.load(definition, entry.addressId, entry.address)
  yield* assertOpen(state)
  if (loaded !== undefined) {
    yield* checkRecordScope(definition, loaded.record)
    yield* checkRecordVersion(definition, loaded.record, loaded.storedVersion)
    entry.target = { kind: 'loaded', document: loaded }
    return yield* begin(entry, loaded.tracker)
  }
  yield* requireOwner(state, entry)
  yield* assertOpen(state)
  const value = yield* initialValue(definition, seed)
  const id = yield* guard(storage.mintId(DocumentId))
  yield* assertOpen(state)
  const tracker = yield* Tracker.make(value)
  const record = documentCreate(definition, entry.address, id)
  entry.target = { kind: 'created', record, version: definition.version, tracker }
  return yield* begin(entry, tracker)
})

/** Acquire a fork copy: its source value becomes the creation base of the copied incarnation. */
const acquireForkCopy = Effect.fnUntraced(function* (
  state: TxState,
  entry: DocumentEntry,
  definition: AnyDefinition,
  target: Extract<Target, { readonly kind: 'fork-copy' }>,
) {
  const { guard, storage } = state.host
  const { record, source } = target
  const stored = yield* guard(storage.document(source.id, source.at))
  yield* assertOpen(state)
  if (stored === undefined) {
    return yield* refused(`Fork source document ${source.id} cannot be read`)
  }
  const from = stored.record
  if (
    !('history' in from) ||
    from.kind !== record.kind ||
    from.key !== record.key ||
    from.history !== record.history ||
    from.fork !== record.fork
  ) {
    return yield* refused(`Fork source document ${source.id} does not match the copied record`)
  }
  const value = yield* materializeValue(definition, record, stored.version, stored.value)
  const tracker = yield* Tracker.make(value)
  entry.definition = definition
  entry.target = { kind: 'created', record, version: definition.version, tracker }
  return yield* begin(entry, tracker)
})

/** Run an entry's acquisition once; later calls await the same draft. */
const start = (
  state: TxState,
  entry: DocumentEntry,
  acquisition: Effect.Effect<Draft, DocumentFailure>,
): Effect.Effect<Draft, DocumentFailure> => {
  const deferred = Deferred.makeUnsafe<Draft, DocumentFailure>()
  entry.acquisition = deferred
  return track(
    state,
    Effect.onExit(acquisition, (exit) => Deferred.done(deferred, exit)),
  )
}

/** Detach a family seed now, so later changes by the caller do not reach the initializer. */
const ownedSeed = (definition: AnyDefinition, seed: unknown) => {
  if (definition.family !== true) return Effect.undefined
  const copied = copyJson(seed)
  return copied instanceof Invalid
    ? refused(`Document family ${definition.kind} seed: ${copied.message}`)
    : Effect.succeed(copied)
}

const terminalTask = (state: TxState, entry: Pick<DocumentEntry, 'address'>) => {
  const scope = entry.address.scope
  return scope.kind === 'task' &&
    state.tasks.get(scope.taskId)?.write?.record.state.status === 'terminal'
    ? rejected(`Task ${scope.taskId} is terminal`)
    : Effect.void
}

export const doc = Effect.fnUntraced(function* (
  state: TxState,
  token: AnyToken,
  args: ReadonlyArray<unknown>,
): Effect.fn.Return<Draft, DocumentFailure> {
  yield* assertOpen(state)
  const definition = token.definition
  const { address, id, next } = yield* resolveAddress(definition, args)
  yield* terminalTask(state, { address })
  const latest = state.latestDocuments.get(id)
  if (latest !== undefined && !latest.retireOnCommit) {
    if (latest.acquisition !== undefined) return yield* Deferred.await(latest.acquisition)
    if (latest.target?.kind === 'fork-copy') {
      return yield* start(state, latest, acquireForkCopy(state, latest, definition, latest.target))
    }
  }
  const seed = yield* ownedSeed(definition, args[next])
  const entry: DocumentEntry = { addressId: id, address, definition, retireOnCommit: false }
  state.documents.push(entry)
  state.latestDocuments.set(id, entry)
  // Decided now, so a pending acquisition of a retired incarnation and its successor stay apart.
  const skipLoad = latest?.retireOnCommit === true
  return yield* start(state, entry, acquire(state, entry, definition, seed, skipLoad))
})

/** Find the current incarnation a retirement without acquisition retires. */
const findRetirement = Effect.fnUntraced(function* (
  state: TxState,
  entry: DocumentEntry,
  definition: AnyDefinition,
) {
  const { cache, guard, storage } = state.host
  const record =
    (yield* cache.cached(entry.addressId))?.record ??
    (yield* guard(storage.findDocument(entry.address, 'current')))
  yield* assertOpen(state)
  if (record === undefined) return
  yield* checkRecordScope(definition, record)
  entry.target = { kind: 'retire-only', record }
})

export const retireDoc = Effect.fnUntraced(function* (
  state: TxState,
  token: AnyToken,
  args: ReadonlyArray<unknown>,
): Effect.fn.Return<void, DocumentFailure> {
  yield* assertOpen(state)
  const definition = token.definition
  const { address, id } = yield* resolveAddress(definition, args)
  const latest = state.latestDocuments.get(id)
  if (latest?.retireOnCommit === true) return
  if (latest?.target?.kind === 'fork-copy') {
    yield* checkRecordScope(definition, latest.target.record)
    latest.retireOnCommit = true
    return
  }
  if (latest?.acquisition !== undefined) {
    // An acquired draft persists its final content before it retires.
    latest.retireOnCommit = true
    yield* track(state, Deferred.await(latest.acquisition))
    return
  }
  const entry: DocumentEntry = { addressId: id, address, definition, retireOnCommit: true }
  state.documents.push(entry)
  state.latestDocuments.set(id, entry)
  yield* track(state, findRetirement(state, entry, definition))
})

/** Prepare every open change; this settles every draft. */
export const prepareChanges = (state: TxState) =>
  Effect.forEach(
    state.documents,
    (entry) => {
      const change = entry.change
      if (change === undefined) return Effect.void
      return change.prepare.pipe(
        Effect.mapError((error) => new DocumentError({ message: error.message })),
        Effect.flatMap((prepared) =>
          Effect.sync(() => {
            entry.prepared = prepared
          }),
        ),
      )
    },
    { discard: true },
  )

/** Abort every change, so none of the transaction's preparations can be adopted. */
export const abortChanges = (state: TxState) =>
  Effect.forEach(state.documents, (entry) => entry.change?.abort ?? Effect.void, {
    discard: true,
  })
