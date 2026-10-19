import { Effect, Schema } from 'effect'
import { EntryId } from '../../storage/schema/Ids.ts'
import type { ConversationId } from '../../storage/schema/Ids.ts'
import type { DocumentAddress } from '../../storage/schema/Document.ts'
import type { JsonObject } from '../../storage/schema/Json.ts'
import type { SessionClosed, SessionFailed, SnapshotError } from '../SessionError.ts'
import type { Cache, Loaded } from './cache.ts'
import {
  checkRecordScope,
  checkRecordVersion,
  materialize,
  refused,
  resolveAddress,
} from './documents.ts'
import type { AnyToken } from './documents.ts'
import type { Host } from './transaction/state.ts'

/**
 * Committed document reads, ported from pi's `session/session.ts`. A current read is served from
 * the cache when it holds the definition's version, and loaded on the mutation line otherwise; an
 * as-of read always reads Storage on the line.
 */

/** Run a read on the mutation line, once the Session is still open and healthy. */
export type OnLine = <A, E, R>(
  effect: Effect.Effect<A, E, R>,
) => Effect.Effect<A, E | SessionClosed | SessionFailed, R>

const isEntryId = Schema.is(EntryId)

export const snapshot = Effect.fnUntraced(function* (
  cache: Cache,
  usable: Effect.Effect<void, SessionClosed | SessionFailed>,
  onLine: OnLine,
  token: AnyToken,
  args: ReadonlyArray<unknown>,
): Effect.fn.Return<JsonObject | undefined, SnapshotError> {
  // A closed or failed Session refuses reads, cached ones too.
  yield* usable
  const definition = token.definition
  const { address, id } = yield* resolveAddress(definition, args)
  const hit = yield* cache.cached(id)
  const loaded =
    hit?.valueVersion === definition.version
      ? hit
      : yield* onLine(cache.load(definition, id, address))
  return yield* valueOf(definition, loaded)
})

/** The committed value of a loaded incarnation, once its scope and version check out. */
const valueOf = (definition: AnyToken['definition'], loaded: Loaded | undefined) =>
  loaded === undefined
    ? Effect.undefined
    : Effect.andThen(
        Effect.andThen(
          checkRecordScope(definition, loaded.record),
          checkRecordVersion(definition, loaded.record, loaded.storedVersion),
        ),
        loaded.tracker.value,
      )

/** A committed read by a caller that holds the line already, such as a transaction. */
export const snapshotOnLine = (cache: Cache, token: AnyToken, args: ReadonlyArray<unknown>) =>
  Effect.flatMap(resolveAddress(token.definition, args), ({ address, id }) =>
    Effect.flatMap(cache.load(token.definition, id, address), (loaded) =>
      valueOf(token.definition, loaded),
    ),
  )

/** Read the incarnation alive at an entry's commit, in the conversation that owns the entry. */
const readAsOf = Effect.fnUntraced(function* (
  host: Host,
  token: AnyToken,
  address: DocumentAddress,
  conversationId: ConversationId,
  at: EntryId,
): Effect.fn.Return<JsonObject | undefined, SnapshotError> {
  const { guard, storage } = host
  const visible = yield* guard(storage.visibleEntry(conversationId, at))
  if (visible === undefined) {
    return yield* refused(`Entry ${at} is not visible from conversation ${conversationId}`)
  }
  const scope = { kind: 'conversation' as const, conversationId: visible.entry.conversationId }
  const record = yield* guard(storage.findDocument({ ...address, scope }, visible.commitSeq))
  if (record === undefined) return undefined
  const stored = yield* guard(storage.document(record.id, visible.commitSeq))
  if (stored === undefined) {
    return yield* refused(`Historical document ${record.id} (${record.kind}) cannot be read`)
  }
  return yield* materialize(token.definition, stored)
})

export const snapshotAsOf = (
  host: Host,
  onLine: OnLine,
  token: AnyToken,
  args: ReadonlyArray<unknown>,
): Effect.Effect<JsonObject | undefined, SnapshotError> =>
  Effect.flatMap(resolveAddress(token.definition, args), ({ address, next }) => {
    const scope = address.scope
    if (scope.kind !== 'conversation') {
      return refused('Session.snapshotAsOf() requires a conversation document')
    }
    const at = args[next]
    if (!isEntryId(at)) return refused('Session.snapshotAsOf() requires an entry ID')
    return onLine(readAsOf(host, token, address, scope.conversationId, at))
  })
