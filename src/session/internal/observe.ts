import { Effect } from 'effect'
import type { Scope } from 'effect'
import type { JsonObject } from '../../storage/schema/Json.ts'
import type { SessionClosed, SessionFailed, SnapshotError } from '../SessionError.ts'
import type { Cache, Loaded } from './cache.ts'
import { checkRecordScope, checkRecordVersion, resolveAddress } from './documents.ts'
import type { AnyToken } from './documents.ts'
import type { Attached, Observers } from './observers.ts'
import type { OnLine } from './snapshots.ts'

/** Build an observation of a committed value of a definition version; `unpin` runs once it retires. */
export type Create<O> = (
  value: Readonly<JsonObject>,
  version: number,
  unpin: Effect.Effect<void>,
) => Effect.Effect<Attached<O>>

/** Attach an observation to a loaded incarnation until its scope closes or it retires. */
const attach = <O>(
  cache: Cache,
  observers: Observers,
  loaded: Loaded,
  create: Create<O>,
): Effect.Effect<O, never, Scope.Scope> =>
  Effect.gen(function* () {
    const value = yield* loaded.tracker.value
    const recordId = loaded.record.id
    const acquire = Effect.gen(function* () {
      // Retirement only unpins: the observer stays attached, so that a close still ends it.
      const unpin = yield* cache.pin(loaded.addressId, recordId)
      const attached = yield* create(value, loaded.valueVersion, unpin)
      yield* observers.attach(recordId, attached.observer)
      return { attached, unpin }
    })
    const { attached } = yield* Effect.acquireRelease(acquire, (held) =>
      Effect.all([
        observers.detach(recordId, held.attached.observer),
        held.unpin,
        held.attached.close,
      ]),
    )
    return attached.observation
  })

/**
 * Attach an observation to the current incarnation at a token's address, on the mutation line,
 * so that no commit lands between its baseline and its attachment. The incarnation stays pinned
 * in the cache until the observation's scope closes or the incarnation retires.
 */
export const observe = <O>(
  cache: Cache,
  observers: Observers,
  onLine: OnLine,
  usable: Effect.Effect<void, SessionClosed | SessionFailed>,
  token: AnyToken,
  args: ReadonlyArray<unknown>,
  create: Create<O>,
): Effect.Effect<O | undefined, SnapshotError, Scope.Scope> =>
  onLine(
    Effect.gen(function* (): Effect.fn.Return<O | undefined, SnapshotError, Scope.Scope> {
      const definition = token.definition
      const { address, id } = yield* resolveAddress(definition, args)
      const loaded = yield* cache.load(definition, id, address)
      if (loaded === undefined) return undefined
      yield* checkRecordScope(definition, loaded.record)
      yield* checkRecordVersion(definition, loaded.record, loaded.storedVersion)
      // The load may have waited on Storage while the Session started closing.
      yield* usable
      return yield* attach(cache, observers, loaded, create)
    }),
  )
