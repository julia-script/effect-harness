/**
 * Domain snapshots stored through Effect persistence services.
 */
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import type * as Scope from 'effect/Scope'
import * as KeyValueStore from 'effect/persistence/KeyValueStore'
import * as EventJournal from 'effect/eventlog/EventJournal'
import * as EventLogMessage from 'effect/eventlog/EventLogMessage'
import * as Record from '../Record.ts'
import { Corrupt, Invalid, Io, rejected, uncertain, type StorageError } from '../StorageError.ts'
import { Store } from '../Store.ts'
import * as backend from './internal/backend.ts'
import { validateState } from './internal/state.ts'

/**
 * Versioned snapshot containing state, receipts and retained observer frames.
 *
 * @category schemas
 */
export const Snapshot = Schema.Struct({
  version: Schema.Literal(1),
  state: Record.State,
  frames: Schema.Array(Record.Frame),
})

/**
 * Snapshot namespace for one domain Session.
 *
 * **Details**
 *
 * key defaults to @effect-harness/durable/session and also identifies journal coordination.
 *
 * **Gotchas**
 *
 * Give independent sessions distinct keys in a shared backend. Equal keys address the same
 * domain state.
 *
 * @category models
 */
export interface Options {
  readonly key?: string | undefined
}

/**
 * Acquires domain storage using native key/value persistence and journal coordination.
 *
 * **When to use**
 *
 * Use when the application supplies Effect persistence Layers and wants domain storage
 * independent of a database driver.
 *
 * **Details**
 *
 * State, receipts and retained frames are stored together under one versioned key. The
 * default key is @effect-harness/durable/session. The journal coordinates initialization and
 * each update; saved snapshots supply coherent observer reads.
 *
 * **Gotchas**
 *
 * All writers of a key need compatible shared coordination. For SQLite, build KeyValueStore
 * and SqlEventJournal from the same native client; for memory, share both service instances.
 * Do not wrap Store operations in an external database transaction. Uncertain
 * write/coordination outcomes poison the Store. Other backend combinations need their own
 * serialization verification; retired bespoke SQLite tables are not imported.
 *
 * @category constructors
 */
export const make = Effect.fnUntraced(function* (
  options: Options = {},
): Effect.fn.Return<
  Store['Service'],
  StorageError,
  Scope.Scope | EventJournal.EventJournal | KeyValueStore.KeyValueStore
> {
  const context = yield* Effect.context<never>()
  const journal = yield* EventJournal.EventJournal
  const values = KeyValueStore.toSchemaStore(yield* KeyValueStore.KeyValueStore, Snapshot)
  const key = options.key ?? '@effect-harness/durable/session'
  const withLock = journal.withLock(EventLogMessage.StoreId.make(key))
  // Capture the callback Exit so native coordinator defects can be translated
  // without swallowing defects or interruption from domain code. A candidate
  // is published only by the final single-value save, after validation succeeds.
  const atomic = <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, E | StorageError, R> =>
    withLock(Effect.exit(effect)).pipe(
      Effect.catchDefect((cause) =>
        Effect.fail(uncertain('Snapshot coordination settlement is uncertain', cause)),
      ),
      Effect.flatMap((exit) => exit),
    )
  const read = values
    .get(key)
    .pipe(
      Effect.mapError((cause) =>
        Schema.isSchemaError(cause)
          ? rejected('Invalid persisted domain snapshot', Corrupt, cause)
          : rejected('Cannot read domain snapshot', Io, cause),
      ),
    )
  const save = Effect.fnUntraced(
    function* (snapshot: backend.Snapshot) {
      yield* values.set(key, { version: 1, ...snapshot })
    },
    Effect.mapError((cause) =>
      Schema.isSchemaError(cause)
        ? rejected('Cannot encode domain snapshot', Invalid, cause)
        : uncertain('Domain snapshot write outcome is uncertain', cause),
    ),
  )
  // Initialization and every read/change/write use the same native coordination.
  yield* atomic(
    Effect.gen(function* () {
      if (Option.isNone(yield* read)) yield* save({ state: Record.emptyState(), frames: [] })
    }),
  )

  const load = Effect.gen(function* () {
    const value = yield* read
    if (Option.isNone(value)) return yield* rejected('Domain snapshot is missing', Corrupt)
    const state = yield* validateState(value.value.state)
    let previous = 0
    for (const frame of value.value.frames) {
      if (frame.seq <= previous || frame.seq >= state.nextSeq)
        return yield* rejected('Domain snapshot journal sequence is corrupt', Corrupt)
      previous = frame.seq
    }
    return { state, frames: value.value.frames }
  })
  yield* load
  // A single saved value is coherent. Reading in the construction context
  // also keeps child fibers from inheriting a coordinator's temporary services.
  const committed = load.pipe(Effect.setContext(context))
  return yield* backend.make({
    load,
    committed,
    save,
    atomic,
  })
})

/**
 * Provides scoped snapshot storage under a configured key.
 *
 * **Details**
 *
 * Consumes application-supplied KeyValueStore and EventJournal services. Reopening an
 * existing key loads and validates saved state and receipts.
 *
 * @see {@link make} for coordination and failure constraints.
 * @category layers
 */
export const layerWith = (
  options: Options = {},
): Layer.Layer<Store, StorageError, EventJournal.EventJournal | KeyValueStore.KeyValueStore> =>
  Layer.effect(Store, make(options))

/**
 * Provides snapshot storage with the default session key.
 *
 * **Gotchas**
 *
 * Use layerWith with a distinct key when multiple independent Sessions share the persistence
 * backend.
 *
 * @see {@link layerWith} for namespace configuration.
 * @category layers
 */
export const layer = layerWith()
