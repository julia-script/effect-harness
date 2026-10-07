/**
 * Domain snapshots stored through Effect persistence services.
 *
 * @since 0.0.0
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
 * @since 0.0.0
 */
export const Snapshot = Schema.Struct({
  version: Schema.Literal(1),
  state: Record.State,
  frames: Schema.Array(Record.Frame),
})

/**
 * Namespace for one domain session in the application's key/value table.
 *
 * @category models
 * @since 0.0.0
 */
export interface Options {
  readonly key?: string | undefined
}

/**
 * Creates a Store from a KeyValueStore and EventJournal coordination.
 *
 * **Details**
 *
 * State, receipts and retained frames are one schema-encoded value. All writers
 * for a snapshot key must share a coordinating journal and key/value backend.
 * For SQLite, provide native SqlEventJournal and KeyValueStore layers built from
 * the same client. For memory, share one EventJournal.layerMemory instance.
 * The Store owns its commits; do not wrap its operations in external database
 * transactions. Observer reads use the construction context and saved state.
 * This format does not import the retired harness SQLite tables.
 *
 * @category constructors
 * @since 0.0.0
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
 * Provides snapshot storage from application-supplied Effect persistence services.
 *
 * @category layers
 * @since 0.0.0
 */
export const layerWith = (
  options: Options = {},
): Layer.Layer<Store, StorageError, EventJournal.EventJournal | KeyValueStore.KeyValueStore> =>
  Layer.effect(Store, make(options))

/**
 * Snapshot storage with the default session key.
 *
 * @category layers
 * @since 0.0.0
 */
export const layer = layerWith()
