/**
 * Scoped journal observers and consumed-value watch handles.
 */
import * as handle from './internal/handle.ts'
const WatchProto = handle.prototype('@effect-harness/durable/Observation/Watch')
const StateProto = handle.prototype('@effect-harness/durable/Observation/State')
import type * as Pipeable from 'effect/Pipeable'
import type * as Inspectable from 'effect/Inspectable'
import { identity } from 'effect/Function'
import type * as Types from 'effect/Types'
import * as Predicate from 'effect/Predicate'
import * as Option from 'effect/Option'
import * as Arr from 'effect/Array'
import { cursor as journalCursor } from './storage/internal/state.ts'
import * as Cause from 'effect/Cause'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Ref from 'effect/Ref'
import type * as Scope from 'effect/Scope'
import * as Stream from 'effect/Stream'
import * as Document from './Document.ts'
import type * as Record from './Record.ts'
import { rejected, type StorageError, Corrupt } from './StorageError.ts'
import type { Service as StoreService } from './Store.ts'
import { findDocument, materialize } from './storage/internal/state.ts'

/**
 * Reason a scoped document or conversation observation ended.
 *
 * @category models
 */
export type End = 'stopped' | 'cancelled' | 'session_closed' | 'retired' | 'listener_error'
const ChangeTypeId = '~@effect-harness/durable/Observation/Change'
/**
 * Committed document value, mutation operations, sequence and reset flag.
 *
 * @category models
 */
export interface Change<out T extends object> {
  readonly [ChangeTypeId]: { readonly _T: Types.Covariant<T> }
  readonly seq: Record.Seq
  readonly value: Readonly<T> | null
  readonly ops: ReadonlyArray<Record.Op>
  readonly reset: boolean
}
const WatchTypeId = '~@effect-harness/durable/Observation/Watch'
/**
 * Scoped initial document value and stream of committed changes.
 *
 * @category models
 */
export interface Watch<out T extends object> extends Pipeable.Pipeable, Inspectable.Inspectable {
  readonly [WatchTypeId]: { readonly _T: Types.Covariant<T> }
  readonly value: Readonly<T> | null
  readonly record: Record.Document
  readonly changes: Stream.Stream<Change<T>, StorageError>
  readonly closed: Effect.Effect<End>
  /** effect-review-allow P3-scope-in-r-not-dispose-method: semantic subscription completion stops future deliveries and resolves closed as stopped; resource release remains owned by Scope. */
  readonly stop: Effect.Effect<void>
  readonly listen: <E, R>(
    listener: (change: Change<T>) => Effect.Effect<void, E, R>,
  ) => Effect.Effect<void, StorageError | E, R>
}
/**
 * Creates a scoped document watch from a detached baseline.
 *
 * @category combinators
 */
export const watch = Effect.fnUntraced(function* <T extends object>(
  store: StoreService,
  token: Document.Document<T>,
  target: Document.Target = {},
  migrationCache?: Document.MigrationCache,
): Effect.fn.Return<Option.Option<Watch<T>>, StorageError, Scope.Scope> {
  const logical = yield* Document.address(token, target)
  const baseline = yield* store.committed
  const found = findDocument(baseline, logical, 'current')
  if (Option.isNone(found)) return Option.none()
  const persisted = found.value
  const snapshot = yield* materialize(persisted, 'current')
  if (Option.isNone(snapshot)) return Option.none()
  const initial = yield* Document.typed(token, snapshot.value, migrationCache)
  const terminal = yield* Deferred.make<End>()
  const ended = yield* Ref.make(false)
  const started = yield* Ref.make(false)
  const value = yield* Ref.make<Readonly<T> | null>(initial.value)
  let version = initial.version
  const stop = Effect.fnUntraced(function* (reason: End) {
    if (yield* Ref.getAndSet(ended, true)) return
    yield* Deferred.succeed(terminal, reason)
  })
  yield* Effect.addFinalizer(() => stop('cancelled'))
  yield* Effect.forkScoped(
    Effect.gen(function* () {
      let after = yield* journalCursor(baseline.nextSeq)
      while (!(yield* Ref.get(ended))) {
        const journal = yield* store.journal(after)
        if (journal.state.nextSeq - 1 > after) {
          after = yield* journalCursor(journal.state.nextSeq)
        }
        yield* Effect.sleep('20 millis')
      }
    }).pipe(
      Effect.catch((error) =>
        stop(error.reason._tag === 'Closed' ? 'session_closed' : 'listener_error'),
      ),
    ),
  )
  interface Cursor {
    readonly after: Record.Seq | 0
    readonly pending: ReadonlyArray<Change<T>>
    readonly retire: boolean
  }
  const stream = Stream.unwrap(
    Effect.gen(function* () {
      if (yield* Ref.getAndSet(started, true))
        return yield* rejected('Watch may only be consumed once')
      if (yield* Ref.get(ended)) return yield* rejected('Watch is stopped')
      return Stream.unfold<Cursor, Change<T>, StorageError, never>(
        { after: yield* journalCursor(baseline.nextSeq), pending: [], retire: false },
        Effect.fnUntraced(function* (cursor) {
          if (yield* Ref.get(ended)) return undefined
          if (cursor.retire) {
            yield* stop('retired')
            return undefined
          }
          let after = cursor.after
          const pending = [...cursor.pending]
          let refresh = true
          while ((refresh || Arr.isArrayEmpty(pending)) && !(yield* Ref.get(ended))) {
            refresh = false
            const journal = yield* store.journal(after).pipe(
              Effect.catchIf(
                (error) => error.reason._tag === 'Closed',
                () => stop('session_closed').pipe(Effect.as(undefined)),
              ),
            )
            if (journal === undefined) return undefined
            after = yield* journalCursor(journal.state.nextSeq)
            const relevant = Arr.flatMap(journal.frames, (frame) =>
              Arr.filter(
                frame.documents,
                (publication) => publication.record.id === persisted.record.id,
              ).map((publication) => ({ frame, publication })),
            )
            if (pending.length + relevant.length > 100) {
              pending.length = 0
              const current = Arr.findFirst(
                journal.state.documents,
                (item) => item.record.id === persisted.record.id,
              )
              const currentSnapshot = yield* Option.match(current, {
                onNone: () => Effect.succeed(Option.none<Document.Snapshot<Record.JsonObject>>()),
                onSome: (document) => materialize(document, 'current'),
              })
              const replacement = yield* Option.match(currentSnapshot, {
                onNone: () => Effect.succeed(null),
                onSome: (snapshot) =>
                  Document.typed(token, snapshot, migrationCache).pipe(
                    Effect.map((typed) => typed.value),
                  ),
              })
              const seq = after === 0 ? initial.record.createdAt : after
              pending.push(
                makeChange({
                  seq,
                  value: replacement,
                  ops:
                    replacement === null
                      ? []
                      : [['replace', yield* Document.encode(token, replacement)]],
                  reset: true,
                }),
              )
            } else
              for (const { frame, publication } of relevant) {
                if (publication.value === null) {
                  pending.push(makeChange({ seq: frame.seq, value: null, ops: [], reset: false }))
                  continue
                }
                const frameVersion = publication.version
                if (frameVersion === undefined)
                  return yield* rejected('Committed document publication lacks version', Corrupt)
                const converted = yield* Document.typed(
                  token,
                  Document.makeSnapshot({
                    record: publication.record,
                    version: frameVersion,
                    value: publication.value,
                    deltasSinceBase: 0,
                  }),
                  migrationCache,
                )
                const reset = frameVersion !== version
                version = converted.version
                pending.push(
                  makeChange({
                    seq: frame.seq,
                    value: converted.value,
                    ops: reset
                      ? [['replace', yield* Document.encode(token, converted.value)]]
                      : publication.ops,
                    reset,
                  }),
                )
              }
            if (Arr.isArrayEmpty(pending)) yield* Effect.sleep('20 millis')
          }
          if (yield* Ref.get(ended)) return undefined
          const next = pending.shift()
          if (next === undefined) return undefined
          yield* Ref.set(value, next.value)
          if (next.value === null) yield* stop('retired')
          return [next, { after, pending, retire: next.value === null }] as const
        }),
      )
    }),
  )
  return Option.some(
    makeWatch({
      get value() {
        return Ref.getUnsafe(value)
      },
      record: initial.record,
      changes: stream,
      closed: Deferred.await(terminal),
      stop: stop('stopped'),
      listen: (listener) =>
        Effect.yieldNow.pipe(
          Effect.andThen(Stream.runForEach(stream, listener)),
          Effect.catchCause((cause) =>
            stop(Cause.hasInterruptsOnly(cause) ? 'cancelled' : 'listener_error').pipe(
              Effect.andThen(Effect.failCause(cause)),
            ),
          ),
          Effect.ensuring(stop('cancelled')),
        ),
    }),
  )
})

/**
 * Streams committed journal frames with bounded overflow resets.
 *
 * @category combinators
 */
export const commits = (store: StoreService): Stream.Stream<Record.Frame, StorageError> =>
  Stream.unwrap(
    Effect.gen(function* () {
      const state = yield* store.committed
      return Stream.unfold<
        { after: Record.Seq | 0; pending: ReadonlyArray<Record.Frame> },
        Record.Frame,
        StorageError,
        never
      >(
        { after: yield* journalCursor(state.nextSeq), pending: [] },
        Effect.fnUntraced(function* (cursor) {
          const pending = [...cursor.pending]
          let after = cursor.after
          while (Arr.isReadonlyArrayEmpty(pending)) {
            const journal = yield* store.journal(after)
            pending.push(...journal.frames)
            after = yield* journalCursor(journal.state.nextSeq)
            if (Arr.isReadonlyArrayEmpty(pending)) yield* Effect.sleep('20 millis')
          }
          const frame = pending.shift()
          if (frame === undefined) return undefined
          return [frame, { after, pending }] as const
        }),
      )
    }),
  )

/** An immediately hydrated, scoped view bound to one durable incarnation. */
const StateTypeId = '~@effect-harness/durable/Observation/State'
/**
 * Live document value and cursor maintained by a scoped subscription.
 *
 * @category models
 */
export interface State<out T extends object> extends Pipeable.Pipeable, Inspectable.Inspectable {
  readonly [StateTypeId]: { readonly _T: Types.Covariant<T> }
  readonly value: Readonly<T> | null
  readonly record: Record.Document
  readonly cursor: number
  readonly closed: Effect.Effect<End>
}
/**
 * Creates a scoped mutable state view of consumed document changes.
 *
 * @category combinators
 */
export const state = Effect.fnUntraced(function* <T extends object>(
  store: StoreService,
  token: Document.Document<T>,
  target: Document.Target = {},
  migrationCache?: Document.MigrationCache,
): Effect.fn.Return<Option.Option<State<T>>, StorageError, Scope.Scope> {
  const found = yield* watch(store, token, target, migrationCache)
  if (Option.isNone(found)) return Option.none()
  const subscription = found.value
  const cursor = yield* Ref.make(0)
  yield* subscription
    .listen(() => Ref.update(cursor, (value) => value + 1))
    .pipe(Effect.ignore, Effect.forkScoped)
  return Option.some(
    makeState({
      get value() {
        return subscription.value
      },
      record: subscription.record,
      get cursor() {
        return Ref.getUnsafe(cursor)
      },
      closed: subscription.closed,
    }),
  )
})

/**
 * Creates a document change carrier without changing its input.
 *
 * @category constructors
 */
export const makeChange = <T extends object>(
  input: Omit<Change<T>, typeof ChangeTypeId>,
): Change<T> => {
  const value = Object.assign({}, input, { [ChangeTypeId]: { _T: identity } })
  Object.defineProperties(value, Object.getOwnPropertyDescriptors(input))
  Object.defineProperty(value, ChangeTypeId, { enumerable: false })
  return value
}
/**
 * Checks whether a value carries the nominal `Change` marker.
 *
 * **Gotchas**
 *
 * This checks library identity, not the validity of arbitrary fields or stored JSON.
 *
 * @category guards
 */
export const isChange = (input: unknown): input is Change<object> =>
  Predicate.hasProperty(input, ChangeTypeId)

/**
 * Creates a watch handle with live getters and shared inspection.
 *
 * @category constructors
 */
export const makeWatch = <T extends object>(
  input: handle.Input<Watch<T>, typeof WatchTypeId>,
): Watch<T> => {
  const value = handle.make(WatchProto, handle.marked(input, WatchTypeId, { _T: identity }))
  return value
}

/**
 * Checks whether a value carries the nominal `Watch` marker.
 *
 * **Gotchas**
 *
 * This checks library identity, not the validity of arbitrary fields or stored JSON.
 *
 * @category guards
 */
export const isWatch = (input: unknown): input is Watch<object> =>
  Predicate.hasProperty(input, WatchTypeId)

/**
 * Creates a state handle with live getters and shared inspection.
 *
 * @category constructors
 */
export const makeState = <T extends object>(
  input: handle.Input<State<T>, typeof StateTypeId>,
): State<T> => {
  const value = handle.make(StateProto, handle.marked(input, StateTypeId, { _T: identity }))
  return value
}

/**
 * Checks whether a value carries the nominal `State` marker.
 *
 * **Gotchas**
 *
 * This checks library identity, not the validity of arbitrary fields or stored JSON.
 *
 * @category guards
 */
export const isState = (input: unknown): input is State<object> =>
  Predicate.hasProperty(input, StateTypeId)
