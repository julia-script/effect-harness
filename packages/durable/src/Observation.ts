import { cursor as journalCursor } from './storage/State.ts'
import * as Cause from 'effect/Cause'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Ref from 'effect/Ref'
import * as Scope from 'effect/Scope'
import * as Stream from 'effect/Stream'
import * as Document from './Document.ts'
import * as Record from './Record.ts'
import { rejected, type StorageError, Corrupt } from './StorageError.ts'
import type { Service as StoreService } from './Store.ts'
import { findDocument, materialize } from './storage/State.ts'

export type End = 'stopped' | 'cancelled' | 'session_closed' | 'retired' | 'listener_error'
export interface Change<T extends object> {
  readonly seq: Record.Seq
  readonly value: Readonly<T> | null
  readonly ops: ReadonlyArray<Record.Op>
  readonly reset: boolean
}
export interface Watch<T extends object> {
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
export const watch = Effect.fnUntraced(function* <T extends object>(
  store: StoreService,
  token: Document.Document<T>,
  target: Document.Target = {},
  migrationCache?: Document.MigrationCache,
): Effect.fn.Return<Watch<T> | undefined, StorageError, Scope.Scope> {
  const logical = yield* Document.address(token, target)
  const baseline = yield* store.committed
  const persisted = findDocument(baseline, logical, 'current')
  if (persisted === undefined) return undefined
  const snapshot = yield* materialize(persisted, 'current')
  if (snapshot === undefined) return undefined
  const initial = yield* Document.typed(token, snapshot, migrationCache)
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
          while ((refresh || pending.length === 0) && !(yield* Ref.get(ended))) {
            refresh = false
            const journal = yield* store.journal(after).pipe(
              Effect.catchIf(
                (error) => error.reason._tag === 'Closed',
                () => stop('session_closed').pipe(Effect.as(undefined)),
              ),
            )
            if (journal === undefined) return undefined
            after = yield* journalCursor(journal.state.nextSeq)
            const relevant = journal.frames.flatMap((frame) =>
              frame.documents
                .filter((publication) => publication.record.id === persisted.record.id)
                .map((publication) => ({ frame, publication })),
            )
            if (pending.length + relevant.length > 100) {
              pending.length = 0
              const current = journal.state.documents.find(
                (item) => item.record.id === persisted.record.id,
              )
              const currentSnapshot =
                current === undefined ? undefined : yield* materialize(current, 'current')
              const replacement =
                currentSnapshot === undefined
                  ? null
                  : (yield* Document.typed(token, currentSnapshot, migrationCache)).value
              const seq = after === 0 ? initial.record.createdAt : after
              pending.push({
                seq,
                value: replacement,
                ops:
                  replacement === null
                    ? []
                    : [['replace', yield* Document.encode(token, replacement)]],
                reset: true,
              })
            } else
              for (const { frame, publication } of relevant) {
                if (publication.value === null) {
                  pending.push({ seq: frame.seq, value: null, ops: [], reset: false })
                  continue
                }
                const frameVersion = publication.version
                if (frameVersion === undefined)
                  return yield* rejected('Committed document publication lacks version', Corrupt)
                const converted = yield* Document.typed(
                  token,
                  {
                    record: publication.record,
                    version: frameVersion,
                    value: publication.value,
                    deltasSinceBase: 0,
                  },
                  migrationCache,
                )
                const reset = frameVersion !== version
                version = converted.version
                pending.push({
                  seq: frame.seq,
                  value: converted.value,
                  ops: reset
                    ? [['replace', yield* Document.encode(token, converted.value)]]
                    : publication.ops,
                  reset,
                })
              }
            if (pending.length === 0) yield* Effect.sleep('20 millis')
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
  return {
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
  }
})

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
          while (pending.length === 0) {
            const journal = yield* store.journal(after)
            pending.push(...journal.frames)
            after = yield* journalCursor(journal.state.nextSeq)
            if (pending.length === 0) yield* Effect.sleep('20 millis')
          }
          const frame = pending.shift()
          if (frame === undefined) return undefined
          return [frame, { after, pending }] as const
        }),
      )
    }),
  )

/** An immediately hydrated, scoped view bound to one durable incarnation. */
export interface State<T extends object> {
  readonly value: Readonly<T> | null
  readonly record: Record.Document
  readonly cursor: number
  readonly closed: Effect.Effect<End>
}
export const state = Effect.fnUntraced(function* <T extends object>(
  store: StoreService,
  token: Document.Document<T>,
  target: Document.Target = {},
  migrationCache?: Document.MigrationCache,
): Effect.fn.Return<State<T> | undefined, StorageError, Scope.Scope> {
  const subscription = yield* watch(store, token, target, migrationCache)
  if (subscription === undefined) return undefined
  const cursor = yield* Ref.make(0)
  yield* subscription
    .listen(() => Ref.update(cursor, (value) => value + 1))
    .pipe(Effect.ignore, Effect.forkScoped)
  return {
    get value() {
      return subscription.value
    },
    record: subscription.record,
    get cursor() {
      return Ref.getUnsafe(cursor)
    },
    closed: subscription.closed,
  }
})
