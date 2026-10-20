import { Effect, Queue, Stream } from 'effect'
import type { Cause } from 'effect'
import type { Op } from '../../storage/schema/Document.ts'
import type { DocumentId } from '../../storage/schema/Ids.ts'
import type { JsonObject } from '../../storage/schema/Json.ts'
import type { CommitChange } from '../Document.ts'
import type { Change, State, Watch } from '../Observation.ts'
import type { SessionFailed } from '../SessionError.ts'

/**
 * Observers of committed document incarnations, ported from pi's `session/observation.ts`.
 *
 * Every adopted commit forwards its document changes to the observers of each incarnation, on the
 * mutation line, so an observer attached on the line misses no commit after its baseline. When
 * the Session closes or fails, every observer ends.
 */

type Value = Readonly<JsonObject> | null
type DocumentChange = Extract<CommitChange, { readonly type: 'document' }>

/** Changes a watch can fall behind on before they collapse into one root replacement. */
const MAX_PENDING_CHANGES = 100

/** Canonical terminal operations of a retired incarnation. */
const RETIREMENT: ReadonlyArray<Op> = [['r', null]]

export interface Observer {
  /** Forward one committed change of the observed incarnation. */
  readonly advance: (change: DocumentChange) => Effect.Effect<void>
  /** Stop delivery: the Session closed, or failed with `failure`. */
  readonly end: (failure?: SessionFailed) => Effect.Effect<void>
}

export interface Observers {
  readonly attach: (recordId: DocumentId, observer: Observer) => Effect.Effect<void>
  readonly detach: (recordId: DocumentId, observer: Observer) => Effect.Effect<void>
  readonly publish: (changes: ReadonlyArray<CommitChange>) => Effect.Effect<void>
  /** End every observer, and every one attached later. */
  readonly end: (failure?: SessionFailed) => Effect.Effect<void>
}

export const make = (): Observers => {
  const attached = new Map<DocumentId, Set<Observer>>()
  let ended: { readonly failure: SessionFailed | undefined } | undefined
  return {
    attach: (recordId, observer) =>
      Effect.suspend(() => {
        if (ended !== undefined) return observer.end(ended.failure)
        const observers = attached.get(recordId) ?? new Set()
        observers.add(observer)
        attached.set(recordId, observers)
        return Effect.void
      }),
    detach: (recordId, observer) =>
      Effect.sync(() => {
        const observers = attached.get(recordId)
        observers?.delete(observer)
        if (observers?.size === 0) attached.delete(recordId)
      }),
    publish: (changes) =>
      Effect.forEach(changes, (change) =>
        change.type === 'document'
          ? Effect.forEach([...(attached.get(change.record.id) ?? [])], (observer) =>
              observer.advance(change),
            )
          : Effect.void,
      ),
    end: (failure) =>
      Effect.suspend(() => {
        if (ended !== undefined) return Effect.void
        ended = { failure }
        const all: Array<Observer> = []
        for (const observers of attached.values()) all.push(...observers)
        attached.clear()
        return Effect.forEach(all, (observer) => observer.end(failure))
      }),
  }
}

/**
 * Operations an observer applies for one change. An observer of another definition version holds
 * a differently shaped value, so it receives the new value as a root replacement.
 */
const observedOps = (observed: { version: number }, change: DocumentChange): ReadonlyArray<Op> => {
  if (change.value === null) return RETIREMENT
  if (change.version === undefined || change.version === observed.version) return change.ops
  observed.version = change.version
  return [['r', change.value]]
}

/** An observation and the observer that feeds it. */
export interface Attached<O> {
  readonly observation: O
  readonly observer: Observer
  /** Stop the observation when its scope closes. */
  readonly close: Effect.Effect<void>
}

/**
 * A watch of an incarnation whose committed value is `value`, of definition version `version`.
 * `unpin` runs once the incarnation retires.
 */
export const makeWatch = Effect.fnUntraced(function* (
  value: Readonly<JsonObject>,
  version: number,
  unpin: Effect.Effect<void>,
) {
  const queue = yield* Queue.unbounded<Change<JsonObject>, SessionFailed | Cause.Done>()
  const observed = { version }
  let retired = false
  const observer: Observer = {
    advance: (change) =>
      Effect.gen(function* () {
        if (retired) return
        const ops = observedOps(observed, change)
        // A migration-only base changes nothing for an observer of the new version.
        if (ops.length === 0) return
        const next: Value = change.value
        if (next === null) retired = true
        if (Queue.sizeUnsafe(queue) >= MAX_PENDING_CHANGES) {
          yield* Effect.ignore(Queue.clear(queue))
          yield* Queue.offer(queue, { value: next, ops: [['r', next]] })
        } else {
          yield* Queue.offer(queue, { value: next, ops })
        }
        if (retired) yield* unpin
      }),
    // Pending changes are dropped: a closed or failed Session delivers nothing more.
    end: (failure) =>
      Effect.andThen(
        Effect.ignore(Queue.clear(queue)),
        failure === undefined ? Queue.end(queue) : Queue.fail(queue, failure),
      ),
  }
  // Ends after retirement, which stays pending like any change until it is pulled.
  const changes = Stream.takeUntil(Stream.fromQueue(queue), (change) => change.value === null)
  const watch: Watch<JsonObject> = { value, changes }
  const attached: Attached<Watch<JsonObject>> = {
    observation: watch,
    observer,
    close: Effect.asVoid(Queue.shutdown(queue)),
  }
  return attached
})

/** A state of an incarnation whose committed value is `value`; see `makeWatch`. */
export const makeState = (
  value: Readonly<JsonObject>,
  version: number,
  unpin: Effect.Effect<void>,
): Attached<State<JsonObject>> => {
  const readers = new Set<Queue.Queue<Value, Cause.Done>>()
  const observed = { version }
  let current: Value = value
  let done = false
  const finish = Effect.suspend(() => {
    done = true
    const all = [...readers]
    readers.clear()
    return Effect.forEach(all, (reader) => Queue.end(reader), { discard: true })
  })
  const observer: Observer = {
    advance: (change) =>
      Effect.gen(function* () {
        if (done || observedOps(observed, change).length === 0) return
        current = change.value
        for (const reader of readers) yield* Queue.offer(reader, current)
        if (current === null) yield* Effect.andThen(finish, unpin)
      }),
    end: () => finish,
  }
  const reader = Effect.gen(function* () {
    if (done) return Stream.succeed(current)
    const queue = yield* Queue.sliding<Value, Cause.Done>(1)
    yield* Queue.offer(queue, current)
    readers.add(queue)
    return Stream.fromQueue(queue).pipe(
      Stream.ensuring(
        Effect.sync(() => {
          readers.delete(queue)
        }),
      ),
    )
  })
  const state: State<JsonObject> = {
    get: Effect.sync(() => current),
    changes: Stream.unwrap(reader),
  }
  return { observation: state, observer, close: finish }
}
