import { Deferred, Duration, Effect, Exit, Option, PubSub, Semaphore } from 'effect'
import type { Scope } from 'effect'
import { cast } from 'effect/Function'
import { Storage } from '../../storage/Storage.ts'
import type { Service as StorageService } from '../../storage/Storage.ts'
import { StorageError } from '../../storage/StorageError.ts'
import type {
  CommitPublication,
  Options,
  Service,
  SessionEnd,
  TransactionScope,
  Tx,
} from '../Session.ts'
import { SessionClosed, SessionFailed } from '../SessionError.ts'
import * as Cache from './cache.ts'
import type { AnyToken } from './documents.ts'
import { addressId } from './documents.ts'
import { observe } from './observe.ts'
import * as Observers from './observers.ts'
import { snapshot, snapshotAsOf } from './snapshots.ts'
import type { OnLine } from './snapshots.ts'
import { abandon, discard, makeTransaction, settle } from './transaction.ts'
import { publication } from './transaction/assemble.ts'
import { adopt } from './transaction/plans.ts'
import type { Guarded, Host } from './transaction/state.ts'

/**
 * Session kernel, ported from pi's `session/session.ts`: one mutation line, the document tracker
 * cache, and committed publication. pi's document observation is not ported yet.
 *
 * The line is a one-permit semaphore held for a whole commit, from its callback to its
 * publication. Lifecycle is three one-shot `Deferred`s. Storage failures are final: the first
 * `StorageError` completes `failure`, and every Storage call from then on, also one already
 * underway, fails with `SessionFailed`. `closing` completes when the scope starts closing and
 * admits no more commits; `ended` completes once, with whichever end came first.
 *
 * Effect's `Latch` is a gate that suspends its waiters while closed, so it fits neither: a failed
 * or closing Session must fail callers at once, not hold them.
 */
interface Lifecycle {
  readonly failure: Deferred.Deferred<never, SessionFailed>
  readonly closing: Deferred.Deferred<void>
  readonly ended: Deferred.Deferred<SessionEnd>
}

const failed = (cause: StorageError) =>
  new SessionFailed({ message: 'Session failed after a storage error; reopen it', cause })

const healthy = (lifecycle: Lifecycle): Effect.Effect<void, SessionFailed> =>
  Effect.flatMap(
    Deferred.poll(lifecycle.failure),
    Option.getOrElse(() => Effect.void),
  )

/** Fail the Session with `error`, and end its observers; the first failure wins. */
const fail = (lifecycle: Lifecycle, observers: Observers.Observers, error: StorageError) => {
  const failure = failed(error)
  return Effect.andThen(
    Effect.all([
      Deferred.fail(lifecycle.failure, failure),
      Deferred.succeed(lifecycle.ended, { reason: 'failed', error }),
      observers.end(failure),
    ]),
    Deferred.await(lifecycle.failure),
  )
}

/** A `StorageRequestError` fails only its call; any other Storage failure fails the Session. */
const guard =
  (lifecycle: Lifecycle, observers: Observers.Observers): Host['guard'] =>
  <A, E>(effect: Effect.Effect<A, E>): Effect.Effect<A, Guarded<E>> => {
    const isStorageError = (error: E): error is Extract<E, StorageError> =>
      error instanceof StorageError
    return Effect.flatMap(healthy(lifecycle), () =>
      effect.pipe(
        Effect.catchIf(isStorageError, (error) => fail(lifecycle, observers, error)),
        Effect.tap(() => healthy(lifecycle)),
      ),
    )
  }

const closed = new SessionClosed({ message: 'Session is closed' })

/** Commit listeners, run on the line after each publication; see `Service.observeCommits`. */
type Listeners = Set<(publication: CommitPublication) => Effect.Effect<void>>

/** Run every listener in registration order; a defect in one is logged, and the next still runs. */
const notify = (listeners: Listeners, published: CommitPublication) =>
  Effect.forEach(
    [...listeners],
    (listener) =>
      Effect.ignoreCause(
        Effect.suspend(() => listener(published)),
        {
          log: 'Error',
          message: 'Session commit listener failed',
        },
      ),
    { discard: true },
  )

/** Register `listener` while the caller's scope is open. */
const observeCommits =
  (listeners: Listeners): Service['observeCommits'] =>
  (listener) =>
    Effect.acquireRelease(
      Effect.sync(() => {
        listeners.add(listener)
      }),
      () =>
        Effect.sync(() => {
          listeners.delete(listener)
        }),
    )

/** Persist an assembled batch, adopt its document changes, and publish it. */
const persist = (
  host: Host,
  observers: Observers.Observers,
  pubsub: PubSub.PubSub<CommitPublication>,
  listeners: Listeners,
  transaction: ReturnType<typeof makeTransaction>,
  assembly: Effect.Success<ReturnType<typeof settle>>,
) =>
  Effect.flatMap(
    host
      .guard(host.storage.commit(assembly.writes))
      .pipe(Effect.onError(() => discard(transaction))),
    (seq) =>
      Effect.gen(function* () {
        const documents = yield* adopt(host.cache, assembly.plans, seq)
        for (const { record } of documents) yield* host.cache.touch(addressId(record))
        yield* observers.publish(documents)
        const published = publication(assembly, documents, seq)
        yield* PubSub.publish(pubsub, published)
        yield* notify(listeners, published)
      }),
  )

const runCommit = Effect.fnUntraced(function* <A, E, R>(
  lifecycle: Lifecycle,
  host: Host,
  observers: Observers.Observers,
  pubsub: PubSub.PubSub<CommitPublication>,
  listeners: Listeners,
  change: (tx: Tx) => Effect.Effect<A, E, R>,
  scope: TransactionScope | undefined,
) {
  yield* healthy(lifecycle)
  // A healthy Session has ended only by closing: this commit queued behind the finalizer.
  if (yield* Deferred.isDone(lifecycle.ended)) return yield* closed
  const transaction = makeTransaction(host, scope)
  const result = yield* Effect.onExit(change(transaction.tx), (exit) =>
    Exit.isSuccess(exit) ? Effect.void : abandon(transaction),
  )
  // A callback that caught a failed read must not commit, nor succeed as if it had.
  yield* healthy(lifecycle).pipe(Effect.onError(() => discard(transaction)))
  const assembly = yield* settle(transaction)
  if (assembly.writes.length === 0) {
    yield* discard(transaction)
    return result
  }
  // Once admitted, interruption waits for Storage to settle and the commit to publish.
  yield* Effect.uninterruptible(persist(host, observers, pubsub, listeners, transaction, assembly))
  return result
})

const DEFAULT_RETENTION = Duration.minutes(10)

/** Retention in milliseconds; anything that is not a duration of zero or more is the default. */
const retentionMillis = (input: Options['documentRetention']): number => {
  const fallback = Duration.toMillis(DEFAULT_RETENTION)
  if (input === undefined || (typeof input === 'number' && Number.isNaN(input))) return fallback
  return Option.match(Duration.fromInput(input), {
    onNone: () => fallback,
    onSome: (duration) => {
      const millis = Duration.toMillis(duration)
      return Number.isNaN(millis) || millis < 0 ? fallback : millis
    },
  })
}

const close = Effect.fnUntraced(function* (
  lifecycle: Lifecycle,
  line: Semaphore.Semaphore,
  cache: Cache.Cache,
  observers: Observers.Observers,
  pubsub: PubSub.PubSub<CommitPublication>,
) {
  yield* Deferred.done(lifecycle.closing, Exit.void)
  // Observers end before admitted commits settle: they see nothing committed while closing.
  yield* observers.end()
  // Admitted commits settle before the Session, and then its Storage, closes.
  yield* Semaphore.withPermit(
    line,
    Effect.andThen(cache.clear, Deferred.succeed(lifecycle.ended, { reason: 'closed' })),
  )
  yield* PubSub.shutdown(pubsub)
})

/** The Session's committed document reads and observations. */
const documentReads = (
  host: Host,
  observers: Observers.Observers,
  usable: Effect.Effect<void, SessionClosed | SessionFailed>,
  onLine: OnLine,
): Pick<Service, 'snapshot' | 'snapshotAsOf' | 'watchDoc' | 'documentState'> => {
  const { cache } = host
  return {
    // Overload resolution ends here: past it, tokens and values are untyped JSON.
    snapshot: cast((token: AnyToken, ...args: ReadonlyArray<unknown>) =>
      snapshot(cache, usable, onLine, token, args),
    ),
    snapshotAsOf: cast((token: AnyToken, ...args: ReadonlyArray<unknown>) =>
      snapshotAsOf(host, onLine, token, args),
    ),
    watchDoc: cast((token: AnyToken, ...args: ReadonlyArray<unknown>) =>
      observe(cache, observers, onLine, usable, token, args, Observers.makeWatch),
    ),
    documentState: cast((token: AnyToken, ...args: ReadonlyArray<unknown>) =>
      observe(cache, observers, onLine, usable, token, args, (value, version, release) =>
        Effect.succeed(Observers.makeState(value, version, release)),
      ),
    ),
  }
}

export const make = Effect.fnUntraced(function* (
  options: Options,
): Effect.fn.Return<Service, never, Storage | Scope.Scope> {
  const storage: StorageService = yield* Storage
  const line = yield* Semaphore.make(1)
  const pubsub = yield* PubSub.unbounded<CommitPublication>()
  const listeners: Listeners = new Set()
  const lifecycle: Lifecycle = {
    failure: yield* Deferred.make<never, SessionFailed>(),
    closing: yield* Deferred.make<void>(),
    ended: yield* Deferred.make<SessionEnd>(),
  }
  const retention = retentionMillis(options.documentRetention)
  const scope = yield* Effect.scope
  const observers = Observers.make()
  const guarded = guard(lifecycle, observers)
  const cache = Cache.make({ storage, guard: guarded, retention, line, scope })
  const host: Host = {
    storage,
    guard: guarded,
    cache,
    conversationCreated: options.conversationCreated,
  }
  yield* Effect.addFinalizer(() => close(lifecycle, line, cache, observers, pubsub))
  const usable = Effect.suspend((): Effect.Effect<void, SessionClosed | SessionFailed> =>
    Deferred.isDoneUnsafe(lifecycle.closing) ? Effect.fail(closed) : healthy(lifecycle),
  )
  /** Run on the line, refusing when the Session closed or failed. */
  const onLine: OnLine = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    Effect.suspend((): Effect.Effect<A, E | SessionClosed | SessionFailed, R> =>
      Deferred.isDoneUnsafe(lifecycle.closing)
        ? Effect.fail(closed)
        : Semaphore.withPermit(
            line,
            Effect.andThen(healthy(lifecycle), effect).pipe(Effect.ensuring(cache.sweep)),
          ),
    )
  const service: Service = {
    commit: (change, transactionScope) =>
      onLine(runCommit(lifecycle, host, observers, pubsub, listeners, change, transactionScope)),
    subscribe: PubSub.subscribe(pubsub),
    observeCommits: observeCommits(listeners),
    ended: Deferred.await(lifecycle.ended),
    ...documentReads(host, observers, usable, onLine),
    unloadDocuments: onLine(cache.clear),
  }
  return service
})
