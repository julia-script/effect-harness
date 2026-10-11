/** One scoped coordination line, document cache and bounded non-blocking observers. */
import * as Cause from 'effect/Cause'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Option from 'effect/Option'
import * as Queue from 'effect/Queue'
import * as Semaphore from 'effect/Semaphore'
import * as Stream from 'effect/Stream'
import type * as Domain from '../Record.js'
import type * as Record from '../Record.js'
import type * as Session from '../Session.js'
import type * as Transaction from '../Transaction.js'
import { SessionError, type Failure } from '../SessionError.js'
import { Storage } from '../Storage.js'
import * as StorageFormat from '../StorageFormat.js'
import { StorageError } from '../StorageError.js'
import * as Value from './DocumentValue.js'
import type * as ObservationRuntime from './ObservationRuntime.js'
import type * as DocumentBackend from './DocumentBackend.js'

export type Event =
  | { readonly _tag: 'commit'; readonly value: Session.Commit }
  | { readonly _tag: 'document'; readonly value: Record.StoredDocument | null }

export interface Subscriber {
  readonly queue: Queue.Queue<Event, SessionError | Cause.Done>
  readonly documentId?: Domain.DocumentId
}

export interface State {
  readonly initializers: ReadonlyArray<Initializer>
  readonly storage: Storage['Service']
  readonly semaphore: Semaphore.Semaphore
  readonly cache: Map<string, Option.Option<Record.StoredDocument>>
  readonly subscribers: Set<Subscriber>
  readonly reconciliations: Set<Queue.Queue<void, Cause.Done | SessionError>>
  readonly observations: Map<string, ObservationRuntime.Mount>
  readonly documentMounts: Set<DocumentBackend.Mount>
  observationCommitSeq: Session.Commit['seq'] | undefined
  readonly requireSchedulerFormat: Effect.Effect<StorageFormat.Format, StorageFormat.Failure>
  readonly requireDocumentPatchFormat: Effect.Effect<StorageFormat.Format, StorageFormat.Failure>
  readonly checkToolCallFormat: Effect.Effect<void, Failure>
  scheduler: object | undefined
  closed: boolean
  poison: Failure | undefined
}

export type Initializer = (
  tx: Transaction.Transaction,
  conversation: Domain.Conversation,
) => Effect.Effect<void, Failure>

const states = new WeakMap<object, State>()
const owners = new WeakMap<Storage['Service'], State>()
export const register = (self: object, state: State): void => {
  states.set(self, state)
}
export const Transactions = Context.Reference<ReadonlySet<State>>('effect-harness/Transactions', {
  defaultValue: () => new Set(),
})

export const get = Effect.fnUntraced(function* (self: object) {
  const state = states.get(self)
  if (state === undefined)
    return yield* new SessionError({
      reason: 'invalid',
      operation: 'session.access',
      message: 'Invalid Session instance',
    })
  return state
})

export const open = Effect.fnUntraced(function* (state: State) {
  if (state.poison !== undefined) return yield* state.poison
  if (state.closed)
    return yield* new SessionError({
      reason: 'closed',
      operation: 'session.access',
      message: 'Session scope is closed',
    })
})

export const line = Effect.fnUntraced(function* <A, E, R>(
  self: object,
  operation: string,
  use: (state: State) => Effect.Effect<A, E, R>,
): Effect.fn.Return<A, E | Failure, R> {
  const state = yield* get(self)
  yield* open(state)
  const transactions = yield* Transactions
  if (transactions.has(state))
    return yield* new SessionError({
      reason: 'conflict',
      operation,
      message: 'Use Transaction operations inside a Session.commit callback',
    })
  return yield* state.semaphore.withPermit(
    Effect.gen(function* () {
      yield* open(state)
      return yield* use(state)
    }),
  )
})

export const scan = <A, E>(
  self: object,
  operation: string,
  build: (state: State) => Stream.Stream<A, E>,
) =>
  Stream.fromPull(
    Effect.gen(function* () {
      const state = yield* get(self)
      yield* open(state)
      const pull = yield* Stream.toPull(build(state))
      return { pull }
    }).pipe(Effect.map(({ pull }) => line(self, operation, () => pull))),
  ).pipe(Stream.scoped)

export const load = Effect.fnUntraced(function* (state: State, address: Record.DocumentAddress) {
  const key = yield* Value.key(address)
  const cached = state.cache.get(key)
  if (cached !== undefined) return cached
  const record = yield* state.storage.findDocument(address)
  let stored: Option.Option<Record.StoredDocument> = Option.none()
  if (Option.isSome(record)) {
    stored = yield* state.storage.document(record.value.id)
    if (Option.isNone(stored))
      return yield* new SessionError({
        reason: 'invalid',
        operation: 'document.load',
        message: 'Current document has no stored content',
      })
  }
  state.cache.set(key, stored)
  return stored
})

export const subscribe = Effect.fnUntraced(function* (
  state: State,
  documentId?: Domain.DocumentId,
) {
  const queue = yield* Queue.dropping<Event, SessionError | Cause.Done>(64)
  const subscriber: Subscriber = { queue, ...(documentId === undefined ? {} : { documentId }) }
  state.subscribers.add(subscriber)
  yield* Effect.addFinalizer(() =>
    Effect.gen(function* () {
      state.subscribers.delete(subscriber)
      yield* Queue.shutdown(queue)
    }),
  )
  return subscriber
})

export const emit = Effect.fnUntraced(function* (
  state: State,
  subscriber: Subscriber,
  event: Event,
) {
  if (!(yield* Queue.offer(subscriber.queue, event))) {
    state.subscribers.delete(subscriber)
    yield* Queue.fail(
      subscriber.queue,
      new SessionError({
        reason: 'overflow',
        operation: 'session.observe',
        message: 'Observer exceeded its pending frame limit',
      }),
    )
  }
})

export const publish = Effect.fnUntraced(function* (
  state: State,
  commit: Session.Commit,
  documents: ReadonlyMap<Domain.DocumentId, Record.StoredDocument | null>,
) {
  state.observationCommitSeq = commit.seq
  for (const mount of state.documentMounts) yield* mount.publish(documents)
  for (const mount of state.observations.values()) yield* mount.publish(commit, documents)
  for (const queue of state.reconciliations) yield* Queue.offer(queue, undefined)
  for (const subscriber of state.subscribers) {
    if (subscriber.documentId === undefined)
      yield* emit(state, subscriber, { _tag: 'commit', value: commit })
    else if (documents.has(subscriber.documentId)) {
      const value = documents.get(subscriber.documentId) ?? null
      if (value === null) {
        state.subscribers.delete(subscriber)
        yield* Queue.end(subscriber.queue)
      } else yield* emit(state, subscriber, { _tag: 'document', value })
    }
  }
})

export const stop = Effect.fnUntraced(function* (state: State, error?: Failure) {
  state.poison = error
  for (const mount of state.documentMounts) yield* mount.close(error)
  state.documentMounts.clear()
  for (const mount of state.observations.values()) yield* mount.close(error)
  state.observations.clear()
  for (const queue of state.reconciliations) yield* Queue.end(queue)
  state.reconciliations.clear()
  for (const subscriber of state.subscribers) {
    if (error === undefined) yield* Queue.end(subscriber.queue)
    else
      yield* Queue.fail(
        subscriber.queue,
        new SessionError({
          reason: 'closed',
          operation: 'session.observe',
          message: 'Session cannot continue after an uncertain commit',
          cause: error,
        }),
      )
  }
  state.subscribers.clear()
})

export const make = Effect.fnUntraced(function* (initializers: ReadonlyArray<Initializer> = []) {
  const storage = yield* Storage
  const capability = yield* Effect.serviceOption(StorageFormat.StorageFormat)
  const guardContext = Context.make(Storage, storage)
  const guard = (version: 2 | 3) =>
    StorageFormat.requireSupportedFormat({
      formatVersion: version,
      minimumReaderVersion: version,
    }).pipe(
      Effect.updateContext((_context: Context.Context<never>) =>
        Option.isSome(capability)
          ? Context.add(guardContext, StorageFormat.StorageFormat, capability.value)
          : guardContext,
      ),
    )
  const semaphore = yield* Semaphore.make(1)
  const checkToolCallFormat: Effect.Effect<void, Failure> = Effect.gen(function* () {
    if (Option.isNone(capability) || capability.value.storage !== storage)
      return yield* new SessionError({
        reason: 'invalid',
        operation: 'usage.format',
        message: 'Tool-call receipts require the matching StorageFormat capability',
      })
    const format = yield* capability.value.inspect
    yield* StorageFormat.validate(format)
    if (format.minimumReaderVersion < 9)
      return yield* new SessionError({
        reason: 'invalid',
        operation: 'usage.format',
        message: 'Tool-call receipts require container and minimum reader protection 9/9',
      })
  }).pipe(
    Effect.catchCause((fullCause) =>
      Effect.gen(function* () {
        const mapped = Cause.map(fullCause, (cause): Failure => {
          if (cause._tag !== 'StorageFormatError') return cause
          if (cause.reason === 'unavailable')
            return new SessionError({
              reason: 'invalid',
              operation: 'usage.format',
              message: cause.message,
              cause,
            })
          return new StorageError({
            reason: 'unsupportedFormat',
            operation: 'usage.format',
            message: cause.message,
            cause,
          })
        })
        for (const reason of mapped.reasons)
          if (
            reason._tag === 'Fail' &&
            reason.error._tag === 'StorageError' &&
            reason.error.reason === 'uncertain'
          )
            yield* stop(state, reason.error)
        return yield* Effect.failCause(mapped)
      }),
    ),
  )
  const state: State = {
    initializers,
    storage,
    semaphore,
    cache: new Map(),
    subscribers: new Set(),
    reconciliations: new Set(),
    observations: new Map(),
    documentMounts: new Set(),
    observationCommitSeq: undefined,
    requireSchedulerFormat: guard(2),
    requireDocumentPatchFormat: guard(3),
    checkToolCallFormat,
    scheduler: undefined,
    closed: false,
    poison: undefined,
  }
  return yield* Effect.acquireRelease(
    Effect.suspend(() => {
      if (owners.has(storage))
        return Effect.fail(
          new SessionError({
            reason: 'conflict',
            operation: 'session.make',
            message: 'Storage already has an active Session coordinator',
          }),
        )
      owners.set(storage, state)
      return Effect.succeed(state)
    }),
    (state) =>
      Effect.gen(function* () {
        state.closed = true
        yield* semaphore.withPermit(
          Effect.gen(function* () {
            yield* stop(state)
            state.cache.clear()
            if (owners.get(storage) === state) owners.delete(storage)
          }),
        )
      }),
  )
})

/** Latest-state scheduler signals attach atomically. Overflow coalesces into a full scan. */
export const reconcile = (self: object) =>
  line(self, 'session.reconcile', (state) =>
    Effect.gen(function* () {
      const queue = yield* Queue.sliding<void, Cause.Done | SessionError>(1)
      state.reconciliations.add(queue)
      yield* Effect.addFinalizer(() =>
        Effect.gen(function* () {
          state.reconciliations.delete(queue)
          yield* Queue.shutdown(queue)
        }),
      )
      return queue
    }),
  )
