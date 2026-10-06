import * as Effect from 'effect/Effect'
import * as FiberHandle from 'effect/FiberHandle'
import * as Deferred from 'effect/Deferred'
import * as Ref from 'effect/Ref'
import * as Scope from 'effect/Scope'
import * as Schema from 'effect/Schema'
import * as Semaphore from 'effect/Semaphore'
import * as Record from '../Record.ts'
import { rejected, StorageError, Invalid, Closed, Poisoned, Conflict } from '../StorageError.ts'
import { Store, type CommitOptions, type Candidate } from '../Store.ts'
import { applyWrites, detachedEffect, materialize, validate } from './State.ts'

export interface Snapshot {
  readonly state: Record.State
  readonly frames: ReadonlyArray<Record.Frame>
}
export interface Backend {
  readonly load: Effect.Effect<Snapshot, StorageError>
  readonly committed: Effect.Effect<Snapshot, StorageError>
  readonly save: (snapshot: Snapshot) => Effect.Effect<void, StorageError>
  readonly atomic: <A, E, R>(
    effect: Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, StorageError | E, R>
}
/** Keeps global, document and conversation tails for exact bounded observer backlogs. */
export const retainFrames = (frames: ReadonlyArray<Record.Frame>): ReadonlyArray<Record.Frame> => {
  const counts = new Map<Record.DocumentId, number>()
  const paths = new Map<string, number>()
  const conversations = new Map<Record.ConversationId, number>()
  const categories = new Map<string, number>()
  const retained: Array<Record.Frame> = []
  for (let index = frames.length - 1; index >= 0; index--) {
    const frame = frames[index]
    if (frame === undefined) continue
    let retain = index >= frames.length - 101
    for (const publication of frame.documents) {
      const count = (counts.get(publication.record.id) ?? 0) + 1
      counts.set(publication.record.id, count)
      if (count <= 101) retain = true
      const changed = new Set(
        publication.ops.map((op) =>
          JSON.stringify([publication.record.id, op[0] === 'replace' ? [] : op[1]]),
        ),
      )
      for (const path of changed) {
        const count = (paths.get(path) ?? 0) + 1
        paths.set(path, count)
        if (count <= 101) retain = true
      }
    }
    const touched = new Set<Record.ConversationId>()
    const buckets = new Set<string>()
    for (const write of frame.writes) {
      if (write.type === 'conversation') touched.add(write.value.id)
      else if (write.type === 'entry' || write.type === 'task' || write.type === 'submission')
        touched.add(write.value.conversationId)
      if (write.type === 'entry' || write.type === 'submission')
        buckets.add(JSON.stringify([write.value.conversationId, write.type]))
      else if (
        write.type === 'task' &&
        (write.value.state.status === 'terminal' || write.value.state.status === 'completing')
      ) {
        const outcome = write.value.state.outcome
        const status =
          outcome !== null && typeof outcome === 'object' && !Array.isArray(outcome)
            ? Reflect.get(outcome, 'status')
            : null
        buckets.add(
          JSON.stringify([
            write.value.conversationId,
            'task',
            write.value.kind,
            write.value.state.status,
            status,
          ]),
        )
      }
    }
    for (const publication of frame.documents)
      if (publication.record.scope.kind === 'conversation')
        touched.add(publication.record.scope.conversationId)
    for (const id of touched) {
      const count = (conversations.get(id) ?? 0) + 1
      conversations.set(id, count)
      if (count <= 101) retain = true
    }
    for (const bucket of buckets) {
      const count = (categories.get(bucket) ?? 0) + 1
      categories.set(bucket, count)
      if (count <= 101) retain = true
    }
    if (retain) retained.push(frame)
  }
  return retained.reverse()
}
const receiptResult = Effect.fnUntraced(function* (input: unknown) {
  yield* Effect.try({
    try: () => {
      const visited = new Set<object>()
      const visit = (value: unknown): void => {
        if (value === null || typeof value === 'string' || typeof value === 'boolean') return
        if (typeof value === 'number' && Number.isFinite(value)) return
        if (typeof value !== 'object' || value === null)
          throw new TypeError('Receipt results must be JSON or void')
        if (visited.has(value)) throw new TypeError('Receipt results cannot be cyclic')
        if (
          !Array.isArray(value) &&
          Object.getPrototypeOf(value) !== Object.prototype &&
          Object.getPrototypeOf(value) !== null
        )
          throw new TypeError('Receipt results cannot contain service or class instances')
        visited.add(value)
        for (const key of Reflect.ownKeys(value)) {
          if (Array.isArray(value) && key === 'length') continue
          const descriptor = Object.getOwnPropertyDescriptor(value, key)
          if (
            typeof key !== 'string' ||
            descriptor?.enumerable !== true ||
            !('value' in descriptor)
          )
            throw new TypeError('Receipt results require enumerable JSON data properties')
          visit(descriptor.value)
        }
        if (Array.isArray(value))
          for (let index = 0; index < value.length; index++)
            if (!Object.hasOwn(value, index))
              throw new TypeError('Receipt arrays cannot have holes')
        visited.delete(value)
      }
      visit(input)
    },
    catch: (cause) => rejected('Receipt result is not serializable JSON', Invalid, cause),
  })
  return yield* validate(Schema.Json, input)
})
export const make = Effect.fnUntraced(function* (
  backend: Backend,
  release: Effect.Effect<void, StorageError> = Effect.void,
) {
  const semaphore = yield* Semaphore.make(1)
  const cleanupScope = yield* Effect.acquireRelease(Scope.make(), (scope, exit) =>
    Scope.close(scope, exit),
  )
  const handle = yield* FiberHandle.make<boolean, never>().pipe(Scope.provide(cleanupScope))
  const started = yield* Ref.make(false)
  const terminal = yield* Deferred.make<void, StorageError>()
  let closed = false
  let readers = 0
  let poison: StorageError | undefined
  const shutdown = Effect.uninterruptibleMask((restore) =>
    Effect.gen(function* () {
      if (!(yield* Ref.getAndSet(started, true))) {
        closed = true
        yield* FiberHandle.run(
          handle,
          Effect.gen(function* () {
            while (readers > 0) yield* Effect.sleep('1 millis')
            yield* release
          }).pipe(Effect.uninterruptible, Deferred.into(terminal)),
        )
      }
      yield* restore(Deferred.await(terminal))
    }),
  )
  yield* Effect.addFinalizer(() => shutdown.pipe(Effect.orDie))
  const usable = Effect.suspend(() => {
    if (closed) return Effect.fail(rejected('Store is closed', Closed))
    if (poison !== undefined)
      return Effect.fail(rejected('Store is poisoned; reopen it', Poisoned, poison))
    return Effect.void
  })
  const snapshot = (load: Effect.Effect<Snapshot, StorageError>) =>
    Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        yield* usable
        readers++
        return yield* restore(load).pipe(
          Effect.ensuring(
            Effect.sync(() => {
              readers--
            }),
          ),
        )
      }),
    )
  const read = (load: Effect.Effect<Snapshot, StorageError>) =>
    snapshot(load).pipe(Effect.flatMap((value) => detachedEffect(value.state)))
  const transact = <A, E, R>(
    change: (state: Record.State) => Effect.Effect<Candidate<A>, E, R>,
    options: CommitOptions = {},
  ): Effect.Effect<A, StorageError | E, R> =>
    Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        yield* usable
        readers++
        // Match native SQL Activity ordering: acquire the database transaction
        // before the domain semaphore. Otherwise concurrent host transactions
        // can hold the semaphore while waiting for an Activity's SQL lease.
        return yield* restore(
          backend.atomic(
            semaphore.withPermit(
              Effect.uninterruptibleMask((restore) =>
                Effect.gen(function* () {
                  yield* usable
                  const snapshot = yield* backend.load
                  if (options.key !== undefined) {
                    const receipt = snapshot.state.receipts.find((item) => item.key === options.key)
                    if (receipt !== undefined) {
                      if (receipt.fingerprint !== (options.fingerprint ?? ''))
                        return yield* rejected(
                          'Idempotency key reused with different input',
                          Conflict,
                        )
                      // The generic result type is chosen by the same stable operation key, not a runtime decoder.
                      return (
                        receipt.resultIsVoid === true
                          ? undefined
                          : yield* detachedEffect(receipt.result)
                      ) as A
                    }
                  }
                  const candidate = yield* restore(change(yield* detachedEffect(snapshot.state)))
                  if (options.key !== undefined && candidate.result !== undefined)
                    yield* receiptResult(candidate.result)
                  if (
                    candidate.writes.length === 0 &&
                    candidate.state.nextId === snapshot.state.nextId &&
                    options.key === undefined
                  )
                    return candidate.result
                  let state = yield* applyWrites(
                    {
                      ...snapshot.state,
                      nextId: Math.max(snapshot.state.nextId, candidate.state.nextId),
                    },
                    candidate.writes,
                  )
                  const seq = yield* validate(Record.Seq, snapshot.state.nextSeq)
                  if (options.key !== undefined)
                    state = {
                      ...state,
                      receipts: [
                        ...state.receipts,
                        {
                          key: options.key,
                          fingerprint: options.fingerprint ?? '',
                          result:
                            candidate.result === undefined
                              ? null
                              : yield* receiptResult(candidate.result),
                          ...(candidate.result === undefined ? { resultIsVoid: true } : {}),
                          seq,
                        },
                      ],
                    }
                  state = yield* validate(Record.State, state)
                  const publications: Array<Record.Publication> = []
                  const documentIds = new Set(
                    candidate.writes.flatMap((write) => {
                      if (write.type === 'document.create' || write.type === 'document.copy')
                        return [write.record.id]
                      if (write.type === 'document.change' || write.type === 'document.retire')
                        return [write.id]
                      return []
                    }),
                  )
                  for (const id of documentIds) {
                    const document = state.documents.find((item) => item.record.id === id)
                    if (document === undefined) continue
                    if (document.record.retiredAt !== undefined) {
                      publications.push({ record: document.record, value: null, ops: [] })
                      continue
                    }
                    const snapshotValue = yield* materialize(document, 'current')
                    if (snapshotValue === undefined) continue
                    const write = candidate.writes.find(
                      (item) => item.type === 'document.change' && item.id === id,
                    )
                    let ops: ReadonlyArray<Record.Op> = [['replace', snapshotValue.value]]
                    if (write?.type === 'document.change')
                      ops =
                        write.publicationOps ??
                        (write.content.kind === 'delta' ? write.content.ops : ops)
                    publications.push({
                      record: document.record,
                      version: snapshotValue.version,
                      value: snapshotValue.value,
                      ops,
                    })
                  }
                  const frame = {
                    seq,
                    writes: yield* detachedEffect(candidate.writes),
                    documents: publications,
                  }
                  yield* backend
                    .save({ state, frames: retainFrames([...snapshot.frames, frame]) })
                    .pipe(
                      Effect.tapError((error) =>
                        Effect.sync(() => {
                          if (error instanceof StorageError && error.certainty === 'uncertain')
                            poison = error
                        }),
                      ),
                    )
                  return candidate.result
                }),
              ),
            ),
          ),
        ).pipe(
          Effect.tapError((error) =>
            Effect.sync(() => {
              if (error instanceof StorageError && error.certainty === 'uncertain') poison = error
            }),
          ),
          Effect.ensuring(
            Effect.sync(() => {
              readers--
            }),
          ),
        )
      }),
    )
  const commit = Effect.fnUntraced(function* (
    writes: ReadonlyArray<Record.Write>,
    options?: CommitOptions,
  ) {
    return yield* transact(
      (state) => Effect.succeed({ state, writes, result: state.nextSeq }),
      options,
    ).pipe(Effect.flatMap((seq) => validate(Record.Seq, seq)))
  })
  return Store.of({
    mintId: <S extends Schema.Constraint>(schema: S) =>
      transact((state) =>
        Effect.gen(function* () {
          if (!Number.isSafeInteger(state.nextId)) return yield* rejected('ID space is exhausted')
          yield* validate(schema, state.nextId)
          return { state: { ...state, nextId: state.nextId + 1 }, writes: [], result: state.nextId }
        }),
      ).pipe(Effect.flatMap((id) => validate(schema, id))),
    read: read(backend.load),
    committed: read(backend.committed),
    transact,
    commit,
    seal: Effect.sync(() => {
      closed = true
    }),
    journal: Effect.fnUntraced(function* (after) {
      const loaded = yield* snapshot(backend.committed)
      const frames = loaded.frames.filter((frame) => frame.seq > after)
      const oldest = loaded.frames[0]
      return {
        state: yield* detachedEffect(loaded.state),
        frames: yield* detachedEffect(frames),
        reset: frames.length > 100 || (oldest !== undefined && after < oldest.seq - 1),
      }
    }),
    awaitClosed: Deferred.await(terminal),
  })
})
