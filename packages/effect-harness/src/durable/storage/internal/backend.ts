/** Scoped backend coordination over committed storage snapshots. */
import * as MutableHashMap from 'effect/MutableHashMap'
import * as Arr from 'effect/Array'
import * as Option from 'effect/Option'
import * as Outcome from '../../workflow/Outcome.ts'
import * as Effect from 'effect/Effect'
import * as FiberHandle from 'effect/FiberHandle'
import * as Deferred from 'effect/Deferred'
import * as Ref from 'effect/Ref'
import * as Scope from 'effect/Scope'
import * as Schema from 'effect/Schema'
import { StrictReceiptJson } from '../StrictReceiptJson.ts'
import * as Semaphore from 'effect/Semaphore'
import * as Record from '../../Record.ts'
import {
  rejected,
  StorageError,
  InvalidError,
  ClosedError,
  PoisonedError,
  ConflictError,
} from '../../StorageError.ts'
import { makeCandidate, Store } from '../../Store.ts'
import { applyWrites, detachedEffect, materialize, validate } from './state.ts'
export interface Backend {
  readonly load: Effect.Effect<Backend.Snapshot, StorageError>
  readonly readContext?: Effect.Effect<object> | undefined
  readonly committed: Effect.Effect<Backend.Snapshot, StorageError>
  readonly save: (snapshot: Backend.Snapshot) => Effect.Effect<void, StorageError>
  readonly atomic: <A, E, R>(
    effect: Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, StorageError | E, R>
}
/** Keeps global, document and conversation tails for exact bounded observer backlogs. */
export const retainFrames = (self: ReadonlyArray<Record.Frame>): Array<Record.Frame> => {
  const counts = MutableHashMap.empty<Record.DocumentId, number>()
  const paths = MutableHashMap.empty<string, number>()
  const conversations = MutableHashMap.empty<Record.ConversationId, number>()
  const categories = MutableHashMap.empty<string, number>()
  const retained: Array<Record.Frame> = []
  for (let index = self.length - 1; index >= 0; index--) {
    const frame = self[index]
    if (frame === undefined) continue
    let retain = index >= self.length - 101
    for (const publication of frame.documents) {
      const count =
        (Option.getOrUndefined(MutableHashMap.get(counts, publication.record.id)) ?? 0) + 1
      MutableHashMap.set(counts, publication.record.id, count)
      if (count <= 101) retain = true
      const changed = Arr.dedupe(
        publication.ops.map((op) =>
          JSON.stringify([publication.record.id, op[0] === 'replace' ? [] : op[1]]),
        ),
      )
      for (const path of changed) {
        const count = (Option.getOrUndefined(MutableHashMap.get(paths, path)) ?? 0) + 1
        MutableHashMap.set(paths, path, count)
        if (count <= 101) retain = true
      }
    }
    const touched: Array<Record.ConversationId> = []
    const buckets: Array<string> = []
    for (const write of frame.writes) {
      if (write._tag === 'conversation') touched.push(write.value.id)
      else if (write._tag === 'entry' || write._tag === 'task' || write._tag === 'submission')
        touched.push(write.value.conversationId)
      if (write._tag === 'entry' || write._tag === 'submission')
        buckets.push(JSON.stringify([write.value.conversationId, write._tag]))
      else if (
        write._tag === 'task' &&
        (write.value.state.status === 'terminal' || write.value.state.status === 'completing')
      ) {
        const status = Outcome.classifyTaskOrUndefined(write.value)?.rawDirectStatus ?? null
        buckets.push(
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
      if (publication.record.scope._tag === 'conversation')
        touched.push(publication.record.scope.conversationId)
    for (const id of Arr.dedupe(touched)) {
      const count = (Option.getOrUndefined(MutableHashMap.get(conversations, id)) ?? 0) + 1
      MutableHashMap.set(conversations, id, count)
      if (count <= 101) retain = true
    }
    for (const bucket of Arr.dedupe(buckets)) {
      const count = (Option.getOrUndefined(MutableHashMap.get(categories, bucket)) ?? 0) + 1
      MutableHashMap.set(categories, bucket, count)
      if (count <= 101) retain = true
    }
    if (retain) retained.push(frame)
  }
  return retained.reverse()
}
const receiptResult = (input: unknown) =>
  Schema.decodeUnknownEffect(StrictReceiptJson)(input).pipe(
    Effect.mapError((cause) =>
      rejected('Receipt result is not serializable JSON', InvalidError, cause),
    ),
  )
export const make = Effect.fnUntraced(function* (
  backend: Backend,
  release: Effect.Effect<void, StorageError> = Effect.void,
): Effect.fn.Return<Store['Service'], never, Scope.Scope> {
  const semaphore = yield* Semaphore.make(1)
  const cleanupScope = yield* Effect.acquireRelease(Scope.make(), (scope, exit) =>
    Scope.close(scope, exit),
  )
  const handle = yield* FiberHandle.make<boolean, never>().pipe(Scope.provide(cleanupScope))
  const started = yield* Ref.make(false)
  const drained = yield* Deferred.make<void>()
  const terminal = yield* Deferred.make<void, StorageError>()
  const lifecycle = yield* Ref.make<{
    readonly closed: boolean
    readonly readers: number
    readonly poison: StorageError | undefined
  }>({ closed: false, readers: 0, poison: undefined })
  const shutdown = Effect.uninterruptibleMask((restore) =>
    Effect.gen(function* () {
      if (!(yield* Ref.getAndSet(started, true))) {
        const empty = yield* Ref.modify(lifecycle, (state) => [
          state.readers === 0,
          { ...state, closed: true },
        ])
        if (empty) yield* Deferred.succeed(drained, undefined)
        yield* FiberHandle.run(
          handle,
          Effect.gen(function* () {
            yield* Deferred.await(drained)
            yield* release
          }).pipe(Effect.uninterruptible, Deferred.into(terminal)),
        )
      }
      yield* restore(Deferred.await(terminal))
    }),
  )
  yield* Effect.addFinalizer(() => shutdown.pipe(Effect.orDie))
  const failure = (state: {
    readonly closed: boolean
    readonly poison: StorageError | undefined
  }) => {
    if (state.closed) return rejected('Store is closed', ClosedError)
    if (state.poison !== undefined)
      return rejected('Store is poisoned; reopen it', PoisonedError, state.poison)
    return undefined
  }
  const usable = Ref.get(lifecycle).pipe(
    Effect.flatMap((state) => {
      const error = failure(state)
      return error === undefined ? Effect.void : Effect.fail(error)
    }),
  )
  const admit = Ref.modify(lifecycle, (state) => {
    const error = failure(state)
    return [error, error === undefined ? { ...state, readers: state.readers + 1 } : state] as const
  }).pipe(Effect.flatMap((error) => (error === undefined ? Effect.void : Effect.fail(error))))
  const settled = Ref.modify(lifecycle, (state) => {
    const readers = state.readers - 1
    return [state.closed && readers === 0, { ...state, readers }] as const
  }).pipe(Effect.flatMap((empty) => (empty ? Deferred.succeed(drained, undefined) : Effect.void)))
  const snapshot = (load: Effect.Effect<Backend.Snapshot, StorageError>) =>
    Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        yield* admit
        return yield* restore(load).pipe(Effect.ensuring(settled))
      }),
    )
  const read = (load: Effect.Effect<Backend.Snapshot, StorageError>) =>
    snapshot(load).pipe(Effect.flatMap((value) => detachedEffect(value.state)))
  const transact = <A, E, R>(
    change: (state: Record.State) => Effect.Effect<Store.Candidate<A>, E, R>,
    options: Store.CommitOptions = {},
  ): Effect.Effect<A, StorageError | E, R> =>
    Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        yield* admit
        // Backend coordination is acquired before this Store's local permit.
        return yield* restore(
          backend.atomic(
            semaphore.withPermit(
              Effect.uninterruptibleMask((restore) =>
                Effect.gen(function* () {
                  yield* usable
                  const snapshot = yield* backend.load
                  if (options.key !== undefined) {
                    const receipt = Arr.findFirst(
                      snapshot.state.receipts,
                      (item) => item.key === options.key,
                    )
                    if (Option.isSome(receipt)) {
                      if (receipt.value.fingerprint !== (options.fingerprint ?? ''))
                        return yield* rejected(
                          'Idempotency key reused with different input',
                          ConflictError,
                        )
                      // The generic result type is chosen by the same stable operation key, not a runtime decoder.
                      return (
                        receipt.value.resultIsVoid === true
                          ? undefined
                          : yield* detachedEffect(receipt.value.result)
                      ) as A
                    }
                  }
                  const candidate = yield* restore(change(yield* detachedEffect(snapshot.state)))
                  if (options.key !== undefined && candidate.result !== undefined)
                    yield* receiptResult(candidate.result)
                  if (
                    Arr.isReadonlyArrayEmpty(candidate.writes) &&
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
                  const documentIds = Arr.dedupe(
                    Arr.flatMap(candidate.writes, (write) => {
                      if (write._tag === 'document.create' || write._tag === 'document.copy')
                        return [write.record.id]
                      if (write._tag === 'document.change' || write._tag === 'document.retire')
                        return [write.id]
                      return []
                    }),
                  )
                  for (const id of documentIds) {
                    const document = Arr.findFirst(state.documents, (item) => item.record.id === id)
                    if (Option.isNone(document)) continue
                    if (document.value.record.retiredAt !== undefined) {
                      publications.push({ record: document.value.record, value: null, ops: [] })
                      continue
                    }
                    const snapshotOption = yield* materialize(document.value, 'current')
                    if (Option.isNone(snapshotOption)) continue
                    const snapshotValue = snapshotOption.value
                    const write = Arr.findFirst(
                      candidate.writes,
                      (item) => item._tag === 'document.change' && item.id === id,
                    )
                    let ops: ReadonlyArray<Record.Op> = [['replace', snapshotValue.value]]
                    if (Option.isSome(write) && write.value._tag === 'document.change')
                      ops =
                        write.value.publicationOps ??
                        (write.value.content._tag === 'delta' ? write.value.content.ops : ops)
                    publications.push({
                      record: document.value.record,
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
                        error instanceof StorageError && error.certainty === 'uncertain'
                          ? Ref.update(lifecycle, (state) => ({ ...state, poison: error }))
                          : Effect.void,
                      ),
                    )
                  return candidate.result
                }),
              ),
            ),
          ),
        ).pipe(
          Effect.tapError((error) =>
            error instanceof StorageError && error.certainty === 'uncertain'
              ? Ref.update(lifecycle, (state) => ({ ...state, poison: error }))
              : Effect.void,
          ),
          Effect.ensuring(settled),
        )
      }),
    )
  const commit = (writes: ReadonlyArray<Record.Write>, options?: Store.CommitOptions) =>
    transact(
      (state) => Effect.succeed(makeCandidate({ state, writes, result: state.nextSeq })),
      options,
    ).pipe(Effect.flatMap((seq) => validate(Record.Seq, seq)))
  return Store.of({
    read: read(backend.load),
    ...(backend.readContext === undefined ? {} : { readContext: backend.readContext }),
    committed: read(backend.committed),
    transact,
    commit,
    seal: Ref.update(lifecycle, (state) => ({ ...state, closed: true })),
    journal: Effect.fnUntraced(function* (after) {
      const loaded = yield* snapshot(backend.committed)
      const frames = Arr.filter(loaded.frames, (frame) => frame.seq > after)
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

export declare namespace Backend {
  export interface Snapshot {
    readonly state: Record.State
    readonly frames: ReadonlyArray<Record.Frame>
  }
}
