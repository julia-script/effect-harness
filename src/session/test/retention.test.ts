/* oxlint-disable max-lines, max-lines-per-function -- ported describe blocks */
// Ported from pi's `packages/durable/test/session-document-retention.test.ts`. The Session's
// document cache is bounded by use: unattached documents leave it after the retention period and
// load from Storage again; documents with a watch or a document state never leave it. Cache sizes
// are internal here, so eviction shows as Storage document reads.
import { describe, expect, test } from 'bun:test'
import { Deferred, Effect, Exit, Fiber, Scope, Stream } from 'effect'
import type { Duration } from 'effect'
import { TestClock } from 'effect/testing'
import { Storage } from '../../storage/index.ts'
import type { Ids, Write } from '../../storage/schema/index.ts'
import { Document, Session, SessionError } from '../index.ts'
import { createConversation } from './support.ts'

interface Retained {
  readonly session: Session.Service
  /** The memory store behind the Session. */
  readonly storage: Storage.Service
  /** Every batch the Session committed. */
  readonly commits: Array<ReadonlyArray<Write.StorageWrite>>
  /** Storage `document` reads so far, as pi's `documentReadCount`. */
  readonly reads: () => number
}

/** A Session over `storage` with the given retention, counting its document reads. */
const openOver = (storage: Storage.Service, documentRetention: Duration.Input) =>
  Effect.gen(function* () {
    const commits: Array<ReadonlyArray<Write.StorageWrite>> = []
    let reads = 0
    const counted: Storage.Service = {
      ...storage,
      commit: (writes) =>
        Effect.tap(storage.commit(writes), () =>
          Effect.sync(() => {
            commits.push(writes)
          }),
        ),
      document: (id, at) =>
        Effect.suspend(() => {
          reads++
          return storage.document(id, at)
        }),
    }
    const session = yield* Session.makeWith({ documentRetention }).pipe(
      Effect.provideService(Storage.Storage, counted),
    )
    const retained: Retained = { session, storage, commits, reads: () => reads }
    return retained
  })

const open = (documentRetention: Duration.Input) =>
  Effect.flatMap(Storage.makeMemory, (storage) => openOver(storage, documentRetention))

/** A memory store whose next `findDocument` waits at a gate, as pi's `holdFindDocument`. */
const holding = Effect.gen(function* () {
  const memory = yield* Storage.makeMemory
  const entered = yield* Deferred.make<void>()
  const gate = yield* Deferred.make<void>()
  let held = false
  const storage: Storage.Service = {
    ...memory,
    findDocument: (address, at) =>
      Effect.suspend(() => {
        if (!held) return memory.findDocument(address, at)
        held = false
        return Effect.andThen(
          Effect.andThen(Deferred.done(entered, Exit.void), Deferred.await(gate)),
          memory.findDocument(address, at),
        )
      }),
  }
  return {
    storage,
    entered,
    gate,
    hold: () => {
      held = true
    },
  }
})

/** Run a test body under the TestClock: time moves only by `TestClock.adjust`. */
const run = <A, E>(body: Effect.Effect<A, E, Scope.Scope>): Promise<A> =>
  // oxlint-disable-next-line effecttsgo/strict-effect-provide -- each test is an entry point
  Effect.runPromise(Effect.scoped(Effect.orDie(body)).pipe(Effect.provide(TestClock.layer())))

/** The Storage document reads an effect made, with its result. */
const counting = <A, E, R>(retained: Retained, effect: Effect.Effect<A, E, R>) =>
  Effect.gen(function* () {
    const before = retained.reads()
    const value = yield* effect
    return { value, reads: retained.reads() - before }
  })

type Counter = { count: number }

const CounterDoc = Document.make<Counter>({
  kind: 'test.retention.counter',
  version: 1,
  scope: 'conversation',
  history: 'latest',
  fork: 'initial',
  initial: () => Effect.succeed({ count: 0 }),
})
const RewindableDoc = Document.make<Counter>({
  kind: 'test.retention.rewindable',
  version: 1,
  scope: 'conversation',
  history: 'rewindable',
  fork: 'asOf',
  initial: () => Effect.succeed({ count: 0 }),
})
/** A third-party conversation family: one member per event. */
const EventDoc = Document.makeFamily<{ seq: number }, number>({
  kind: 'test.retention.event',
  version: 1,
  scope: 'conversation',
  history: 'latest',
  fork: 'initial',
  family: true,
  initial: (seq) => Effect.succeed({ seq }),
})
const SessionCounterDoc = Document.make<Counter>({
  kind: 'test.retention.session-counter',
  version: 1,
  scope: 'session',
  initial: () => Effect.succeed({ count: 0 }),
})

const bump = (session: Session.Service, conversationId: Ids.ConversationId, by = 1) =>
  session.commit((tx) =>
    Effect.map(tx.doc(CounterDoc, conversationId), (draft) => {
      draft.count += by
    }),
  )

const useSessionCounter = (session: Session.Service) =>
  session.commit((tx) => Effect.asVoid(tx.doc(SessionCounterDoc)))

const nothing = (session: Session.Service) => session.commit(() => Effect.void)

/** An observation that must exist, attached to `scope`. */
const attachedTo = <A, E, R>(
  scope: Scope.Scope,
  observation: Effect.Effect<A | undefined, E, R | Scope.Scope>,
) => Scope.provide(Effect.flatMap(observation, Effect.fromNullishOr), scope)

describe('document cache retention', () => {
  test('evicts an unattached document once its retention passed, and loads it again with the same value', () =>
    run(
      Effect.gen(function* () {
        const retained = yield* open(60_000)
        const { session } = retained
        const conversationId = yield* createConversation(session)
        yield* bump(session, conversationId, 3)
        // Not yet: inside the retention period.
        yield* TestClock.adjust(59_000)
        yield* bump(session, conversationId, 0)
        const inside = yield* counting(retained, session.snapshot(CounterDoc, conversationId))
        expect(inside).toEqual({ value: { count: 3 }, reads: 0 })
        // Past it, with no use since: it is evicted.
        yield* TestClock.adjust(61_000)
        yield* nothing(session)
        yield* useSessionCounter(session)
        const past = yield* counting(retained, session.snapshot(CounterDoc, conversationId))
        expect(past.value).toEqual({ count: 3 })
        expect(past.reads).toBeGreaterThan(0)
      }),
    ))

  test('evicts with retention 0 right after use, on the line', () =>
    run(
      Effect.gen(function* () {
        const retained = yield* open(0)
        const { session } = retained
        const conversationId = yield* createConversation(session)
        yield* bump(session, conversationId)
        const first = yield* counting(retained, session.snapshot(CounterDoc, conversationId))
        expect(first.value).toEqual({ count: 1 })
        expect(first.reads).toBeGreaterThan(0)
        // The read's own load is evicted right after it, too.
        const second = yield* counting(retained, session.snapshot(CounterDoc, conversationId))
        expect(second.reads).toBeGreaterThan(0)
      }),
    ))

  test('never evicts a document a watch or a document state observes, and evicts it once the last detaches', () =>
    run(
      Effect.gen(function* () {
        const retained = yield* open(1_000)
        const { session } = retained
        const conversationId = yield* createConversation(session)
        yield* bump(session, conversationId)
        const watchScope = yield* Scope.make()
        const stateScope = yield* Scope.make()
        const watch = yield* attachedTo(watchScope, session.watchDoc(CounterDoc, conversationId))
        const state = yield* attachedTo(
          stateScope,
          session.documentState(CounterDoc, conversationId),
        )
        yield* TestClock.adjust(1_000_000)
        // Still cached: a commit long after its retention passed loads nothing from Storage.
        const second = yield* counting(retained, bump(session, conversationId))
        expect(second.reads).toBe(0)
        expect((yield* state.get)?.count).toBe(2)
        const seen = yield* Stream.runCollect(Stream.take(watch.changes, 1))
        expect(seen.map(({ value }) => value?.count)).toEqual([2])
        yield* TestClock.adjust(1_000_000)
        yield* useSessionCounter(session)
        const read = yield* counting(retained, session.snapshot(CounterDoc, conversationId))
        expect(read).toEqual({ value: { count: 2 }, reads: 0 })

        // The state alone still keeps it.
        yield* Scope.close(watchScope, Exit.void)
        yield* TestClock.adjust(1_000_000)
        const third = yield* counting(retained, bump(session, conversationId))
        expect(third.reads).toBe(0)
        expect((yield* state.get)?.count).toBe(3)

        // Once the last detaches, it is idle again and leaves after the retention.
        yield* Scope.close(stateScope, Exit.void)
        yield* TestClock.adjust(1_000_000)
        yield* nothing(session)
        yield* useSessionCounter(session)
        const after = yield* counting(retained, session.snapshot(CounterDoc, conversationId))
        expect(after.value).toEqual({ count: 3 })
        expect(after.reads).toBeGreaterThan(0)
      }),
    ))

  test('never takes a tracker from a transaction that holds it', () =>
    run(
      Effect.gen(function* () {
        const { session } = yield* open(0)
        const conversationId = yield* createConversation(session)
        yield* bump(session, conversationId)
        yield* session.commit((tx) =>
          Effect.gen(function* () {
            const draft = yield* tx.doc(CounterDoc, conversationId)
            // Retention passes while the transaction holds the document; eviction waits on the
            // line behind it.
            yield* TestClock.adjust(1_000_000)
            yield* Effect.yieldNow
            draft.count += 10
          }),
        )
        expect(yield* session.snapshot(CounterDoc, conversationId)).toEqual({ count: 11 })
        yield* bump(session, conversationId)
        expect(yield* session.snapshot(CounterDoc, conversationId)).toEqual({ count: 12 })
      }),
    ))

  test('writes after a reload continue the stored deltas and bases', () =>
    run(
      Effect.gen(function* () {
        const storage = yield* Storage.makeMemory
        const commits = yield* Effect.scoped(
          Effect.gen(function* () {
            const retained = yield* openOver(storage, 0)
            const { session } = retained
            const conversationId = yield* createConversation(session)
            for (let n = 0; n < 5; n++) yield* bump(session, conversationId)
            expect(yield* session.snapshot(CounterDoc, conversationId)).toEqual({ count: 5 })
            return { conversationId, writes: retained.commits.flat() }
          }),
        )
        // Every bump after the first is a change of the same incarnation, never a second creation.
        const creations = commits.writes.filter(
          (write) =>
            write.type === 'document.create' && write.record.kind === CounterDoc.definition.kind,
        )
        expect(creations).toHaveLength(1)
        // And a fresh Session over the same storage reads the same value.
        const reopened = yield* openOver(storage, 0)
        expect(yield* reopened.session.snapshot(CounterDoc, commits.conversationId)).toEqual({
          count: 5,
        })
      }),
    ))

  test('keeps history and forks of rewindable documents after eviction', () =>
    run(
      Effect.gen(function* () {
        const { session } = yield* open(0)
        const conversationId = yield* createConversation(session)
        const write = (count: number) =>
          session.commit((tx) =>
            Effect.gen(function* () {
              const draft = yield* tx.doc(RewindableDoc, conversationId)
              draft.count = count
              return (yield* tx.appendEntry(conversationId, { kind: 'note', data: null })).id
            }),
          )
        const first = yield* write(1)
        const second = yield* write(2)
        expect(yield* session.snapshotAsOf(RewindableDoc, conversationId, first)).toEqual({
          count: 1,
        })
        expect(yield* session.snapshotAsOf(RewindableDoc, conversationId, second)).toEqual({
          count: 2,
        })
        const fork = yield* session.commit((tx) =>
          Effect.map(
            tx.forkConversation(conversationId, first, { ownership: { kind: 'ownerless' } }),
            ({ id }) => id,
          ),
        )
        expect(yield* session.snapshot(RewindableDoc, fork)).toEqual({ count: 1 })
        expect(yield* session.snapshot(RewindableDoc, conversationId)).toEqual({ count: 2 })
      }),
    ))

  test('bounds a third-party family with a member per event', () =>
    run(
      Effect.gen(function* () {
        const retained = yield* open(0)
        const { session } = retained
        const conversationId = yield* createConversation(session)
        for (let seq = 0; seq < 200; seq++) {
          yield* session.commit((tx) =>
            Effect.asVoid(tx.doc(EventDoc, conversationId, `${seq}`, seq)),
          )
        }
        const member = yield* counting(retained, session.snapshot(EventDoc, conversationId, '123'))
        expect(member.value).toEqual({ seq: 123 })
        expect(member.reads).toBeGreaterThan(0)
        const last = yield* counting(retained, session.snapshot(EventDoc, conversationId, '199'))
        expect(last.reads).toBeGreaterThan(0)
      }),
    ))

  test('keeps the cache of a long-lived Session bounded across many conversations', () =>
    run(
      Effect.gen(function* () {
        const retained = yield* open(10_000)
        const { session } = retained
        const conversationIds: Array<Ids.ConversationId> = []
        for (let n = 0; n < 300; n++) {
          const conversationId = yield* createConversation(session)
          yield* bump(session, conversationId)
          conversationIds.push(conversationId)
          yield* TestClock.adjust(1_000)
        }
        // Only what was used in the last 10 s, about ten conversations' worth, is still cached.
        let cached = 0
        for (const conversationId of conversationIds) {
          const read = yield* counting(retained, session.snapshot(CounterDoc, conversationId))
          expect(read.value).toEqual({ count: 1 })
          if (read.reads === 0) cached++
        }
        expect(cached).toBeLessThanOrEqual(12)
      }),
    ))

  test('evicts after a commit whose settlement fails', () =>
    run(
      Effect.gen(function* () {
        const retained = yield* open(10_000)
        const { session } = retained
        const conversationId = yield* createConversation(session)
        yield* bump(session, conversationId)
        yield* useSessionCounter(session)
        // Retention passes inside a commit whose settlement fails. pi throws from a checkpoint
        // predicate; here the change cannot be prepared (a non-JSON number).
        const error = yield* Effect.flip(
          session.commit((tx) =>
            Effect.gen(function* () {
              const draft = yield* tx.doc(SessionCounterDoc)
              draft.count = Number.NaN
              yield* TestClock.adjust(1_000_000)
            }),
          ),
        )
        expect(error).toBeInstanceOf(SessionError.DocumentError)
        const after = yield* counting(retained, session.snapshot(CounterDoc, conversationId))
        expect(after.value).toEqual({ count: 1 })
        expect(after.reads).toBeGreaterThan(0)
        expect(yield* session.snapshot(SessionCounterDoc)).toEqual({ count: 0 })
      }),
    ))

  test('evicts on its timer without a commit', () =>
    run(
      Effect.gen(function* () {
        const retained = yield* open(20)
        const { session } = retained
        const conversationId = yield* createConversation(session)
        yield* bump(session, conversationId)
        // A commit sweeps only when it ends, so a load inside this one shows the timer evicted.
        const inside = yield* counting(retained, bump(session, conversationId))
        expect(inside.reads).toBe(0)
        yield* TestClock.adjust(20)
        const after = yield* counting(retained, bump(session, conversationId))
        expect(after.reads).toBeGreaterThan(0)
        expect(yield* session.snapshot(CounterDoc, conversationId)).toEqual({ count: 3 })
      }),
    ))

  test('does not keep a replacement cached for an observer of the retired incarnation', () =>
    run(
      Effect.gen(function* () {
        const retained = yield* open(0)
        const { session } = retained
        const conversationId = yield* createConversation(session)
        yield* bump(session, conversationId)
        const state = yield* Effect.flatMap(
          session.documentState(CounterDoc, conversationId),
          Effect.fromNullishOr,
        )
        yield* session.commit((tx) => tx.retireDoc(CounterDoc, conversationId))
        expect(yield* state.get).toBeNull()
        yield* bump(session, conversationId, 5)
        const read = yield* counting(retained, session.snapshot(CounterDoc, conversationId))
        expect(read.value).toEqual({ count: 5 })
        expect(read.reads).toBeGreaterThan(0)
      }),
    ))

  test('refuses an observer whose load loses a race with close', () =>
    run(
      Effect.gen(function* () {
        const held = yield* holding
        const sessionScope = yield* Scope.make()
        const { session } = yield* Scope.provide(openOver(held.storage, 0), sessionScope)
        const conversationId = yield* createConversation(session)
        yield* bump(session, conversationId)
        held.hold()
        const attaching = yield* Effect.forkChild(
          Effect.scoped(Effect.exit(session.documentState(CounterDoc, conversationId))),
        )
        yield* Deferred.await(held.entered)
        const closing = yield* Effect.forkChild(Scope.close(sessionScope, Exit.void))
        yield* Effect.yieldNow
        yield* Deferred.done(held.gate, Exit.void)
        const attached = yield* Fiber.join(attaching)
        yield* Fiber.join(closing)
        // pi rejects the attach; a pin it left behind would be unobservable once closed.
        expect(Exit.isFailure(attached)).toBe(true)
      }),
    ))

  test('leaves no pin behind when an attaching observer is interrupted or fails', () =>
    run(
      Effect.gen(function* () {
        const held = yield* holding
        const retained = yield* openOver(held.storage, 0)
        const { session } = retained
        const conversationId = yield* createConversation(session)
        yield* bump(session, conversationId)
        held.hold()
        const attaching = yield* Effect.forkChild(
          Effect.scoped(session.documentState(CounterDoc, conversationId)),
        )
        yield* Deferred.await(held.entered)
        yield* Fiber.interrupt(attaching)
        yield* Deferred.done(held.gate, Exit.void)
        // Retention 0: once the line sweeps, every read loads from Storage, so nothing is pinned.
        const unpinned = Effect.gen(function* () {
          yield* nothing(session)
          for (let n = 0; n < 2; n++) {
            const read = yield* counting(retained, session.snapshot(CounterDoc, conversationId))
            expect(read.value).toEqual({ count: 1 })
            expect(read.reads).toBeGreaterThan(0)
          }
        })
        yield* unpinned
        // An observer whose scope fails right after it attached detaches, too.
        const failed = yield* Effect.exit(
          Effect.scoped(
            Effect.andThen(
              session.watchDoc(CounterDoc, conversationId),
              Effect.fail('after attach'),
            ),
          ),
        )
        expect(Exit.isFailure(failed)).toBe(true)
        yield* unpinned
      }),
    ))

  test('evicts on its timer after an observer detaches, and tolerates a detach run twice', () =>
    run(
      Effect.gen(function* () {
        const retained = yield* open(20)
        const { session } = retained
        const conversationId = yield* createConversation(session)
        yield* bump(session, conversationId)
        const scope = yield* Scope.make()
        yield* attachedTo(scope, session.documentState(CounterDoc, conversationId))
        yield* Scope.close(scope, Exit.void)
        yield* Scope.close(scope, Exit.void)
        // No commit: the timer alone evicts it.
        yield* TestClock.adjust(20)
        const read = yield* counting(retained, session.snapshot(CounterDoc, conversationId))
        expect(read.value).toEqual({ count: 1 })
        expect(read.reads).toBeGreaterThan(0)
      }),
    ))

  test('migrates a document again after its eviction', () =>
    run(
      Effect.gen(function* () {
        const V1 = Document.make<{ count: number }>({
          kind: 'test.retention.migrating',
          version: 1,
          scope: 'session',
          initial: () => Effect.succeed({ count: 1 }),
        })
        let migrations = 0
        const V2 = Document.make<{ count: number; migrated: boolean }>({
          kind: 'test.retention.migrating',
          version: 2,
          scope: 'session',
          initial: () => Effect.succeed({ count: 0, migrated: true }),
          migrate: (value) =>
            Effect.sync(() => {
              migrations++
              return {
                count: typeof value['count'] === 'number' ? value['count'] : 0,
                migrated: true,
              }
            }),
        })
        const { session } = yield* open(0)
        yield* session.commit((tx) => Effect.asVoid(tx.doc(V1)))
        expect(yield* session.snapshot(V2)).toEqual({ count: 1, migrated: true })
        expect(yield* session.snapshot(V2)).toEqual({ count: 1, migrated: true })
        // Not written back, so each cold load migrates again.
        expect(migrations).toBe(2)
      }),
    ))

  test('keeps a reloaded newer version cached while an observer of the older one is attached', () =>
    run(
      Effect.gen(function* () {
        const V1 = Document.make<Counter>({
          kind: 'test.retention.versioned',
          version: 1,
          scope: 'session',
          initial: () => Effect.succeed({ count: 3 }),
        })
        const V2 = Document.make<Counter & { v2: true }>({
          kind: 'test.retention.versioned',
          version: 2,
          scope: 'session',
          initial: () => Effect.succeed({ count: 0, v2: true }),
          migrate: (value) =>
            Effect.succeed({
              count: typeof value['count'] === 'number' ? value['count'] : 0,
              v2: true as const,
            }),
        })
        const retained = yield* open(0)
        const { session } = retained
        yield* session.commit((tx) => Effect.asVoid(tx.doc(V1)))
        const scope = yield* Scope.make()
        yield* attachedTo(scope, session.documentState(V1))
        // The newer token reloads; the observer's pin keeps the reloaded tracker cached.
        const reload = yield* counting(retained, session.snapshot(V2))
        expect(reload.value).toEqual({ count: 3, v2: true })
        expect(reload.reads).toBeGreaterThan(0)
        const cached = yield* counting(retained, session.snapshot(V2))
        expect(cached).toEqual({ value: { count: 3, v2: true }, reads: 0 })
        yield* Scope.close(scope, Exit.void)
        yield* nothing(session)
        const after = yield* counting(retained, session.snapshot(V2))
        expect(after.reads).toBeGreaterThan(0)
      }),
    ))

  const invalid: ReadonlyArray<readonly [string, Duration.Input]> = [
    ['NaN', Number.NaN],
    ['-5', -5],
    // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- an invalid input is the point
    ["'soon'", 'soon' as unknown as Duration.Input],
  ]
  for (const [label, retention] of invalid) {
    test(`treats a retention of ${label}, which is not a duration, as the default`, () =>
      run(
        Effect.gen(function* () {
          const retained = yield* open(retention)
          const { session } = retained
          const conversationId = yield* createConversation(session)
          yield* bump(session, conversationId)
          yield* TestClock.adjust(60_000)
          yield* nothing(session)
          // Not dropped at once, as a retention of 0 would: the ten-minute default holds it.
          const read = yield* counting(retained, session.snapshot(CounterDoc, conversationId))
          expect(read).toEqual({ value: { count: 1 }, reads: 0 })
        }),
      ))
  }

  test('treats an Infinity retention as never', () =>
    run(
      Effect.gen(function* () {
        const retained = yield* open(Number.POSITIVE_INFINITY)
        const { session } = retained
        const conversationId = yield* createConversation(session)
        yield* bump(session, conversationId)
        yield* TestClock.adjust(1e15)
        yield* nothing(session)
        const read = yield* counting(retained, session.snapshot(CounterDoc, conversationId))
        expect(read).toEqual({ value: { count: 1 }, reads: 0 })
      }),
    ))

  test('loads many evicted documents in one commit and evicts them all after it', () =>
    run(
      Effect.gen(function* () {
        const retained = yield* open(0)
        const { session } = retained
        const conversationIds: Array<Ids.ConversationId> = []
        for (let n = 0; n < 50; n++) {
          const conversationId = yield* createConversation(session)
          yield* bump(session, conversationId)
          conversationIds.push(conversationId)
        }
        // One commit loads 50 evicted documents from storage, each touched as it loads.
        const loads = yield* counting(
          retained,
          session.commit((tx) =>
            Effect.forEach(
              conversationIds,
              (conversationId) =>
                Effect.andThen(
                  Effect.map(tx.doc(CounterDoc, conversationId), (draft) => {
                    draft.count++
                  }),
                  Effect.yieldNow,
                ),
              { discard: true },
            ),
          ),
        )
        expect(loads.reads).toBe(50)
        for (const conversationId of conversationIds) {
          const read = yield* counting(retained, session.snapshot(CounterDoc, conversationId))
          expect(read.value).toEqual({ count: 2 })
          expect(read.reads).toBeGreaterThan(0)
        }
      }),
    ))
})
