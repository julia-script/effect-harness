/* oxlint-disable max-lines, max-lines-per-function -- ported describe blocks */
/**
 * Ported from pi's `packages/durable/test/session-documents.test.ts`. Close listeners and the
 * synchronous delivery of commit listeners are not ported.
 */
import { describe, expect, test } from 'bun:test'
import { Deferred, Effect, Exit, Fiber, Option, PubSub, Scope } from 'effect'
import type { Tracker } from '../../delta/index.ts'
import { Storage, StorageError } from '../../storage/index.ts'
import { Ids } from '../../storage/schema/index.ts'
import type { Json, Write } from '../../storage/schema/index.ts'
import { Document, Session, SessionError } from '../index.ts'
import { createConversation } from './support.ts'

type Live = {
  message?: string
  items: Array<string>
  nested: { count: number }
  other: { label: string }
}

let liveInitCount = 0
const LiveDoc = Document.make<Live>({
  kind: 'test.live',
  version: 1,
  scope: 'conversation',
  history: 'latest',
  fork: 'initial',
  initial: () =>
    Effect.sync(() => {
      liveInitCount++
      return { items: [], nested: { count: 0 }, other: { label: 'x' } }
    }),
})

const RewindableLiveDoc = Document.make<Live>({
  kind: 'test.live',
  version: 1,
  scope: 'conversation',
  history: 'rewindable',
  fork: 'asOf',
  initial: () => Effect.succeed({ items: [], nested: { count: 0 }, other: { label: 'x' } }),
})

const LiveDocV2 = Document.make<Live>({
  kind: 'test.live',
  version: 2,
  scope: 'conversation',
  history: 'latest',
  fork: 'initial',
  initial: () => Effect.succeed({ items: [], nested: { count: 0 }, other: { label: 'x' } }),
})

type Counter = { count: number }
const CounterDoc = Document.make<Counter>({
  kind: 'test.counter',
  version: 1,
  scope: 'session',
  initial: () => Effect.succeed({ count: 0 }),
})

type Member = { seed: string; hits: number }
const seeds: Array<string> = []
const MemberDoc = Document.makeFamily<Member, string>({
  kind: 'test.member',
  version: 1,
  family: true,
  scope: 'session',
  initial: (seed) =>
    Effect.sync(() => {
      seeds.push(seed)
      return { seed, hits: 0 }
    }),
})

/** Holds calls until released, and reports when the first held call arrives. */
interface Gate {
  readonly entered: Effect.Effect<void>
  readonly release: Effect.Effect<void>
}

interface Held {
  readonly entered: Deferred.Deferred<void>
  readonly gate: Deferred.Deferred<void>
}

const makeGate = Effect.gen(function* () {
  const held: Held = { entered: yield* Deferred.make<void>(), gate: yield* Deferred.make<void>() }
  return held
})

const pass = (held: Held | undefined) =>
  held === undefined
    ? Effect.void
    : Effect.andThen(Deferred.done(held.entered, Exit.void), Deferred.await(held.gate))

/** Memory storage with observable commits, held calls, and injected commit failures. */
interface Controlled {
  readonly storage: Storage.Service
  /** Exact batches the Session admitted. */
  readonly admitted: Array<ReadonlyArray<Write.StorageWrite>>
  /** Detached copies of admitted batches, for value assertions. */
  readonly commits: Array<ReadonlyArray<Write.StorageWrite>>
  readonly counts: { mints: number }
  readonly holdCommits: Effect.Effect<Gate>
  readonly holdFindDocument: Effect.Effect<Gate>
  readonly failNextCommit: () => void
}

const control = (backend: Storage.Service): Controlled => {
  const admitted: Array<ReadonlyArray<Write.StorageWrite>> = []
  const commits: Array<ReadonlyArray<Write.StorageWrite>> = []
  const counts = { mints: 0 }
  const gates = new Map<'commit' | 'find', Held>()
  let failNext = false
  const hold = (kind: 'commit' | 'find') =>
    Effect.map(makeGate, (held): Gate => {
      gates.set(kind, held)
      return {
        entered: Deferred.await(held.entered),
        release: Effect.suspend(() => {
          if (gates.get(kind) === held) gates.delete(kind)
          return Deferred.done(held.gate, Exit.void)
        }),
      }
    })
  return {
    storage: {
      ...backend,
      commit: (writes) =>
        Effect.suspend(() => {
          admitted.push(writes)
          commits.push(structuredClone(writes))
          return Effect.andThen(
            pass(gates.get('commit')),
            Effect.suspend(() => {
              if (!failNext) return backend.commit(writes)
              failNext = false
              return Effect.fail(new StorageError.StorageError({ message: 'disk vanished' }))
            }),
          )
        }),
      mintId: (id) =>
        Effect.suspend(() => {
          counts.mints++
          return backend.mintId(id)
        }),
      findDocument: (address, at) =>
        Effect.andThen(
          Effect.suspend(() => pass(gates.get('find'))),
          backend.findDocument(address, at),
        ),
    },
    admitted,
    commits,
    counts,
    holdCommits: hold('commit'),
    holdFindDocument: hold('find'),
    failNextCommit: () => {
      failNext = true
    },
  }
}

interface Harness extends Controlled {
  readonly session: Session.Service
  /** Every publication delivered so far, oldest first. */
  readonly publications: Effect.Effect<ReadonlyArray<Session.CommitPublication>>
}

const open = Effect.gen(function* () {
  const controlled = control(yield* Storage.makeMemory)
  // These tests pin the cache's identities and loads, so documents stay for the Session's life.
  const session = yield* Session.makeWith({ documentRetention: Infinity }).pipe(
    Effect.provideService(Storage.Storage, controlled.storage),
  )
  const subscription = yield* session.subscribe
  const delivered: Array<Session.CommitPublication> = []
  const publications = Effect.map(
    PubSub.takeUpTo(subscription, Number.MAX_SAFE_INTEGER),
    (taken): ReadonlyArray<Session.CommitPublication> => {
      delivered.push(...taken)
      return delivered
    },
  )
  const harness: Harness = { ...controlled, session, publications }
  return harness
})

const run = <A, E>(body: (harness: Harness) => Effect.Effect<A, E, Scope.Scope>): Promise<A> =>
  Effect.runPromise(Effect.scoped(Effect.orDie(Effect.flatMap(open, body))))

type DocumentChange = Extract<Session.CommitChange, { readonly type: 'document' }>

const documentChanges = (publication: Session.CommitPublication): ReadonlyArray<DocumentChange> =>
  publication.changes.filter((change): change is DocumentChange => change.type === 'document')

const last = <A>(items: ReadonlyArray<A>): A => {
  const item = items.at(-1)
  if (item === undefined) throw new Error('expected an item')
  return item
}

const first = <A>(items: ReadonlyArray<A>): A => {
  const item = items[0]
  if (item === undefined) throw new Error('expected an item')
  return item
}

/** Identity check of differently typed values. */
const same = (actual: unknown, expected: unknown) => {
  expect(actual).toBe(expected)
}

/** The creations of `test.live` documents in a batch. */
const createsLive = (writes: ReadonlyArray<Write.StorageWrite>) =>
  writes.filter((write) => write.type === 'document.create' && write.record.kind === 'test.live')

/** A snapshot that must exist. */
const present = <A, E>(read: Effect.Effect<A | undefined, E>) =>
  Effect.flatMap(read, (value) => Effect.orDie(Effect.fromNullishOr(value)))

/** A conversation with one committed live document. */
const setupLive = (harness: Harness) =>
  Effect.gen(function* () {
    const conversationId = yield* createConversation(harness.session)
    yield* harness.session.commit((tx) =>
      Effect.map(tx.doc(LiveDoc, conversationId), (live) => {
        live.items.push('a', 'b')
      }),
    )
    return conversationId
  })

type PendingLive = Fiber.Fiber<Tracker.Draft<Live>, SessionError.TxDocumentError>

/** Let forked fibers and pending Storage calls run. */
const flush = Effect.sleep('5 millis')

describe('Session document transactions', () => {
  test('creates an initial base on first access and adopts it after Storage success', () =>
    run((harness) =>
      Effect.gen(function* () {
        const { session } = harness
        const conversationId = yield* createConversation(session)
        yield* session.commit((tx) =>
          Effect.map(tx.doc(LiveDoc, conversationId), (live) => {
            live.message = 'hello'
          }),
        )
        const writes = last(harness.commits)
        expect(writes).toHaveLength(1)
        expect(writes[0]).toMatchObject({
          type: 'document.create',
          record: {
            kind: 'test.live',
            scope: { kind: 'conversation', conversationId },
            history: 'latest',
            fork: 'initial',
          },
          content: {
            kind: 'base',
            version: 1,
            value: { message: 'hello', items: [], nested: { count: 0 } },
          },
        })
        const snapshot = yield* session.snapshot(LiveDoc, conversationId)
        expect(snapshot).toEqual({
          message: 'hello',
          items: [],
          nested: { count: 0 },
          other: { label: 'x' },
        })
        const publication = last(yield* harness.publications)
        const published = first(documentChanges(publication))
        expect(published.record.createdAt).toBe(publication.seq)
        same(published.value, snapshot)
        expect(published.conversationId).toBe(conversationId)
        const create = last(harness.admitted).find((write) => write.type === 'document.create')
        same(create?.type === 'document.create' ? create.content.value : undefined, published.value)
      }),
    ))

  test('never creates on snapshot and returns undefined when absent', () =>
    run((harness) =>
      Effect.gen(function* () {
        const { session } = harness
        const conversationId = yield* createConversation(session)
        const before = harness.commits.length
        expect(yield* session.snapshot(LiveDoc, conversationId)).toBeUndefined()
        expect(yield* session.snapshot(CounterDoc)).toBeUndefined()
        expect(yield* session.snapshot(MemberDoc, 'k')).toBeUndefined()
        expect(harness.commits.length).toBe(before)
        expect(harness.counts.mints).toBe(1)
      }),
    ))

  test('reads committed values in a transaction, before its first table write', () =>
    run((harness) =>
      Effect.gen(function* () {
        const { session } = harness
        const conversationId = yield* createConversation(session)
        const before = harness.commits.length
        expect(yield* session.commit((tx) => tx.snapshot(LiveDoc, conversationId))).toBeUndefined()
        expect(harness.commits.length).toBe(before)
        yield* session.commit((tx) =>
          Effect.map(tx.doc(LiveDoc, conversationId), (draft) => {
            draft.items.push('a')
          }),
        )
        const read = yield* session.commit((tx) =>
          Effect.gen(function* () {
            const draft = yield* tx.doc(LiveDoc, conversationId)
            draft.items.push('b')
            // Drafts of this transaction are not committed.
            return yield* tx.snapshot(LiveDoc, conversationId)
          }),
        )
        expect(read?.items).toEqual(['a'])
        const refused = yield* Effect.flip(
          session.commit((tx) =>
            Effect.andThen(
              tx.appendEntry(conversationId, { kind: 'note' }),
              tx.snapshot(LiveDoc, conversationId),
            ),
          ),
        )
        expect(refused).toBeInstanceOf(SessionError.ReadAfterWrite)
      }),
    ))

  test('returns shared immutable snapshots and keeps prior revisions stable', () =>
    run((harness) =>
      Effect.gen(function* () {
        const { session } = harness
        const conversationId = yield* setupLive(harness)
        const before = yield* present(session.snapshot(LiveDoc, conversationId))
        expect(yield* session.snapshot(LiveDoc, conversationId)).toBe(before)
        yield* session.commit((tx) =>
          Effect.map(tx.doc(LiveDoc, conversationId), (live) => {
            live.nested.count = 1
          }),
        )
        const after = yield* present(session.snapshot(LiveDoc, conversationId))
        expect(after).not.toBe(before)
        expect(before.nested.count).toBe(0)
        expect(after.nested.count).toBe(1)
        // Unchanged subtrees are structurally shared between immutable revisions.
        expect(after.items).toBe(before.items)
        expect(after.other).toBe(before.other)
      }),
    ))

  test('adopts by pointer swap and shares operation payloads with the published revision', () =>
    run((harness) =>
      Effect.gen(function* () {
        const { session } = harness
        const conversationId = yield* setupLive(harness)
        yield* session.commit((tx) =>
          Effect.map(tx.doc(LiveDoc, conversationId), (live) => {
            live.other = { label: 'y' }
            live.items.push('c')
          }),
        )
        const snapshot = yield* present(session.snapshot(LiveDoc, conversationId))
        const published = first(documentChanges(last(yield* harness.publications)))
        same(published.value, snapshot)
        const admitted = last(harness.admitted).find((write) => write.type === 'document.change')
        if (admitted?.type !== 'document.change' || admitted.content.kind !== 'delta') {
          throw new Error('Expected delta')
        }
        expect(published.ops).toBe(admitted.content.ops)
        const set = published.ops.find((op) => op[0] === 's')
        expect(set).toEqual(['s', ['other'], { label: 'y' }])
        // Trusted immutability: the Session makes no second copy of operation payloads.
        expect(set?.[2]).toBe(snapshot.other)
      }),
    ))

  test('copies assigned values per placement', () =>
    run((harness) =>
      Effect.gen(function* () {
        const { session } = harness
        const conversationId = yield* setupLive(harness)
        const value = { label: 'shared' }
        yield* session.commit((tx) =>
          Effect.map(tx.doc(LiveDoc, conversationId), (live) => {
            live.other = value
            ;(live as Tracker.Draft<Live> & { copy?: { label: string } }).copy = value
            value.label = 'mutated'
          }),
        )
        const snapshot = yield* present(session.snapshot(LiveDoc, conversationId))
        const copy = 'copy' in snapshot ? snapshot.copy : undefined
        expect(snapshot.other).toEqual({ label: 'shared' })
        expect(copy).toEqual({ label: 'shared' })
        expect(copy).not.toBe(snapshot.other)
        expect(snapshot.other).not.toBe(value)
      }),
    ))

  test('suppresses writes and publications for empty batches', () =>
    run((harness) =>
      Effect.gen(function* () {
        const { session } = harness
        const conversationId = yield* setupLive(harness)
        const commits = harness.commits.length
        const published = (yield* harness.publications).length
        yield* session.commit((tx) =>
          Effect.map(tx.doc(LiveDoc, conversationId), (live) => {
            live.nested.count = 0
            live.items.push('z')
            live.items.pop()
          }),
        )
        expect(harness.commits.length).toBe(commits)
        expect((yield* harness.publications).length).toBe(published)
      }),
    ))

  test('writes and publishes replayable nonempty structural no-ops', () =>
    run((harness) =>
      Effect.gen(function* () {
        const { session } = harness
        const conversationId = yield* setupLive(harness)
        const before = yield* present(session.snapshot(LiveDoc, conversationId))
        yield* session.commit((tx) =>
          Effect.map(tx.doc(LiveDoc, conversationId), (live) => {
            const head = live.items.shift()
            if (head !== undefined) live.items.unshift(head)
          }),
        )
        expect(last(harness.commits)[0]).toMatchObject({
          type: 'document.change',
          content: { kind: 'delta' },
        })
        const after = yield* present(session.snapshot(LiveDoc, conversationId))
        expect(after).toEqual(before)
        expect(after).not.toBe(before)
        const published = first(documentChanges(last(yield* harness.publications)))
        expect(published.ops.length).toBeGreaterThan(0)
      }),
    ))

  test('revokes escaped drafts when the callback settles', () =>
    run((harness) =>
      Effect.gen(function* () {
        const { session } = harness
        const conversationId = yield* setupLive(harness)
        const escaped = yield* session.commit((tx) =>
          Effect.map(tx.doc(LiveDoc, conversationId), (live) => {
            live.message = 'inside'
            return { live, items: live.items }
          }),
        )
        // A settled draft reads as empty and refuses writes; in strict mode a refused set throws.
        expect(escaped.live.message).toBeUndefined()
        expect(() => {
          escaped.live.message = 'outside'
        }).toThrow(TypeError)
        expect(escaped.items.length).toBeUndefined()
        expect(() => escaped.items.push('outside')).toThrow(TypeError)
        const snapshot = yield* present(session.snapshot(LiveDoc, conversationId))
        expect(snapshot.message).toBe('inside')

        const returned = yield* session.commit((tx) => tx.doc(LiveDoc, conversationId))
        expect(returned.message).toBeUndefined()
        expect(returned.items).toBeUndefined()
      }),
    ))

  test('aborts every change when the callback fails', () =>
    run((harness) =>
      Effect.gen(function* () {
        const { session } = harness
        const conversationId = yield* setupLive(harness)
        const before = yield* present(session.snapshot(LiveDoc, conversationId))
        const commits = harness.commits.length
        const error = yield* Effect.flip(
          session.commit((tx) =>
            Effect.gen(function* () {
              const live = yield* tx.doc(LiveDoc, conversationId)
              const counter = yield* tx.doc(CounterDoc)
              live.message = 'lost'
              counter.count = 5
              return yield* Effect.fail('callback failed')
            }),
          ),
        )
        expect(error).toBe('callback failed')
        expect(harness.commits.length).toBe(commits)
        expect(yield* session.snapshot(LiveDoc, conversationId)).toBe(before)
        expect(yield* session.snapshot(CounterDoc)).toBeUndefined()
        yield* session.commit((tx) =>
          Effect.map(tx.doc(LiveDoc, conversationId), (live) => {
            live.message = 'kept'
          }),
        )
        expect((yield* present(session.snapshot(LiveDoc, conversationId))).message).toBe('kept')
      }),
    ))

  test('memoizes concurrent duplicate acquisition and initializes once', () =>
    run((harness) =>
      Effect.gen(function* () {
        const { session } = harness
        const conversationId = yield* createConversation(session)
        const initCount = liveInitCount
        const mints = harness.counts.mints
        yield* session.commit((tx) =>
          Effect.gen(function* () {
            const [one, two] = yield* Effect.all(
              [tx.doc(LiveDoc, conversationId), tx.doc(LiveDoc, conversationId)],
              { concurrency: 'unbounded' },
            )
            expect(one).toBe(two)
            expect(yield* tx.doc(LiveDoc, conversationId)).toBe(one)
            one.message = 'once'
          }),
        )
        expect(liveInitCount).toBe(initCount + 1)
        expect(harness.counts.mints).toBe(mints + 1)
        const creates = last(harness.commits).filter((write) => write.type === 'document.create')
        expect(creates).toHaveLength(1)
      }),
    ))

  test('uses the first family seed and ignores seeds for existing members', () =>
    run(({ session }) =>
      Effect.gen(function* () {
        seeds.length = 0
        yield* session.commit((tx) =>
          Effect.gen(function* () {
            const one = yield* tx.doc(MemberDoc, 'k', 'first')
            const two = yield* tx.doc(MemberDoc, 'k', 'second')
            expect(two).toBe(one)
            one.hits++
          }),
        )
        yield* session.commit((tx) =>
          Effect.gen(function* () {
            ;(yield* tx.doc(MemberDoc, 'k', 'third')).hits++
            ;(yield* tx.doc(MemberDoc, 'other', 'fourth')).hits++
          }),
        )
        expect(seeds).toEqual(['first', 'fourth'])
        expect(yield* session.snapshot(MemberDoc, 'k')).toEqual({ seed: 'first', hits: 2 })
        expect(yield* session.snapshot(MemberDoc, 'other')).toEqual({ seed: 'fourth', hits: 1 })
      }),
    ))

  test('rejects a callback that succeeds with a pending acquisition and drains it', () =>
    run((harness) =>
      Effect.gen(function* () {
        const { session } = harness
        const conversationId = yield* setupLive(harness)
        yield* session.unloadDocuments
        const gate = yield* harness.holdFindDocument
        const commits = harness.commits.length
        let pending: PendingLive | undefined
        const commit = yield* Effect.forkChild(
          session.commit((tx) =>
            Effect.gen(function* () {
              pending = yield* Effect.forkDetach(tx.doc(LiveDoc, conversationId), {
                startImmediately: true,
              })
            }),
          ),
        )
        yield* gate.entered
        yield* flush
        // The line stays held until the late acquisition settles.
        const settledEarly = commit.pollUnsafe() !== undefined
        yield* gate.release
        const error = yield* Effect.flip(Fiber.join(commit))
        expect(error).toBeInstanceOf(SessionError.TransactionError)
        expect(error.message).toContain('pending Tx operations')
        if (pending === undefined) throw new Error('expected a pending acquisition')
        const late = yield* Effect.flip(Fiber.join(pending))
        expect(late).toBeInstanceOf(SessionError.TransactionError)
        expect(late).toMatchObject({ message: 'Transaction has settled' })
        expect(harness.commits.length).toBe(commits)
        yield* session.commit((tx) =>
          Effect.map(tx.doc(LiveDoc, conversationId), (live) => {
            live.message = 'after'
          }),
        )
        expect((yield* present(session.snapshot(LiveDoc, conversationId))).message).toBe('after')
        expect(settledEarly).toBe(false)
      }),
    ))

  test('does not initialize or mint for an absent acquisition that finishes after settlement', () =>
    run((harness) =>
      Effect.gen(function* () {
        const { session } = harness
        let initialized = 0
        const LateDoc = Document.make<Counter>({
          kind: 'test.late',
          version: 1,
          scope: 'session',
          initial: () =>
            Effect.sync(() => {
              initialized++
              return { count: 0 }
            }),
        })
        const gate = yield* harness.holdFindDocument
        const mints = harness.counts.mints
        let pending: Fiber.Fiber<Tracker.Draft<Counter>, SessionError.TxDocumentError> | undefined
        const commit = yield* Effect.forkChild(
          session.commit((tx) =>
            Effect.gen(function* () {
              pending = yield* Effect.forkDetach(tx.doc(LateDoc), { startImmediately: true })
            }),
          ),
        )
        yield* gate.entered
        // Let the callback settle while its acquisition is still held.
        yield* flush
        yield* gate.release
        const error = yield* Effect.flip(Fiber.join(commit))
        expect(error.message).toContain('pending Tx operations')
        if (pending === undefined) throw new Error('expected a pending acquisition')
        const late = yield* Effect.flip(Fiber.join(pending))
        expect(late).toMatchObject({ message: 'Transaction has settled' })
        expect(initialized).toBe(0)
        expect(harness.counts.mints).toBe(mints)
      }),
    ))

  test('rejects with the callback error when it fails with a pending acquisition', () =>
    run((harness) =>
      Effect.gen(function* () {
        const { session } = harness
        const conversationId = yield* setupLive(harness)
        yield* session.unloadDocuments
        const gate = yield* harness.holdFindDocument
        let pending: PendingLive | undefined
        const commit = yield* Effect.forkChild(
          session.commit((tx) =>
            Effect.gen(function* () {
              pending = yield* Effect.forkDetach(tx.doc(LiveDoc, conversationId), {
                startImmediately: true,
              })
              return yield* Effect.fail('callback failed')
            }),
          ),
        )
        yield* gate.entered
        // Let the callback settle while its acquisition is still held.
        yield* flush
        yield* gate.release
        expect(yield* Effect.flip(Fiber.join(commit))).toBe('callback failed')
        if (pending === undefined) throw new Error('expected a pending acquisition')
        const late = yield* Effect.flip(Fiber.join(pending))
        expect(late).toMatchObject({ message: 'Transaction has settled' })
      }),
    ))

  test('rejects Tx use after the callback settles', () =>
    run((harness) =>
      Effect.gen(function* () {
        const { session } = harness
        const conversationId = yield* setupLive(harness)
        const captured = yield* session.commit((tx) => Effect.succeed(tx))
        const settled = { message: 'Transaction has settled' }
        const doc = yield* Effect.flip(captured.doc(LiveDoc, conversationId))
        expect(doc).toBeInstanceOf(SessionError.TransactionError)
        expect(doc).toMatchObject(settled)
        const conversation = yield* Effect.flip(captured.conversation(conversationId))
        expect(conversation).toMatchObject(settled)
        const task = yield* Effect.flip(
          captured.setTask({
            id: Ids.TaskId.make(999),
            conversationId,
            kind: 'missing',
            version: 1,
            input: null,
            background: false,
            abortRequested: false,
            state: { status: 'pending', checkpoint: { phase: 'start' } },
          }),
        )
        expect(task).toMatchObject(settled)
      }),
    ))

  test('rejects tokens whose semantics or version disagree with the stored incarnation', () =>
    run((harness) =>
      Effect.gen(function* () {
        const { session } = harness
        const conversationId = yield* setupLive(harness)
        const semantics = 'does not match the supplied definition semantics'
        const snapshot = yield* Effect.flip(session.snapshot(RewindableLiveDoc, conversationId))
        expect(snapshot).toBeInstanceOf(SessionError.DocumentError)
        expect(snapshot.message).toContain(semantics)
        const acquired = yield* Effect.flip(
          session.commit((tx) => tx.doc(RewindableLiveDoc, conversationId)),
        )
        expect(acquired).toBeInstanceOf(SessionError.DocumentError)
        expect(acquired.message).toContain(semantics)
        const migrated = yield* Effect.flip(
          session.commit((tx) => tx.doc(LiveDocV2, conversationId)),
        )
        expect(migrated).toBeInstanceOf(SessionError.DocumentError)
        expect(migrated.message).toContain('requires migration from version 1')
      }),
    ))

  test('rejects non-JSON initializer values and draft placements before Storage admission', () =>
    run((harness) =>
      Effect.gen(function* () {
        const { session } = harness
        const conversationId = yield* createConversation(session)
        const DateDoc = Document.make<Json.JsonObject>({
          kind: 'test.date',
          version: 1,
          scope: 'session',
          initial: () =>
            Effect.sync(() => {
              const value: Json.JsonObject = {}
              Reflect.set(value, 'at', new Date())
              return value
            }),
        })
        const initial = yield* Effect.flip(session.commit((tx) => tx.doc(DateDoc)))
        expect(initial).toBeInstanceOf(SessionError.DocumentError)
        expect(initial.message).toContain('strict JSON')
        const commits = harness.commits.length
        const placed = yield* Effect.flip(
          session.commit((tx) =>
            Effect.map(tx.doc(LiveDoc, conversationId), (live) => {
              Reflect.apply(Array.prototype.push, live.items, [undefined])
            }),
          ),
        )
        expect(placed).toBeInstanceOf(SessionError.DocumentError)
        expect(placed.message).toContain('strict JSON')
        expect(harness.commits.length).toBe(commits)
        expect(yield* session.snapshot(LiveDoc, conversationId)).toBeUndefined()
      }),
    ))

  test('rolls back prepared documents when batch assembly fails', () =>
    run((harness) =>
      Effect.gen(function* () {
        const { session } = harness
        const conversationId = yield* setupLive(harness)
        yield* session.commit((tx) =>
          Effect.map(tx.doc(CounterDoc), (counter) => {
            counter.count = 1
          }),
        )
        const live = yield* session.snapshot(LiveDoc, conversationId)
        const counter = yield* session.snapshot(CounterDoc)
        const commits = harness.commits.length
        const error = yield* Effect.flip(
          session.commit((tx) =>
            Effect.gen(function* () {
              ;(yield* tx.doc(LiveDoc, conversationId)).message = 'lost'
              ;(yield* tx.doc(CounterDoc)).count = 2
              // Replacing a missing task fails during assembly, after every change was prepared.
              yield* tx.setTask({
                id: Ids.TaskId.make(999),
                conversationId,
                kind: 'missing',
                version: 1,
                input: null,
                background: false,
                abortRequested: false,
                state: { status: 'pending', checkpoint: { phase: 'start' } },
              })
            }),
          ),
        )
        expect(error).toBeInstanceOf(SessionError.TransactionError)
        expect(error.message).toContain('Task 999 does not exist')
        expect(harness.commits.length).toBe(commits)
        expect(yield* session.snapshot(LiveDoc, conversationId)).toBe(live)
        expect(yield* session.snapshot(CounterDoc)).toBe(counter)
        yield* session.commit((tx) =>
          Effect.gen(function* () {
            ;(yield* tx.doc(LiveDoc, conversationId)).message = 'next'
            ;(yield* tx.doc(CounterDoc)).count = 3
          }),
        )
        expect((yield* present(session.snapshot(CounterDoc))).count).toBe(3)
      }),
    ))

  test('fails the Session after a Storage failure and publishes nothing', () =>
    run((harness) =>
      Effect.gen(function* () {
        const { session } = harness
        const conversationId = yield* setupLive(harness)
        const before = yield* present(session.snapshot(LiveDoc, conversationId))
        const published = (yield* harness.publications).length
        harness.failNextCommit()
        const error = yield* Effect.flip(
          session.commit((tx) =>
            Effect.map(tx.doc(LiveDoc, conversationId), (live) => {
              live.message = 'uncertain'
            }),
          ),
        )
        expect(error).toBeInstanceOf(SessionError.SessionFailed)
        expect(error).toMatchObject({ cause: { message: 'disk vanished' } })
        expect((yield* harness.publications).length).toBe(published)
        expect(before.message).toBeUndefined()
        const snapshot = yield* Effect.flip(session.snapshot(LiveDoc, conversationId))
        expect(snapshot).toBeInstanceOf(SessionError.SessionFailed)
        const later = yield* Effect.flip(session.commit(() => Effect.void))
        expect(later).toBeInstanceOf(SessionError.SessionFailed)
      }),
    ))

  test('keeps the previous revision unchanged through Storage settlement', () =>
    run((harness) =>
      Effect.gen(function* () {
        const { session } = harness
        const conversationId = yield* setupLive(harness)
        const before = yield* present(session.snapshot(LiveDoc, conversationId))
        const copy = structuredClone(before)
        const gate = yield* harness.holdCommits
        const commit = yield* Effect.forkChild(
          session.commit((tx) =>
            Effect.map(tx.doc(LiveDoc, conversationId), (live) => {
              live.items.push('c')
              live.nested.count = 9
            }),
          ),
        )
        yield* gate.entered
        // A committed read does not wait for the commit Storage holds.
        const during = yield* session
          .snapshot(LiveDoc, conversationId)
          .pipe(Effect.timeoutOption('200 millis'))
        yield* gate.release
        expect(Option.getOrUndefined(during)).toBe(before)
        expect(before).toEqual(copy)
        yield* Fiber.join(commit)
        const after = yield* present(session.snapshot(LiveDoc, conversationId))
        expect(after.items).toEqual(['a', 'b', 'c'])
        expect(before).toEqual(copy)
      }),
    ))

  test('retires documents and creates a new incarnation at the same address', () =>
    run((harness) =>
      Effect.gen(function* () {
        const { session } = harness
        const conversationId = yield* setupLive(harness)
        const oldId = first(documentChanges(last(yield* harness.publications))).record.id
        yield* session.commit((tx) =>
          Effect.gen(function* () {
            const live = yield* tx.doc(LiveDoc, conversationId)
            live.message = 'final'
            yield* tx.retireDoc(LiveDoc, conversationId)
            const replacement = yield* tx.doc(LiveDoc, conversationId)
            expect(replacement).not.toBe(live)
            replacement.message = 'new'
          }),
        )
        const writes = last(harness.commits)
        expect(writes).toHaveLength(3)
        expect(
          writes.filter((write) => write.type === 'document.change' && write.id === oldId),
        ).toHaveLength(1)
        expect(writes).toContainEqual({ type: 'document.retire', id: oldId })
        expect(createsLive(writes)).toHaveLength(1)
        const publication = last(yield* harness.publications)
        const [retired, created] = documentChanges(publication)
        expect(retired).toMatchObject({ record: { id: oldId }, value: null, ops: [] })
        expect(retired?.record.retiredAt).toBe(publication.seq)
        expect(created?.record.id).not.toBe(oldId)
        expect(created?.record.createdAt).toBe(publication.seq)
        expect(created).toMatchObject({ value: { message: 'new', items: [] }, ops: [] })
        same(yield* session.snapshot(LiveDoc, conversationId), created?.value)

        yield* session.commit((tx) => tx.retireDoc(LiveDoc, conversationId))
        expect(yield* session.snapshot(LiveDoc, conversationId)).toBeUndefined()
        yield* session.unloadDocuments
        expect(yield* session.snapshot(LiveDoc, conversationId)).toBeUndefined()
        // Retiring an absent address is a no-op.
        const commits = harness.commits.length
        yield* session.commit((tx) => tx.retireDoc(LiveDoc, conversationId))
        expect(harness.commits.length).toBe(commits)
      }),
    ))

  test('retires without acquisition and recreates both existing and absent addresses', () =>
    run((harness) =>
      Effect.gen(function* () {
        const { session } = harness
        const conversationId = yield* setupLive(harness)
        const oldId = first(documentChanges(last(yield* harness.publications))).record.id
        yield* session.unloadDocuments
        yield* session.commit((tx) =>
          Effect.gen(function* () {
            const [, live] = yield* Effect.all(
              [tx.retireDoc(LiveDoc, conversationId), tx.doc(LiveDoc, conversationId)],
              { concurrency: 'unbounded' },
            )
            live.message = 'replacement'
          }),
        )
        const existingWrites = last(harness.commits)
        expect(existingWrites).toHaveLength(2)
        expect(existingWrites).toContainEqual({ type: 'document.retire', id: oldId })
        expect(createsLive(existingWrites)).toHaveLength(1)

        yield* session.commit((tx) =>
          Effect.gen(function* () {
            const [, member] = yield* Effect.all(
              [tx.retireDoc(MemberDoc, 'absent'), tx.doc(MemberDoc, 'absent', 'seed')],
              { concurrency: 'unbounded' },
            )
            member.hits = 1
          }),
        )
        const absentWrites = last(harness.commits)
        expect(absentWrites.filter((write) => write.type === 'document.retire')).toHaveLength(0)
        expect(absentWrites.filter((write) => write.type === 'document.create')).toHaveLength(1)
        expect(yield* session.snapshot(MemberDoc, 'absent')).toEqual({ seed: 'seed', hits: 1 })
      }),
    ))

  test('retires the existing incarnation when retirement races a pending acquisition', () =>
    run((harness) =>
      Effect.gen(function* () {
        const { session } = harness
        const conversationId = yield* setupLive(harness)
        const oldId = first(documentChanges(last(yield* harness.publications))).record.id
        yield* session.commit((tx) =>
          Effect.gen(function* () {
            const [live] = yield* Effect.all(
              [tx.doc(LiveDoc, conversationId), tx.retireDoc(LiveDoc, conversationId)],
              { concurrency: 'unbounded' },
            )
            live.message = 'final'
          }),
        )
        const firstWrites = last(harness.commits)
        expect(firstWrites).toHaveLength(2)
        const change = firstWrites.find((write) => write.type === 'document.change')
        expect(change?.type === 'document.change' ? [change.id, change.content.kind] : []).toEqual([
          oldId,
          'delta',
        ])
        expect(firstWrites).toContainEqual({ type: 'document.retire', id: oldId })
        expect(yield* session.snapshot(LiveDoc, conversationId)).toBeUndefined()

        yield* session.commit((tx) =>
          Effect.map(tx.doc(LiveDoc, conversationId), (live) => {
            live.message = 'second'
          }),
        )
        const secondId = first(documentChanges(last(yield* harness.publications))).record.id
        yield* session.commit((tx) =>
          Effect.gen(function* () {
            const [old, , fresh] = yield* Effect.all(
              [
                tx.doc(LiveDoc, conversationId),
                tx.retireDoc(LiveDoc, conversationId),
                tx.doc(LiveDoc, conversationId),
              ],
              { concurrency: 'unbounded' },
            )
            expect(fresh).not.toBe(old)
            fresh.message = 'third'
          }),
        )
        const writes = last(harness.commits)
        expect(writes).toHaveLength(2)
        expect(writes).toContainEqual({ type: 'document.retire', id: secondId })
        expect(createsLive(writes)).toHaveLength(1)
        expect((yield* present(session.snapshot(LiveDoc, conversationId))).message).toBe('third')
      }),
    ))

  test('reloads an unloaded document from Storage', () =>
    run((harness) =>
      Effect.gen(function* () {
        const { session } = harness
        const conversationId = yield* setupLive(harness)
        const loaded = yield* present(session.snapshot(LiveDoc, conversationId))
        yield* session.unloadDocuments
        const reloaded = yield* present(session.snapshot(LiveDoc, conversationId))
        expect(reloaded).not.toBe(loaded)
        expect(reloaded).toEqual(loaded)
        yield* session.commit((tx) =>
          Effect.map(tx.doc(LiveDoc, conversationId), (live) => {
            live.items.push('c')
          }),
        )
        yield* session.unloadDocuments
        const after = yield* present(session.snapshot(LiveDoc, conversationId))
        expect(after.items).toEqual(['a', 'b', 'c'])
      }),
    ))

  test('returns the callback result and publishes the commit once', () =>
    run((harness) =>
      Effect.gen(function* () {
        const { session } = harness
        const conversationId = yield* setupLive(harness)
        const published = (yield* harness.publications).length
        const result = yield* session.commit((tx) =>
          Effect.map(tx.doc(LiveDoc, conversationId), (live) => {
            live.message = 'm'
            return 'done'
          }),
        )
        expect(result).toBe('done')
        expect((yield* harness.publications).length).toBe(published + 1)
      }),
    ))

  test('settles admitted commits before close and rejects later admission', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const scope = yield* Scope.make()
        const harness = yield* Scope.provide(open, scope)
        const { session } = harness
        const conversationId = yield* setupLive(harness)
        const gate = yield* harness.holdCommits
        const setMessage = (message: string) =>
          session.commit((tx) =>
            Effect.map(tx.doc(LiveDoc, conversationId), (live) => {
              live.message = message
            }),
          )
        const commit = yield* Effect.forkChild(setMessage('admitted'))
        yield* gate.entered
        const queued = yield* Effect.forkChild(setMessage('queued'))
        const admittedSnapshot = yield* Effect.forkChild(session.snapshot(MemberDoc, 'absent'))
        yield* Effect.yieldNow
        const closed = yield* Effect.forkChild(Scope.close(scope, Exit.void))
        yield* Effect.yieldNow
        const refused = yield* Effect.flip(session.commit(() => Effect.void))
        expect(refused).toBeInstanceOf(SessionError.SessionClosed)
        const read = yield* Effect.flip(session.snapshot(LiveDoc, conversationId))
        expect(read).toBeInstanceOf(SessionError.SessionClosed)
        yield* gate.release
        yield* Fiber.join(commit)
        yield* Fiber.join(queued)
        expect(yield* Fiber.join(admittedSnapshot)).toBeUndefined()
        yield* Fiber.join(closed)
        expect(yield* session.ended).toEqual({ reason: 'closed' })
      }).pipe(Effect.orDie),
    ))
})
