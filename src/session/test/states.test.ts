/* oxlint-disable max-lines, max-lines-per-function -- ported describe blocks */
/**
 * Ported from pi's `packages/durable/test/session-states.test.ts`. pi's document state is a Chord
 * replicated state; here it is `Observation.State`, so Chord's `value` maps onto `state.get` and
 * its subscriptions onto `state.changes`, and `dispose` onto closing the state's scope.
 *
 * Mapped rather than ported verbatim:
 * - "contiguous Chord deliveries": Chord delivery sequence numbers do not exist here. The test
 *   checks that `state.changes`, pulled between commits, delivers the baseline and then every
 *   later value.
 * - "shares exact committed value references with Chord": checks that `state.get` is the very
 *   value the commit published, and shares unchanged subtrees with the snapshot.
 * - "captures a late baseline without redelivering an already covered commit": checks that the
 *   first value `state.changes` delivers is the late baseline, and that the next is the next
 *   commit's value alone.
 *
 * Skipped: the `mintCount` assertion of "never creates an absent document", since this Storage
 * mints no IDs outside commits; the commit count shows nothing was created.
 */
import { describe, expect, test } from 'bun:test'
import { Effect, Exit, PubSub, Scope, Stream } from 'effect'
import { Storage } from '../../storage/index.ts'
import { Document, Session } from '../index.ts'
import { createConversation, openSession, record, runSession } from './support.ts'
import type { Harness, Recorder } from './support.ts'

type State = { value: number; retained: { label: string } }

const StateDoc = Document.make<State>({
  kind: 'state.state',
  version: 1,
  scope: 'session',
  initial: () => Effect.succeed({ value: 0, retained: { label: 'stable' } }),
})

const FamilyDoc = Document.makeFamily<{ value: number }, string>({
  kind: 'state.family',
  version: 1,
  family: true,
  scope: 'session',
  initial: (seed) => Effect.succeed({ value: seed.length }),
})

/** Run a test body against a Session whose state document exists. */
const runWithState = <A, E>(body: (harness: Harness) => Effect.Effect<A, E, Scope.Scope>) =>
  runSession((harness) =>
    Effect.andThen(
      harness.session.commit((tx) => Effect.asVoid(tx.doc(StateDoc))),
      body(harness),
    ),
  )

const setValue = (session: Session.Service, value: number) =>
  session.commit((tx) =>
    Effect.map(tx.doc(StateDoc), (draft) => {
      draft.value = value
    }),
  )

/** The document state of `StateDoc`, which must exist. */
const stateOf = (session: Session.Service) =>
  Effect.flatMap(session.documentState(StateDoc), Effect.fromNullishOr)

/** A Session that counts its Storage `document` reads, as pi's `documentReadCount`. */
const openCounted = Effect.gen(function* () {
  const memory = yield* Storage.makeMemory
  let reads = 0
  const recorder: Recorder = record({
    ...memory,
    document: (id, at) =>
      Effect.suspend(() => {
        reads++
        return memory.document(id, at)
      }),
  })
  const session = yield* Session.make.pipe(Effect.provideService(Storage.Storage, recorder.storage))
  return { session, recorder, reads: () => reads }
})

describe('Session document states', () => {
  test('never creates an absent document', () =>
    runSession(({ session, recorder }) =>
      Effect.gen(function* () {
        const commits = recorder.commits.length
        expect(yield* session.documentState(StateDoc)).toBeUndefined()
        expect(yield* session.documentState(FamilyDoc, 'missing')).toBeUndefined()
        expect(recorder.commits).toHaveLength(commits)
        expect(yield* session.snapshot(StateDoc)).toBeUndefined()
      }),
    ))

  test('returns an immediately hydrated read-only state that delivers every later value', () =>
    runWithState(({ session }) =>
      Effect.gen(function* () {
        const baseline = yield* Effect.flatMap(session.snapshot(StateDoc), Effect.fromNullishOr)
        const state = yield* stateOf(session)
        expect(yield* state.get).toBe(baseline)
        const pull = yield* Stream.toPull(state.changes)
        const deliveries: Array<Readonly<State> | null> = [...(yield* pull)]

        yield* setValue(session, 1)
        deliveries.push(...(yield* pull))
        yield* setValue(session, 2)
        deliveries.push(...(yield* pull))

        expect(deliveries[0]).toBe(baseline)
        expect(deliveries.map((value) => value?.value)).toEqual([0, 1, 2])
        expect(yield* state.get).toBe(
          yield* Effect.flatMap(session.snapshot(StateDoc), Effect.fromNullishOr),
        )
      }),
    ))

  test('creates independent disposable states for one incarnation', () =>
    runWithState(({ session }) =>
      Effect.gen(function* () {
        const scope = yield* Scope.make()
        const first = yield* Scope.provide(stateOf(session), scope)
        const second = yield* stateOf(session)
        expect(second).not.toBe(first)
        yield* setValue(session, 1)
        expect((yield* first.get)?.value).toBe(1)
        expect((yield* second.get)?.value).toBe(1)

        yield* Scope.close(scope, Exit.void)
        yield* setValue(session, 2)
        expect((yield* first.get)?.value).toBe(1)
        expect((yield* second.get)?.value).toBe(2)
        // A disposed state's changes deliver its last value and end.
        expect((yield* Stream.runCollect(first.changes)).map((value) => value?.value)).toEqual([1])
      }),
    ))

  test('shares exact committed value references with the publication and the snapshot', () =>
    runWithState(({ session }) =>
      Effect.gen(function* () {
        const state = yield* stateOf(session)
        const publication = yield* Effect.scoped(
          Effect.gen(function* () {
            const subscription = yield* session.subscribe
            yield* setValue(session, 4)
            return yield* PubSub.take(subscription)
          }),
        )
        const change = publication.changes.find((each) => each.type === 'document')
        expect(change?.type).toBe('document')
        const current: unknown = yield* state.get
        expect(current).toBe(change?.type === 'document' ? change.value : undefined)
        expect((yield* state.get)?.retained).toBe((yield* session.snapshot(StateDoc))?.retained)
      }),
    ))

  test('captures a late baseline without redelivering an already covered commit', () =>
    runWithState(({ session }) =>
      Effect.gen(function* () {
        yield* setValue(session, 1)
        const state = yield* stateOf(session)
        const pull = yield* Stream.toPull(state.changes)
        expect((yield* state.get)?.value).toBe(1)
        expect((yield* pull).map((value) => value?.value)).toEqual([1])
        yield* setValue(session, 2)
        expect((yield* pull).map((value) => value?.value)).toEqual([2])
      }),
    ))

  test('publishes null retirement and never follows a replacement incarnation', () =>
    runWithState(({ session }) =>
      Effect.gen(function* () {
        const oldState = yield* stateOf(session)
        yield* session.commit((tx) =>
          Effect.gen(function* () {
            yield* tx.retireDoc(StateDoc)
            ;(yield* tx.doc(StateDoc)).value = 10
          }),
        )
        expect(yield* oldState.get).toBeNull()

        const replacement = yield* stateOf(session)
        expect((yield* replacement.get)?.value).toBe(10)
        yield* setValue(session, 11)
        expect(yield* oldState.get).toBeNull()
        expect((yield* replacement.get)?.value).toBe(11)
        // The retired state's changes end with its retirement.
        expect(yield* Stream.runCollect(oldState.changes)).toEqual([null])
      }),
    ))

  test('cold-loads a definition-free fork copy', () =>
    Effect.runPromise(
      Effect.scoped(
        Effect.orDie(
          Effect.gen(function* () {
            const Copied = Document.make<{ value: number }>({
              kind: 'state.copied',
              version: 1,
              scope: 'conversation',
              history: 'latest',
              fork: 'current',
              initial: () => Effect.succeed({ value: 0 }),
            })
            const { session, reads } = yield* openCounted
            const parentId = yield* createConversation(session)
            const entry = yield* session.commit((tx) =>
              Effect.gen(function* () {
                const created = yield* tx.appendEntry(parentId, { kind: 'point' })
                ;(yield* tx.doc(Copied, parentId)).value = 7
                return created
              }),
            )
            const childId = yield* session.commit((tx) =>
              Effect.map(
                tx.forkConversation(parentId, entry.id, { ownership: { kind: 'ownerless' } }),
                ({ id }) => id,
              ),
            )
            const before = reads()
            const state = yield* Effect.flatMap(
              session.documentState(Copied, childId),
              Effect.fromNullishOr,
            )
            expect(yield* state.get).toEqual({ value: 7 })
            expect(reads()).toBeGreaterThan(before)
          }),
        ),
      ),
    ))

  test('hydrates a migrated tracker without writing and skips an equal version-base update', () =>
    runSession(({ session, recorder }) =>
      Effect.gen(function* () {
        const Old = Document.make<{ value: number }>({
          kind: 'state.migration',
          version: 1,
          scope: 'session',
          initial: () => Effect.succeed({ value: 3 }),
        })
        const Current = Document.make<{ value: number; migrated: boolean }>({
          kind: 'state.migration',
          version: 2,
          scope: 'session',
          initial: () => Effect.succeed({ value: 0, migrated: false }),
          migrate: (value) =>
            Effect.succeed({
              value: typeof value['value'] === 'number' ? value['value'] : 0,
              migrated: true,
            }),
        })
        yield* session.commit((tx) => Effect.asVoid(tx.doc(Old)))
        yield* session.unloadDocuments
        const commits = recorder.commits.length
        const state = yield* Effect.flatMap(session.documentState(Current), Effect.fromNullishOr)
        expect(yield* state.get).toEqual({ value: 3, migrated: true })
        expect(recorder.commits).toHaveLength(commits)
        const baseline = yield* state.get

        yield* session.commit((tx) => Effect.asVoid(tx.doc(Current)))
        expect(recorder.commits).toHaveLength(commits + 1)
        expect(yield* state.get).toBe(baseline)
        yield* session.commit((tx) =>
          Effect.map(tx.doc(Current), (draft) => {
            draft.value = 4
          }),
        )
        expect(yield* state.get).toEqual({ value: 4, migrated: true })
      }),
    ))

  test('continues from exact committed values after the tracker cache unloads', () =>
    Effect.runPromise(
      Effect.scoped(
        Effect.orDie(
          Effect.gen(function* () {
            const { session, reads } = yield* openCounted
            yield* session.commit((tx) => Effect.asVoid(tx.doc(StateDoc)))
            const state = yield* stateOf(session)
            const baseline = yield* Effect.flatMap(state.get, Effect.fromNullishOr)
            const before = reads()
            yield* session.unloadDocuments
            const reloaded = yield* session.snapshot(StateDoc)
            expect(reloaded).toEqual(baseline)
            expect(reloaded).not.toBe(baseline)
            expect(reads()).toBeGreaterThan(before)
            yield* setValue(session, 6)
            expect((yield* state.get)?.value).toBe(6)
          }),
        ),
      ),
    ))

  test('exposes trusted shared immutable values without freezing', () =>
    runWithState(({ session }) =>
      Effect.gen(function* () {
        const snapshot = yield* session.snapshot(StateDoc)
        const state = yield* stateOf(session)
        const value = yield* state.get
        expect(value).toBe(snapshot ?? null)
        expect(Object.isFrozen(value)).toBe(false)
        expect(Object.isFrozen(value?.retained)).toBe(false)
      }),
    ))

  test('keeps its last value and ends its changes when the Session closes', () =>
    Effect.runPromise(
      Effect.scoped(
        Effect.orDie(
          Effect.gen(function* () {
            const sessionScope = yield* Scope.make()
            const { session } = yield* Scope.provide(openSession, sessionScope)
            yield* session.commit((tx) => Effect.asVoid(tx.doc(StateDoc)))
            const state = yield* stateOf(session)
            yield* setValue(session, 5)
            yield* Scope.close(sessionScope, Exit.void)
            expect((yield* state.get)?.value).toBe(5)
            expect((yield* Stream.runCollect(state.changes)).map((value) => value?.value)).toEqual([
              5,
            ])
          }),
        ),
      ),
    ))
})
