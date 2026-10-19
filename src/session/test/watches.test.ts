/* oxlint-disable max-lines, max-lines-per-function -- ported describe blocks */
/**
 * Ported from pi's `packages/durable/test/session-watches.test.ts`. pi's `start(listener)` is a
 * run of `watch.changes`, and `stop()` or cancellation closes the watch's scope. pi's watch end
 * reasons map onto the stream: `retired` ends it after the `[['r', null]]` change,
 * `session_closed` ends it and drops pending changes, `session_failed` fails it with
 * `SessionFailed`, and `listener_error` is a failing consumer of that one watch.
 *
 * Not ported:
 * - "preserves commit Context values without inheriting producer cancellation": a change carries
 *   no Context, and an Effect consumer runs in its own fiber, so there is no producer
 *   cancellation to inherit.
 * - "supports idempotent stop and rejects repeated or late start": closing a Scope is idempotent
 *   by construction, and a stream has no `start` to repeat or call late.
 * - The `inline` delivery assertion of "keeps the acquisition revision until start": a stream
 *   delivers only when its consumer pulls.
 * - Storage mint and document read counts: the recorder here counts commits only.
 */
import { describe, expect, test } from 'bun:test'
import { Cause, Deferred, Effect, Exit, Fiber, PubSub, Scope, Stream } from 'effect'
import { Storage } from '../../storage/index.ts'
import { applyBatches } from '../../storage/internal/delta.ts'
import { Document, Session, SessionError } from '../index.ts'
import type { Observation } from '../index.ts'
import { openSession, record, runSession } from './support.ts'

type State = { value: number; items: Array<string>; retained: { label: string } }

const StateDoc = Document.make<State>({
  kind: 'watch.state',
  version: 1,
  scope: 'session',
  initial: () => Effect.succeed({ value: 0, items: ['a', 'b'], retained: { label: 'stable' } }),
})

type Watch = Observation.Watch<State>
type Change = Observation.Change<State>

const createState = (session: Session.Service) =>
  session.commit((tx) => Effect.asVoid(tx.doc(StateDoc)))

const setValue = (session: Session.Service, value: number) =>
  session.commit((tx) =>
    Effect.map(tx.doc(StateDoc), (state) => {
      state.value = value
    }),
  )

const retire = (session: Session.Service) => session.commit((tx) => tx.retireDoc(StateDoc))

/** A watch of the present `StateDoc`. */
const watchState = (session: Session.Service) =>
  Effect.flatMap(session.watchDoc(StateDoc), Effect.fromNullishOr)

const valueOf = (change: Change) => change.value?.value ?? null

const documentChanges = (publication: Session.CommitPublication) =>
  publication.changes.filter(
    (change): change is Extract<Session.CommitChange, { readonly type: 'document' }> =>
      change.type === 'document',
  )

/**
 * Consume a watch in a child fiber that blocks on its first change until `release` completes,
 * recording each value. The fiber ends when the stream does.
 */
const blockedConsumer = (watch: Watch) =>
  Effect.gen(function* () {
    const entered = yield* Deferred.make<void>()
    const release = yield* Deferred.make<void>()
    const values: Array<number | null> = []
    const fiber = yield* Effect.forkChild(
      Stream.runForEach(watch.changes, (change) =>
        Effect.gen(function* () {
          values.push(valueOf(change))
          if (values.length === 1) {
            yield* Deferred.done(entered, Exit.void)
            yield* Deferred.await(release)
          }
        }),
      ),
    )
    return { entered, release, values, fiber }
  })

/** A Session in its own scope, so a test can close it. */
const openClosable = Effect.gen(function* () {
  const scope = yield* Scope.make()
  const harness = yield* Scope.provide(openSession, scope)
  return { ...harness, close: Scope.close(scope, Exit.void) }
})

describe('Session document watches', () => {
  test('never creates an absent document', () =>
    runSession(({ session, recorder }) =>
      Effect.gen(function* () {
        const commits = recorder.commits.length
        expect(yield* session.watchDoc(StateDoc)).toBeUndefined()
        expect(recorder.commits.length).toBe(commits)
        expect(yield* session.snapshot(StateDoc)).toBeUndefined()
      }),
    ))

  test('keeps the acquisition revision and delivers exact committed frames', () =>
    runSession(({ session }) =>
      Effect.gen(function* () {
        yield* createState(session)
        const watch = yield* watchState(session)
        const initial = watch.value
        const subscription = yield* session.subscribe
        yield* setValue(session, 1)
        const firstPublished = documentChanges(yield* PubSub.take(subscription))[0]
        yield* setValue(session, 2)
        const secondPublished = documentChanges(yield* PubSub.take(subscription))[0]
        expect(watch.value).toBe(initial)

        const deliveries = yield* Stream.runCollect(Stream.take(watch.changes, 2))
        expect(deliveries.map((change) => valueOf(change))).toEqual([1, 2])
        // Published values are untyped JSON; identity is what matters.
        const frames: ReadonlyArray<{ readonly value: unknown; readonly ops: unknown }> = deliveries
        expect(frames[0]?.value).toBe(firstPublished?.value)
        expect(frames[0]?.ops).toBe(firstPublished?.ops)
        expect(frames[1]?.value).toBe(secondPublished?.value)
        expect(frames[1]?.ops).toBe(secondPublished?.ops)
        expect(initial.value).toBe(0)
      }),
    ))

  test('serializes consumption and buffers exact frames committed while one is in flight', () =>
    runSession(({ session }) =>
      Effect.gen(function* () {
        yield* createState(session)
        const watch = yield* watchState(session)
        const entered = yield* Deferred.make<void>()
        const release = yield* Deferred.make<void>()
        const values: Array<number> = []
        let active = 0
        let maxActive = 0
        const consumer = yield* Effect.forkChild(
          Stream.runForEach(Stream.take(watch.changes, 20), (change) =>
            Effect.gen(function* () {
              active++
              maxActive = Math.max(maxActive, active)
              values.push(valueOf(change) ?? -1)
              if (values.length === 1) {
                yield* Deferred.done(entered, Exit.void)
                yield* Deferred.await(release)
              }
              active--
            }),
          ),
        )
        yield* setValue(session, 1)
        yield* Deferred.await(entered)
        for (let value = 2; value <= 20; value++) yield* setValue(session, value)
        expect(values).toEqual([1])
        yield* Deferred.done(release, Exit.void)
        yield* Fiber.join(consumer)
        expect(maxActive).toBe(1)
        expect(values).toEqual(Array.from({ length: 20 }, (_, index) => index + 1))
      }),
    ))

  test('allows a consumer to initiate a later Session commit', () =>
    runSession(({ session }) =>
      Effect.gen(function* () {
        yield* createState(session)
        const watch = yield* watchState(session)
        const values: Array<number> = []
        const consumer = yield* Effect.forkChild(
          Stream.runForEach(Stream.take(watch.changes, 2), (change) => {
            const current = valueOf(change) ?? -1
            values.push(current)
            return current === 1 ? setValue(session, 2) : Effect.void
          }),
        )
        yield* setValue(session, 1)
        yield* Fiber.join(consumer)
        expect(values).toEqual([1, 2])
      }),
    ))

  test('collapses 101 pending commits to one root replacement', () =>
    runSession(({ session }) =>
      Effect.gen(function* () {
        yield* createState(session)
        const watch = yield* watchState(session)
        for (let value = 1; value <= 101; value++) yield* setValue(session, value)
        const newest = yield* session.snapshot(StateDoc)
        // A later commit shows the reset was the only pending change.
        yield* setValue(session, 102)
        const deliveries = yield* Stream.runCollect(Stream.take(watch.changes, 2))
        expect(deliveries[0]).toEqual({ value: newest ?? null, ops: [['r', newest ?? null]] })
        expect(deliveries[0]?.value).toBe(newest)
        expect(valueOf(deliveries[0]!)).toBe(101)
        expect(valueOf(deliveries[1]!)).toBe(102)
        expect(deliveries[1]?.ops).toEqual([['s', ['value'], 102]])
      }),
    ))

  test('never folds the in-flight frame into an overflow reset', () =>
    runSession(({ session }) =>
      Effect.gen(function* () {
        yield* createState(session)
        const watch = yield* watchState(session)
        const entered = yield* Deferred.make<void>()
        const release = yield* Deferred.make<void>()
        const deliveries: Array<Change> = []
        const consumer = yield* Effect.forkChild(
          Stream.runForEach(Stream.take(watch.changes, 2), (change) =>
            Effect.gen(function* () {
              deliveries.push(change)
              if (deliveries.length === 1) {
                yield* Deferred.done(entered, Exit.void)
                yield* Deferred.await(release)
              }
            }),
          ),
        )
        yield* setValue(session, 1)
        yield* Deferred.await(entered)
        for (let value = 2; value <= 102; value++) yield* setValue(session, value)
        const newest = yield* session.snapshot(StateDoc)
        yield* Deferred.done(release, Exit.void)
        yield* Fiber.join(consumer)
        expect(deliveries.map((change) => valueOf(change))).toEqual([1, 102])
        expect(deliveries[1]).toEqual({ value: newest ?? null, ops: [['r', newest ?? null]] })
        expect(deliveries).toHaveLength(2)
      }),
    ))

  test('folds retirement into an overflow reset and then ends', () =>
    runSession(({ session }) =>
      Effect.gen(function* () {
        yield* createState(session)
        const watch = yield* watchState(session)
        for (let value = 1; value <= 100; value++) yield* setValue(session, value)
        yield* retire(session)
        const deliveries = yield* Stream.runCollect(watch.changes)
        expect(deliveries).toEqual([{ value: null, ops: [['r', null]] }])
      }),
    ))

  test('delivers replayable structural no-op commits instead of suppressing them', () =>
    runSession(({ session }) =>
      Effect.gen(function* () {
        yield* createState(session)
        const watch = yield* watchState(session)
        const initial = watch.value
        yield* session.commit((tx) =>
          Effect.map(tx.doc(StateDoc), (state) => {
            const first = state.items.shift()
            if (first !== undefined) state.items.unshift(first)
          }),
        )
        const newest = yield* session.snapshot(StateDoc)
        expect(newest).not.toBe(initial)
        expect(newest).toEqual(initial)
        const [delivered] = yield* Stream.runCollect(Stream.take(watch.changes, 1))
        expect(delivered?.value).toBe(newest)
        expect(delivered?.ops.length).toBeGreaterThan(0)
      }),
    ))

  test('keeps earlier immutable revisions stable', () =>
    runSession(({ session }) =>
      Effect.gen(function* () {
        yield* createState(session)
        const watch = yield* watchState(session)
        const initial = watch.value
        yield* setValue(session, 1)
        const [delivered] = yield* Stream.runCollect(Stream.take(watch.changes, 1))
        expect(delivered?.value).toBe(yield* session.snapshot(StateDoc))
        expect(delivered?.value).not.toBe(initial)
        expect(delivered?.value?.retained).toBe(initial.retained)
        expect(initial.value).toBe(0)
      }),
    ))

  test('delivers retirement and does not follow recreation', () =>
    runSession(({ session }) =>
      Effect.gen(function* () {
        yield* createState(session)
        const oldWatch = yield* watchState(session)
        yield* session.commit((tx) =>
          Effect.gen(function* () {
            yield* tx.retireDoc(StateDoc)
            ;(yield* tx.doc(StateDoc)).value = 10
          }),
        )
        // The stream ends after the retirement change.
        const values = yield* Stream.runCollect(oldWatch.changes)
        expect(values).toEqual([{ value: null, ops: [['r', null]] }])

        const replacement = yield* watchState(session)
        expect(replacement.value.value).toBe(10)
        yield* setValue(session, 11)
        const [next] = yield* Stream.runCollect(Stream.take(replacement.changes, 1))
        expect(valueOf(next!)).toBe(11)
      }),
    ))

  test('Session close discards retirement buffered before consumption', () =>
    Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const { session, close } = yield* openClosable
          yield* createState(session)
          const watch = yield* watchState(session)
          const baseline = watch.value
          yield* retire(session)
          yield* close
          expect(yield* session.ended).toEqual({ reason: 'closed' })
          expect(yield* Stream.runCollect(watch.changes)).toEqual([])
          expect(watch.value).toBe(baseline)
        }),
      ).pipe(Effect.orDie),
    ))

  test('Session close discards retirement behind an in-flight consumer', () =>
    Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const { session, close } = yield* openClosable
          yield* createState(session)
          const watch = yield* watchState(session)
          const { entered, release, values, fiber } = yield* blockedConsumer(watch)
          yield* setValue(session, 1)
          yield* Deferred.await(entered)
          yield* retire(session)
          yield* close
          yield* Deferred.done(release, Exit.void)
          yield* Fiber.join(fiber)
          expect(values).toEqual([1])
        }),
      ).pipe(Effect.orDie),
    ))

  test('interrupts acquisition without leaking a registered watch', () =>
    Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const memory = yield* Storage.makeMemory
          const recorder = record(memory)
          let held:
            | { entered: Deferred.Deferred<void>; release: Deferred.Deferred<void> }
            | undefined
          const gated: Storage.Service = {
            ...recorder.storage,
            findDocument: (...args) =>
              Effect.gen(function* () {
                const gate = held
                if (gate !== undefined) {
                  held = undefined
                  yield* Deferred.done(gate.entered, Exit.void)
                  yield* Deferred.await(gate.release)
                }
                return yield* recorder.storage.findDocument(...args)
              }),
          }
          const scope = yield* Scope.make()
          const session = yield* Scope.provide(
            Session.make.pipe(Effect.provideService(Storage.Storage, gated)),
            scope,
          )
          yield* createState(session)
          yield* session.unloadDocuments
          const gate = {
            entered: yield* Deferred.make<void>(),
            release: yield* Deferred.make<void>(),
          }
          held = gate
          const acquisition = yield* Effect.forkChild(Effect.scoped(session.watchDoc(StateDoc)))
          yield* Deferred.await(gate.entered)
          const interrupted = yield* Effect.forkChild(Fiber.interrupt(acquisition))
          // Let the interruption reach the held acquisition before Storage answers.
          yield* Effect.yieldNow
          yield* Effect.yieldNow
          yield* Deferred.done(gate.release, Exit.void)
          yield* Fiber.join(interrupted)
          const exit = yield* Fiber.await(acquisition)
          expect(Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause)).toBe(true)
          // The Session is still usable and closes cleanly.
          yield* setValue(session, 1)
          expect((yield* session.snapshot(StateDoc))?.value).toBe(1)
          yield* Scope.close(scope, Exit.void)
          expect(yield* session.ended).toEqual({ reason: 'closed' })
        }),
      ).pipe(Effect.orDie),
    ))

  test('closing the watch scope stops future delivery without aborting an in-flight consumer', () =>
    runSession(({ session }) =>
      Effect.gen(function* () {
        yield* createState(session)
        const watchScope = yield* Scope.make()
        const watch = yield* Scope.provide(watchState(session), watchScope)
        const { entered, release, values, fiber } = yield* blockedConsumer(watch)
        yield* setValue(session, 1)
        yield* Deferred.await(entered)
        yield* Scope.close(watchScope, Exit.void)
        yield* setValue(session, 2)
        yield* Deferred.done(release, Exit.void)
        const exit = yield* Fiber.await(fiber)
        expect(Exit.isSuccess(exit) || Cause.hasInterruptsOnly(exit.cause)).toBe(true)
        expect(values).toEqual([1])
      }),
    ))

  test('Session close stops future delivery without joining an in-flight consumer', () =>
    Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const { session, close } = yield* openClosable
          yield* createState(session)
          const watch = yield* watchState(session)
          const { entered, release, values, fiber } = yield* blockedConsumer(watch)
          yield* setValue(session, 1)
          yield* Deferred.await(entered)
          // Close completes while the consumer is still blocked.
          yield* close
          expect(yield* session.ended).toEqual({ reason: 'closed' })
          yield* Deferred.done(release, Exit.void)
          yield* Fiber.join(fiber)
          expect(values).toEqual([1])
        }),
      ).pipe(Effect.orDie),
    ))

  test('Session failure fails the stream with SessionFailed', () =>
    runSession(({ session, recorder }) =>
      Effect.gen(function* () {
        yield* createState(session)
        const watch = yield* watchState(session)
        recorder.failCommits()
        const failed = yield* Effect.flip(setValue(session, 1))
        expect(failed).toBeInstanceOf(SessionError.SessionFailed)
        const error = yield* Effect.flip(Stream.runCollect(watch.changes))
        expect(error).toBeInstanceOf(SessionError.SessionFailed)
      }),
    ))

  test('a failing consumer of one watch does not affect another watch', () =>
    runSession(({ session }) =>
      Effect.gen(function* () {
        yield* createState(session)
        const failing = yield* watchState(session)
        const healthy = yield* watchState(session)
        const failed = yield* Effect.forkChild(
          Stream.runForEach(failing.changes, () => Effect.fail('consumer failed' as const)),
        )
        let healthyCalls = 0
        const healthyConsumer = yield* Effect.forkChild(
          Stream.runForEach(Stream.take(healthy.changes, 1), () =>
            Effect.sync(() => {
              healthyCalls++
            }),
          ),
        )
        yield* setValue(session, 1)
        const end = yield* Effect.flip(Fiber.join(failed))
        expect(end).toBe('consumer failed')
        yield* Fiber.join(healthyConsumer)
        expect(healthyCalls).toBe(1)
      }),
    ))

  test('hydrates migration without writing and observes the later exact edit', () =>
    runSession(({ session, recorder }) =>
      Effect.gen(function* () {
        const Old = Document.make<{ value: number }>({
          kind: 'watch.migration',
          version: 1,
          scope: 'session',
          initial: () => Effect.succeed({ value: 3 }),
        })
        const Current = Document.make<{ value: number; migrated: boolean }>({
          kind: 'watch.migration',
          version: 2,
          scope: 'session',
          initial: () => Effect.succeed({ value: 0, migrated: false }),
          migrate: (value) => Effect.succeed({ value: Number(value.value), migrated: true }),
        })
        yield* session.commit((tx) => tx.doc(Old))
        yield* session.unloadDocuments
        const commits = recorder.commits.length
        const watch = yield* Effect.flatMap(session.watchDoc(Current), Effect.fromNullishOr)
        expect(watch.value).toEqual({ value: 3, migrated: true })
        expect(recorder.commits.length).toBe(commits)
        // The migration-only base changes nothing for the watch.
        yield* session.commit((tx) => tx.doc(Current))
        yield* session.commit((tx) =>
          Effect.map(tx.doc(Current), (current) => {
            current.value = 4
          }),
        )
        const [next] = yield* Stream.runCollect(Stream.take(watch.changes, 1))
        expect(next?.value?.value).toBe(4)
        expect(next?.ops).toEqual([['s', ['value'], 4]])
      }),
    ))

  test('can replay every delivered exact operation batch from the acquisition revision', () =>
    runSession(({ session }) =>
      Effect.gen(function* () {
        yield* createState(session)
        const watch = yield* watchState(session)
        yield* session.commit((tx) =>
          Effect.map(tx.doc(StateDoc), (state) => {
            state.value = 3
            state.items.splice(0, 1)
          }),
        )
        const [change] = yield* Stream.runCollect(Stream.take(watch.changes, 1))
        const replica = yield* applyBatches(watch.value, [change?.ops ?? []])
        expect(replica).toEqual(change?.value ?? {})
        expect(replica).toEqual({ value: 3, items: ['b'], retained: { label: 'stable' } })
      }),
    ))

  test('continues after the tracker cache unloads', () =>
    runSession(({ session }) =>
      Effect.gen(function* () {
        yield* createState(session)
        const watch = yield* watchState(session)
        const baseline = watch.value
        yield* session.unloadDocuments
        const reloaded = yield* session.snapshot(StateDoc)
        expect(reloaded).toEqual(baseline)
        expect(reloaded).not.toBe(baseline)
        yield* setValue(session, 7)
        const [next] = yield* Stream.runCollect(Stream.take(watch.changes, 1))
        expect(valueOf(next!)).toBe(7)
      }),
    ))
})
