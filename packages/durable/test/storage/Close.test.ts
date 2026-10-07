import * as Identity from '../../src/Identity.ts'
import { assert, describe, it } from '@effect/vitest'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Fiber from 'effect/Fiber'
import * as Scope from 'effect/Scope'
import * as Exit from 'effect/Exit'
import * as Cause from 'effect/Cause'
import * as Record from '@effect-harness/durable/Record'
import * as Session from '@effect-harness/durable/Session'
import * as Store from '@effect-harness/durable/Store'
import * as Backend from '../../src/storage/internal/backend.ts'
import * as Cancellation from '@effect-harness/durable/workflow/Cancellation'
import { rejected, Io } from '@effect-harness/durable/StorageError'

const child = Effect.acquireRelease(Scope.make(), (scope, exit) => Scope.close(scope, exit))
describe('scoped Session and Store cleanup', () => {
  it.live(
    'seals admission, drains admitted work and preserves cleanup after a receipt waiter is cancelled',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const scope = yield* child
          let saved: Backend.Snapshot = { state: Record.emptyState(), frames: [] }
          let releases = 0
          const store = yield* Backend.make(
            {
              load: Effect.sync(() => saved),
              committed: Effect.sync(() => saved),
              save: (value) =>
                Effect.sync(() => {
                  saved = value
                }),
              atomic: (effect) => effect,
            },
            Effect.sync(() => {
              releases++
            }),
          ).pipe(Scope.provide(scope))
          const session = yield* Session.make.pipe(
            Effect.provideService(Store.Store, store),
            Scope.provide(scope),
          )
          assert.strictEqual(Reflect.has(store, 'close'), false)
          assert.strictEqual(Reflect.has(session, 'close'), false)
          const root = yield* session.root()
          const entered = yield* Deferred.make<void>()
          const release = yield* Deferred.make<void>()
          const admitted = yield* session
            .transaction((tx) =>
              Effect.gen(function* () {
                yield* Deferred.succeed(entered, undefined)
                yield* Deferred.await(release)
                return yield* tx.appendEntry(root.id, { kind: 'admitted' })
              }),
            )
            .pipe(Effect.forkScoped)
          yield* Deferred.await(entered)
          const closing = yield* Scope.close(scope, Exit.void).pipe(Effect.forkScoped)
          yield* Effect.yieldNow
          const late = yield* session
            .transaction((tx) => tx.appendEntry(root.id, { kind: 'late' }))
            .pipe(Effect.result)
          assert.strictEqual(late._tag, 'Failure')
          if (late._tag === 'Failure') assert.strictEqual(late.failure.reason._tag, 'Closed')
          assert.strictEqual(releases, 0)
          const waiter = yield* session.awaitClosed.pipe(Effect.forkScoped)
          yield* Fiber.interrupt(waiter)
          const cancelled = yield* Fiber.await(waiter)
          assert.ok(Exit.isFailure(cancelled))
          // Only this receipt waiter is cancelled; runtime-assigned fiber IDs vary.
          if (Exit.isFailure(cancelled)) assert.ok(Cause.hasInterruptsOnly(cancelled.cause))
          const repeated = yield* session.awaitClosed.pipe(Effect.forkScoped)
          yield* Deferred.succeed(release, undefined)
          const entry = yield* Fiber.join(admitted)
          yield* Fiber.join(closing)
          yield* Fiber.join(repeated)
          yield* Scope.close(scope, Exit.void)
          yield* session.awaitClosed
          assert.strictEqual(releases, 1)
          assert.deepStrictEqual(
            saved.state.entries.map((item) => item.entry.id),
            [entry.id],
          )
          const sealed = yield* session.committed.pipe(Effect.flip)
          assert.strictEqual(sealed.reason._tag, 'Closed')
          assert.strictEqual(sealed.message, 'Session is closed')
        }),
      ),
  )
  it.live(
    'retains typed backend failure after handle emptiness and waits for admitted readers',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const scope = yield* child
          const entered = yield* Deferred.make<void>()
          const release = yield* Deferred.make<void>()
          let releases = 0
          const failure = rejected('release fixture', Io)
          const value: Backend.Snapshot = { state: Record.emptyState(), frames: [] }
          const store = yield* Backend.make(
            {
              load: Effect.succeed(value),
              committed: Deferred.succeed(entered, undefined).pipe(
                Effect.andThen(Deferred.await(release)),
                Effect.as(value),
              ),
              save: () => Effect.void,
              atomic: (effect) => effect,
            },
            Effect.sync(() => {
              releases++
            }).pipe(Effect.andThen(Effect.fail(failure))),
          ).pipe(Scope.provide(scope))
          const session = yield* Session.make.pipe(
            Effect.provideService(Store.Store, store),
            Scope.provide(scope),
          )
          const reader = yield* store.committed.pipe(Effect.forkScoped)
          yield* Deferred.await(entered)
          const closing = yield* Scope.close(scope, Exit.void).pipe(Effect.exit, Effect.forkScoped)
          yield* Effect.yieldNow
          assert.strictEqual(releases, 0)
          yield* Deferred.succeed(release, undefined)
          assert.deepStrictEqual(yield* Fiber.join(reader), value.state)
          const closed = yield* Fiber.join(closing)
          assert.ok(Exit.isFailure(closed))
          if (Exit.isFailure(closed)) assert.strictEqual(Cause.squash(closed.cause), failure)
          for (let repeat = 0; repeat < 2; repeat++) {
            const receipt = yield* store.awaitClosed.pipe(Effect.result)
            assert.strictEqual(receipt._tag, 'Failure')
            if (receipt._tag === 'Failure') assert.strictEqual(receipt.failure, failure)
            const combined = yield* session.awaitClosed.pipe(Effect.result)
            assert.strictEqual(combined._tag, 'Failure')
            if (combined._tag === 'Failure') assert.strictEqual(combined.failure, failure)
          }
          assert.strictEqual(releases, 1)
        }),
      ),
  )
  it.live(
    'scopes registrations, snapshots reverse cleanup and joins every finalizer before backend release',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const scope = yield* child
          const calls: string[] = []
          const value: Backend.Snapshot = { state: Record.emptyState(), frames: [] }
          const store = yield* Backend.make(
            {
              load: Effect.succeed(value),
              committed: Effect.succeed(value),
              save: () => Effect.void,
              atomic: (effect) => effect,
            },
            Effect.sync(() => {
              calls.push('backend')
            }),
          ).pipe(Scope.provide(scope))
          const session = yield* Session.make.pipe(
            Effect.provideService(Store.Store, store),
            Scope.provide(scope),
          )
          const removed = yield* child
          yield* session
            .onClose(
              Effect.sync(() => {
                calls.push('removed')
              }),
            )
            .pipe(Scope.provide(removed))
          const first = Effect.sync(() => {
            calls.push('first')
          })
          const registerFirst = session.onClose(first)
          yield* registerFirst.pipe(Scope.provide(removed))
          yield* registerFirst
          yield* Scope.close(removed, Exit.void)
          const captured = yield* child
          const started = yield* Deferred.make<void>()
          const release = yield* Deferred.make<void>()
          yield* session
            .onClose(
              Effect.sync(() => {
                calls.push('second')
              }).pipe(
                Effect.andThen(Deferred.succeed(started, undefined)),
                Effect.andThen(Deferred.await(release)),
              ),
            )
            .pipe(Scope.provide(captured))
          const closing = yield* Scope.close(scope, Exit.void).pipe(Effect.forkScoped)
          yield* Deferred.await(started)
          yield* Scope.close(captured, Exit.void)
          assert.deepStrictEqual(calls, ['second'])
          yield* Deferred.succeed(release, undefined)
          yield* Fiber.join(closing)
          yield* session.awaitClosed
          assert.deepStrictEqual(calls, ['second', 'first', 'backend'])
          const rejectedRegistration = yield* session.onClose(Effect.void).pipe(Effect.result)
          assert.strictEqual(rejectedRegistration._tag, 'Failure')
        }),
      ),
  )
  it.live('Cancellation registration membership belongs independently to each caller Scope', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const scope = yield* child
        const first = yield* child
        const second = yield* child
        let value: Backend.Snapshot = { state: Record.emptyState(), frames: [] }
        const store = yield* Backend.make({
          load: Effect.sync(() => value),
          committed: Effect.sync(() => value),
          save: (next) =>
            Effect.sync(() => {
              value = next
            }),
          atomic: (effect) => effect,
        }).pipe(Scope.provide(scope))
        const session = yield* Session.make.pipe(
          Effect.provideService(Store.Store, store),
          Scope.provide(scope),
        )
        const root = yield* session.root()
        const taskId = yield* session.transaction((tx) =>
          tx.createTask({
            conversationId: root.id,
            kind: 'registration-fixture',
            version: 1,
            input: null,
            background: false,
            abortRequested: false,
            state: { status: 'pending' },
          }),
        )
        let calls = 0
        const cancel = Effect.sync(() => {
          calls++
        })
        const capabilities = yield* Cancellation.Cancellation
        const identity = {
          sessionId: Identity.SessionId.make('registration-fixture'),
          conversationId: root.id,
          taskId,
        }
        const register = capabilities.register(identity, cancel)
        yield* register.pipe(Scope.provide(first))
        yield* register.pipe(Scope.provide(second))
        yield* Scope.close(first, Exit.void)
        const reached = { tasks: (yield* session.committed).tasks, conversations: [] }
        yield* capabilities.cancel(identity.sessionId, reached)
        assert.strictEqual(calls, 1)
        yield* Scope.close(second, Exit.void)
        yield* capabilities.cancel(identity.sessionId, reached)
        assert.strictEqual(calls, 1)
      }).pipe(Effect.provide(Cancellation.layer)),
    ),
  )
  it.live('retains Session cleanup defects while joining the remaining cleanup and backend', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const scope = yield* child
        const calls: string[] = []
        const failure = new Error('cleanup defect fixture')
        const value: Backend.Snapshot = { state: Record.emptyState(), frames: [] }
        const store = yield* Backend.make(
          {
            load: Effect.succeed(value),
            committed: Effect.succeed(value),
            save: () => Effect.void,
            atomic: (effect) => effect,
          },
          Effect.sync(() => {
            calls.push('backend')
          }),
        ).pipe(Scope.provide(scope))
        const session = yield* Session.make.pipe(
          Effect.provideService(Store.Store, store),
          Scope.provide(scope),
        )
        yield* session.onClose(
          Effect.sync(() => {
            calls.push('remaining')
          }),
        )
        yield* session.onClose(Effect.die(failure))
        assert.ok(Exit.isFailure(yield* Scope.close(scope, Exit.void).pipe(Effect.exit)))
        for (let repeat = 0; repeat < 2; repeat++) {
          const receipt = yield* session.awaitClosed.pipe(Effect.exit)
          assert.ok(Exit.isFailure(receipt))
          if (Exit.isFailure(receipt)) assert.strictEqual(Cause.squash(receipt.cause), failure)
        }
        yield* store.awaitClosed
        assert.deepStrictEqual(calls, ['remaining', 'backend'])
      }),
    ),
  )
  it.live(
    'immediate invocation interruption before body entry joins startup and never strands Scope cleanup',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const scope = yield* child
          let entered = false
          let backendReleased = false
          const registered = yield* Deferred.make<void>()
          const value: Backend.Snapshot = { state: Record.emptyState(), frames: [] }
          const store = yield* Backend.make(
            {
              load: Effect.succeed(value),
              committed: Effect.succeed(value),
              save: () => Effect.void,
              atomic: (effect) => effect,
            },
            Effect.sync(() => {
              backendReleased = true
            }),
          ).pipe(Scope.provide(scope))
          const session = yield* Session.make.pipe(
            Effect.provideService(Store.Store, store),
            Scope.provide(scope),
          )
          const observable = Session.Session.of({
            ...session,
            onClose: (cleanup) =>
              session
                .onClose(cleanup)
                .pipe(Effect.andThen(Deferred.succeed(registered, undefined)), Effect.asVoid),
          })
          const invocation = yield* Cancellation.activity(
            {
              sessionId: Identity.SessionId.make('startup-regression'),
              conversationId: Record.ROOT_CONVERSATION_ID,
              taskId: Record.TaskId.make(2),
            },
            observable,
            Effect.sync(() => {
              entered = true
            }).pipe(Effect.andThen(Effect.never)),
          ).pipe(Effect.exit, Effect.forkScoped)
          yield* Deferred.await(registered)
          const closing = yield* Scope.close(scope, Exit.void).pipe(
            Effect.forkScoped({ startImmediately: true }),
          )
          yield* Fiber.interrupt(invocation)
          assert.strictEqual(entered, false)
          yield* Fiber.join(closing)
          yield* session.awaitClosed
          assert.strictEqual(backendReleased, true)
        }),
      ),
  )
  it.live(
    'outer invocation interruption cannot settle body completion before blocked resources release',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const scope = yield* child
          let backendReleased = false
          let bodyReleased = false
          const value: Backend.Snapshot = { state: Record.emptyState(), frames: [] }
          const store = yield* Backend.make(
            {
              load: Effect.succeed(value),
              committed: Effect.succeed(value),
              save: () => Effect.void,
              atomic: (effect) => effect,
            },
            Effect.sync(() => {
              backendReleased = true
            }),
          ).pipe(Scope.provide(scope))
          const session = yield* Session.make.pipe(
            Effect.provideService(Store.Store, store),
            Scope.provide(scope),
          )
          const entered = yield* Deferred.make<void>()
          const finalizing = yield* Deferred.make<void>()
          const release = yield* Deferred.make<void>()
          yield* Effect.addFinalizer(() => Deferred.succeed(release, undefined))
          const body = Effect.acquireRelease(Effect.void, () =>
            Deferred.succeed(finalizing, undefined).pipe(
              Effect.andThen(Deferred.await(release)),
              Effect.andThen(
                Effect.sync(() => {
                  bodyReleased = true
                }),
              ),
            ),
          ).pipe(Effect.andThen(Deferred.succeed(entered, undefined)), Effect.andThen(Effect.never))
          const invocation = yield* Cancellation.activity(
            {
              sessionId: Identity.SessionId.make('scope-regression'),
              conversationId: Record.ROOT_CONVERSATION_ID,
              taskId: Record.TaskId.make(2),
            },
            session,
            body,
          ).pipe(Effect.exit, Effect.forkScoped)
          yield* Deferred.await(entered)
          const interrupting = yield* Fiber.interrupt(invocation).pipe(Effect.forkScoped)
          yield* Deferred.await(finalizing)
          const closing = yield* Scope.close(scope, Exit.void).pipe(Effect.forkScoped)
          yield* Effect.addFinalizer(() => Deferred.succeed(release, undefined))
          yield* Effect.yieldNow
          assert.strictEqual(bodyReleased, false)
          assert.strictEqual(backendReleased, false)
          assert.strictEqual(closing.pollUnsafe(), undefined)
          yield* Deferred.succeed(release, undefined)
          yield* Fiber.join(interrupting)
          yield* Fiber.join(closing)
          yield* session.awaitClosed
          assert.strictEqual(bodyReleased, true)
          assert.strictEqual(backendReleased, true)
        }),
      ),
  )
})
