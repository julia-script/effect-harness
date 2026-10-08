import { assert, describe, it } from '@effect/vitest'
import { uncertain } from '../../src/StorageError.ts'
import * as Document from '../../src/Document.ts'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as Option from 'effect/Option'
import * as Ref from 'effect/Ref'
import * as Schema from 'effect/Schema'
import * as TestClock from 'effect/testing/TestClock'
import * as Record from '../../src/Record.ts'
import { Persistence } from '../../src/Persistence.ts'
import * as Task from '../../src/Task.ts'
import { TaskRuntime } from '../../src/TaskRuntime.ts'
import * as Memory from '../../src/storage/Memory.ts'
import * as Session from '../../src/internal/Session.ts'
import * as Scheduler from '../../src/internal/Scheduler.ts'

const fixture = Effect.gen(function* () {
  const persistence = yield* Memory.make
  const session = yield* Session.make.pipe(Effect.provideService(Persistence, persistence))
  yield* session.initialize
  return session
})
const checkpoint = Schema.Struct({ phase: Schema.String, count: Schema.Int })
const simple = Task.define({
  name: 'test/count',
  version: 1,
  input: Schema.Int,
  checkpoint,
  result: Schema.Int,
  initial: () => ({ phase: 'count', count: 0 }),
  run: (task) =>
    Effect.succeed(
      task.checkpoint.count === task.input
        ? Task.complete(task.input)
        : Task.continueWith({ phase: 'count', count: task.checkpoint.count + 1 }),
    ),
})
const seed = Effect.fn('test.seed')(function* (
  session: Session.Service,
  definition: Task.BoundDefinition,
  input: unknown,
  state: 'pending' | 'running' = 'pending',
) {
  const prepared = yield* definition.prepare(input)
  return yield* session.transaction((tx) =>
    tx.createTask({
      conversationId: Record.ROOT_CONVERSATION_ID,
      kind: definition.name,
      version: definition.version,
      input: prepared.input,
      background: false,
      abortRequested: false,
      state: { status: state, checkpoint: prepared.checkpoint },
    }),
  )
})
const record = Effect.fn('test.record')(function* (session: Session.Service, id: Record.TaskId) {
  const found = yield* session.task(id)
  assert.isTrue(Option.isSome(found))
  return Option.getOrThrow(found)
})

describe('embedded task scheduler', () => {
  it.effect('reconciles running tasks without running until explicit resume', () =>
    Effect.gen(function* () {
      const session = yield* fixture
      const definition = yield* Task.bind(simple)
      const id = yield* seed(session, definition, 3, 'running')
      const scheduler = yield* Scheduler.make(session, [definition])
      assert.strictEqual((yield* record(session, id)).state.status, 'pending')
      yield* Effect.yieldNow
      assert.strictEqual((yield* record(session, id)).state.status, 'pending')
      yield* scheduler.resume
      const done = yield* scheduler.await(id)
      assert.deepEqual(done.state.outcome, { status: 'completed', result: 3 })
    }),
  )

  it.effect('blocks missing definitions and resumes after installation', () =>
    Effect.gen(function* () {
      const session = yield* fixture
      const definition = yield* Task.bind(simple)
      const id = yield* seed(session, definition, 2)
      const scheduler = yield* Scheduler.make(session, [])
      yield* scheduler.resume
      yield* Effect.yieldNow
      assert.strictEqual((yield* record(session, id)).state.status, 'pending')
      yield* scheduler.install([definition])
      assert.deepEqual((yield* scheduler.await(id)).state.outcome, {
        status: 'completed',
        result: 2,
      })
    }),
  )

  it.effect(
    'close pauses a running invocation and reopening preserves its committed checkpoint and memos',
    () =>
      Effect.gen(function* () {
        const session = yield* fixture
        const entered = yield* Deferred.make<void>()
        const calls = yield* Ref.make(0)
        const definition = yield* Task.bind(
          Task.define({
            name: 'test/recovery',
            version: 1,
            input: Schema.Null,
            checkpoint,
            result: Schema.Int,
            initial: () => ({ phase: 'start', count: 0 }),
            run: Effect.fn('test.recovery.run')(function* (task) {
              const runtime = yield* TaskRuntime
              yield* Ref.update(calls, (n) => n + 1)
              if (task.checkpoint.phase === 'start') {
                yield* runtime.memo('decision', 42)
                yield* runtime.checkpoint({ phase: 'finish', count: 1 })
                yield* Deferred.succeed(entered, undefined)
                return yield* Effect.never
              }
              const memo = yield* runtime.memo('decision', 99)
              assert.strictEqual(memo, 42)
              return Task.complete(task.checkpoint.count)
            }),
          }),
        )
        const first = yield* Scheduler.make(session, [definition])
        const id = yield* first.create(definition, null, {
          conversationId: Record.ROOT_CONVERSATION_ID,
        })
        yield* Deferred.await(entered)
        yield* first.close
        const paused = yield* record(session, id)
        assert.strictEqual(paused.abortRequested, false)
        assert.strictEqual(paused.state.status, 'running')
        assert.deepEqual(paused.state.checkpoint, { phase: 'finish', count: 1 })
        const reopened = yield* Scheduler.make(session, [definition])
        assert.strictEqual((yield* record(session, id)).state.status, 'pending')
        yield* reopened.resume
        assert.deepEqual((yield* reopened.await(id)).state.outcome, {
          status: 'completed',
          result: 1,
        })
        assert.strictEqual(yield* Ref.get(calls), 2)
      }),
  )

  it.effect('stores prototype-named memos as ordinary keys across record codecs and recovery', () =>
    Effect.gen(function* () {
      const session = yield* fixture
      const entered = yield* Deferred.make<void>()
      const values = [
        ['toString', 0],
        ['__proto__', null],
        ['constructor', false],
      ] as const
      const definition = yield* Task.bind(
        Task.define({
          name: 'test/memo-keys',
          version: 1,
          input: Schema.Null,
          checkpoint,
          result: Schema.Null,
          initial: () => ({ phase: 'write', count: 0 }),
          run: Effect.fn('test.memo-keys.run')(function* (task) {
            const runtime = yield* TaskRuntime
            for (const [key, value] of values) {
              if (task.checkpoint.phase === 'write') {
                assert.strictEqual(yield* runtime.memo(key), undefined)
                assert.strictEqual(yield* runtime.memo(key, value), value)
              }
              assert.strictEqual(yield* runtime.memo(key, 'replacement'), value)
              assert.strictEqual(yield* runtime.memo(key), value)
            }
            if (task.checkpoint.phase === 'write') {
              yield* runtime.checkpoint({ phase: 'read', count: 1 })
              yield* Deferred.succeed(entered, undefined)
              return yield* Effect.never
            }
            return Task.complete(null)
          }),
        }),
      )
      const first = yield* Scheduler.make(session, [definition])
      const id = yield* first.create(definition, null, {
        conversationId: Record.ROOT_CONVERSATION_ID,
      })
      yield* Deferred.await(entered)
      yield* first.close
      const paused = yield* record(session, id)
      const encoded = yield* Schema.encodeEffect(Record.Task)(paused)
      const decoded = yield* Schema.decodeEffect(Record.Task)(encoded)
      assert.isDefined(decoded.memos)
      if (decoded.memos !== undefined) {
        for (const [key, value] of values) {
          assert.isTrue(Object.hasOwn(decoded.memos, key))
          assert.strictEqual(decoded.memos[key], value)
        }
      }
      const reopened = yield* Scheduler.make(session, [definition])
      yield* reopened.resume
      assert.deepEqual((yield* reopened.await(id)).state.outcome, {
        status: 'completed',
        result: null,
      })
    }),
  )

  it.effect(
    'creates children and parks the parent in one commit; a completed child wakes its parent',
    () =>
      Effect.gen(function* () {
        const session = yield* fixture
        const child = yield* Task.bind(simple)
        const parent = yield* Task.bind(
          Task.define({
            name: 'test/parent',
            version: 1,
            input: Schema.Null,
            checkpoint,
            result: Schema.Json,
            initial: () => ({ phase: 'spawn', count: 0 }),
            run: Effect.fn('test.parent.run')(function* (task) {
              const runtime = yield* TaskRuntime
              if (task.checkpoint.phase === 'spawn') {
                yield* runtime.commit(
                  Effect.fn('test.parent.spawn')(function* (tx) {
                    const prepared = yield* child.prepare(2)
                    const id = yield* tx.createTask({
                      conversationId: runtime.conversationId,
                      owner: runtime.taskId,
                      kind: child.name,
                      version: child.version,
                      input: prepared.input,
                      background: false,
                      abortRequested: false,
                      state: { status: 'pending', checkpoint: prepared.checkpoint },
                    })
                    return Task.wait({ phase: 'join', count: id }, [id])
                  }),
                )
                return undefined
              }
              const outcomes = yield* runtime.outcomes([
                yield* Schema.decodeEffect(Record.TaskId)(task.checkpoint.count),
              ])
              return Task.complete(outcomes)
            }),
          }),
        )
        const scheduler = yield* Scheduler.make(session, [child, parent])
        const id = yield* scheduler.create(parent, null, {
          conversationId: Record.ROOT_CONVERSATION_ID,
        })
        assert.deepEqual((yield* scheduler.await(id)).state.outcome, {
          status: 'completed',
          result: [{ status: 'completed', result: 2 }],
        })
      }),
  )

  it.effect(
    'holds a completed owner until ordinary owned-conversation work ends, while background work does not hold it',
    () =>
      Effect.gen(function* () {
        const session = yield* fixture
        const childEntered = yield* Deferred.make<void>()
        const childRelease = yield* Deferred.make<void>()
        const child = yield* Task.bind(
          Task.define({
            name: 'test/held-child',
            version: 1,
            input: Schema.Null,
            checkpoint,
            result: Schema.Null,
            initial: () => ({ phase: 'hold', count: 0 }),
            run: Effect.fn('test.child.hold')(function* () {
              yield* Deferred.succeed(childEntered, undefined)
              yield* Deferred.await(childRelease)
              return Task.complete(null)
            }),
          }),
        )
        const parent = yield* Task.bind(
          Task.define({
            name: 'test/held-owner',
            version: 1,
            input: Schema.Null,
            checkpoint,
            result: Schema.Null,
            initial: () => ({ phase: 'spawn', count: 0 }),
            run: Effect.fn('test.owner.spawn')(function* () {
              const runtime = yield* TaskRuntime
              yield* runtime.commit(
                Effect.fn('test.owner.commit')(function* (tx) {
                  const conversation = yield* tx.createConversation({
                    ownership: { _tag: 'task', taskId: runtime.taskId },
                  })
                  const prepared = yield* child.prepare(null)
                  yield* tx.createTask({
                    conversationId: conversation.id,
                    kind: child.name,
                    version: child.version,
                    input: prepared.input,
                    background: false,
                    abortRequested: false,
                    state: { status: 'pending', checkpoint: prepared.checkpoint },
                  })
                  return Task.complete(null)
                }),
              )
              return undefined
            }),
          }),
        )
        const scheduler = yield* Scheduler.make(session, [child, parent])
        const id = yield* scheduler.create(parent, null, {
          conversationId: Record.ROOT_CONVERSATION_ID,
        })
        yield* Deferred.await(childEntered)
        assert.strictEqual((yield* record(session, id)).state.status, 'completing')
        yield* Deferred.succeed(childRelease, undefined)
        assert.strictEqual((yield* scheduler.await(id)).state.status, 'terminal')
      }),
  )

  it.effect(
    'persists abort intent, interrupts running work, cleans up bottom-up, and fences late commits',
    () =>
      Effect.gen(function* () {
        const session = yield* fixture
        const entered = yield* Deferred.make<TaskRuntime['Service']>()
        const order = yield* Ref.make<ReadonlyArray<string>>([])
        const child = yield* Task.bind(
          Task.define({
            name: 'test/abort-child',
            version: 1,
            input: Schema.Null,
            checkpoint,
            result: Schema.Null,
            initial: () => ({ phase: 'running', count: 0 }),
            run: Effect.fn('test.abortChild.run')(function* () {
              const runtime = yield* TaskRuntime
              yield* Deferred.succeed(entered, runtime)
              return yield* Effect.never
            }),
            abort: Effect.fn('test.abortChild.abort')(function* () {
              yield* Ref.update(order, (items) => [...items, 'child'])
              return Task.aborted()
            }),
          }),
        )
        const parent = yield* Task.bind(
          Task.define({
            name: 'test/abort-parent',
            version: 1,
            input: Schema.Null,
            checkpoint,
            result: Schema.Null,
            initial: () => ({ phase: 'spawn', count: 0 }),
            run: Effect.fn('test.abortParent.run')(function* () {
              const runtime = yield* TaskRuntime
              yield* runtime.commit(
                Effect.fn('test.abortParent.commit')(function* (tx) {
                  const prepared = yield* child.prepare(null)
                  const id = yield* tx.createTask({
                    conversationId: runtime.conversationId,
                    owner: runtime.taskId,
                    kind: child.name,
                    version: child.version,
                    input: prepared.input,
                    background: false,
                    abortRequested: false,
                    state: { status: 'pending', checkpoint: prepared.checkpoint },
                  })
                  return Task.wait({ phase: 'join', count: 0 }, [id])
                }),
              )
              return undefined
            }),
            abort: Effect.fn('test.abortParent.abort')(function* () {
              yield* Ref.update(order, (items) => [...items, 'parent'])
              return Task.aborted()
            }),
          }),
        )
        const scheduler = yield* Scheduler.make(session, [child, parent])
        const id = yield* scheduler.create(parent, null, {
          conversationId: Record.ROOT_CONVERSATION_ID,
        })
        const endedRuntime = yield* Deferred.await(entered)
        yield* scheduler.abort(id)
        assert.deepEqual(yield* Ref.get(order), ['child', 'parent'])
        assert.deepEqual((yield* record(session, id)).state.outcome, { status: 'aborted' })
        const late = yield* Effect.exit(endedRuntime.checkpoint({ phase: 'bad', count: 0 }))
        assert.isTrue(Exit.isFailure(late))
      }),
  )

  it.effect(
    'does not overlap invocations even with repeated concurrent resume and wake operations',
    () =>
      Effect.gen(function* () {
        const session = yield* fixture
        const entered = yield* Deferred.make<void>()
        const release = yield* Deferred.make<void>()
        const calls = yield* Ref.make(0)
        const definition = yield* Task.bind(
          Task.define({
            name: 'test/once',
            version: 1,
            input: Schema.Null,
            checkpoint,
            result: Schema.Null,
            initial: () => ({ phase: 'hold', count: 0 }),
            run: Effect.fn('test.once.run')(function* () {
              yield* Ref.update(calls, (n) => n + 1)
              yield* Deferred.succeed(entered, undefined)
              yield* Deferred.await(release)
              return Task.complete(null)
            }),
          }),
        )
        const scheduler = yield* Scheduler.make(session, [definition])
        const id = yield* scheduler.create(definition, null, {
          conversationId: Record.ROOT_CONVERSATION_ID,
        })
        yield* Deferred.await(entered)
        yield* Effect.forEach(
          Array.from({ length: 25 }),
          () => scheduler.resume.pipe(Effect.andThen(scheduler.wake)),
          { concurrency: 'unbounded' },
        )
        assert.strictEqual(yield* Ref.get(calls), 1)
        yield* Deferred.succeed(release, undefined)
        yield* scheduler.await(id)
        assert.strictEqual(yield* Ref.get(calls), 1)
      }),
  )

  it.effect('preserves absolute deadlines across reopening instead of restarting the delay', () =>
    Effect.gen(function* () {
      const session = yield* fixture
      const entered = yield* Deferred.make<void>()
      const definition = yield* Task.bind(
        Task.define({
          name: 'test/deadline',
          version: 1,
          input: Schema.Null,
          checkpoint: Schema.Struct({ phase: Schema.String, until: Schema.Finite }),
          result: Schema.Null,
          initial: () => ({ phase: 'start', until: 0 }),
          run: Effect.fn('test.deadline.run')(function* (task) {
            const runtime = yield* TaskRuntime
            let until = task.checkpoint.until
            if (task.checkpoint.phase === 'start') {
              until = (yield* runtime.now) + 1000
              yield* runtime.checkpoint({ phase: 'sleep', until })
              yield* Deferred.succeed(entered, undefined)
            }
            yield* runtime.sleepUntil(until)
            return Task.complete(null)
          }),
        }),
      )
      const scheduler = yield* Scheduler.make(session, [definition])
      const id = yield* scheduler.create(definition, null, {
        conversationId: Record.ROOT_CONVERSATION_ID,
      })
      yield* Deferred.await(entered)
      yield* scheduler.close
      yield* TestClock.adjust('2 seconds')
      const reopened = yield* Scheduler.make(session, [definition])
      yield* reopened.resume
      assert.deepEqual((yield* reopened.await(id)).state.outcome, {
        status: 'completed',
        result: null,
      })
    }),
  )
  it.effect('fail-fast cancellation drains siblings before the waiting parent resumes', () =>
    Effect.gen(function* () {
      const session = yield* fixture
      const cancelled = yield* Ref.make(false)
      const sibling = yield* Task.bind(
        Task.define({
          name: 'test/fail-fast-sibling',
          version: 1,
          input: Schema.Null,
          checkpoint,
          result: Schema.Null,
          initial: () => ({ phase: 'hold', count: 0 }),
          run: () => Effect.never,
          abort: () => Ref.set(cancelled, true).pipe(Effect.as(Task.aborted())),
        }),
      )
      const parent = yield* Task.bind(
        Task.define({
          name: 'test/fail-fast-parent',
          version: 1,
          input: Schema.Null,
          checkpoint,
          result: Schema.Boolean,
          initial: () => ({ phase: 'join', count: 0 }),
          run: () => Ref.get(cancelled).pipe(Effect.map(Task.complete)),
        }),
      )
      const parentId = yield* seed(session, parent, null)
      const siblingId = yield* session.transaction(
        Effect.fn('test.seedFailFast')(function* (tx) {
          const prepared = yield* sibling.prepare(null)
          const siblingId = yield* tx.createTask({
            conversationId: Record.ROOT_CONVERSATION_ID,
            owner: parentId,
            kind: sibling.name,
            version: sibling.version,
            input: prepared.input,
            background: false,
            abortRequested: false,
            state: { status: 'pending', checkpoint: prepared.checkpoint },
          })
          const failedId = yield* tx.createTask({
            conversationId: Record.ROOT_CONVERSATION_ID,
            owner: parentId,
            kind: sibling.name,
            version: sibling.version,
            input: prepared.input,
            background: false,
            abortRequested: false,
            state: {
              status: 'terminal',
              outcome: { status: 'failed', error: { message: 'failed' } },
            },
          })
          const saved = yield* tx.task(parentId)
          if (Option.isSome(saved))
            yield* tx.write({
              _tag: 'task',
              value: {
                ...saved.value,
                state: {
                  status: 'waiting',
                  checkpoint: { phase: 'join', count: 0 },
                  on: [siblingId, failedId],
                  policy: 'failFast',
                },
              },
            })
          return siblingId
        }),
      )
      const scheduler = yield* Scheduler.make(session, [parent, sibling])
      yield* scheduler.resume
      assert.deepEqual((yield* scheduler.await(parentId)).state.outcome, {
        status: 'completed',
        result: true,
      })
      assert.deepEqual((yield* record(session, siblingId)).state.outcome, { status: 'aborted' })
    }),
  )

  it.effect(
    'background tasks do not hold foreground idle or receive ordinary conversation abort',
    () =>
      Effect.gen(function* () {
        const session = yield* fixture
        const entered = yield* Deferred.make<void>()
        const background = yield* Task.bind(
          Task.define({
            name: 'test/background',
            version: 1,
            input: Schema.Null,
            checkpoint,
            result: Schema.Null,
            initial: () => ({ phase: 'hold', count: 0 }),
            run: () => Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)),
          }),
        )
        const scheduler = yield* Scheduler.make(session, [background])
        const id = yield* scheduler.create(background, null, {
          conversationId: Record.ROOT_CONVERSATION_ID,
          background: true,
        })
        yield* Deferred.await(entered)
        yield* scheduler.idle(Record.ROOT_CONVERSATION_ID)
        yield* scheduler.abortConversation(Record.ROOT_CONVERSATION_ID)
        assert.strictEqual((yield* record(session, id)).abortRequested, false)
        yield* scheduler.abortConversation(Record.ROOT_CONVERSATION_ID, true)
        assert.deepEqual((yield* record(session, id)).state.outcome, { status: 'aborted' })
      }),
  )

  it.effect('retires newly acquired task-scoped documents atomically with completion', () =>
    Effect.gen(function* () {
      const session = yield* fixture
      const notes = Document.defineUnsafe({
        kind: 'test/task-notes',
        version: 1,
        scope: 'task',
        schema: Schema.Struct({ note: Schema.String }),
        initial: () => ({ note: 'initial' }),
      })
      const definition = yield* Task.bind(
        Task.define({
          name: 'test/task-document',
          version: 1,
          input: Schema.Null,
          checkpoint,
          result: Schema.Null,
          initial: () => ({ phase: 'complete', count: 0 }),
          run: Effect.fn('test.document.run')(function* () {
            const runtime = yield* TaskRuntime
            yield* runtime.commit(
              Effect.fn('test.document.commit')(function* (tx) {
                const draft = yield* tx.doc(notes, { owner: runtime.taskId })
                draft.note = 'updated'
                return Task.complete(null)
              }),
            )
          }),
        }),
      )
      const scheduler = yield* Scheduler.make(session, [definition])
      const id = yield* scheduler.create(definition, null, {
        conversationId: Record.ROOT_CONVERSATION_ID,
      })
      yield* scheduler.await(id)
      assert.isTrue(Option.isNone(yield* session.snapshot(notes, { owner: id })))
    }),
  )
  it.effect('an uncertain checkpoint stops execution without manufacturing a fault outcome', () =>
    Effect.gen(function* () {
      const backing = yield* Memory.make
      let injected = false
      const store: Persistence['Service'] = {
        ...backing,
        commit: (writes, nextId) =>
          Effect.gen(function* () {
            const committed = yield* backing.commit(writes, nextId)
            if (
              !injected &&
              writes.some(
                (write) =>
                  write._tag === 'task' &&
                  Schema.is(
                    Schema.Struct({ phase: Schema.Literal('uncertain'), count: Schema.Int }),
                  )(write.value.state.checkpoint),
              )
            ) {
              injected = true
              return yield* uncertain('Acknowledgment lost after checkpoint commit')
            }
            return committed
          }),
      }
      const session = yield* Session.make.pipe(Effect.provideService(Persistence, store))
      yield* session.initialize
      const definition = yield* Task.bind(
        Task.define({
          name: 'test/uncertain',
          version: 1,
          input: Schema.Null,
          checkpoint,
          result: Schema.Null,
          initial: () => ({ phase: 'start', count: 0 }),
          run: Effect.fn('test.uncertain.run')(function* () {
            const runtime = yield* TaskRuntime
            yield* runtime.checkpoint({ phase: 'uncertain', count: 1 })
            return Task.complete(null)
          }),
        }),
      )
      const scheduler = yield* Scheduler.make(session, [definition])
      const id = yield* scheduler.create(definition, null, {
        conversationId: Record.ROOT_CONVERSATION_ID,
      })
      const outcome = yield* Effect.result(scheduler.await(id))
      assert.strictEqual(outcome._tag, 'Failure')
      const saved = yield* backing.task(id)
      if (Option.isSome(saved)) {
        assert.strictEqual(saved.value.state.status, 'running')
        assert.deepEqual(saved.value.state.checkpoint, { phase: 'uncertain', count: 1 })
      }
    }),
  )
})
