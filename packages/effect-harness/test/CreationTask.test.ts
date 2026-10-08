import { assert, describe, it } from '@effect/vitest'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Option from 'effect/Option'
import * as Ref from 'effect/Ref'
import * as Schema from 'effect/Schema'
import * as Agent from '../src/Agent.ts'
import * as Invocation from '../src/Invocation.ts'
import { Persistence } from '../src/Persistence.ts'
import * as Record from '../src/Record.ts'
import * as Task from '../src/Task.ts'
import * as State from '../src/internal/ConversationState.ts'
import * as CreationTask from '../src/internal/CreationTask.ts'
import * as Scheduler from '../src/internal/Scheduler.ts'
import * as Session from '../src/internal/Session.ts'
import * as Memory from '../src/storage/Memory.ts'
import * as Native from './embedded/NativeFixture.ts'

const fixture = Effect.fn('test.creation.fixture')(function* (cwd?: string) {
  const store = yield* Memory.make
  const session = yield* Session.make.pipe(Effect.provideService(Persistence, store))
  yield* session.initialize
  yield* session.transaction(
    Effect.fn(function* (tx) {
      const state = yield* tx.doc(State.AgentDoc, { owner: Record.ROOT_CONVERSATION_ID })
      if (cwd !== undefined) state.cwd = cwd
    }),
  )
  const native = yield* Native.makeExecutor()
  const definition = yield* CreationTask.make(session, native.executor, {
    cwd: '/host/default',
    settings: Agent.defaultSettings,
    report: () => Effect.void,
  })
  return { session, definition, ...native }
})
const seed = Effect.fn('test.creation.seed')(function* (
  session: Session.Service,
  definition: Task.BoundDefinition,
) {
  const prepared = yield* definition.prepare(null)
  return yield* session.transaction((tx) =>
    tx.createTask({
      conversationId: Record.ROOT_CONVERSATION_ID,
      kind: definition.name,
      version: definition.version,
      input: prepared.input,
      background: false,
      abortRequested: false,
      state: { status: 'pending', checkpoint: prepared.checkpoint },
    }),
  )
})

describe('conversation creation task', () => {
  for (const cwd of [undefined, '/conversation/workspace'])
    it.effect(`invokes creation once with cwd ${cwd ?? '/host/default'}`, () =>
      Effect.gen(function* () {
        const { session, definition, registry } = yield* fixture(cwd)
        const seen = yield* Ref.make<
          ReadonlyArray<{ readonly id: Record.ConversationId; readonly cwd: string }>
        >([])
        yield* registry.install([
          {
            name: 'creation-observer',
            hooks: [
              {
                operation: 'conversation',
                handlers: {
                  conversationCreated: Effect.fn(function* (id) {
                    const invocation = yield* Invocation.Invocation
                    yield* Ref.update(seen, (values) => [...values, { id, cwd: invocation.cwd }])
                    // A hook can mutate domain state because its invocation is outside that lock.
                    yield* session
                      .transaction((tx) => tx.appendEntry(id, { kind: 'created-hook' }))
                      .pipe(Effect.orDie)
                  }),
                },
              },
            ],
          },
        ])
        const scheduler = yield* Scheduler.make(session, [definition])
        const task = yield* scheduler.create(definition, null, {
          conversationId: Record.ROOT_CONVERSATION_ID,
        })
        assert.deepEqual((yield* scheduler.await(task)).state.outcome, {
          status: 'completed',
          result: null,
        })
        yield* scheduler.resume
        assert.deepEqual(yield* Ref.get(seen), [
          { id: Record.ROOT_CONVERSATION_ID, cwd: cwd ?? '/host/default' },
        ])
      }),
    )

  it.effect('resumes an interrupted callback and starts saved creation tasks only on resume', () =>
    Effect.gen(function* () {
      const { session, definition, registry } = yield* fixture()
      const entered = yield* Deferred.make<void>()
      const calls = yield* Ref.make(0)
      yield* registry.install([
        {
          name: 'creation-recovery',
          hooks: [
            {
              operation: 'conversation',
              handlers: {
                conversationCreated: Effect.fn(function* () {
                  const count = yield* Ref.updateAndGet(calls, (value) => value + 1)
                  if (count === 1) {
                    yield* Deferred.succeed(entered, undefined)
                    return yield* Effect.never
                  }
                }),
              },
            },
          ],
        },
      ])
      const id = yield* seed(session, definition)
      const first = yield* Scheduler.make(session, [definition])
      yield* Effect.yieldNow
      assert.strictEqual(yield* Ref.get(calls), 0)
      yield* first.resume
      yield* Deferred.await(entered)
      // While the external hook is waiting, other conversation mutations still commit.
      yield* session.transaction((tx) =>
        tx.appendEntry(Record.ROOT_CONVERSATION_ID, { kind: 'concurrent-write' }),
      )
      yield* first.close
      const paused = Option.getOrThrow(yield* session.task(id))
      assert.strictEqual(paused.state.status, 'running')
      assert.strictEqual(paused.abortRequested, false)
      const reopened = yield* Scheduler.make(session, [definition])
      assert.strictEqual(yield* Ref.get(calls), 1)
      yield* reopened.resume
      assert.deepEqual((yield* reopened.await(id)).state.outcome, {
        status: 'completed',
        result: null,
      })
      assert.strictEqual(yield* Ref.get(calls), 2)
    }),
  )

  it.effect('uses a saved post-success acknowledgement without invoking the hook again', () =>
    Effect.gen(function* () {
      const { session, definition, registry } = yield* fixture()
      const calls = yield* Ref.make(0)
      yield* registry.install([
        {
          name: 'creation-memo',
          hooks: [
            {
              operation: 'conversation',
              handlers: {
                conversationCreated: () => Ref.update(calls, (count) => count + 1),
              },
            },
          ],
        },
      ])
      const id = yield* seed(session, definition)
      yield* session.transaction(
        Effect.fn(function* (tx) {
          const task = Option.getOrThrow(yield* tx.task(id))
          yield* tx.write({
            _tag: 'task',
            value: {
              ...task,
              memos: { created: true },
              state: { status: 'running', checkpoint: { phase: 'created' } },
            },
          })
        }),
      )
      const scheduler = yield* Scheduler.make(session, [definition])
      yield* scheduler.resume
      assert.deepEqual((yield* scheduler.await(id)).state.outcome, {
        status: 'completed',
        result: null,
      })
      assert.strictEqual(yield* Ref.get(calls), 0)
    }),
  )

  it.effect(
    'preserves a hook defect as a faulted task rather than reporting it as a callback failure',
    () =>
      Effect.gen(function* () {
        const { session, executor, registry } = yield* fixture()
        const reports = yield* Ref.make(0)
        const definition = yield* CreationTask.make(session, executor, {
          cwd: '.',
          settings: Agent.defaultSettings,
          report: () => Ref.update(reports, (count) => count + 1),
        })
        yield* registry.install([
          {
            name: 'creation-defect',
            hooks: [
              {
                operation: 'conversation',
                handlers: {
                  conversationCreated: () => Effect.die('broken creation hook'),
                },
              },
            ],
          },
        ])
        const scheduler = yield* Scheduler.make(session, [definition])
        const id = yield* scheduler.create(definition, null, {
          conversationId: Record.ROOT_CONVERSATION_ID,
        })
        const settled = yield* scheduler.await(id)
        assert.strictEqual(
          (yield* Schema.decodeUnknownEffect(Task.Outcome)(settled.state.outcome)).status,
          'faulted',
        )
        assert.strictEqual(yield* Ref.get(reports), 0)
      }),
  )
})
