import { ResourceScope, withLayer } from '../../src/testing/Storage.ts'
import * as Exit from 'effect/Exit'
import * as Scope from 'effect/Scope'
import { assert, describe, it } from '@effect/vitest'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Fiber from 'effect/Fiber'
import * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as Workflow from 'effect/workflow/Workflow'
import * as Inspection from '../../src/Inspection.ts'
import * as Ownership from '../../src/Ownership.ts'
import * as Record from '../../src/Record.ts'
import * as Session from '../../src/Session.ts'
import * as Store from '../../src/Store.ts'
import * as Memory from '../../src/storage/Memory.ts'

const Work = Workflow.make('inspection/ordinary', {
  payload: { value: Schema.String },
  success: Schema.Void,
  idempotencyKey: ({ value }) => value,
})
const layers = Layer.mergeAll(Session.layer, Ownership.layerDeclarations([Work])).pipe(
  Layer.provideMerge(Memory.layer),
)
const reserve = (
  session: Session.Service,
  input: Record.Json = {
    workflow: Work._tag,
    executionId: 'never-executed',
    payload: { deliberately: 'invalid-native-payload' },
  },
) =>
  session.transaction((tx) =>
    tx.createTask({
      conversationId: Record.ROOT_CONVERSATION_ID,
      kind: Work._tag,
      version: 1,
      input,
      background: false,
      abortRequested: false,
      state: { status: 'pending' },
    }),
  )
const update = (session: Session.Service, id: Record.TaskId, state: Record.Task['state']) =>
  session.transaction(
    Effect.fnUntraced(function* (tx) {
      const task = yield* tx.task(id)
      if (task === undefined) return yield* Effect.die('Fixture task missing')
      yield* tx.write({ type: 'task', value: { ...task, state } })
    }),
  )
const replay = (
  before: Inspection.Graph,
  ops: ReadonlyArray<Inspection.GraphOp>,
): Inspection.Graph => {
  let value = before
  for (const op of ops) {
    if (op[0] === 'replace') value = op[1]
    else {
      const tasks = { ...value.tasks }
      if (op[0] === 'delete') delete tasks[op[1][1]]
      else tasks[op[1][1]] = op[2]
      value = { tasks }
    }
  }
  return value
}

describe('committed native Workflow inspection and ownership graph', () => {
  it.live(
    'inspects unavailable bindings and waits without executing or validating user payloads',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const session = yield* Session.Session
          yield* session.root()
          const ready = yield* reserve(session)
          const missing = yield* reserve(session, {
            workflow: 'absent',
            executionId: 'absent',
            payload: {},
          })
          const invalid = yield* reserve(session, null)
          const owner = yield* reserve(session)
          yield* update(session, owner, {
            status: 'waiting',
            on: [ready, missing],
            policy: 'allSettled',
            checkpoint: { secret: 'never-in-graph' },
          })
          const before = yield* session.committed
          const inspected = yield* Inspection.get(session)
          assert.deepStrictEqual(
            inspected.tasks.map((task) => [task.record.id, task.state]),
            [
              [ready, { kind: 'ready' }],
              [missing, { kind: 'blocked', reason: 'missing_workflow' }],
              [invalid, { kind: 'blocked', reason: 'invalid_binding' }],
              [owner, { kind: 'waiting', on: [ready, missing] }],
            ],
          )
          assert.deepStrictEqual(yield* session.committed, before)
          yield* update(session, ready, { status: 'terminal', outcome: { status: 'completed' } })
          assert.deepStrictEqual(
            (yield* Inspection.get(session)).tasks.find((task) => task.record.id === owner)?.state,
            { kind: 'waiting', on: [missing] },
          )
          assert.ok(!JSON.stringify(Inspection.graph(before)).includes('never-in-graph'))
        }).pipe(Effect.provide(layers)),
      ),
  )

  it.live(
    'reconstructs exact atomic graph commits, reuses unchanged branches and removes terminal nodes',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const session = yield* Session.Session
          const store = yield* Store.Store
          yield* session.root()
          const first = yield* reserve(session)
          const second = yield* reserve(session)
          const baseline = yield* Deferred.make<void>()
          const collected = yield* Inspection.changes(store).pipe(
            Stream.tap((frame) =>
              frame.reset ? Deferred.succeed(baseline, undefined) : Effect.void,
            ),
            Stream.take(3),
            Stream.runCollect,
            Effect.forkScoped,
          )
          yield* Deferred.await(baseline)
          yield* update(session, first, { status: 'running', checkpoint: { hidden: 'payload' } })
          yield* update(session, first, {
            status: 'terminal',
            outcome: { status: 'completed', secret: 'result' },
          })
          const frames = yield* Fiber.join(collected)
          assert.strictEqual(frames.length, 3)
          const [initial, running, terminal] = frames
          assert.ok(initial && running && terminal)
          assert.strictEqual(running.before, initial.value)
          assert.strictEqual(terminal.before, running.value)
          assert.strictEqual(
            running.value.tasks[String(second)],
            initial.value.tasks[String(second)],
          )
          assert.strictEqual(running.value.tasks[String(first)]?.state.status, 'running')
          assert.deepStrictEqual(terminal.ops, [['delete', ['tasks', String(first)]]])
          for (const frame of frames)
            assert.deepStrictEqual(replay(frame.before, frame.ops), frame.value)
          assert.ok(!JSON.stringify(frames.map((frame) => frame.value)).includes('payload'))
        }).pipe(Effect.provide(layers)),
      ),
  )

  it.live(
    'publishes an owned conversation edge atomically and ignores checkpoint-only commits',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const session = yield* Session.Session
          const store = yield* Store.Store
          yield* session.root()
          const task = yield* reserve(session)
          const entered = yield* Deferred.make<void>()
          const collected = yield* Inspection.changes(store).pipe(
            Stream.tap(() => Deferred.succeed(entered, undefined)),
            Stream.take(2),
            Stream.runCollect,
            Effect.forkScoped,
          )
          yield* Deferred.await(entered)
          yield* update(session, task, { status: 'pending', checkpoint: { changed: 1 } })
          const conversation = yield* session.transaction((tx) =>
            tx.createConversation({ ownership: { kind: 'task', taskId: task } }),
          )
          const frames = yield* Fiber.join(collected)
          assert.strictEqual(frames[1]?.reset, false)
          assert.deepStrictEqual(frames[1]?.value.tasks[String(task)]?.conversations, [
            conversation.id,
          ])
          assert.strictEqual(frames[1]?.ops.length, 1)
        }).pipe(Effect.provide(layers)),
      ),
  )

  it.live(
    'bounds a stalled consumer at100 frames with a latest snapshot and resumes exact deltas',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const session = yield* Session.Session
          const store = yield* Store.Store
          yield* session.root()
          const task = yield* reserve(session)
          const entered = yield* Deferred.make<void>()
          const release = yield* Deferred.make<void>()
          const replacement = yield* Deferred.make<void>()
          const frames: Inspection.GraphChange[] = []
          const observing = yield* Inspection.changes(store).pipe(
            Stream.take(3),
            Stream.runForEach((frame) =>
              Effect.gen(function* () {
                frames.push(frame)
                if (frames.length === 1) {
                  yield* Deferred.succeed(entered, undefined)
                  yield* Deferred.await(release)
                }
                if (frames.length === 2) yield* Deferred.succeed(replacement, undefined)
              }),
            ),
            Effect.forkScoped,
          )
          yield* Deferred.await(entered)
          for (let i = 0; i < 101; i++)
            yield* update(session, task, { status: i % 2 === 0 ? 'running' : 'pending' })
          yield* Deferred.succeed(release, undefined)
          yield* Deferred.await(replacement)
          yield* update(session, task, { status: 'terminal', outcome: null })
          yield* Fiber.join(observing)
          assert.strictEqual(frames[1]?.reset, true)
          assert.strictEqual(frames[1]?.before, frames[0]?.value)
          assert.strictEqual(frames[1]?.value.tasks[String(task)]?.state.status, 'running')
          assert.strictEqual(frames[2]?.reset, false)
          for (const frame of frames)
            assert.deepStrictEqual(replay(frame.before, frame.ops), frame.value)
        }).pipe(Effect.provide(layers)),
      ),
  )

  it.live('ends on storage close and joins cancellation without leaving a polling fiber', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const session = yield* Session.Session
        const store = yield* Store.Store
        yield* session.root()
        const entered = yield* Deferred.make<void>()
        const first = yield* Inspection.changes(store).pipe(
          Stream.tap(() => Deferred.succeed(entered, undefined)),
          Stream.runDrain,
          Effect.forkScoped,
        )
        yield* Deferred.await(entered)
        yield* Fiber.interrupt(first)
        const interrupted = yield* Fiber.await(first)
        assert.strictEqual(interrupted._tag, 'Failure')
        const collected = yield* Inspection.changes(store).pipe(
          Stream.tap(() => Effect.flatMap(ResourceScope, (scope) => Scope.close(scope, Exit.void))),
          Stream.runCollect,
        )
        assert.strictEqual(collected.length, 1)
      }).pipe((effect) => withLayer(effect, layers)),
    ),
  )
})
