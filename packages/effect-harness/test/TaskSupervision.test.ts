import * as TemporaryStorage from './fixtures/Storage.js'
import { assert, describe, it } from '@effect/vitest'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Fiber from 'effect/Fiber'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as Document from '../src/Document.js'
import * as Record from '../src/Record.js'
import * as Registry from '../src/Registry.js'
import * as Session from '../src/Session.js'
import * as Settings from '../src/Settings.js'

import * as Task from '../src/Task.js'
import { TaskExecution } from '../src/TaskExecution.js'
import * as TaskSupervision from '../src/TaskSupervision.js'
import * as Toolkit from '../src/Toolkit.js'
import * as Transaction from '../src/Transaction.js'
import * as AgentState from '../src/internal/AgentState.js'

const root = Record.ROOT_CONVERSATION_ID
const contract = (kind: string) => ({
  kind,
  version: 1,
  input: Schema.Natural,
  checkpoint: Schema.Struct({ phase: Schema.Literal('run') }),
  success: Schema.Natural,
  failure: Schema.String,
})
const finish = <C extends Task.CheckpointCodec>(
  definition: {
    readonly kind: string
    readonly version: number
    readonly input: typeof Schema.Natural
    readonly checkpoint: C
    readonly success: typeof Schema.Natural
    readonly failure: typeof Schema.String
  },
  result = 0,
) =>
  TaskExecution.pipe(
    Effect.flatMap((access) =>
      access.commit((tx) =>
        access.progress(tx, definition, {
          status: 'completing',
          outcome: { status: 'completed', result },
        }),
      ),
    ),
  )
const install = <R, E>(registry: Registry.Service, registration: Registry.Registration<R, E>) =>
  registry.install(registration)
const registration = <
  I extends Schema.Constraint,
  C extends Task.CheckpointCodec,
  S extends Schema.Constraint,
  F extends Schema.Constraint,
  H extends Task.Phases<I, C>,
  B extends Task.Handler<I, C> | undefined,
  R,
  E,
>(
  definition: Task.Task<I, C, S, F, H, B, R, E>,
) =>
  Registry.withTask(
    Registry.toolkit({ id: definition.kind, version: 1 }, Toolkit.make()),
    definition,
  )
const open = Effect.gen(function* () {
  const session = yield* Session.make()
  yield* Session.commit(session, Transaction.ensureRoot)
  const registry = yield* Registry.make
  const scheduler = yield* TaskSupervision.make(session).pipe(
    Effect.provideService(Registry.Registry, registry),
  )
  return { session, registry, scheduler }
})
const settle = Effect.forEach(Array.from({ length: 100 }), () => Effect.yieldNow, { discard: true })

describe('native task ownership', () => {
  it.effect(
    'native background creation installs a real independent supervisor that owns a conversation',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const { session, registry, scheduler } = yield* open
          const entered = yield* Deferred.make<void>()
          let backgroundId: Record.TaskId | undefined
          let conversationId: Record.ConversationId | undefined
          const backgroundContract = contract('supervision.realBackground')
          const background = Task.make({
            ...backgroundContract,
            initial: () => ({ phase: 'run' as const }),
            phases: {
              run: () =>
                Effect.gen(function* () {
                  const access = yield* TaskExecution
                  conversationId = (yield* access.commit((tx) => access.createConversation(tx))).id
                  yield* Deferred.succeed(entered, undefined)
                  return yield* Effect.never
                }),
            },
          })
          const parentContract = contract('supervision.realBackgroundParent')
          const parent = Task.make({
            ...parentContract,
            initial: () => ({ phase: 'run' as const }),
            phases: {
              run: () =>
                Effect.gen(function* () {
                  const access = yield* TaskExecution
                  const extraOptions = {
                    restartPolicy: 'resume' as const,
                    owner: access.taskId,
                    background: false,
                  }
                  backgroundId = (yield* access.commit((tx) =>
                    access.createBackgroundSupervisor(tx, background, 0, extraOptions),
                  )).id
                  yield* Deferred.await(entered)
                  yield* finish(parentContract)
                }),
            },
          })
          yield* install(registry, registration(background))
          yield* install(registry, registration(parent))
          const ref = yield* scheduler.start(parent, 0, { conversationId: root })
          assert.strictEqual((yield* scheduler.wait(ref)).status, 'completed')
          const record = (yield* scheduler.inspect(backgroundId!)).record
          assert.isTrue(record.background)
          assert.isUndefined(record.owner)
          const conversation = yield* Session.conversation(session, conversationId!)
          assert.isTrue(Option.isSome(conversation))
          if (Option.isSome(conversation))
            assert.strictEqual(conversation.value.owner?.taskId, backgroundId)
          yield* scheduler.abortConversation(root)
          assert.isFalse((yield* scheduler.inspect(backgroundId!)).record.abortRequested)
          yield* scheduler.abortConversation(root, { includeBackground: true })
          assert.strictEqual((yield* scheduler.waitForTask(backgroundId!)).state.status, 'terminal')
        }),
      ).pipe(Effect.provide(TemporaryStorage.layer)),
  )

  it.effect(
    'rejects invalid child admission and self, ancestor, missing, unowned failFast and cyclic waits',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const { session, registry, scheduler } = yield* open
          const childContract = contract('supervision.admissionChild')
          let rejected = 0
          let siblingId: Record.TaskId | undefined
          const child = Task.make({
            ...childContract,
            initial: () => ({ phase: 'run' as const }),
            phases: {
              run: ({ input }) =>
                Effect.gen(function* () {
                  const access = yield* TaskExecution
                  const owner = yield* Schema.decodeEffect(Record.TaskId)(input)
                  const absent = yield* Schema.decodeEffect(Record.TaskId)(900000)
                  for (const [id, policy] of [
                    [access.taskId, 'allSettled'],
                    [owner, 'allSettled'],
                    [absent, 'allSettled'],
                    [siblingId!, 'failFast'],
                    [siblingId!, 'allSettled'],
                  ] as const) {
                    const failure = yield* access
                      .commit((tx) =>
                        access.progress(tx, childContract, {
                          status: 'waiting',
                          checkpoint: { phase: 'run' },
                          on: [id],
                          policy,
                        }),
                      )
                      .pipe(Effect.flip)
                    assert.strictEqual(failure._tag, 'TaskError')
                    rejected++
                  }
                  yield* finish(childContract)
                }),
            },
          })
          const siblingContract = contract('supervision.admissionSibling')
          const sibling = Task.make({
            ...siblingContract,
            initial: () => ({ phase: 'run' as const }),
            phases: { run: () => finish(siblingContract) },
          })
          const parentContract = contract('supervision.admissionParent')
          const parent = Task.make({
            ...parentContract,
            initial: () => ({ phase: 'run' as const }),
            phases: {
              run: () =>
                Effect.gen(function* () {
                  const access = yield* TaskExecution
                  yield* access.commit((tx) =>
                    Effect.gen(function* () {
                      const conversation = yield* access.createConversation(tx)
                      for (const options of [
                        { background: true },
                        { conversationId: conversation.id },
                      ]) {
                        const failure = yield* access
                          .createChild(tx, child, access.taskId, options)
                          .pipe(Effect.flip)
                        assert.strictEqual(failure._tag, 'TaskError')
                        rejected++
                      }
                      const extraOptions = { restartPolicy: 'resume' as const, owner: undefined }
                      const first = yield* access.createChild(
                        tx,
                        child,
                        access.taskId,
                        extraOptions,
                      )
                      const admitted = yield* Transaction.task(tx, first.id)
                      assert.isTrue(Option.isSome(admitted))
                      if (Option.isSome(admitted))
                        assert.strictEqual(admitted.value.owner, access.taskId)
                      const second = yield* access.createChild(tx, sibling, 0)
                      siblingId = second.id
                      const stored = yield* Transaction.task(tx, second.id)
                      assert.isTrue(Option.isSome(stored))
                      if (Option.isSome(stored))
                        yield* Transaction.putTask(tx, {
                          ...stored.value,
                          state: {
                            status: 'waiting',
                            checkpoint: { phase: 'run' },
                            on: [first.id],
                            policy: 'allSettled',
                          },
                        })
                    }),
                  )
                  yield* finish(parentContract)
                }),
            },
          })
          yield* install(registry, registration(child))
          yield* install(registry, registration(sibling))
          yield* install(registry, registration(parent))
          const ref = yield* scheduler.start(parent, 0, { conversationId: root })
          assert.strictEqual((yield* scheduler.wait(ref)).status, 'completed')
          assert.strictEqual(rejected, 7)
          assert.strictEqual((yield* Session.scanTasks(session).pipe(Stream.runCollect)).length, 3)
          assert.deepStrictEqual(yield* scheduler.outcomes([siblingId!, siblingId!]), [
            { status: 'completed', result: 0 },
            { status: 'completed', result: 0 },
          ])
          assert.strictEqual(
            (yield* scheduler
              .waitForTask(yield* Schema.decodeEffect(Record.TaskId)(900000))
              .pipe(Effect.flip))._tag,
            'TaskError',
          )
        }),
      ).pipe(Effect.provide(TemporaryStorage.layer)),
  )

  for (const outcome of [
    { status: 'failed', result: 'bad', error: { message: 'bad' } },
    { status: 'faulted', error: { message: 'fault' } },
    { status: 'aborted', reason: 'cancel' },
    { status: 'orphaned', reason: 'missing' },
  ] as const)
    it.effect(`${outcome.status} completing outcome cascades and cleans bottom-up`, () =>
      Effect.scoped(
        Effect.gen(function* () {
          const { registry, scheduler } = yield* open
          const entered = yield* Deferred.make<void>()
          const childContract = contract('supervision.heldFailureChild')
          let aborted = 0
          const child = Task.make({
            ...childContract,
            initial: () => ({ phase: 'run' as const }),
            phases: {
              run: () => Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)),
            },
            abort: () =>
              Effect.gen(function* () {
                aborted++
                const access = yield* TaskExecution
                yield* access.commit((tx) =>
                  access.progress(tx, childContract, {
                    status: 'completing',
                    outcome: { status: 'aborted' },
                  }),
                )
              }),
          })
          const parentContract = contract('supervision.heldFailureParent')
          const parent = Task.make({
            ...parentContract,
            initial: () => ({ phase: 'run' as const }),
            phases: {
              run: () =>
                Effect.gen(function* () {
                  const access = yield* TaskExecution
                  yield* access.commit((tx) => access.createChild(tx, child, 0))
                  yield* Deferred.await(entered)
                  yield* access.commit((tx) =>
                    access.progress(tx, parentContract, { status: 'completing', outcome }),
                  )
                }),
            },
          })
          yield* install(registry, registration(child))
          yield* install(registry, registration(parent))
          const ref = yield* scheduler.start(parent, 0, { conversationId: root })
          assert.deepStrictEqual(yield* scheduler.wait(ref), outcome)
          assert.strictEqual(aborted, 1)
        }),
      ).pipe(Effect.provide(TemporaryStorage.layer)),
    )

  it.effect(
    'ordinary abort respects backgrounds; explicit inclusion captures background work inside owned conversations',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const { registry, scheduler } = yield* open
          const entered = yield* Deferred.make<void>()
          const cleanupStarted = yield* Deferred.make<void>()
          const cleanupRelease = yield* Deferred.make<void>()
          const parentStarted = yield* Deferred.make<void>()
          const lateStarted = yield* Deferred.make<void>()
          let backgroundId: Record.TaskId | undefined
          let backgroundRuns = 0
          const leaves: Array<Record.TaskId> = []
          const leafContract = contract('supervision.backgroundLeaf')
          const leaf = Task.make({
            ...leafContract,
            initial: () => ({ phase: 'run' as const }),
            phases: { run: () => Effect.never },
            abort: () =>
              Effect.gen(function* () {
                const access = yield* TaskExecution
                yield* access.commit((tx) =>
                  access.progress(tx, leafContract, {
                    status: 'completing',
                    outcome: { status: 'aborted' },
                  }),
                )
              }),
          })
          const backgroundContract = contract('supervision.background')
          const background = Task.make({
            ...backgroundContract,
            initial: () => ({ phase: 'run' as const }),
            phases: {
              run: () =>
                Effect.gen(function* () {
                  backgroundRuns++
                  const access = yield* TaskExecution
                  // A real background supervisor owns a separate transcript and ordinary leaf work.
                  yield* access.commit((tx) =>
                    Effect.gen(function* () {
                      const conversation = yield* access.createConversation(tx)
                      leaves.push(
                        (yield* access.createConversationTask(tx, conversation.id, leaf, 0)).id,
                      )
                    }),
                  )
                  yield* Deferred.succeed(backgroundRuns === 1 ? entered : lateStarted, undefined)
                  return yield* Effect.never
                }),
            },
            abort: () =>
              Effect.gen(function* () {
                yield* Deferred.succeed(cleanupStarted, undefined)
                yield* Deferred.await(cleanupRelease)
                const access = yield* TaskExecution
                yield* access.commit((tx) =>
                  access.progress(tx, backgroundContract, {
                    status: 'completing',
                    outcome: { status: 'aborted' },
                  }),
                )
              }),
          })
          const parentContract = contract('supervision.backgroundParent')
          const parent = Task.make({
            ...parentContract,
            initial: () => ({ phase: 'run' as const }),
            phases: {
              run: () =>
                Effect.gen(function* () {
                  const access = yield* TaskExecution
                  backgroundId = yield* access.commit((tx) =>
                    Effect.gen(function* () {
                      const conversation = yield* access.createConversation(tx)
                      const ref = yield* access.createConversationTask(
                        tx,
                        conversation.id,
                        background,
                        0,
                        { background: true },
                      )
                      return ref.id
                    }),
                  )
                  yield* Deferred.succeed(parentStarted, undefined)
                  return yield* Effect.never
                }),
            },
          })
          yield* install(registry, registration(leaf))
          yield* install(registry, registration(background))
          yield* install(registry, registration(parent))
          const ref = yield* scheduler.start(parent, 0, { conversationId: root })
          yield* Deferred.await(parentStarted)
          yield* Deferred.await(entered)
          yield* scheduler.abort(ref.id)
          assert.strictEqual((yield* scheduler.wait(ref)).status, 'aborted')
          assert.isFalse((yield* scheduler.inspect(backgroundId!)).record.abortRequested)
          assert.isFalse((yield* scheduler.inspect(leaves[0]!)).record.abortRequested)
          const included = yield* scheduler
            .abort(ref.id, { includeBackground: true })
            .pipe(Effect.forkChild)
          yield* Deferred.await(cleanupStarted)
          assert.strictEqual((yield* scheduler.inspect(leaves[0]!)).record.state.status, 'terminal')
          assert.isUndefined(included.pollUnsafe())
          // A later background task inside the reached conversation is not part of the snapshot.
          const late = yield* scheduler.start(background, 0, {
            conversationId: (yield* scheduler.inspect(backgroundId!)).record.conversationId,
            background: true,
          })
          yield* Deferred.await(lateStarted)
          assert.strictEqual(backgroundRuns, 2)
          assert.isFalse((yield* scheduler.inspect(late.id)).record.abortRequested)
          assert.isFalse((yield* scheduler.inspect(leaves[1]!)).record.abortRequested)
          yield* Deferred.succeed(cleanupRelease, undefined)
          yield* Fiber.join(included)
          assert.strictEqual((yield* scheduler.waitForTask(backgroundId!)).state.status, 'terminal')
          assert.strictEqual((yield* scheduler.inspect(late.id)).record.state.status, 'running')
        }),
      ).pipe(Effect.provide(TemporaryStorage.layer)),
  )
  for (const [policy, ownedConversation] of [
    ['allSettled', false],
    ['allSettled', true],
    ['failFast', false],
  ] as const)
    it.effect(
      `${policy}${ownedConversation ? ' owned-conversation' : ''}: atomic two-child wait, ordered receipts and parent choice`,
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const { session, registry, scheduler } = yield* open
            const release = yield* Deferred.make<void>()
            const entered = yield* Deferred.make<void>()
            const siblingContract = contract('supervision.sibling')
            let cleanup = 0
            const sibling = Task.make({
              ...siblingContract,
              initial: () => ({ phase: 'run' as const }),
              phases: {
                run: () =>
                  Effect.gen(function* () {
                    yield* Deferred.succeed(entered, undefined)
                    yield* Deferred.await(release)
                    yield* finish(siblingContract, 7)
                  }),
              },
              abort: () =>
                Effect.gen(function* () {
                  cleanup++
                  const access = yield* TaskExecution
                  yield* access.commit((tx) =>
                    access.progress(tx, siblingContract, {
                      status: 'completing',
                      outcome: { status: 'aborted', reason: 'sibling failed' },
                    }),
                  )
                }),
            })
            const failContract = contract('supervision.failure')
            const failure = Task.make({
              ...failContract,
              initial: () => ({ phase: 'run' as const }),
              phases: {
                run: () =>
                  Effect.gen(function* () {
                    yield* Deferred.await(entered)
                    return yield* Effect.fail('domain failure')
                  }),
              },
            })
            const parentContract = {
              ...contract('supervision.parent'),
              checkpoint: Schema.Union([
                Schema.Struct({ phase: Schema.Literal('spawn') }),
                Schema.Struct({
                  phase: Schema.Literal('joined'),
                  ids: Schema.Array(Record.TaskId),
                }),
              ]),
            }
            let starts = 0
            let joins = 0
            const parent = Task.make({
              ...parentContract,
              initial: () => ({ phase: 'spawn' as const }),
              phases: {
                spawn: () =>
                  Effect.gen(function* () {
                    starts++
                    const access = yield* TaskExecution
                    yield* access.commit((tx) =>
                      Effect.gen(function* () {
                        const first = yield* access.createChild(tx, failure, 0)
                        const second = ownedConversation
                          ? yield* access.createConversationTask(
                              tx,
                              (yield* access.createConversation(tx)).id,
                              sibling,
                              0,
                            )
                          : yield* access.createChild(tx, sibling, 0)
                        yield* access.progress(tx, parentContract, {
                          status: 'waiting',
                          checkpoint: { phase: 'joined', ids: [second.id, first.id] },
                          on: [first.id, second.id],
                          policy,
                        })
                      }),
                    )
                  }),
                joined: ({ checkpoint }) =>
                  Effect.gen(function* () {
                    joins++
                    const access = yield* TaskExecution
                    const receipts = yield* access.outcomes(checkpoint.ids)
                    assert.deepStrictEqual(
                      receipts.map((item) => item.status),
                      [policy === 'failFast' ? 'aborted' : 'completed', 'failed'],
                    )
                    yield* finish(parentContract, 42)
                  }),
              },
            })
            yield* install(registry, registration(sibling))
            yield* install(registry, registration(failure))
            yield* install(registry, registration(parent))
            const ref = yield* scheduler.start(parent, 0, { conversationId: root })
            yield* Deferred.await(entered)
            yield* settle
            if (policy === 'allSettled') {
              assert.strictEqual((yield* scheduler.inspect(ref.id)).record.state.status, 'waiting')
              yield* Deferred.succeed(release, undefined)
            }
            assert.deepStrictEqual(yield* scheduler.wait(ref), { status: 'completed', result: 42 })
            assert.strictEqual(starts, 1)
            assert.strictEqual(joins, 1)
            assert.strictEqual(cleanup, policy === 'failFast' ? 1 : 0)
            const tasks = yield* Session.scanTasks(session).pipe(Stream.runCollect)
            assert.strictEqual(tasks.length, 3)
            assert.isTrue(tasks.every((record) => record.state.status === 'terminal'))
            const direct = tasks.filter((record) => record.owner === ref.id)
            assert.strictEqual(direct.length, ownedConversation ? 1 : 2)
            if (ownedConversation) {
              const conversationTask = tasks.find(
                (record) => record.id !== ref.id && record.owner === undefined,
              )!
              const conversation = yield* Session.conversation(
                session,
                conversationTask.conversationId,
              )
              assert.isTrue(Option.isSome(conversation))
              if (Option.isSome(conversation))
                assert.strictEqual(conversation.value.owner?.taskId, ref.id)
            }
          }),
        ).pipe(Effect.provide(TemporaryStorage.layer)),
    )

  it.effect('completion holds task documents until owned-conversation foreground work drains', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { session, registry, scheduler } = yield* open
        const release = yield* Deferred.make<void>()
        const entered = yield* Deferred.make<void>()
        const doc = Document.define({
          kind: 'supervision.held',
          version: 1,
          scope: 'task',
          schema: Schema.Struct({ n: Schema.Natural }),
          initial: () => ({ n: 9 }),
        })
        const childContract = contract('supervision.conversationChild')
        const child = Task.make({
          ...childContract,
          initial: () => ({ phase: 'run' as const }),
          phases: {
            run: () =>
              Deferred.succeed(entered, undefined).pipe(
                Effect.andThen(Deferred.await(release)),
                Effect.andThen(finish(childContract, 7)),
              ),
          },
        })
        const parentContract = contract('supervision.heldParent')
        const parent = Task.make({
          ...parentContract,
          initial: () => ({ phase: 'run' as const }),
          phases: {
            run: () =>
              Effect.gen(function* () {
                const access = yield* TaskExecution
                yield* access.commit((tx) =>
                  Effect.gen(function* () {
                    yield* Transaction.ensureDocument(tx, doc, {
                      scope: { _tag: 'task', taskId: access.taskId },
                    })
                    const conversation = yield* access.createConversation(tx)
                    yield* access.createConversationTask(tx, conversation.id, child, 0)
                    yield* access.progress(tx, parentContract, {
                      status: 'completing',
                      outcome: { status: 'completed', result: 10 },
                    })
                  }),
                )
              }),
          },
        })
        yield* install(registry, registration(child))
        yield* install(registry, registration(parent))
        const ref = yield* scheduler.start(parent, 0, { conversationId: root })
        yield* Deferred.await(entered)
        assert.deepStrictEqual(yield* scheduler.failure, Option.none())
        const target = { scope: { _tag: 'task' as const, taskId: ref.id } }
        assert.strictEqual((yield* scheduler.inspect(ref.id)).record.state.status, 'completing')
        assert.isTrue(Option.isSome(yield* Session.snapshot(session, doc, target)))
        yield* Deferred.succeed(release, undefined)
        assert.deepStrictEqual(yield* scheduler.wait(ref), { status: 'completed', result: 10 })
        assert.isTrue(Option.isNone(yield* Session.snapshot(session, doc, target)))
        assert.strictEqual(
          (yield* Session.scanConversations(session, { ownerTaskId: ref.id }).pipe(
            Stream.runCollect,
          )).length,
          1,
        )
      }),
    ).pipe(Effect.provide(TemporaryStorage.layer)),
  )

  it.effect(
    'stored configuration inheritance, overrides, asOf forks and captured live settings',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const { session, registry, scheduler } = yield* open
          const settings = yield* Settings.Settings
          let ids: Array<Record.ConversationId> = []
          const first = yield* Session.commit(session, (tx) =>
            Effect.gen(function* () {
              yield* Transaction.ensureDocument(tx, AgentState.document, AgentState.target(root))
              yield* Transaction.setDocument(tx, AgentState.document, AgentState.target(root), {
                instructions: 'old',
                tools: ['one'],
                extensions: ['ext'],
              })
              return yield* Transaction.appendEntry(tx, root, { kind: 'agent.old', data: null })
            }),
          )
          yield* Session.commit(session, (tx) =>
            Transaction.setDocument(tx, AgentState.document, AgentState.target(root), {
              instructions: 'new',
              tools: ['two'],
              modelTools: { mode: 'explicit', names: ['visible'] },
              toolkits: { inherit: false, registrations: ['selected'] },
            }),
          )
          const parentContract = contract('supervision.inheritance')
          const parent = Task.make({
            ...parentContract,
            initial: () => ({ phase: 'run' as const }),
            phases: {
              run: () =>
                Effect.gen(function* () {
                  const access = yield* TaskExecution
                  assert.strictEqual((yield* access.agent).instructions, 'new')
                  assert.strictEqual((yield* access.settings).maxTurns, 8)
                  yield* settings.update({ maxTurns: 9 })
                  assert.strictEqual((yield* access.settings).maxTurns, 9)
                  ids = yield* access.commit((tx) =>
                    Effect.gen(function* () {
                      const current = yield* access.createConversation(tx)
                      const override = yield* access.createConversation(tx, {
                        agent: {
                          instructions: 'override',
                          tools: null,
                          modelTools: null,
                          toolkits: { inherit: true, registrations: ['overridden'] },
                        },
                      })
                      const fork = yield* access.createConversation(tx, {
                        fork: { conversationId: root, at: first.id },
                      })
                      return [current.id, override.id, fork.id]
                    }),
                  )
                  yield* finish(parentContract)
                }),
            },
          })
          yield* install(registry, registration(parent))
          yield* scheduler.wait(yield* scheduler.start(parent, 0, { conversationId: root }))
          const snapshots = yield* Effect.forEach(ids, (id) =>
            Session.snapshot(session, AgentState.document, AgentState.target(id)),
          )
          assert.deepStrictEqual(
            snapshots.map((found) => (Option.isSome(found) ? found.value.value : undefined)),
            [
              {
                instructions: 'new',
                tools: ['two'],
                modelTools: { mode: 'explicit', names: ['visible'] },
                toolkits: { inherit: false, registrations: ['selected'] },
              },
              {
                instructions: 'override',
                toolkits: { inherit: true, registrations: ['overridden'] },
              },
              { instructions: 'old', tools: ['one'], extensions: ['ext'] },
            ],
          )
          yield* Session.commit(session, (tx) =>
            Transaction.setDocument(tx, AgentState.document, AgentState.target(root), {
              instructions: 'later',
            }),
          )
          assert.deepStrictEqual(
            yield* Session.snapshot(session, AgentState.document, AgentState.target(ids[0]!)),
            snapshots[0],
          )
        }),
      ).pipe(Effect.provide(Layer.merge(TemporaryStorage.layer, Settings.layer({ maxTurns: 8 })))),
  )
})
