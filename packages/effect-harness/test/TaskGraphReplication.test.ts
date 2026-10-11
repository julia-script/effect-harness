import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as Scope from 'effect/Scope'
import * as Stream from 'effect/Stream'
import * as Schema from 'effect/Schema'
import * as LanguageModel from 'effect/ai/LanguageModel'
import * as Harness from 'effect-harness/Harness'
import { HarnessBackend } from 'effect-harness/HarnessBackend'
import * as HarnessRuntime from 'effect-harness/HarnessRuntime'
import * as Graph from 'effect-harness/TaskGraph'
import * as Record from 'effect-harness/Record'
import * as Session from 'effect-harness/Session'
import * as Storage from 'effect-harness/Storage'
import * as Transaction from 'effect-harness/Transaction'

describe('public structural replica', () => {
  it.effect('task-first atomic ownership, terminal retirement and remount epoch', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const model = yield* LanguageModel.make({
          generateText: () => Effect.die('No execution permitted'),
          streamText: () => Stream.die('No execution permitted'),
        })
        const runtime = yield* HarnessRuntime.make().pipe(
          Effect.provideService(LanguageModel.LanguageModel, model),
        )
        yield* Session.commit(runtime.session, Transaction.ensureRoot)
        const client = yield* Harness.make.pipe(
          Effect.provideService(HarnessBackend, runtime.backend),
        )
        const consumer = yield* Scope.make()
        const pull = yield* Stream.toPull(Graph.watch(client)).pipe(
          Effect.provideService(Scope.Scope, consumer),
        )
        const frame = (yield* pull)[0]
        if (frame._tag !== 'Snapshot') return yield* Effect.die('Initial snapshot required')
        let replica = frame
        const owner = yield* Session.commit(runtime.session, (tx) =>
          Transaction.createTask(tx, {
            conversationId: Record.ROOT_CONVERSATION_ID,
            kind: 'observation.ownership',
            version: 1,
            input: { privateInput: true },
            background: false,
            abortRequested: false,
            state: { status: 'pending', checkpoint: { phase: 'admit' } },
          }),
        )
        for (const update of yield* pull) replica = yield* Graph.reduce(replica, update)
        assert.deepStrictEqual(replica, yield* Graph.snapshot(client))
        const child = yield* Session.commit(runtime.session, (tx) =>
          Effect.gen(function* () {
            const conversation = yield* Transaction.createConversation(tx, {
              ownership: { _tag: 'task', taskId: owner.id },
            })
            const task = yield* Transaction.createTask(tx, {
              conversationId: owner.conversationId,
              kind: 'observation.child',
              version: 1,
              owner: owner.id,
              input: null,
              background: false,
              abortRequested: false,
              state: { status: 'running', checkpoint: { phase: 'work' } },
            })
            yield* Transaction.putTask(tx, {
              ...owner,
              state: {
                status: 'completing',
                outcome: { status: 'completed', result: { privateResult: true } },
              },
            })
            return { task, conversation }
          }),
        )
        for (const update of yield* pull) replica = yield* Graph.reduce(replica, update)
        assert.deepStrictEqual(replica, yield* Graph.snapshot(client))
        const node = replica.value.tasks.find((t) => t.id === owner.id)!
        assert.strictEqual(node.status, 'completing')
        assert.strictEqual(node.heldOutcomeStatus, 'completed')
        assert.deepStrictEqual(node.ownedConversations, [child.conversation.id])
        assert.notProperty(node, 'input')
        assert.notProperty(node, 'outcome')
        yield* Session.commit(runtime.session, (tx) =>
          Effect.gen(function* () {
            yield* Transaction.putTask(tx, {
              ...child.task,
              state: { status: 'terminal', outcome: { status: 'completed', result: null } },
            })
            yield* Transaction.putTask(tx, {
              ...owner,
              state: {
                status: 'terminal',
                outcome: { status: 'completed', result: { privateResult: true } },
              },
            })
          }),
        )
        for (const update of yield* pull) replica = yield* Graph.reduce(replica, update)
        assert.deepStrictEqual(replica.value.tasks, [])
        assert.deepStrictEqual(replica, yield* Graph.snapshot(client))
        const epoch = replica.revision.epoch
        yield* Scope.close(consumer, Exit.void)
        const fresh = yield* Graph.snapshot(client)
        assert.notStrictEqual(fresh.revision.epoch, epoch)
        assert.deepStrictEqual(fresh.value, replica.value)
      }),
    ).pipe(Effect.provide(Storage.layerMemory)),
  )

  it.effect('rejects duplicates, gaps, foreign epoch and invalid wire protocol', () =>
    Effect.gen(function* () {
      const initial: Graph.Snapshot = {
        _tag: 'Snapshot',
        protocol: 1,
        target: { _tag: 'sessionGraph' },
        revision: { epoch: 'one', cursor: 0 },
        value: { tasks: [], conversations: [] },
      }
      const frame: Graph.Frame = {
        _tag: 'Delta',
        protocol: 1,
        target: initial.target,
        epoch: 'one',
        from: 0,
        to: 1,
        ops: [{ _tag: 'Replace', value: initial.value }],
      }
      const next = yield* Graph.reduce(initial, frame)
      assert.strictEqual((yield* Graph.reduce(next, frame).pipe(Effect.flip)).reason, 'continuity')
      assert.strictEqual(
        (yield* Graph.reduce(initial, { ...frame, epoch: 'other' }).pipe(Effect.flip)).reason,
        'continuity',
      )
      assert.strictEqual(
        (yield* Graph.reduce(initial, { ...frame, from: 1, to: 2 }).pipe(Effect.flip)).reason,
        'continuity',
      )
      assert.strictEqual(
        (yield* Graph.reduce(initial, initial).pipe(Effect.flip)).reason,
        'continuity',
      )
      const bad = { ...frame }
      Reflect.set(bad, 'protocol', 2)
      assert.strictEqual((yield* Graph.reduce(initial, bad).pipe(Effect.flip)).reason, 'invalid')
      const rebase: Graph.Frame = {
        ...initial,
        _tag: 'Rebase',
        revision: { epoch: 'one', cursor: 22 },
      }
      const rebased = yield* Graph.reduce(initial, rebase)
      assert.strictEqual(rebased.revision.cursor, 22)
      assert.isTrue(Schema.is(Graph.SnapshotSchema)(rebased))
    }),
  )
})
