import * as Identity from '../../src/Identity.ts'
import { assert, describe, it } from '@effect/vitest'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Schedule from 'effect/Schedule'
import * as Schema from 'effect/Schema'
import * as Scope from 'effect/Scope'
import * as Workflow from 'effect/workflow/Workflow'
import * as WorkflowEngine from 'effect/workflow/WorkflowEngine'
import * as Store from '@effect-harness/durable/Store'
import * as Session from '@effect-harness/durable/Session'
import * as Directory from '@effect-harness/durable/SessionDirectory'
import * as Ownership from '@effect-harness/durable/Ownership'
import * as View from '@effect-harness/durable/View'
import * as Event from '@effect-harness/durable/Event'
import * as Memory from '@effect-harness/durable/storage/Memory'
import * as Cancellation from '@effect-harness/durable/workflow/Cancellation'

class Decoder extends Context.Service<
  Decoder,
  {
    readonly label: string
    readonly seen: Array<{
      readonly label: string
      readonly executionId: string | undefined
      readonly active: number
    }>
  }
>()('test/durable/services/Decoder') {}
const decodedId = Schema.Int.pipe(
  Schema.middlewareDecoding((effect) =>
    Effect.gen(function* () {
      const decoder = yield* Decoder
      const parent = yield* Effect.serviceOption(WorkflowEngine.WorkflowInstance)
      decoder.seen.push({
        label: decoder.label,
        executionId: Option.isSome(parent) ? parent.value.executionId : undefined,
        active: Option.isSome(parent) ? parent.value.activityState.count : 0,
      })
      return yield* effect
    }),
  ),
)
const retry = Schedule.spaced('1 millis')
const Work = Workflow.make('test/durable/services/Work', {
  payload: { n: decodedId },
  success: decodedId,
  error: Schema.String,
  idempotencyKey: ({ n }) => String(n),
  suspendedRetrySchedule: retry,
})

describe('contextual durable service construction', () => {
  it.effect(
    'constructs View and Event from the provided services with the same committed mount',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const store = yield* Memory.make
          const session = yield* Session.make.pipe(Effect.provideService(Store.Store, store))
          const root = yield* session.root()
          const views = yield* View.make.pipe(Effect.provideService(Store.Store, store))
          const events = yield* Event.make.pipe(Effect.provideService(View.View, views))
          const structural = yield* views.watch(root.id)
          const semantic = yield* events.watch(root.id)
          assert.strictEqual(semantic.snapshot.entries, structural.value.entries)
          assert.strictEqual(semantic.snapshot.entries.length, 0)
        }),
      ),
  )

  it.effect(
    'snapshots explicit registrations while preserving exact scoped Session references',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const store = yield* Memory.make
          const first = yield* Session.make.pipe(Effect.provideService(Store.Store, store))
          const second = yield* Session.make.pipe(Effect.provideService(Store.Store, store))
          const registrations = new Map([[Identity.SessionId.make('first'), first]])
          const context = yield* Layer.build(Directory.layer).pipe(
            Effect.provideService(Directory.Registrations, registrations),
          )
          const directory = Context.get(context, Directory.SessionDirectory)
          registrations.set(Identity.SessionId.make('first'), second)
          registrations.set(Identity.SessionId.make('late'), second)
          assert.strictEqual(yield* directory.resolve(Identity.SessionId.make('first')), first)
          assert.strictEqual(
            (yield* directory.resolve(Identity.SessionId.make('late')).pipe(Effect.flip)).reason
              ._tag,
            'NotFound',
          )
          const root = yield* first.root()
          assert.strictEqual(
            (yield* (yield* directory.resolve(Identity.SessionId.make('first')))
              .conversation(root.id)
              .pipe(Effect.map(Option.getOrUndefined)))?.id,
            root.id,
          )
          const single = yield* Directory.SessionDirectory.pipe(
            Effect.provide(Directory.layerSingle(Identity.SessionId.make('single'))),
            Effect.provideService(Session.Session, second),
          )
          assert.strictEqual(yield* single.resolve(Identity.SessionId.make('single')), second)
        }),
      ),
  )

  it.effect('uses the exact captured invocation Session instead of the ambient Session', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const store = yield* Memory.make
        const invocation = yield* Session.make.pipe(Effect.provideService(Store.Store, store))
        const ambient = yield* Session.make.pipe(Effect.provideService(Store.Store, store))
        const root = yield* invocation.root()
        const taskId = yield* invocation.transaction((tx) =>
          tx.createTask({
            conversationId: root.id,
            kind: 'identity',
            version: 1,
            input: null,
            background: false,
            abortRequested: false,
            state: { status: 'running' },
          }),
        )
        const identity = {
          sessionId: Identity.SessionId.make('invocation'),
          conversationId: root.id,
          taskId,
        }
        const current = yield* Cancellation.activity(identity, invocation, Ownership.Current).pipe(
          Effect.provide(Cancellation.layer),
          Effect.provideService(Session.Session, ambient),
        )
        assert.strictEqual(current.session, invocation)
        assert.strictEqual(current.taskId, taskId)
        assert.strictEqual((yield* current.check.pipe(Effect.flip)).reason._tag, 'Closed')
        assert.strictEqual(yield* invocation.isClosed, false)
      }),
    ),
  )

  it.effect(
    'keeps metadata engine-free and restores captured schema services with each call native identity',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const seen: Decoder['Service']['seen'] = []
          const captured = Decoder.of({ label: 'captured', seen })
          const scope = yield* Effect.acquireRelease(Scope.make(), (owned, exit) =>
            Scope.close(owned, exit),
          )
          const stale = WorkflowEngine.WorkflowInstance.initial(Work, 'construction-parent', scope)
          const metadata = yield* Ownership.Declarations.pipe(
            Effect.provide(Ownership.layerDeclarations([Work])),
            Effect.provideService(Decoder, captured),
            Effect.provideService(WorkflowEngine.WorkflowInstance, stale),
          )
          assert.strictEqual(Option.getOrUndefined(metadata.get(Work._tag)), Work)
          assert.strictEqual(Object.hasOwn(metadata, 'execute'), false)
          assert.strictEqual(seen.length, 0)
          const layer = Work.toLayer(({ n }) => Effect.succeed(n)).pipe(
            Layer.provideMerge(WorkflowEngine.layerMemory),
            Layer.provide(Layer.succeed(Decoder, captured)),
          )
          yield* Effect.gen(function* () {
            const engine = yield* WorkflowEngine.WorkflowEngine
            const schedules: Array<unknown> = []
            const parents: Array<string | undefined> = []
            const intercepted = WorkflowEngine.WorkflowEngine.of({
              ...engine,
              execute: (workflow, options) =>
                Effect.gen(function* () {
                  schedules.push(options.suspendedRetrySchedule)
                  const parent = yield* Effect.serviceOption(WorkflowEngine.WorkflowInstance)
                  parents.push(Option.isSome(parent) ? parent.value.executionId : undefined)
                  return yield* engine.execute(workflow, options)
                }),
            })
            for (const executionId of ['parent-one', 'parent-two']) {
              seen.length = 0
              const parent = WorkflowEngine.WorkflowInstance.initial(Work, executionId, scope)
              const value = yield* Ownership.execute({
                workflow: Work._tag,
                executionId: 'child-' + executionId,
                payload: { n: 7 },
              }).pipe(
                Effect.provideService(Ownership.Declarations, metadata),
                Effect.provideService(Decoder, Decoder.of({ label: 'wrong caller', seen })),
                Effect.provideService(WorkflowEngine.WorkflowInstance, parent),
                Effect.provideService(WorkflowEngine.WorkflowEngine, intercepted),
              )
              assert.strictEqual(value, 7)
              assert.deepStrictEqual(seen[0], { label: 'captured', executionId, active: 1 })
              assert.strictEqual(parent.activityState.count, 0)
              assert.ok(seen.every((item) => item.label === 'captured'))
            }
            assert.deepStrictEqual(schedules, [retry, retry])
            assert.deepStrictEqual(parents, ['parent-one', 'parent-two'])
          }).pipe(Effect.provide(layer))
        }),
      ),
  )
})
