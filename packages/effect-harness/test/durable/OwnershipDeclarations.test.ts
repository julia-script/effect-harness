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

import * as Ownership from 'effect-harness/durable/Ownership'

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
        executionId: Option.getOrUndefined(Option.map(parent, (instance) => instance.executionId)),
        active: Option.getOrElse(
          Option.map(parent, (instance) => instance.activityState.count),
          () => 0,
        ),
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

describe('OwnershipDeclarations', () => {
  it.effect(
    'keeps metadata engine-free and restores captured schema services with each call native identity',
    () =>
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
                parents.push(
                  Option.getOrUndefined(Option.map(parent, (instance) => instance.executionId)),
                )
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
  )
})
