import { assert, describe, it } from '@effect/vitest'
import * as Cause from 'effect/Cause'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import * as Prompt from 'effect/ai/Prompt'
import * as Hook from 'effect-harness/Hook'
import * as HookReducer from 'effect-harness/HookReducer'
import * as Registry from 'effect-harness/Registry'
import { HookExecution } from 'effect-harness/HookExecution'
import { ExecutionError } from 'effect-harness/ExecutionError'
import { SessionError } from 'effect-harness/SessionError'
import { StorageError } from 'effect-harness/StorageError'
import { TaskError } from 'effect-harness/TaskError'
import { ToolExecution } from 'effect-harness/ToolExecution'
import { call, execution, registration } from './fixtures/Registry.js'

const storage = new StorageError({ reason: 'invalid', operation: 'hook', message: 'critical-one' })
const session = new SessionError({ reason: 'invalid', operation: 'hook', message: 'critical-two' })
const criticalCauses: ReadonlyArray<
  readonly [string, Cause.Cause<string | StorageError | SessionError | ExecutionError | TaskError>]
> = [
  ['critical and defect', Cause.combine(Cause.fail(storage), Cause.die('defect-sibling'))],
  ['multiple critical', Cause.combine(Cause.fail(storage), Cause.fail(session))],
  ['ordinary before critical', Cause.combine(Cause.fail('ordinary-sibling'), Cause.fail(storage))],
  ['critical before ordinary', Cause.combine(Cause.fail(storage), Cause.fail('ordinary-sibling'))],
  ['critical and interrupt', Cause.combine(Cause.fail(storage), Cause.interrupt())],
  ['ordinary and interrupt', Cause.combine(Cause.fail('ordinary-sibling'), Cause.interrupt())],
  ['single storage', Cause.fail(storage)],
  ['single session', Cause.fail(session)],
  [
    'single execution',
    Cause.fail(
      new ExecutionError({ reason: 'closed', operation: 'hook', message: 'execution-critical' }),
    ),
  ],
  [
    'single task',
    Cause.fail(new TaskError({ reason: 'faulted', operation: 'hook', message: 'task-critical' })),
  ],
] as const
const access = Layer.effect(HookExecution, ToolExecution).pipe(Layer.provide(execution))

const assertComplete = (result: Exit.Exit<unknown, unknown>, original: Cause.Cause<unknown>) => {
  assert.isTrue(Exit.isFailure(result))
  if (!Exit.isFailure(result)) return assert.fail('Expected full hook failure')
  assert.strictEqual(result.cause.reasons.length, original.reasons.length)
  for (let i = 0; i < original.reasons.length; i++) {
    const expected = original.reasons[i]!
    const actual = result.cause.reasons[i]!
    assert.strictEqual(actual._tag, expected._tag)
    if (Cause.isFailReason(expected)) {
      assert.isTrue(Cause.isFailReason(actual))
      if (!Cause.isFailReason(actual)) return assert.fail('Expected typed hook failure')
      assert.isTrue(Schema.is(HookReducer.HookFailure)(actual.error))
      let error = actual.error
      while (Schema.is(HookReducer.HookFailure)(error)) error = error.cause
      assert.strictEqual(error, expected.error)
    } else assert.deepEqual(actual, expected)
  }
}

describe('Complete critical hook causes', () => {
  for (const [label, cause] of criticalCauses) {
    it.effect(`${label} survives repeated protection`, () =>
      Effect.gen(function* () {
        const result = yield* Effect.exit(
          HookReducer.protect(
            'beforeRequest',
            HookReducer.protect('beforeRequest', Effect.failCause(cause)),
          ),
        )
        assertComplete(result, cause)
      }),
    )
    it.effect(`${label} survives captured callback and dispatch`, () =>
      Effect.scoped(
        Effect.gen(function* () {
          const registry = yield* Registry.make
          let invoked = 0
          let reported = 0
          yield* registry.install(
            Registry.hook(
              { id: 'critical', version: 1 },
              Hook.make({ event: 'beforeRequest', execute: () => Effect.failCause(cause) }),
            ),
          )
          yield* registry.install(
            Registry.hook(
              { id: 'later', version: 1 },
              Hook.make({
                event: 'beforeRequest',
                execute: () =>
                  Effect.sync(() => {
                    invoked++
                    return Prompt.make('later')
                  }),
              }),
            ),
          )
          const lease = yield* registry.acquire
          const result = yield* Effect.exit(
            lease.dispatchHook(
              { _tag: 'beforeRequest', prompt: Prompt.make('original') },
              {
                report: () =>
                  Effect.sync(() => {
                    reported++
                  }),
              },
            ),
          )
          assertComplete(result, cause)
          assert.strictEqual(invoked, 0)
          assert.strictEqual(reported, 0)
        }),
      ).pipe(Effect.provide(access)),
    )
  }

  it.effect('schema failure remains critical with its defect sibling', () =>
    Effect.gen(function* () {
      const error = yield* Schema.decodeUnknownEffect(Schema.Finite)('invalid').pipe(Effect.flip)
      const cause = Cause.combine(Cause.fail(error), Cause.die('schema-defect'))
      assertComplete(
        yield* Effect.exit(HookReducer.protect('beforeRequest', Effect.failCause(cause))),
        cause,
      )
    }),
  )

  for (const [label, effect] of [
    ['ordinary', Effect.fail('ordinary')],
    ['defect', Effect.die('ordinary-defect')],
    [
      'ordinary compound',
      Effect.failCause(Cause.combine(Cause.fail('ordinary'), Cause.fail('ordinary-sibling'))),
    ],
    [
      'ordinary with defect',
      Effect.failCause(Cause.combine(Cause.fail('ordinary'), Cause.die('ordinary-defect'))),
    ],
    ['critical value in defect', Effect.die(storage)],
  ] as const) {
    it.effect(`${label} still reports and continues notifications`, () =>
      Effect.scoped(
        Effect.gen(function* () {
          const registry = yield* Registry.make
          let reported = 0
          let later = 0
          yield* registry.install(
            Registry.hook(
              { id: 'ordinary', version: 1 },
              Hook.make({ event: 'afterTools', execute: () => effect }),
            ),
          )
          yield* registry.install(
            Registry.hook(
              { id: 'later', version: 1 },
              Hook.make({
                event: 'afterTools',
                execute: () =>
                  Effect.sync(() => {
                    later++
                  }),
              }),
            ),
          )
          const lease = yield* registry.acquire
          yield* lease.dispatchHook(
            { _tag: 'afterTools', results: [] },
            {
              report: (failure) =>
                Effect.sync(() => {
                  assert.strictEqual(failure.reason, 'callback')
                  reported++
                }),
            },
          )
          assert.strictEqual(reported, 1)
          assert.strictEqual(later, 1)
        }),
      ).pipe(Effect.provide(access)),
    )
    it.effect(`${label} still blocks beforeTool at the first decision`, () =>
      Effect.scoped(
        Effect.gen(function* () {
          const registry = yield* Registry.make
          yield* registry.install(registration('tool', 'test'))
          let reported = 0
          let later = 0
          yield* registry.install(
            Registry.hook(
              { id: 'ordinary', version: 1 },
              Hook.make({ event: 'beforeTool', execute: () => effect }),
            ),
          )
          yield* registry.install(
            Registry.hook(
              { id: 'later', version: 1 },
              Hook.make({
                event: 'beforeTool',
                execute: () =>
                  Effect.sync(() => {
                    later++
                    return { _tag: 'allow' as const }
                  }),
              }),
            ),
          )
          const lease = yield* registry.acquire
          const result = yield* lease.dispatchHook(
            { _tag: 'beforeTool', call },
            {
              report: () =>
                Effect.sync(() => {
                  reported++
                }),
            },
          )
          assert.strictEqual(result?._tag, 'block')
          assert.strictEqual(reported, 1)
          assert.strictEqual(later, 0)
        }),
      ).pipe(Effect.provide(access)),
    )
  }
})
