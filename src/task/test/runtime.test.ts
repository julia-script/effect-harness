/* oxlint-disable max-lines, max-lines-per-function -- ported describe blocks */
/**
 * Ported from pi's "task runtime" and "task scheduling" (`harness-tasks.test.ts`) and the wait
 * validation cases of `harness-structured.test.ts`.
 */
import { describe, expect, test } from 'bun:test'
import { Deferred, Effect, Exit } from 'effect'
import { Ids } from '../../storage/schema/index.ts'
import type { JoinPolicy } from '../../storage/schema/Task.ts'
import { TaskError } from '../index.ts'
import type { Task } from '../index.ts'
import {
  abortedWith,
  completed,
  conversation,
  createTask,
  oneStep,
  open,
  runTest,
} from './support.ts'
import type { StepRuntime } from './support.ts'

const messageOf = <A, E extends { readonly message: string }, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.match(effect, { onFailure: (error) => error.message, onSuccess: () => 'succeeded' })

describe('task runtime', () => {
  test('refuses runtime operations after the invocation ends, also from a fiber it left behind', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        let captured: StepRuntime<null> | undefined
        const late = yield* Deferred.make<string>()
        const Leaves = oneStep('test.leaves', (_task, runtime) =>
          Effect.gen(function* () {
            captured = runtime
            // A detached fiber outlives the invocation; its writes are refused once it ends.
            yield* Effect.forkDetach(
              Effect.gen(function* () {
                yield* Effect.sleep('20 millis')
                yield* Deferred.succeed(late, yield* messageOf(runtime.memo('late', 1)))
              }),
            )
            yield* runtime.commit(() => Effect.succeed(completed(null)))
          }),
        )
        const { session, scheduler } = yield* open(storage, [Leaves])
        const id = yield* createTask(session, yield* conversation(session), Leaves, null)
        yield* scheduler.waitForTask(id)
        const ended = `Task ${id} invocation has ended`
        expect(yield* Deferred.await(late)).toBe(ended)
        const runtime = yield* Effect.orDie(Effect.fromNullishOr(captured))
        expect(yield* messageOf(runtime.commit(() => Effect.void))).toBe(ended)
        expect(yield* messageOf(runtime.memo('x', 1))).toBe(ended)
        expect(yield* messageOf(runtime.readMemo('x'))).toBe(ended)
        expect(yield* messageOf(runtime.getTask(id))).toBe(ended)
      }),
    ))

  test('keeps the first memo written under a name, including an inherited property name', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const seen: Array<unknown> = []
        const Memo = oneStep<number, Task.RuntimeError>('test.memo', (_task, runtime) =>
          Effect.gen(function* () {
            seen.push(
              yield* runtime.readMemo('toString'),
              yield* runtime.memo('toString', 1),
              yield* runtime.memo('toString', 2),
            )
            const kept = yield* runtime.memo('toString', 3)
            yield* runtime.commit(() => Effect.succeed(completed(kept)))
          }),
        )
        const { session, scheduler } = yield* open(storage, [Memo])
        const id = yield* createTask(session, yield* conversation(session), Memo, null)
        const receipt = yield* scheduler.waitForTask(id)
        expect(seen).toEqual([undefined, 1, 1])
        expect(receipt.state.outcome).toEqual({ status: 'completed', result: 1 })
      }),
    ))

  test('rejects waits on itself, a missing task, and failFast on a task it does not own', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const missing = Ids.TaskId.make(999_999)
        let other: Ids.TaskId | undefined
        const errors: Array<string> = []
        const cases: ReadonlyArray<
          readonly [(runtime: StepRuntime<null>) => ReadonlyArray<Ids.TaskId>, JoinPolicy]
        > = [
          [(runtime) => [runtime.taskId], 'allSettled'],
          [() => [missing], 'allSettled'],
          [() => (other === undefined ? [] : [other]), 'failFast'],
        ]
        const Waits = oneStep('test.waits', (_task, runtime) =>
          Effect.gen(function* () {
            for (const [on, policy] of cases) {
              errors.push(
                yield* messageOf(
                  runtime.commit(() =>
                    Effect.succeed({
                      status: 'waiting' as const,
                      checkpoint: { phase: 'run' as const },
                      on: on(runtime),
                      policy,
                    }),
                  ),
                ),
              )
            }
            yield* runtime.commit(() => Effect.succeed(completed(null)))
          }),
        )
        const Idle = oneStep('test.idle-other', (_task, runtime) =>
          runtime.commit(() => Effect.succeed(completed(null))),
        )
        const { session, scheduler } = yield* open(storage, [Waits, Idle])
        const root = yield* conversation(session)
        other = yield* createTask(session, root, Idle, null)
        yield* scheduler.waitForTask(other)
        const id = yield* createTask(session, root, Waits, null)
        yield* scheduler.waitForTask(id)
        expect(errors).toEqual([
          `Task ${id} cannot wait on ${id}: ${id} -> ${id} is a wait cycle`,
          `Task ${missing} does not exist`,
          `Task ${id} can wait failFast only on tasks it owns; ${other} is not one`,
        ])
      }),
    ))

  test('rejects a wait from an abort handler', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const reached = yield* Deferred.make<void>()
        let error = ''
        const Aborts = oneStep(
          'test.abort-wait',
          () => Effect.andThen(Deferred.done(reached, Exit.void), Effect.never),
          (runtime) =>
            Effect.gen(function* () {
              error = yield* messageOf(
                runtime.commit(() =>
                  Effect.succeed({
                    status: 'waiting' as const,
                    checkpoint: { phase: 'run' as const },
                    on: [],
                    policy: 'allSettled' as const,
                  }),
                ),
              )
              yield* runtime.commit(() => Effect.succeed(abortedWith('no wait')))
            }),
        )
        const { session, scheduler } = yield* open(storage, [Aborts])
        const id = yield* createTask(session, yield* conversation(session), Aborts, null)
        yield* Deferred.await(reached)
        yield* scheduler.abort(id)
        expect((yield* scheduler.waitForTask(id)).state.outcome).toEqual({
          status: 'aborted',
          reason: 'no wait',
        })
        expect(error).toBe(`Abort handler of task ${id} cannot wait`)
      }),
    ))
})

describe('task waits', () => {
  test('rejects an unknown task and returns the receipt of a terminal one', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const Done = oneStep('test.done-receipt', (_task, runtime) =>
          runtime.commit(() => Effect.succeed(completed(null))),
        )
        const { session, scheduler } = yield* open(storage, [Done])
        const missing = Ids.TaskId.make(999_999)
        const error = yield* Effect.flip(scheduler.waitForTask(missing))
        expect(error).toBeInstanceOf(TaskError.TaskNotFound)
        expect(error.message).toBe(`Task ${missing} does not exist`)
        expect(yield* Effect.flip(scheduler.abort(missing))).toBeInstanceOf(TaskError.TaskNotFound)
        const id = yield* createTask(session, yield* conversation(session), Done, null)
        const receipt = yield* scheduler.waitForTask(id)
        expect(yield* scheduler.waitForTask(id)).toEqual(receipt)
        expect(yield* scheduler.abort(id)).toBe('terminal')
      }),
    ))
})
