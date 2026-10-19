/* oxlint-disable max-lines, max-lines-per-function -- ported describe blocks */
/**
 * Ported from pi's `packages/durable/test/harness-tasks.test.ts`, "task phases".
 */
import { describe, expect, test } from 'bun:test'
import { Deferred, Effect, Exit } from 'effect'
import type { Ids } from '../../storage/schema/index.ts'
import { Task } from '../index.ts'
import {
  Boom,
  completed,
  conversation,
  createTask,
  eventually,
  getTask,
  oneStep,
  open,
  runTest,
} from './support.ts'

describe('task phases', () => {
  test('continues one invocation through checkpoint progress and completes with its result', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const seen: Array<number> = []
        const runtimes = new Set<unknown>()
        const Counter = Task.make<{ to: number }, { phase: 'count'; n: number }, number>({
          name: 'test.counter',
          version: 1,
          initial: () => ({ phase: 'count', n: 0 }),
          phases: {
            count: (task, runtime) => {
              seen.push(task.state.checkpoint.n)
              runtimes.add(runtime)
              return runtime.commit((_tx, current) => {
                const n = current.state.checkpoint.n + 1
                return Effect.succeed(
                  n === task.input.to
                    ? completed(n)
                    : { status: 'running' as const, checkpoint: { phase: 'count' as const, n } },
                )
              })
            },
          },
          abort: () => Effect.void,
        })
        const { session, scheduler } = yield* open(storage, [Counter])
        const id = yield* createTask(session, yield* conversation(session), Counter, { to: 3 })
        const receipt = yield* scheduler.waitForTask(id)
        expect(receipt.state.outcome).toEqual({ status: 'completed', result: 3 })
        expect(seen).toEqual([0, 1, 2])
        expect(runtimes.size).toBe(1)
      }),
    ))

  test('faults a phase without durable progress, a failing phase, and a defect', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const Idle = oneStep('test.idle', () => Effect.void)
        // A commit that returns no state is not progress.
        const NoState = oneStep('test.no-state', (_task, runtime) =>
          runtime.commit(() => Effect.void),
        )
        const Fails = oneStep('test.fails', () => Effect.fail(new Boom({ message: 'boom' })))
        const Dies = oneStep('test.dies', () => Effect.die('broken'))
        const tasks = [Idle, NoState, Fails, Dies]
        const { session, scheduler } = yield* open(storage, tasks)
        const id = yield* conversation(session)
        const ids = yield* Effect.forEach(tasks, (task) => createTask(session, id, task, null))
        const outcomes = yield* Effect.forEach(ids, (task) =>
          Effect.map(scheduler.waitForTask(task), (receipt) => receipt.state.outcome),
        )
        expect(outcomes).toEqual([
          {
            status: 'faulted',
            error: { message: 'Task test.idle phase run returned without durable progress' },
          },
          {
            status: 'faulted',
            error: { message: 'Task test.no-state phase run returned without durable progress' },
          },
          { status: 'faulted', error: { message: 'boom' } },
          { status: 'faulted', error: { message: 'broken' } },
        ])
      }),
    ))

  test('keeps a committed terminal outcome when the handler fails afterwards', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const late = yield* Deferred.make<unknown>()
        const Done = oneStep<string, Boom | Task.RuntimeError>('test.done', (_task, runtime) =>
          Effect.gen(function* () {
            yield* runtime.commit(() => Effect.succeed(completed('ok')))
            yield* runtime
              .commit(() => Effect.void)
              .pipe(Effect.catch((error) => Deferred.succeed(late, error)))
            return yield* new Boom({ message: 'after terminal' })
          }),
        )
        const { session, scheduler } = yield* open(storage, [Done])
        const id = yield* createTask(session, yield* conversation(session), Done, null)
        const receipt = yield* scheduler.waitForTask(id)
        expect(receipt.state.outcome).toEqual({ status: 'completed', result: 'ok' })
        expect(String(yield* Deferred.await(late))).toContain(`Task ${id} is terminal`)
      }),
    ))

  test('compares checkpoints by value, including arrays', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const Collect = Task.make<null, { phase: 'collect'; items: Array<string> }, null>({
          name: 'test.collect',
          version: 1,
          initial: () => ({ phase: 'collect', items: [] }),
          phases: {
            collect: (task, runtime) => {
              const { items } = task.state.checkpoint
              // Two rounds of progress, then an equal copy of the checkpoint: no progress.
              const next = items.length < 2 ? [...items, `item${items.length}`] : [...items]
              return runtime.commit(() =>
                Effect.succeed({
                  status: 'running' as const,
                  checkpoint: { phase: 'collect' as const, items: next },
                }),
              )
            },
          },
          abort: () => Effect.void,
        })
        const { session, scheduler } = yield* open(storage, [Collect])
        const id = yield* createTask(session, yield* conversation(session), Collect, null)
        expect((yield* scheduler.waitForTask(id)).state.outcome).toEqual({
          status: 'faulted',
          error: { message: 'Task test.collect phase collect returned without durable progress' },
        })
      }),
    ))

  test('resumes a waiting task once every task in `on` is terminal, whatever the outcome', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const gate = yield* Deferred.make<void>()
        const order: Array<string> = []
        let on: ReadonlyArray<Ids.TaskId> = []
        let outcomes: Array<string> = []
        const Waiter = Task.make<null, { phase: 'wait' } | { phase: 'resume' }, null>({
          name: 'test.waiter',
          version: 1,
          initial: () => ({ phase: 'wait' }),
          phases: {
            wait: (_task, runtime) => {
              order.push('wait')
              const checkpoint = { phase: 'resume' } as const
              return runtime.commit(() =>
                Effect.succeed({
                  status: 'waiting' as const,
                  checkpoint,
                  on,
                  policy: 'allSettled' as const,
                }),
              )
            },
            resume: (_task, runtime) =>
              Effect.gen(function* () {
                order.push('resume')
                outcomes = (yield* runtime.outcomes(on)).map((outcome) => outcome.status)
                yield* runtime.commit(() => Effect.succeed(completed(null)))
              }),
          },
          abort: () => Effect.void,
        })
        const First = oneStep('test.first', (_task, runtime) =>
          Effect.gen(function* () {
            order.push('first')
            yield* Deferred.await(gate)
            yield* runtime.commit(() => Effect.succeed(completed(null)))
          }),
        )
        const Faulting = oneStep('test.faulting', () =>
          Effect.suspend(() => {
            order.push('faulting')
            return Effect.fail(new Boom({ message: 'fails' }))
          }),
        )
        const { session, scheduler } = yield* open(storage, [First, Faulting, Waiter])
        const root = yield* conversation(session)
        const first = yield* createTask(session, root, First, null)
        const faulting = yield* createTask(session, root, Faulting, null)
        on = [first, faulting]
        const waiter = yield* createTask(session, root, Waiter, null)
        yield* eventually(Effect.sync(() => order.length === 3))
        yield* eventually(
          Effect.map(getTask(session, waiter), (record) => record?.state.status === 'waiting'),
        )
        expect(order.toSorted()).toEqual(['faulting', 'first', 'wait'])
        expect((yield* getTask(session, waiter))?.state).toMatchObject({ status: 'waiting', on })
        yield* Deferred.done(gate, Exit.void)
        yield* scheduler.waitForTask(waiter)
        expect(order.at(-1)).toBe('resume')
        expect(outcomes).toEqual(['completed', 'faulted'])
      }),
    ))
})
