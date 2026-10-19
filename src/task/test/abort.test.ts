/* oxlint-disable max-lines, max-lines-per-function -- ported describe blocks */
/**
 * Ported from pi's `packages/durable/test/harness-tasks.test.ts`, "task abort".
 *
 * pi signals a run through `runtime.signal`, and a handler that ignores it keeps running until it
 * returns. Here the run's fiber is interrupted, so pi's "keeps working after the signal" cases use
 * an uninterruptible region, which is how Effect code defers interruption. New cases cover what
 * interruption adds: fibers a phase forked are interrupted with it, and so are its finalizers.
 */
import { describe, expect, test } from 'bun:test'
import { Deferred, Effect, Exit, Fiber } from 'effect'
import type { Ids } from '../../storage/schema/index.ts'
import { Task } from '../index.ts'
import type { Scheduler } from '../index.ts'
import {
  abortedWith,
  Boom,
  completed,
  conversation,
  createTask,
  eventually,
  getTask,
  holdable,
  oneStep,
  open,
  runTest,
  statusOf,
} from './support.ts'
import type { StepRuntime } from './support.ts'

const marked = (session: Parameters<typeof getTask>[0], id: Ids.TaskId) =>
  eventually(Effect.map(getTask(session, id), (record) => record?.abortRequested === true))

describe('task abort', () => {
  test("refuses the run's commits after the mark and settles through a fresh abort invocation", () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const reached = yield* Deferred.make<void>()
        const errors: Array<string> = []
        const runtimes: Array<unknown> = []
        let memoRead: unknown
        const Marked = oneStep(
          'test.marked',
          (task, runtime) =>
            Effect.gen(function* () {
              runtimes.push(runtime)
              yield* runtime.memo('kept', 1)
              yield* Deferred.done(reached, Exit.void)
              // Keep working after the mark: every later write of this run must be refused.
              yield* Effect.uninterruptible(
                Effect.gen(function* () {
                  yield* eventually(
                    Effect.map(
                      runtime.getTask(task.id),
                      (record) => record?.abortRequested === true,
                    ),
                  )
                  memoRead = yield* runtime.readMemo('kept')
                  for (const write of [
                    runtime.memo('late', 2),
                    runtime.commit(() => Effect.succeed(completed(null))),
                  ]) {
                    yield* Effect.catch(write, (error) =>
                      Effect.sync(() => errors.push(error.message)),
                    )
                  }
                }),
              )
            }),
          (runtime) => {
            runtimes.push(runtime)
            return runtime.commit((_tx, current) => {
              expect(current.abortRequested).toBe(true)
              expect(current.memos).toEqual({ kept: 1 })
              return Effect.succeed(abortedWith('mark'))
            })
          },
        )
        const { session, scheduler } = yield* open(storage, [Marked])
        const id = yield* createTask(session, yield* conversation(session), Marked, null)
        yield* Deferred.await(reached)
        expect(yield* scheduler.abort(id)).toBe('marked')
        expect((yield* scheduler.waitForTask(id)).state.outcome).toEqual({
          status: 'aborted',
          reason: 'mark',
        })
        expect(memoRead).toBe(1)
        expect(errors).toEqual([
          `Task ${id} has a durable abort mark`,
          `Task ${id} has a durable abort mark`,
        ])
        expect(runtimes).toHaveLength(2)
        expect(runtimes[1]).not.toBe(runtimes[0])
      }),
    ))

  test('starts no further phase after a mark that lands during a phase with progress', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const reached = yield* Deferred.make<void>()
        const proceed = yield* Deferred.make<void>()
        const phases: Array<string> = []
        const Two = Task.make<null, { phase: 'one' } | { phase: 'two' }, null>({
          name: 'test.mark-boundary',
          version: 1,
          initial: () => ({ phase: 'one' }),
          phases: {
            one: (_task, runtime) =>
              Effect.gen(function* () {
                phases.push('one')
                yield* runtime.commit(() =>
                  Effect.succeed({
                    status: 'running' as const,
                    checkpoint: { phase: 'two' as const },
                  }),
                )
                yield* Deferred.done(reached, Exit.void)
                // Returns normally after the mark: interruption waits for this region.
                yield* Effect.uninterruptible(Deferred.await(proceed))
              }),
            two: () => Effect.sync(() => phases.push('two')),
          },
          abort: (_task, runtime) => {
            phases.push('abort')
            return runtime.commit(() => Effect.succeed(abortedWith('boundary')))
          },
        })
        const { session, scheduler } = yield* open(storage, [Two])
        const id = yield* createTask(session, yield* conversation(session), Two, null)
        yield* Deferred.await(reached)
        const aborting = yield* Effect.forkChild(scheduler.abort(id))
        yield* marked(session, id)
        yield* Deferred.done(proceed, Exit.void)
        expect(yield* Fiber.join(aborting)).toBe('marked')
        expect((yield* scheduler.waitForTask(id)).state.outcome).toEqual({
          status: 'aborted',
          reason: 'boundary',
        })
        expect(phases).toEqual(['one', 'abort'])
      }),
    ))

  test('interrupts and joins the run before returning, with the fibers it forked', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const reached = yield* Deferred.make<void>()
        const ended: Array<string> = []
        const Interrupted = oneStep('test.interrupted', () =>
          Effect.gen(function* () {
            yield* Effect.forkChild(
              Effect.onInterrupt(Effect.never, () => Effect.sync(() => ended.push('child'))),
            )
            yield* Deferred.done(reached, Exit.void)
            return yield* Effect.never
          }).pipe(Effect.onInterrupt(() => Effect.sync(() => ended.push('phase')))),
        )
        const { session, scheduler } = yield* open(storage, [Interrupted])
        const id = yield* createTask(session, yield* conversation(session), Interrupted, null)
        yield* Deferred.await(reached)
        expect(yield* scheduler.abort(id)).toBe('marked')
        expect(ended.toSorted()).toEqual(['child', 'phase'])
        expect(yield* scheduler.abort(id)).toBe('marked')
        expect((yield* scheduler.waitForTask(id)).state.outcome).toEqual({
          status: 'aborted',
          reason: 'test',
        })
      }),
    ))

  test('aborts waiting work before its wait ends, and faults abort handlers that fail or settle nothing', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const gate = yield* Deferred.make<void>()
        const First = oneStep('test.dependency', (_task, runtime) =>
          Effect.andThen(
            Deferred.await(gate),
            runtime.commit(() => Effect.succeed(completed(null))),
          ),
        )
        let first: Ids.TaskId | undefined
        const wait = (_task: unknown, runtime: StepRuntime<null>) =>
          runtime.commit(() =>
            Effect.succeed({
              status: 'waiting' as const,
              checkpoint: { phase: 'run' as const },
              on: first === undefined ? [] : [first],
              policy: 'allSettled' as const,
            }),
          )
        const Lazy = oneStep('test.lazy-abort', wait, () => Effect.void)
        const Failing = oneStep('test.failing-abort', wait, () =>
          Effect.fail(new Boom({ message: 'abort failed' })),
        )
        const { session, scheduler } = yield* open(storage, [First, Lazy, Failing])
        const root = yield* conversation(session)
        first = yield* createTask(session, root, First, null)
        const lazy = yield* createTask(session, root, Lazy, null)
        const failing = yield* createTask(session, root, Failing, null)
        yield* eventually(Effect.map(statusOf(session, lazy), (status) => status === 'waiting'))
        yield* eventually(Effect.map(statusOf(session, failing), (status) => status === 'waiting'))
        yield* scheduler.abort(lazy)
        yield* scheduler.abort(failing)
        expect((yield* scheduler.waitForTask(lazy)).state.outcome).toEqual({
          status: 'faulted',
          error: { message: `Abort handler of task ${lazy} returned without a terminal outcome` },
        })
        expect((yield* scheduler.waitForTask(failing)).state.outcome).toEqual({
          status: 'faulted',
          error: { message: 'abort failed' },
        })
        expect(yield* statusOf(session, first)).toBe('running')
        yield* Deferred.done(gate, Exit.void)
        yield* scheduler.waitForTask(first)
      }),
    ))

  test('does not interrupt a running abort handler when aborted again; an interrupted caller leaves the mark', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const reached = yield* Deferred.make<void>()
        const proceed = yield* Deferred.make<void>()
        const release = yield* Deferred.make<void>()
        let aborts = 0
        let interrupted = false
        const Run = oneStep(
          'test.abort-again',
          // Defers interruption, so the first caller is still joining when it gives up.
          () => Effect.uninterruptible(Deferred.await(release)),
          (runtime) =>
            Effect.gen(function* () {
              aborts++
              yield* Deferred.done(reached, Exit.void)
              yield* Deferred.await(proceed)
              yield* runtime.commit(() => Effect.succeed(abortedWith('once')))
            }).pipe(Effect.onInterrupt(() => Effect.sync(() => (interrupted = true)))),
        )
        const { session, scheduler } = yield* open(storage, [Run])
        const id = yield* createTask(session, yield* conversation(session), Run, null)
        yield* eventually(Effect.map(statusOf(session, id), (status) => status === 'running'))
        const caller = yield* Effect.forkChild(scheduler.abort(id))
        yield* marked(session, id)
        yield* Fiber.interrupt(caller)
        yield* Deferred.done(release, Exit.void)
        yield* Deferred.await(reached)
        expect(yield* scheduler.abort(id)).toBe('marked')
        yield* Deferred.done(proceed, Exit.void)
        expect((yield* scheduler.waitForTask(id)).state.outcome).toEqual({
          status: 'aborted',
          reason: 'once',
        })
        expect(interrupted).toBe(false)
        expect(aborts).toBe(1)
      }),
    ))

  test('keeps a terminal outcome committed by an abort handler that fails afterwards', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const Run = oneStep(
          'test.abort-then-fail',
          () => Effect.never,
          (runtime) =>
            Effect.andThen(
              runtime.commit(() => Effect.succeed(abortedWith('done'))),
              Effect.fail(new Boom({ message: 'after terminal' })),
            ),
        )
        const { session, scheduler } = yield* open(storage, [Run])
        const id = yield* createTask(session, yield* conversation(session), Run, null)
        yield* eventually(Effect.map(statusOf(session, id), (status) => status === 'running'))
        yield* scheduler.abort(id)
        expect((yield* scheduler.waitForTask(id)).state.outcome).toEqual({
          status: 'aborted',
          reason: 'done',
        })
      }),
    ))

  test('never starts the first phase when the abort lands while the reservation settles', () =>
    runTest((backend) =>
      Effect.gen(function* () {
        let ran = false
        const Reserved = oneStep('test.reserved', () =>
          Effect.suspend(() => {
            ran = true
            return Effect.never
          }),
        )
        const held = holdable(backend)
        const { session, scheduler, registry } = yield* open(held.storage)
        const id = yield* createTask(session, yield* conversation(session), Reserved, null)
        const { entered, release } = yield* held.hold
        yield* registry.install(Reserved)
        yield* Deferred.await(entered)
        // The reservation commit is in Storage; the marking commit queues behind it.
        const aborting = yield* Effect.forkChild(scheduler.abort(id))
        yield* release
        expect(yield* Fiber.join(aborting)).toBe('marked')
        expect((yield* scheduler.waitForTask(id)).state.outcome).toEqual({
          status: 'aborted',
          reason: 'test',
        })
        expect(ran).toBe(false)
      }),
    ))

  test('lets an abort mark win over a fault that races it', () =>
    runTest((backend) =>
      Effect.gen(function* () {
        const held = holdable(backend)
        type Abort = Scheduler.Service['abort']
        const marking =
          yield* Deferred.make<
            Fiber.Fiber<'marked' | 'terminal', Effect.Error<ReturnType<Abort>>>
          >()
        let abort: Abort | undefined
        const Racing = oneStep('test.racing-fault', (task) =>
          Effect.gen(function* () {
            const { entered, release } = yield* held.hold
            const fiber = yield* Effect.forkDetach(
              abort === undefined ? Effect.never : abort(task.id),
            )
            yield* Deferred.succeed(marking, fiber)
            yield* Deferred.await(entered)
            // The mark is in Storage, not committed, when the phase fails: the fault step queues
            // behind it.
            yield* Effect.forkDetach(Effect.andThen(Effect.sleep('5 millis'), release))
            return yield* new Boom({ message: 'would fault' })
          }),
        )
        const { session, scheduler } = yield* open(held.storage, [Racing])
        abort = scheduler.abort
        const id = yield* createTask(session, yield* conversation(session), Racing, null)
        expect((yield* scheduler.waitForTask(id)).state.outcome).toEqual({
          status: 'aborted',
          reason: 'test',
        })
        expect(yield* Fiber.join(yield* Deferred.await(marking))).toBe('marked')
      }),
    ))
})
