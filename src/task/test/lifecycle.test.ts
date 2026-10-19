/* oxlint-disable max-lines, max-lines-per-function -- ported describe blocks */
/**
 * Ported from pi's "task close" (`harness-tasks.test.ts`) and "task recovery"
 * (`harness-tasks-recovery.test.ts`). pi reopens a sqlite file; these tests reopen the same memory
 * Storage with a fresh Session and Scheduler. pi's Harness waits for `resume()` before scheduling;
 * here a Scheduler schedules once it opens, so a test that inspects a reopened record before it runs
 * opens without the definition, and installs it afterwards.
 */
import { describe, expect, test } from 'bun:test'
import { Deferred, Effect, Exit, Fiber } from 'effect'
import { Registry, Task, TaskError } from '../index.ts'
import {
  abortedWith,
  completed,
  conversation,
  createTask,
  eventually,
  getTask,
  openClosable,
  runTest,
  settle,
  statusOf,
} from './support.ts'

describe('task close', () => {
  test('stops without outcomes and starts no fresh phase or abort invocation while closing', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const reached = yield* Deferred.make<void>()
        const proceed = yield* Deferred.make<void>()
        const phases: Array<string> = []
        let abortRan = false
        const Two = Task.make<null, { phase: 'one' } | { phase: 'two' }, null>({
          name: 'test.two',
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
                // Returns normally once released; the closing rule wins over the next phase.
                yield* Effect.uninterruptible(Deferred.await(proceed))
              }),
            two: () => Effect.sync(() => phases.push('two')),
          },
          abort: () =>
            Effect.sync(() => {
              abortRan = true
            }),
        })
        const { session, scheduler, close } = yield* openClosable(storage, [Two])
        const id = yield* createTask(session, yield* conversation(session), Two, null)
        yield* Deferred.await(reached)
        const aborting = yield* Effect.forkChild(scheduler.abort(id))
        yield* eventually(
          Effect.map(getTask(session, id), (record) => record?.abortRequested === true),
        )
        const record = yield* getTask(session, id)
        const closing = yield* Effect.forkChild(close)
        yield* Deferred.done(proceed, Exit.void)
        yield* Fiber.join(closing)
        expect(yield* Fiber.join(aborting)).toBe('marked')
        expect(phases).toEqual(['one'])
        expect(abortRan).toBe(false)
        expect(record).toMatchObject({
          abortRequested: true,
          state: { status: 'running', checkpoint: { phase: 'two' } },
        })
        expect(Exit.isFailure(yield* Effect.exit(session.commit(() => Effect.void)))).toBe(true)
        // Close wrote nothing: the reopened record is the marked one, back to pending.
        const reopened = yield* openClosable(storage)
        expect(yield* getTask(reopened.session, id)).toMatchObject({
          abortRequested: true,
          state: { status: 'pending', checkpoint: { phase: 'two' } },
        })
        yield* reopened.close
      }),
    ))

  test('fails waits that are pending when the Scheduler closes', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const Stuck = Task.make<null, { phase: 'run' }, null>({
          name: 'test.stuck',
          version: 1,
          initial: () => ({ phase: 'run' }),
          phases: { run: () => Effect.never },
          abort: () => Effect.void,
        })
        const { session, scheduler, close } = yield* openClosable(storage, [Stuck])
        const id = yield* createTask(session, yield* conversation(session), Stuck, null)
        const waiting = yield* Effect.forkChild(Effect.flip(scheduler.waitForTask(id)))
        yield* settle
        yield* close
        expect(yield* Fiber.join(waiting)).toBeInstanceOf(TaskError.SchedulerClosed)
      }),
    ))
})

/** Fake external service whose operations are idempotent by request key. */
const transferService = () => {
  const applied = new Map<string, number>()
  const service = {
    applied,
    calls: 0,
    apply: (key: string, amount: number) =>
      Effect.sync(() => {
        service.calls++
        if (!applied.has(key)) applied.set(key, amount * 10)
        return applied.get(key) ?? 0
      }),
  }
  return service
}

type TransferState =
  | { readonly phase: 'prepare' }
  | { readonly phase: 'apply'; readonly key: string }

/**
 * Intent/effect/outcome task: `prepare` commits the intent, `apply` performs the effect and commits
 * the outcome. While `interrupt.first` is set, the first `apply` stops after the effect until it is
 * interrupted.
 */
const transferTask = (service: ReturnType<typeof transferService>, interrupt: { first: boolean }) =>
  Task.make<{ amount: number }, TransferState, { receipt: number }>({
    name: 'test.transfer',
    version: 1,
    initial: () => ({ phase: 'prepare' }),
    phases: {
      prepare: (task, runtime) =>
        Effect.andThen(
          runtime.memo('requested', task.input.amount),
          runtime.commit(() =>
            Effect.succeed({
              status: 'running' as const,
              checkpoint: { phase: 'apply' as const, key: `transfer-${task.id}` },
            }),
          ),
        ),
      apply: (task, runtime) =>
        Effect.gen(function* () {
          const receipt = yield* service.apply(task.state.checkpoint.key, task.input.amount)
          const stop = interrupt.first
          interrupt.first = false
          yield* stop ? Effect.never : Effect.void
          yield* runtime.commit(() => Effect.succeed(completed({ receipt })))
        }),
    },
    abort: (_task, runtime) => runtime.commit(() => Effect.succeed(abortedWith('transfer'))),
  })

describe('task recovery', () => {
  test('resumes an intent/effect/outcome task interrupted after its intent across close and reopen', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const service = transferService()
        const Transfer = transferTask(service, { first: true })
        const first = yield* openClosable(storage, [Transfer])
        const id = yield* createTask(first.session, yield* conversation(first.session), Transfer, {
          amount: 7,
        })
        yield* eventually(Effect.sync(() => service.calls === 1))
        const startedAt = (yield* getTask(first.session, id))?.startedAt
        yield* first.close

        // Without the definition the reopened task stays as recovery left it.
        const registry = yield* Registry.make()
        const second = yield* openClosable(storage, [], registry)
        expect(yield* getTask(second.session, id)).toMatchObject({
          abortRequested: false,
          memos: { requested: 7 },
          startedAt,
          state: { status: 'pending', checkpoint: { phase: 'apply', key: `transfer-${id}` } },
        })
        yield* settle
        expect(service.calls).toBe(1)
        yield* registry.install(Transfer)
        const receipt = yield* second.scheduler.waitForTask(id)
        expect(receipt.state.outcome).toEqual({ status: 'completed', result: { receipt: 70 } })
        expect(receipt.startedAt).toBe(startedAt)
        expect(receipt.endedAt).toBeGreaterThanOrEqual(startedAt ?? Infinity)
        expect(service.calls).toBe(2)
        expect(service.applied.size).toBe(1)
        yield* second.close

        const third = yield* openClosable(storage, [Transfer])
        expect(yield* getTask(third.session, id)).toEqual(receipt)
        yield* third.close
      }),
    ))

  test('resumes abort work after close at every abort stage', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const log: Array<string> = []
        const abortGate = { block: true }
        const abortReached = yield* Deferred.make<void>()
        const runRelease = yield* Deferred.make<void>()
        const Abortable = Task.make<null, { phase: 'run' }, null>({
          name: 'test.abortable',
          version: 1,
          initial: () => ({ phase: 'run' }),
          phases: {
            run: () =>
              Effect.andThen(
                Effect.sync(() => log.push('run')),
                // Defers interruption until released, so the mark is durable while the run is active.
                Effect.uninterruptible(Deferred.await(runRelease)),
              ),
          },
          abort: (_task, runtime) =>
            Effect.gen(function* () {
              log.push('abort')
              yield* abortGate.block
                ? Effect.andThen(Deferred.done(abortReached, Exit.void), Effect.never)
                : Effect.void
              yield* runtime.commit(() => Effect.succeed(abortedWith('stop')))
            }),
        })

        // Stage 1: the mark is committed while the run invocation is still active.
        let opened = yield* openClosable(storage, [Abortable])
        const id = yield* createTask(
          opened.session,
          yield* conversation(opened.session),
          Abortable,
          null,
        )
        yield* eventually(Effect.sync(() => log.length === 1))
        const aborting = yield* Effect.forkChild(opened.scheduler.abort(id))
        const session = opened.session
        yield* eventually(
          Effect.map(getTask(session, id), (record) => record?.abortRequested === true),
        )
        const closing = yield* Effect.forkChild(opened.close)
        yield* Deferred.done(runRelease, Exit.void)
        yield* Fiber.join(closing)
        expect(yield* Fiber.join(aborting)).toBe('marked')
        expect(log).toEqual(['run'])

        // Stage 2: reopen runs the abort handler, never the run; close while it is active.
        opened = yield* openClosable(storage, [Abortable])
        yield* Deferred.await(abortReached)
        expect(yield* getTask(opened.session, id)).toMatchObject({
          abortRequested: true,
          state: { status: 'running' },
        })
        yield* opened.close
        expect(log).toEqual(['run', 'abort'])

        // Stage 3: a fresh abort invocation settles the task.
        abortGate.block = false
        opened = yield* openClosable(storage, [Abortable])
        expect((yield* opened.scheduler.waitForTask(id)).state.outcome).toEqual({
          status: 'aborted',
          reason: 'stop',
        })
        yield* opened.close
        expect(log).toEqual(['run', 'abort', 'abort'])

        // Stage 4: the terminal receipt survives reopen and nothing runs again.
        opened = yield* openClosable(storage, [Abortable])
        yield* settle
        expect(yield* opened.scheduler.abort(id)).toBe('terminal')
        expect(yield* statusOf(opened.session, id)).toBe('terminal')
        yield* opened.close
        expect(log).toEqual(['run', 'abort', 'abort'])
      }),
    ))
})
