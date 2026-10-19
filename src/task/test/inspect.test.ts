/* oxlint-disable max-lines, max-lines-per-function -- ported describe blocks */
/**
 * Ported from pi's `harness-inspect.test.ts` and the `TaskRuntime.ownedTasks` cases of
 * `harness-scheduler-indexes.test.ts`, with `abortOwned` from `harness-ownership.test.ts`.
 * The scheduler has no paused state, so inspection is checked while it runs.
 */
import { describe, expect, test } from 'bun:test'
import { Deferred, Effect, Exit, Schema } from 'effect'
import type { TaskId } from '../../storage/schema/Ids.ts'
import { Scheduler, Task, TaskError } from '../index.ts'
import { hold, holds, outcome, statusIs } from './holds.ts'
import {
  Boom,
  abortedWith,
  completed,
  conversation,
  eventually,
  open,
  openClosable,
  runTest,
} from './support.ts'
import type { Step } from './support.ts'

/** A one-phase task that completes once `gate` opens; `migrate` takes older versions. */
const versioned = (
  name: string,
  version: number,
  options: { gate?: Deferred.Deferred<void>; migrate?: Effect.Effect<void, Boom> } = {},
) => {
  const { gate, migrate } = options
  return Task.make<null, Step, null>({
    name,
    version,
    initial: () => ({ phase: 'run' }),
    phases: {
      run: (_task, runtime) =>
        Effect.andThen(gate === undefined ? Effect.void : Deferred.await(gate), () =>
          runtime.commit(() => Effect.succeed(completed(null))),
        ),
    },
    abort: (_task, runtime) => runtime.commit(() => Effect.succeed(abortedWith('test'))),
    ...(migrate === undefined
      ? {}
      : {
          migrate: () => Effect.as(migrate, { input: null, checkpoint: { phase: 'run' as const } }),
        }),
  })
}

const stateIn = (inspection: Scheduler.Inspection, id: TaskId) =>
  inspection.tasks.find((entry) => entry.record.id === id)?.state

/** Lists its owned tasks once `look.<name>` opens, then completes once `<name>` opens. */
const probes = () => {
  const tasks = holds()
  const owned = new Map<string, ReadonlyArray<TaskId>>()
  const runtimes = new Map<string, Task.Runtime<{ readonly name: string }, Step, null>>()
  const Probe = Task.make<{ readonly name: string }, Step, null>({
    name: 'test.probe',
    version: 1,
    initial: () => ({ phase: 'run' }),
    phases: {
      run: (task, runtime) =>
        Effect.gen(function* () {
          runtimes.set(task.input.name, runtime)
          yield* Deferred.await(tasks.gate(`look.${task.input.name}`))
          const records = yield* runtime.ownedTasks
          owned.set(
            task.input.name,
            records.map((record) => record.id),
          )
          yield* Deferred.await(tasks.gate(task.input.name))
          yield* runtime.commit(() => Effect.succeed(completed(null)))
        }),
    },
    abort: (_task, runtime) => runtime.commit(() => Effect.succeed(abortedWith('test'))),
  })
  return { ...tasks, Probe, owned, runtimes, all: [...tasks.tasks, Probe] }
}

describe('Scheduler.inspect', () => {
  test("derives every live task's state without running task code", () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const gate = Deferred.makeUnsafe<void>()
        const Gate = versioned('test.gate', 1, { gate })
        let migrations = 0
        const tasks = holds()
        const registered: ReadonlyArray<Task.Any> = [
          Gate,
          tasks.Waiter,
          versioned('test.migrating', 2, {
            migrate: Effect.sync(() => {
              migrations++
            }),
          }),
          versioned('test.no-migration', 2),
          versioned('test.failing', 2, {
            migrate: Effect.fail(new Boom({ message: 'cannot migrate' })),
          }),
          versioned('test.too-old', 1),
        ]
        const { session, scheduler } = yield* open(storage, registered)
        const root = yield* conversation(session)
        const ids = yield* Effect.orDie(
          session.commit((tx) =>
            Effect.gen(function* () {
              const create = (kind: Task.Task<null, Step, null>) =>
                tx.createTask(kind, null, {
                  ownership: { kind: 'conversation' },
                  conversationId: root,
                })
              const gated = yield* create(Gate)
              return {
                gate: gated,
                dependent: yield* tx.createTask(
                  tasks.Waiter,
                  { on: [gated] },
                  { ownership: { kind: 'conversation' }, conversationId: root },
                ),
                // Stored by definitions other than the registered ones.
                migrating: yield* create(versioned('test.migrating', 1)),
                noMigration: yield* create(versioned('test.no-migration', 1)),
                failing: yield* create(versioned('test.failing', 1)),
                tooOld: yield* create(versioned('test.too-old', 2)),
                missing: yield* create(versioned('test.missing', 1)),
              }
            }),
          ),
        )
        yield* scheduler.waitForTask(ids.migrating)
        expect(migrations).toBe(1)
        yield* eventually(
          Effect.map(
            scheduler.inspect,
            (running) => stateIn(running, ids.dependent)?.kind === 'waiting',
          ),
        )
        const running = yield* scheduler.inspect
        expect(running.scheduling).toBe('running')
        expect(stateIn(running, ids.gate)).toEqual({ kind: 'running' })
        expect(stateIn(running, ids.dependent)).toEqual({ kind: 'waiting', on: [ids.gate] })
        expect(stateIn(running, ids.migrating)).toBeUndefined()
        expect(stateIn(running, ids.noMigration)).toEqual({
          kind: 'blocked',
          reason: 'migration_failed',
          message: 'Task test.no-migration version 2 has no migration from 1',
        })
        expect(stateIn(running, ids.failing)).toMatchObject({
          kind: 'blocked',
          reason: 'migration_failed',
        })
        expect(stateIn(running, ids.tooOld)).toEqual({ kind: 'blocked', reason: 'task_too_old' })
        expect(stateIn(running, ids.missing)).toEqual({ kind: 'blocked', reason: 'missing_task' })
        // The inspection is a Schema value.
        expect(Schema.is(Scheduler.Inspection)(running)).toBe(true)

        yield* Deferred.done(gate, Exit.void)
        yield* scheduler.waitForTask(ids.dependent)
        const settled = yield* scheduler.inspect
        expect(settled.tasks.map((entry) => entry.record.id)).toEqual([
          ids.noMigration,
          ids.failing,
          ids.tooOld,
          ids.missing,
        ])
      }),
    ))

  test('shows a task holding its outcome as completing', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const tasks = holds()
        const { session, scheduler } = yield* open(storage, tasks.tasks)
        const root = yield* conversation(session)
        const owner = yield* Effect.orDie(
          session.commit((tx) =>
            Effect.gen(function* () {
              const created = yield* tx.createTask(tasks.Hold, hold('owner'), {
                ownership: { kind: 'conversation' },
                conversationId: root,
              })
              yield* tx.createTask(tasks.Hold, hold('child'), {
                ownership: { kind: 'task', taskId: created },
              })
              return created
            }),
          ),
        )
        yield* tasks.open('owner')
        yield* statusIs(session, owner, 'completing')
        expect(stateIn(yield* scheduler.inspect, owner)).toEqual({ kind: 'completing' })
        yield* tasks.open('child')
        expect(yield* outcome(scheduler, owner)).toBe('completed')
      }),
    ))
})

describe('Runtime.ownedTasks', () => {
  test('lists the live tasks a task owns directly, in ID order, also after a reopen', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const tasks = probes()
        let opened = yield* openClosable(storage, tasks.all)
        const root = yield* conversation(opened.session)
        // The probe owns two held tasks, one that ends, a grandchild below the first, and a
        // conversation with a task in it.
        const ids = yield* Effect.orDie(
          opened.session.commit((tx) =>
            Effect.gen(function* () {
              const probe = yield* tx.createTask(
                tasks.Probe,
                { name: 'probe' },
                { ownership: { kind: 'conversation' }, conversationId: root },
              )
              const own = (name: string, owner: TaskId) =>
                tx.createTask(tasks.Hold, hold(name), {
                  ownership: { kind: 'task', taskId: owner },
                })
              const first = yield* own('first', probe)
              const ends = yield* own('ends', probe)
              const second = yield* own('second', probe)
              yield* own('grandchild', first)
              const child = yield* tx.createConversation({
                ownership: { kind: 'task', taskId: probe },
              })
              yield* tx.createTask(tasks.Hold, hold('inside'), {
                ownership: { kind: 'conversation' },
                conversationId: child.id,
              })
              return { probe, first, ends, second }
            }),
          ),
        )
        yield* tasks.open('ends')
        yield* opened.scheduler.waitForTask(ids.ends)
        // The first ends too, but holds its outcome while its grandchild runs: still live.
        yield* tasks.open('first')
        yield* statusIs(opened.session, ids.first, 'completing')
        yield* tasks.open('look.probe')
        yield* eventually(Effect.sync(() => tasks.owned.has('probe')))
        expect(tasks.owned.get('probe')).toEqual([ids.first, ids.second])
        yield* opened.close

        // After a reopen the rerun sees the same tasks, which an earlier invocation created.
        tasks.owned.clear()
        opened = yield* openClosable(storage, tasks.all)
        yield* eventually(Effect.sync(() => tasks.owned.has('probe')))
        expect(tasks.owned.get('probe')).toEqual([ids.first, ids.second])
        yield* opened.close
      }),
    ))

  test('fails once its invocation has ended', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const tasks = probes()
        const { session, scheduler } = yield* open(storage, tasks.all)
        const root = yield* conversation(session)
        const probe = yield* Effect.orDie(
          session.commit((tx) =>
            tx.createTask(
              tasks.Probe,
              { name: 'probe' },
              { ownership: { kind: 'conversation' }, conversationId: root },
            ),
          ),
        )
        yield* eventually(Effect.sync(() => tasks.runtimes.has('probe')))
        const runtime = tasks.runtimes.get('probe')
        // Aborting joins the run invocation, which has ended by then.
        yield* scheduler.abort(probe)
        if (runtime === undefined) return
        const error = yield* Effect.flip(runtime.ownedTasks)
        expect(error).toBeInstanceOf(TaskError.InvocationEnded)
      }),
    ))
})

describe('Runtime.abortOwned', () => {
  /** Aborts the task in its input, or else the first it owns, then completes with what happened. */
  const Aborter = Task.make<{ readonly target: TaskId | null }, Step, string>({
    name: 'test.aborter',
    version: 1,
    initial: () => ({ phase: 'run' }),
    phases: {
      run: (task, runtime) =>
        Effect.gen(function* () {
          const owned = yield* runtime.ownedTasks
          const target = task.input.target ?? owned[0]?.id
          const result = yield* Effect.fromNullishOr(target).pipe(
            Effect.orDie,
            Effect.flatMap(runtime.abortOwned),
            Effect.as('aborted'),
            Effect.catchTag('TaskRejected', (error) => Effect.succeed(error.message)),
          )
          yield* runtime.commit(() => Effect.succeed(completed(result)))
        }),
    },
    abort: (_task, runtime) => runtime.commit(() => Effect.succeed(abortedWith('test'))),
  })

  test('aborts an owned child and continues', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const tasks = holds()
        const { session, scheduler } = yield* open(storage, [...tasks.tasks, Aborter])
        const root = yield* conversation(session)
        const { parent, child } = yield* Effect.orDie(
          session.commit((tx) =>
            Effect.gen(function* () {
              const created = yield* tx.createTask(
                Aborter,
                { target: null },
                { ownership: { kind: 'conversation' }, conversationId: root },
              )
              const below = yield* tx.createTask(tasks.Hold, hold('child'), {
                ownership: { kind: 'task', taskId: created },
              })
              return { parent: created, child: below }
            }),
          ),
        )
        expect((yield* scheduler.waitForTask(parent)).state.outcome).toEqual({
          status: 'completed',
          result: 'aborted',
        })
        expect(yield* outcome(scheduler, child)).toBe('aborted')
      }),
    ))

  test('rejects a task it does not own', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const tasks = holds()
        const { session, scheduler } = yield* open(storage, [...tasks.tasks, Aborter])
        const root = yield* conversation(session)
        const { stranger, parent } = yield* Effect.orDie(
          session.commit((tx) =>
            Effect.gen(function* () {
              const other = yield* tx.createTask(tasks.Hold, hold('stranger'), {
                ownership: { kind: 'conversation' },
                conversationId: root,
              })
              const created = yield* tx.createTask(
                Aborter,
                { target: other },
                { ownership: { kind: 'conversation' }, conversationId: root },
              )
              return { stranger: other, parent: created }
            }),
          ),
        )
        expect((yield* scheduler.waitForTask(parent)).state.outcome).toEqual({
          status: 'completed',
          result: `Task ${parent} does not own task ${stranger}`,
        })
        yield* statusIs(session, stranger, 'running')
        yield* scheduler.abort(stranger)
      }),
    ))
})
