/* oxlint-disable max-lines, max-lines-per-function -- ported describe blocks */
/**
 * Ported from pi's `packages/durable/test/harness-tasks-recovery.test.ts`, "blocked tasks".
 */
import { describe, expect, test } from 'bun:test'
import { Deferred, Effect, Exit, Fiber, Schema } from 'effect'
import { Registry, Task } from '../index.ts'
import {
  abortedWith,
  Boom,
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
import type { Step } from './support.ts'

type Migrate = (
  input: Schema.Json,
  checkpoint: Schema.Json,
  fromVersion: number,
) => Effect.Effect<{ readonly input: null; readonly checkpoint: Step }, unknown>

/** A versioned one-phase task that completes with `result`, optionally migrating older records. */
const versioned = (version: number, result: string, migrate?: Migrate) =>
  Task.make<null, Step, string>({
    name: 'test.versioned',
    version,
    initial: () => ({ phase: 'run' }),
    phases: { run: (_task, runtime) => runtime.commit(() => Effect.succeed(completed(result))) },
    abort: (_task, runtime) => runtime.commit(() => Effect.succeed(abortedWith(result))),
    ...(migrate === undefined ? {} : { migrate }),
  })

const isStep = Schema.is(Schema.Struct({ phase: Schema.Literal('run') }))

describe('blocked tasks', () => {
  test('keeps a task with a missing definition pending until it is installed', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const V1 = versioned(1, 'v1')
        const registry = yield* Registry.make()
        const { session, scheduler, close } = yield* openClosable(storage, [], registry)
        const id = yield* createTask(session, yield* conversation(session), V1, null)
        yield* settle
        expect(yield* statusOf(session, id)).toBe('pending')
        yield* registry.install(V1)
        expect((yield* scheduler.waitForTask(id)).state.outcome).toEqual({
          status: 'completed',
          result: 'v1',
        })
        yield* close
      }),
    ))

  test('keeps a task stored by a newer version pending until a fitting definition is installed', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const registry = yield* Registry.make([versioned(1, 'old')])
        const { session, scheduler, close } = yield* openClosable(storage, [], registry)
        const id = yield* createTask(
          session,
          yield* conversation(session),
          versioned(2, 'new'),
          null,
        )
        yield* settle
        expect(yield* statusOf(session, id)).toBe('pending')
        // The same name replaces the old definition.
        yield* registry.install(versioned(2, 'new'))
        expect((yield* scheduler.waitForTask(id)).state.outcome).toEqual({
          status: 'completed',
          result: 'new',
        })
        yield* close
      }),
    ))

  test('migrates at reservation, leaves the record unchanged when migration fails, and retries only for a new definition', () =>
    runTest((storage, reports) =>
      Effect.gen(function* () {
        let opened = yield* openClosable(storage)
        const id = yield* createTask(
          opened.session,
          yield* conversation(opened.session),
          versioned(1, 'v1'),
          null,
        )
        yield* opened.close

        let failures = 0
        const failing = versioned(2, 'v2', () =>
          Effect.suspend(() => {
            failures++
            return Effect.fail(new Boom({ message: 'cannot migrate' }))
          }),
        )
        const registry = yield* Registry.make([failing])
        opened = yield* openClosable(storage, [], registry)
        yield* eventually(Effect.sync(() => reports.length === 1))
        // An unrelated registry change wakes the scheduler without retrying the failed definition.
        yield* registry.install(Task.make({ ...versioned(1, 'x'), name: 'test.unrelated' }))
        yield* settle
        expect(failures).toBe(1)
        expect(reports).toHaveLength(1)
        expect(reports[0]).toContain('cannot migrate')
        expect(yield* getTask(opened.session, id)).toMatchObject({
          version: 1,
          state: { status: 'pending', checkpoint: { phase: 'run' } },
        })

        const migrations: Array<number> = []
        yield* registry.install(
          versioned(2, 'v2', (_input, checkpoint, fromVersion) =>
            Effect.suspend(() => {
              migrations.push(fromVersion)
              return isStep(checkpoint)
                ? Effect.succeed({ input: null, checkpoint })
                : Effect.fail(new Boom({ message: 'unexpected checkpoint' }))
            }),
          ),
        )
        expect(yield* opened.scheduler.waitForTask(id)).toMatchObject({
          version: 2,
          state: { outcome: { status: 'completed', result: 'v2' } },
        })
        expect(migrations).toEqual([1])
        yield* opened.close
      }),
    ))

  test('blocks an older record whose newer definition has no migration', () =>
    runTest((storage, reports) =>
      Effect.gen(function* () {
        let opened = yield* openClosable(storage)
        const id = yield* createTask(
          opened.session,
          yield* conversation(opened.session),
          versioned(1, 'v1'),
          null,
        )
        yield* opened.close
        opened = yield* openClosable(storage, [versioned(2, 'v2')])
        yield* eventually(Effect.sync(() => reports.length === 1))
        expect(reports[0]).toContain('has no migration from 1')
        expect(yield* getTask(opened.session, id)).toMatchObject({
          version: 1,
          state: { status: 'pending' },
        })
        yield* opened.close
      }),
    ))

  test('settles an aborted blocked task as orphaned in the marking commit', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const { session, scheduler, close } = yield* openClosable(storage)
        const id = yield* createTask(session, yield* conversation(session), versioned(1, 'x'), null)
        expect(yield* scheduler.abort(id)).toBe('marked')
        expect((yield* getTask(session, id))?.state).toEqual({
          status: 'terminal',
          outcome: { status: 'orphaned', reason: 'missing_task' },
        })
        yield* close
      }),
    ))

  test('orphans a marked task whose definition disappeared while its run was active', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const reached = yield* Deferred.make<void>()
        const Running = Task.make<null, Step, null>({
          name: 'test.vanishing',
          version: 1,
          initial: () => ({ phase: 'run' }),
          phases: { run: () => Effect.andThen(Deferred.done(reached, Exit.void), Effect.never) },
          abort: () => Effect.die('must not run'),
        })
        const registry = yield* Registry.make([Running])
        const { session, scheduler, close } = yield* openClosable(storage, [], registry)
        const id = yield* createTask(session, yield* conversation(session), Running, null)
        yield* Deferred.await(reached)
        yield* registry.uninstall(Running.name)
        // An active run means the mark does not orphan directly; the scheduler orphans after the run.
        expect(yield* scheduler.abort(id)).toBe('marked')
        expect((yield* scheduler.waitForTask(id)).state.outcome).toEqual({
          status: 'orphaned',
          reason: 'missing_task',
        })
        yield* close
      }),
    ))

  test('orphans a reopened abort-marked task without a definition', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const release = yield* Deferred.make<void>()
        const Held = Task.make<null, Step, null>({
          name: 'test.held',
          version: 1,
          initial: () => ({ phase: 'run' }),
          phases: { run: () => Effect.uninterruptible(Deferred.await(release)) },
          abort: () => Effect.void,
        })
        let opened = yield* openClosable(storage, [Held])
        const { session } = opened
        const id = yield* createTask(session, yield* conversation(session), Held, null)
        yield* eventually(Effect.map(statusOf(session, id), (status) => status === 'running'))
        const aborting = yield* Effect.forkChild(opened.scheduler.abort(id))
        yield* eventually(
          Effect.map(getTask(session, id), (record) => record?.abortRequested === true),
        )
        const closing = yield* Effect.forkChild(opened.close)
        yield* Deferred.done(release, Exit.void)
        yield* Fiber.join(closing)
        expect(yield* Fiber.join(aborting)).toBe('marked')

        opened = yield* openClosable(storage)
        expect((yield* opened.scheduler.waitForTask(id)).state).toEqual({
          status: 'terminal',
          outcome: { status: 'orphaned', reason: 'missing_task' },
        })
        yield* opened.close
      }),
    ))
})
