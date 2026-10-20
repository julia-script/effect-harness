/* oxlint-disable max-lines, max-lines-per-function -- ported describe blocks */
/**
 * Ported from pi's `packages/durable/test/harness-ownership.test.ts`, "ownership": abort cascades
 * through owned foreground work, held failed owners, and marks derived again after a reopen.
 * Cases about queued inputs, which come with the inbox, are left out.
 */
import { describe, expect, test } from 'bun:test'
import { Effect, Fiber } from 'effect'
import type { Session } from '../../session/index.ts'
import type { Storage } from '../../storage/index.ts'
import type { TaskId } from '../../storage/schema/Ids.ts'
import type { TaskState } from '../../storage/schema/Task.ts'
import { StorageError } from '../../storage/StorageError.ts'
import { Task, TaskError } from '../index.ts'
import { withState } from '../internal/state.ts'
import {
  bareSession,
  hold,
  holdIn,
  holds,
  outcome,
  ownedChild,
  stateOf,
  statusIs,
} from './holds.ts'
import {
  completed,
  conversation,
  eventually,
  getTask,
  open,
  openClosable,
  runTest,
} from './support.ts'

const isTerminal = (session: Session.Service, id: TaskId) =>
  Effect.map(stateOf(session, id), (state) => state.status === 'terminal')

const abortRequested = (session: Session.Service, id: TaskId) =>
  eventually(Effect.map(getTask(session, id), (record) => record?.abortRequested === true))

/** A Storage that fails the first commit writing `id`'s abort mark, once `id` is set. */
const failingMark = (backend: Storage.Service) => {
  let target: TaskId | undefined
  const storage: Storage.Service = {
    ...backend,
    commit: (writes) =>
      Effect.suspend(() => {
        const marks = writes.some(
          (write) =>
            write.type === 'task' && write.value.id === target && write.value.abortRequested,
        )
        if (!marks) return backend.commit(writes)
        target = undefined
        return Effect.fail(new StorageError({ message: 'disk gone' }))
      }),
  }
  return {
    storage,
    failMark: (id: TaskId) =>
      Effect.sync(() => {
        target = id
      }),
  }
}

describe('cascades', () => {
  test('cascades an abort mark to the owned foreground subtree, stopping at a background owner', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const tasks = holds()
        const { session, scheduler } = yield* open(storage, tasks.tasks)
        const root = yield* conversation(session)
        const tree = yield* ownedChild(session, tasks, root, 'owner')
        const deeper = yield* ownedChild(session, tasks, tree.child, 'deeper')
        const shielded = yield* ownedChild(session, tasks, tree.child, 'shielded', {
          background: true,
        })
        expect(yield* scheduler.abort(tree.owner)).toBe('marked')
        for (const id of [tree.owner, tree.inner, deeper.owner, deeper.inner]) {
          expect(yield* outcome(scheduler, id)).toBe('aborted')
        }
        // A nested background owner is a boundary.
        expect(yield* isTerminal(session, shielded.owner)).toBe(false)
        expect(yield* isTerminal(session, shielded.inner)).toBe(false)
        yield* scheduler.abort(shielded.owner)
      }),
    ))

  test('cascades a failed owner but not a completed one', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const tasks = holds()
        const { session, scheduler } = yield* open(storage, tasks.tasks)
        const root = yield* conversation(session)
        const failed = yield* ownedChild(session, tasks, root, 'failed')
        const done = yield* ownedChild(session, tasks, root, 'done')
        yield* tasks.open('failed', 'failed')
        yield* tasks.open('done')
        expect(yield* outcome(scheduler, failed.inner)).toBe('aborted')
        expect(yield* outcome(scheduler, failed.owner)).toBe('failed')
        yield* statusIs(session, done.owner, 'completing')
        expect(yield* isTerminal(session, done.inner)).toBe(false)
        yield* tasks.open('done.inner')
        expect(yield* outcome(scheduler, done.owner)).toBe('completed')
      }),
    ))

  test('aborts a background task directly with its ordinary subtree', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const tasks = holds()
        const { session, scheduler } = yield* open(storage, tasks.tasks)
        const root = yield* conversation(session)
        const tree = yield* ownedChild(session, tasks, root, 'background', { background: true })
        yield* scheduler.abort(tree.owner)
        expect(yield* outcome(scheduler, tree.inner)).toBe('aborted')
      }),
    ))

  test('aborts a conversation: foreground work aborted, background kept', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const tasks = holds()
        const { session, scheduler } = yield* open(storage, tasks.tasks)
        const root = yield* conversation(session)
        const foreground = yield* ownedChild(session, tasks, root, 'foreground')
        const background = yield* ownedChild(session, tasks, root, 'background', {
          background: true,
        })
        yield* scheduler.abortConversation(root)
        for (const id of [foreground.owner, foreground.inner]) {
          expect(yield* isTerminal(session, id)).toBe(true)
        }
        expect(yield* isTerminal(session, background.owner)).toBe(false)
        expect(yield* isTerminal(session, background.inner)).toBe(false)
        yield* scheduler.abort(background.owner)
      }),
    ))

  test('aborts work created below a held failed owner, but not below a terminal one', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const tasks = holds()
        const { session, scheduler } = yield* open(storage, tasks.tasks)
        const root = yield* conversation(session)
        const tree = yield* ownedChild(session, tasks, root, 'owner', { slowInner: true })
        yield* tasks.open('owner', 'failed')
        yield* abortRequested(session, tree.inner)
        expect((yield* stateOf(session, tree.owner)).status).toBe('completing')
        const during = yield* holdIn(session, tasks, tree.child, 'during')
        expect(yield* outcome(scheduler, during)).toBe('aborted')
        yield* tasks.open('abort.owner.inner')
        expect(yield* outcome(scheduler, tree.owner)).toBe('failed')
        // A terminal owner never cascades: work in its conversation runs normally.
        const after = yield* holdIn(session, tasks, tree.child, 'after')
        yield* tasks.open('after')
        expect(yield* outcome(scheduler, after)).toBe('completed')
      }),
    ))

  test("keeps a nested background owner's subtree when its cancelled background owner is aborted", () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const tasks = holds()
        const { session, scheduler } = yield* open(storage, tasks.tasks)
        const root = yield* conversation(session)
        const outer = yield* ownedChild(session, tasks, root, 'outer', { background: true })
        const inner = yield* ownedChild(session, tasks, outer.child, 'inner', { background: true })
        yield* scheduler.abort(outer.owner)
        expect(yield* outcome(scheduler, outer.inner)).toBe('aborted')
        expect(yield* isTerminal(session, inner.owner)).toBe(false)
        expect(yield* isTerminal(session, inner.inner)).toBe(false)
        yield* scheduler.abort(inner.owner)
      }),
    ))

  const crashes: ReadonlyArray<readonly [string, boolean, TaskState]> = [
    [
      'a held failed owner',
      false,
      { status: 'completing', outcome: { status: 'failed', error: { message: 'crash' } } },
    ],
    ['an abort-marked owner', true, { status: 'pending', checkpoint: { phase: 'hold' } }],
  ]
  for (const [label, marked, crashed] of crashes) {
    test(`derives marks a crash left unapplied below ${label} at open`, () =>
      runTest((storage) =>
        Effect.gen(function* () {
          const tasks = holds()
          // Without a scheduler, nothing derives marks: a cancelled owner with a live task below.
          const { session, close } = yield* bareSession(storage)
          const root = yield* conversation(session)
          const tree = yield* ownedChild(session, tasks, root, 'gone')
          yield* Effect.orDie(
            session.commit((tx) =>
              Effect.gen(function* () {
                const record = yield* tx.task(tree.owner)
                if (record !== undefined) {
                  yield* tx.setTask({ ...withState(record, crashed), abortRequested: marked })
                }
              }),
            ),
          )
          yield* close

          const { scheduler } = yield* open(storage, tasks.tasks)
          expect(yield* outcome(scheduler, tree.inner)).toBe('aborted')
          // The owner finishes only after the work below it.
          expect(yield* outcome(scheduler, tree.owner)).toBe(marked ? 'aborted' : 'failed')
        }),
      ))
  }

  test('marks work admitted after reopen below a cancelled owner whose edge was not loaded', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const tasks = holds()
        let opened = yield* openClosable(storage, tasks.tasks)
        const root = yield* conversation(opened.session)
        const tree = yield* ownedChild(opened.session, tasks, root, 'owner', { slowOwner: true })
        yield* tasks.open('owner.inner')
        yield* opened.scheduler.waitForTask(tree.inner)
        // The owner stays live and cancelled in its slow abort handler.
        yield* Effect.forkScoped(opened.scheduler.abort(tree.owner))
        yield* abortRequested(opened.session, tree.owner)
        yield* opened.close

        // The child is empty at open, so nothing loads its edge until new work arrives.
        opened = yield* openClosable(storage, tasks.tasks)
        const late = yield* holdIn(opened.session, tasks, tree.child, 'late')
        expect(yield* outcome(opened.scheduler, late)).toBe('aborted')
        yield* tasks.open('abort.owner')
        expect(yield* outcome(opened.scheduler, tree.owner)).toBe('aborted')
        yield* opened.close
      }),
    ))

  test('cascades from an owner the scheduler orphans', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const tasks = holds()
        const Unregistered = Task.make<null, { readonly phase: 'run' }, null>({
          name: 'test.unregistered',
          version: 1,
          initial: () => ({ phase: 'run' }),
          phases: {
            run: (_task, runtime) => runtime.commit(() => Effect.succeed(completed(null))),
          },
          abort: (_task, runtime) => runtime.commit(() => Effect.succeed(completed(null))),
        })
        const { session, scheduler } = yield* open(storage, tasks.tasks)
        const root = yield* conversation(session)
        const { owner, inner } = yield* Effect.orDie(
          session.commit((tx) =>
            Effect.gen(function* () {
              const created = yield* tx.createTask(Unregistered, null, {
                ownership: { kind: 'conversation' },
                conversationId: root,
              })
              const child = yield* tx.createConversation({
                ownership: { kind: 'task', taskId: created },
              })
              const below = yield* tx.createTask(tasks.Hold, hold('below'), {
                ownership: { kind: 'conversation' },
                conversationId: child.id,
              })
              return { owner: created, inner: below }
            }),
          ),
        )
        expect(yield* scheduler.abort(owner)).toBe('marked')
        expect((yield* scheduler.waitForTask(owner)).state.outcome).toEqual({
          status: 'orphaned',
          reason: 'missing_task',
        })
        expect(yield* outcome(scheduler, inner)).toBe('aborted')
      }),
    ))

  test("cancels only the caller's wait, never the shared work", () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const tasks = holds()
        const { session, scheduler } = yield* open(storage, tasks.tasks)
        const root = yield* conversation(session)
        const tree = yield* ownedChild(session, tasks, root, 'owner')
        const idle = yield* Effect.forkScoped(scheduler.waitForIdle(root))
        yield* Fiber.interrupt(idle)
        expect(yield* isTerminal(session, tree.inner)).toBe(false)

        // Interrupting an abort after its commit leaves the marks in place.
        const slow = yield* ownedChild(session, tasks, root, 'slow', { slowOwner: true })
        const aborting = yield* Effect.forkScoped(scheduler.abortConversation(root))
        yield* abortRequested(session, slow.owner)
        yield* Fiber.interrupt(aborting)
        yield* tasks.open('abort.slow')
        for (const id of [tree.owner, tree.inner, slow.owner, slow.inner]) {
          expect(yield* outcome(scheduler, id)).toBe('aborted')
        }
      }),
    ))

  test('aborts a waiting child whose awaited task completes in the commit that marks its owner', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const tasks = holds()
        const { session, scheduler } = yield* open(storage, tasks.tasks)
        const root = yield* conversation(session)
        const { owner, dependency, blocked } = yield* Effect.orDie(
          session.commit((tx) =>
            Effect.gen(function* () {
              const created = yield* tx.createTask(tasks.Hold, hold('owner'), {
                ownership: { kind: 'conversation' },
                conversationId: root,
              })
              const child = yield* tx.createConversation({
                ownership: { kind: 'task', taskId: created },
              })
              const awaited = yield* tx.createTask(tasks.Hold, hold('dependency'), {
                ownership: { kind: 'conversation' },
                conversationId: root,
              })
              const waiter = yield* tx.createTask(
                tasks.Waiter,
                { on: [awaited] },
                { ownership: { kind: 'conversation' }, conversationId: child.id },
              )
              return { owner: created, dependency: awaited, blocked: waiter }
            }),
          ),
        )
        yield* eventually(
          Effect.sync(() => tasks.runs.get('dependency') === 1 && tasks.runs.get('owner') === 1),
        )
        yield* statusIs(session, blocked, 'waiting')
        // One commit completes the dependency and marks the owner.
        yield* Effect.orDie(
          session.commit((tx) =>
            Effect.gen(function* () {
              const awaited = yield* tx.task(dependency)
              const marked = yield* tx.task(owner)
              if (awaited === undefined || marked === undefined) return
              yield* tx.setTask(withState(awaited, completed(null)))
              yield* tx.setTask({ ...marked, abortRequested: true })
            }),
          ),
        )
        expect(yield* outcome(scheduler, blocked)).toBe('aborted')
      }),
    ))

  test('closes the scheduler on a failed cascade commit, and reopening applies the cascade', () =>
    runTest((backend, reports) =>
      Effect.gen(function* () {
        const tasks = holds()
        const { storage, failMark } = failingMark(backend)
        const opened = yield* openClosable(storage, tasks.tasks)
        const root = yield* conversation(opened.session)
        const tree = yield* ownedChild(opened.session, tasks, root, 'owner')
        yield* failMark(tree.inner)
        yield* tasks.open('owner', 'failed')
        const closed = yield* Effect.flip(opened.scheduler.waitForTask(tree.inner))
        expect(closed).toBeInstanceOf(TaskError.SchedulerClosed)
        expect(reports.join('\n')).toContain('disk gone')
        yield* opened.close

        const { scheduler } = yield* open(backend, tasks.tasks)
        expect(yield* outcome(scheduler, tree.inner)).toBe('aborted')
      }),
    ))
})
