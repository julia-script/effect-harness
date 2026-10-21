/* oxlint-disable max-lines, max-lines-per-function -- ported describe blocks */
/**
 * Ported from pi's `harness-cancellation-barrier.test.ts` and the "scheduler indexes" cases of
 * `harness-scheduler-indexes.test.ts`: work below a cancelled owner, restart marks, and what the
 * ownership index decides against the committed records.
 */
import { describe, expect, test } from 'bun:test'
import { Deferred, Effect, Exit, Fiber } from 'effect'
import type { Session } from '../../session/index.ts'
import type { Storage } from '../../storage/index.ts'
import type { TaskId } from '../../storage/schema/Ids.ts'
import type { TaskRecord } from '../../storage/schema/Task.ts'
import { Task } from '../index.ts'
import { withState } from '../internal/state.ts'
import { bareSession, finishes, hold, holds, outcome, statusIs, watchTaskWrites } from './holds.ts'
import type { HoldInput, Holds } from './holds.ts'
import { eventually, getTask, open, openClosable, runTest, settle } from '../../test/task.ts'

/** In a fresh ownerless conversation, `top` owning `middle` owning `leaf`, all held. */
const chain = (
  session: Session.Service,
  tasks: Holds,
  options: { middleAbandons?: boolean; leaf?: Session.TaskKind<HoldInput> } = {},
) =>
  Effect.orDie(
    session.commit((tx) =>
      Effect.gen(function* () {
        const conversation = yield* tx.createConversation({ ownership: { kind: 'ownerless' } })
        const top = yield* tx.createTask(tasks.Hold, hold('top'), {
          ownership: { kind: 'conversation' },
          conversationId: conversation.id,
        })
        const middle = yield* tx.createTask(tasks.Hold, hold('middle'), {
          ownership: { kind: 'task', taskId: top },
          abandonOnRestart: options.middleAbandons ?? false,
        })
        const leaf = yield* tx.createTask(options.leaf ?? tasks.Hold, hold('leaf'), {
          ownership: { kind: 'task', taskId: middle },
        })
        return { root: conversation.id, top, middle, leaf }
      }),
    ),
  )

/** Replace committed records directly in Storage, as a crash between commits leaves them. */
const rewrite = (
  storage: Storage.Service,
  changes: ReadonlyArray<readonly [TaskId, (record: TaskRecord) => TaskRecord]>,
) =>
  Effect.orDie(
    Effect.gen(function* () {
      const writes = []
      for (const [id, change] of changes) {
        const record = yield* storage.task(id)
        if (record !== undefined) writes.push({ type: 'task' as const, value: change(record) })
      }
      yield* storage.commit(writes)
    }),
  )

/** A held task's definition under another name, which no registry holds. */
const Missing = Task.make<HoldInput, { readonly phase: 'hold' }, null>({
  name: 'test.missing',
  version: 1,
  initial: () => ({ phase: 'hold' }),
  phases: { hold: () => Effect.void },
  abort: () => Effect.void,
})

describe('work below a cancelled owner', () => {
  test('does not start its next phase while the cascade that marks it has not committed', () =>
    runTest((backend) =>
      Effect.gen(function* () {
        const tasks = holds()
        const entered = Deferred.makeUnsafe<void>()
        const advance = Deferred.makeUnsafe<void>()
        const held = Deferred.makeUnsafe<void>()
        const release = Deferred.makeUnsafe<void>()
        let target: TaskId | undefined
        let ranNext = false
        // Holds every commit that abort-marks `target` until released.
        const storage: Storage.Service = {
          ...backend,
          commit: (writes) =>
            Effect.suspend(() => {
              const marks = writes.some(
                (write) =>
                  write.type === 'task' && write.value.id === target && write.value.abortRequested,
              )
              if (!marks) return backend.commit(writes)
              return Deferred.done(held, Exit.void).pipe(
                Effect.andThen(Deferred.await(release)),
                Effect.andThen(backend.commit(writes)),
              )
            }),
        }
        const Child = Task.make<
          null,
          { readonly phase: 'first' } | { readonly phase: 'next' },
          null
        >({
          name: 'test.child',
          version: 1,
          initial: () => ({ phase: 'first' }),
          phases: {
            first: (_task, runtime) =>
              Effect.gen(function* () {
                yield* Deferred.done(entered, Exit.void)
                yield* Deferred.await(advance)
                yield* runtime.commit(() =>
                  Effect.succeed({
                    status: 'running' as const,
                    checkpoint: { phase: 'next' as const },
                  }),
                )
              }),
            next: () =>
              Effect.andThen(
                Effect.sync(() => {
                  ranNext = true
                }),
                Effect.never,
              ),
          },
          abort: (_task, runtime) =>
            runtime.commit(() =>
              Effect.succeed({ status: 'terminal', outcome: { status: 'aborted' } } as const),
            ),
        })
        const { session, scheduler } = yield* open(storage, [...tasks.tasks, Child])
        const { parent, child } = yield* Effect.orDie(
          session.commit((tx) =>
            Effect.gen(function* () {
              const conversation = yield* tx.createConversation({
                ownership: { kind: 'ownerless' },
              })
              const owner = yield* tx.createTask(tasks.Hold, hold('parent'), {
                ownership: { kind: 'conversation' },
                conversationId: conversation.id,
              })
              const below = yield* tx.createTask(Child, null, {
                ownership: { kind: 'task', taskId: owner },
              })
              return { parent: owner, child: below }
            }),
          ),
        )
        target = child
        yield* Deferred.await(entered)
        const aborting = yield* Effect.forkScoped(scheduler.abort(parent))
        yield* Deferred.await(held)
        // The parent's request is durable, the cascade's mark of the child is not: its phase
        // must not advance.
        yield* Deferred.done(advance, Exit.void)
        for (let n = 0; n < 5; n++) yield* settle
        expect(ranNext).toBe(false)
        yield* Deferred.done(release, Exit.void)
        expect(yield* outcome(scheduler, child)).toBe('aborted')
        yield* Fiber.join(aborting)
        expect(ranNext).toBe(false)
      }),
    ))

  test('lets a failFast request in the same pass override a restart mark', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const tasks = holds()
        const bare = yield* bareSession(storage)
        const { top, middle, leaf } = yield* chain(bare.session, tasks, { leaf: Missing })
        yield* bare.close
        // Restart-abandoned `top` waits failFast on a failed `middle` and on `leaf`, whose
        // definition is missing: a restart mark would keep it waiting, a request orphans it.
        yield* rewrite(storage, [
          [
            top,
            (record) => ({
              ...withState(record, {
                status: 'waiting',
                checkpoint: { phase: 'hold' },
                on: [middle, leaf],
                policy: 'failFast',
              }),
              abortRequested: true,
              abortReason: 'restart',
            }),
          ],
          [
            middle,
            (record) =>
              withState(record, {
                status: 'terminal',
                outcome: { status: 'failed', error: { message: 'failed' } },
              }),
          ],
        ])
        const { scheduler } = yield* open(storage, tasks.tasks)
        expect((yield* scheduler.waitForTask(leaf)).state.outcome).toEqual({
          status: 'orphaned',
          reason: 'missing_task',
        })
      }),
    ))

  test('upgrades restart marks down the tree when an abort is requested above them', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const tasks = holds()
        const bare = yield* bareSession(storage)
        const ids = yield* chain(bare.session, tasks, { middleAbandons: true, leaf: Missing })
        yield* bare.close
        // The leaf waits for its definition under the restart cascade.
        const { session, scheduler } = yield* open(storage, tasks.tasks)
        yield* eventually(
          Effect.map(getTask(session, ids.leaf), (record) => record?.abortReason === 'restart'),
        )
        yield* eventually(
          Effect.map(
            scheduler.inspect,
            (inspection) =>
              inspection.tasks.find((entry) => entry.record.id === ids.leaf)?.state.kind ===
              'blocked',
          ),
        )
        // A request on the top task reaches the leaf two levels down, which is then orphaned.
        yield* scheduler.abort(ids.top)
        expect(yield* outcome(scheduler, ids.leaf)).toBe('orphaned')
        expect(yield* outcome(scheduler, ids.middle)).toBe('aborted')
        expect(yield* outcome(scheduler, ids.top)).toBe('aborted')
      }),
    ))

  test('marks a flagged task that holds its outcome, aborting its owned work, and keeps the outcome', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const tasks = holds()
        const bare = yield* bareSession(storage)
        const ids = yield* chain(bare.session, tasks, { middleAbandons: true })
        yield* bare.close
        yield* rewrite(storage, [
          [
            ids.middle,
            (record) =>
              withState(record, {
                status: 'completing',
                outcome: { status: 'completed', result: null },
              }),
          ],
        ])
        const { scheduler } = yield* open(storage, tasks.tasks)
        expect(yield* outcome(scheduler, ids.leaf)).toBe('aborted')
        const settled = yield* scheduler.waitForTask(ids.middle)
        expect(settled).toMatchObject({
          abortReason: 'restart',
          state: { outcome: { status: 'completed' } },
        })
        yield* tasks.open('top')
      }),
    ))
})

describe('scheduler indexes', () => {
  test("takes a cascade's reason from the nearest cancelling owner", () =>
    runTest((backend) =>
      Effect.gen(function* () {
        const tasks = holds()
        const bare = yield* bareSession(backend)
        const ids = yield* chain(bare.session, tasks)
        yield* bare.close
        // `top` requested, `middle` restart-marked: the leaf gets middle's `restart` first.
        yield* rewrite(backend, [
          [ids.top, (record) => ({ ...record, abortRequested: true })],
          [ids.middle, (record) => ({ ...record, abortRequested: true, abortReason: 'restart' })],
        ])
        const reasons: Array<string | undefined> = []
        const storage = watchTaskWrites(backend, (record) => {
          if (record.id === ids.leaf && record.abortRequested) reasons.push(record.abortReason)
        })
        const { scheduler } = yield* open(storage, tasks.tasks)
        for (const id of [ids.leaf, ids.middle, ids.top]) yield* scheduler.waitForTask(id)
        expect(reasons[0]).toBe('restart')
      }),
    ))

  test('finalizes a chain of held owners in the commit that frees the last one', () =>
    runTest((backend) =>
      Effect.gen(function* () {
        const tasks = holds()
        // The commit, by count, that wrote each terminal record.
        const endedIn = new Map<TaskId, number>()
        let commits = 0
        const counted: Storage.Service = {
          ...backend,
          commit: (writes) =>
            Effect.suspend(() => {
              commits++
              return backend.commit(writes)
            }),
        }
        const storage = watchTaskWrites(counted, (record) => {
          if (record.state.status === 'terminal') endedIn.set(record.id, commits)
        })
        const { session, scheduler } = yield* open(storage, tasks.tasks)
        const ids = yield* chain(session, tasks)
        yield* tasks.open('top')
        yield* tasks.open('middle')
        yield* statusIs(session, ids.top, 'completing')
        yield* statusIs(session, ids.middle, 'completing')
        yield* tasks.open('leaf')
        expect(yield* outcome(scheduler, ids.top)).toBe('completed')
        // Both owners end in one finalize commit, after the leaf's.
        expect(endedIn.get(ids.middle)).toBe(endedIn.get(ids.top))
        expect(endedIn.get(ids.middle) ?? 0).toBeGreaterThan(endedIn.get(ids.leaf) ?? 0)
      }),
    ))

  test('keeps a scope busy after reopen for work below an ended task whose chain is not loaded yet', () =>
    runTest((backend) =>
      Effect.gen(function* () {
        const tasks = holds()
        let opened = yield* openClosable(backend, tasks.tasks)
        // root -> owner (ends) -> conversation C -> inner, created after the owner ended.
        const { root, owner, child } = yield* Effect.orDie(
          opened.session.commit((tx) =>
            Effect.gen(function* () {
              const conversation = yield* tx.createConversation({
                ownership: { kind: 'ownerless' },
              })
              const created = yield* tx.createTask(tasks.Hold, hold('owner'), {
                ownership: { kind: 'conversation' },
                conversationId: conversation.id,
              })
              const owned = yield* tx.createConversation({
                ownership: { kind: 'task', taskId: created },
              })
              return { root: conversation.id, owner: created, child: owned.id }
            }),
          ),
        )
        yield* tasks.open('owner')
        yield* opened.scheduler.waitForTask(owner)
        const inner = yield* Effect.orDie(
          opened.session.commit((tx) =>
            tx.createTask(tasks.Hold, hold('inner'), {
              ownership: { kind: 'conversation' },
              conversationId: child,
            }),
          ),
        )
        yield* opened.close
        // Hold the read of C's record, so the chain above `inner` stays unloaded.
        const read = Deferred.makeUnsafe<void>()
        const storage: Storage.Service = {
          ...backend,
          conversation: (id) =>
            id === child
              ? Effect.andThen(Deferred.await(read), backend.conversation(id))
              : backend.conversation(id),
        }
        opened = yield* openClosable(storage, tasks.tasks)
        // A task whose chain is not loaded counts as inside every scope.
        expect(yield* finishes(opened.scheduler.waitForIdle())).toBe(false)
        yield* Deferred.done(read, Exit.void)
        yield* tasks.open('inner')
        yield* opened.scheduler.waitForTask(inner)
        yield* opened.scheduler.waitForIdle(root)
        yield* opened.close
      }),
    ))
})
