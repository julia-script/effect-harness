/* oxlint-disable max-lines-per-function -- ported describe blocks */
/**
 * Ported from pi's `harness-scheduler-retention.test.ts`: chains of ended tasks the ownership index
 * forgets once nothing live is below them, and loads again when work arrives there. Cases about
 * index sizes and queued inputs are left out.
 */
import { describe, expect, test } from 'bun:test'
import { Effect } from 'effect'
import type { Storage } from '../../storage/index.ts'
import { withState } from '../internal/state.ts'
import { hold, holdIn, holds, outcome, statusIs } from './holds.ts'
import { completed, conversation, open, runTest } from '../../test/task.ts'

/** Count the task and conversation reads a Storage serves. */
const countReads = (backend: Storage.Service) => {
  const reads = { count: 0 }
  const storage: Storage.Service = {
    ...backend,
    task: (id) =>
      Effect.suspend(() => {
        reads.count++
        return backend.task(id)
      }),
    conversation: (id) =>
      Effect.suspend(() => {
        reads.count++
        return backend.conversation(id)
      }),
  }
  return { storage, reads }
}

describe('scheduler retention', () => {
  test('holds a live owner for work later created in a forgotten conversation, loading its chain again', () =>
    runTest((backend) =>
      Effect.gen(function* () {
        const tasks = holds()
        const { storage, reads } = countReads(backend)
        const { session, scheduler } = yield* open(storage, tasks.tasks)
        const root = yield* conversation(session)
        // Live P -> ended B -> ended A -> conversation C, empty: forgotten.
        const tree = yield* Effect.orDie(
          session.commit((tx) =>
            Effect.gen(function* () {
              const p = yield* tx.createTask(tasks.Hold, hold('p'), {
                ownership: { kind: 'conversation' },
                conversationId: root,
              })
              const b = yield* tx.createTask(tasks.Hold, hold('b'), {
                ownership: { kind: 'task', taskId: p },
              })
              const a = yield* tx.createTask(tasks.Hold, hold('a'), {
                ownership: { kind: 'task', taskId: b },
              })
              const c = yield* tx.createConversation({ ownership: { kind: 'task', taskId: a } })
              return { p, b, c: c.id }
            }),
          ),
        )
        yield* tasks.open('a')
        yield* tasks.open('b')
        yield* scheduler.waitForTask(tree.b)
        const loaded = reads.count
        const d = yield* holdIn(session, tasks, tree.c, 'd')
        yield* tasks.open('p')
        yield* statusIs(session, tree.p, 'completing')
        expect(reads.count).toBeGreaterThan(loaded)
        yield* tasks.open('d')
        expect(yield* outcome(scheduler, d)).toBe('completed')
        expect(yield* outcome(scheduler, tree.p)).toBe('completed')
      }),
    ))

  test('keeps a chain when one commit ends its last task and creates new work below it', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const tasks = holds()
        const { session, scheduler } = yield* open(storage, tasks.tasks)
        const root = yield* conversation(session)
        // Live P -> ended B -> conversation C with background task `old`, which keeps the chain
        // known without holding B.
        const tree = yield* Effect.orDie(
          session.commit((tx) =>
            Effect.gen(function* () {
              const p = yield* tx.createTask(tasks.Hold, hold('p'), {
                ownership: { kind: 'conversation' },
                conversationId: root,
              })
              const b = yield* tx.createTask(tasks.Hold, hold('b'), {
                ownership: { kind: 'task', taskId: p },
              })
              const c = yield* tx.createConversation({ ownership: { kind: 'task', taskId: b } })
              const old = yield* tx.createTask(tasks.Hold, hold('old'), {
                ownership: { kind: 'conversation' },
                conversationId: c.id,
                background: true,
              })
              return { p, b, c: c.id, old }
            }),
          ),
        )
        yield* tasks.open('b')
        yield* scheduler.waitForTask(tree.b)
        // One commit: the old task ends, leaving nothing live below B, and a replacement starts.
        const replacement = yield* Effect.orDie(
          session.commit((tx) =>
            Effect.gen(function* () {
              const old = yield* tx.task(tree.old)
              const id = yield* tx.createTask(tasks.Hold, hold('new'), {
                ownership: { kind: 'conversation' },
                conversationId: tree.c,
              })
              if (old !== undefined) yield* tx.setTask(withState(old, completed(null)))
              return id
            }),
          ),
        )
        // The chain stayed known through the publication, so P holds for the replacement.
        yield* tasks.open('p')
        yield* statusIs(session, tree.p, 'completing')
        yield* tasks.open('new')
        yield* scheduler.waitForTask(replacement)
        expect(yield* outcome(scheduler, tree.p)).toBe('completed')
      }),
    ))

  test('lets a task wait on a forgotten ended owner', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const tasks = holds()
        const { session, scheduler } = yield* open(storage, tasks.tasks)
        const root = yield* conversation(session)
        const tree = yield* Effect.orDie(
          session.commit((tx) =>
            Effect.gen(function* () {
              const b = yield* tx.createTask(tasks.Hold, hold('b'), {
                ownership: { kind: 'conversation' },
                conversationId: root,
              })
              const c = yield* tx.createConversation({ ownership: { kind: 'task', taskId: b } })
              return { b, c: c.id }
            }),
          ),
        )
        yield* tasks.open('b')
        yield* scheduler.waitForTask(tree.b)
        const waiter = yield* Effect.orDie(
          session.commit((tx) =>
            tx.createTask(
              tasks.Waiter,
              { on: [tree.b] },
              { ownership: { kind: 'conversation' }, conversationId: tree.c },
            ),
          ),
        )
        expect(yield* outcome(scheduler, waiter)).toBe('completed')
      }),
    ))
})
