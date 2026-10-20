/* oxlint-disable max-lines, max-lines-per-function -- ported describe blocks */
/**
 * Ported from pi's `packages/durable/test/harness-ownership.test.ts`, "ownership": holding owners
 * as `completing`, idle waits, and walks through ended tasks. Cases about queued inputs, which come
 * with the inbox, are left out.
 */
import { describe, expect, test } from 'bun:test'
import { Effect } from 'effect'
import type { Session } from '../../session/index.ts'
import type { TaskId } from '../../storage/schema/Ids.ts'
import type { Scheduler } from '../index.ts'
import {
  finishes,
  hold,
  holdIn,
  holds,
  outcome,
  outcomeOf,
  ownedChild,
  stateOf,
  statusIs,
} from './holds.ts'
import type { Holds } from './holds.ts'
import { conversation, eventually, getTask, open, openClosable, runTest } from './support.ts'

/**
 * Live P owns `depth` ended tasks in a chain, the last owning conversation C, empty when they
 * ended. Work started in C later is still P's.
 */
const endedBridge = (
  session: Session.Service,
  scheduler: Scheduler.Service,
  tasks: Holds,
  depth: number,
) =>
  Effect.gen(function* () {
    const root = yield* conversation(session)
    const tree = yield* Effect.orDie(
      session.commit((tx) =>
        Effect.gen(function* () {
          const p = yield* tx.createTask(tasks.Hold, hold('p', true), {
            ownership: { kind: 'conversation' },
            conversationId: root,
          })
          const links: Array<TaskId> = []
          let owner = p
          for (let n = 0; n < depth; n++) {
            owner = yield* tx.createTask(tasks.Hold, hold(`link${n}`), {
              ownership: { kind: 'task', taskId: owner },
            })
            links.push(owner)
          }
          const c = yield* tx.createConversation({ ownership: { kind: 'task', taskId: owner } })
          return { p, links, c: c.id }
        }),
      ),
    )
    for (let n = depth - 1; n >= 0; n--) yield* tasks.open(`link${n}`)
    for (const id of tree.links) yield* scheduler.waitForTask(id)
    return { root, p: tree.p, create: (name: string) => holdIn(session, tasks, tree.c, name) }
  })

describe('ownership', () => {
  test('keeps a conversation busy while its owned foreground subtree has live work, holding the completed owner', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const tasks = holds()
        const { session, scheduler } = yield* open(storage, tasks.tasks)
        const root = yield* conversation(session)
        const tree = yield* ownedChild(session, tasks, root, 'owner')
        yield* tasks.open('owner')
        yield* statusIs(session, tree.owner, 'completing')
        expect(yield* finishes(scheduler.waitForIdle(root))).toBe(false)
        expect(yield* finishes(scheduler.waitForIdle())).toBe(false)
        yield* tasks.open('owner.inner')
        yield* scheduler.waitForIdle(root)
        yield* scheduler.waitForIdle()
        expect(outcomeOf(yield* stateOf(session, tree.owner))).toBe('completed')
      }),
    ))

  test('holds an owner for work in a conversation its ended child task owns', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const tasks = holds()
        const { session, scheduler } = yield* open(storage, tasks.tasks)
        const root = yield* conversation(session)
        const ids = yield* Effect.orDie(
          session.commit((tx) =>
            Effect.gen(function* () {
              const owner = yield* tx.createTask(tasks.Hold, hold('owner'), {
                ownership: { kind: 'conversation' },
                conversationId: root,
              })
              const child = yield* tx.createTask(tasks.Hold, hold('child'), {
                ownership: { kind: 'task', taskId: owner },
              })
              const owned = yield* tx.createConversation({
                ownership: { kind: 'task', taskId: child },
              })
              return { owner, child, owned: owned.id }
            }),
          ),
        )
        yield* tasks.open('child')
        expect(yield* outcome(scheduler, ids.child)).toBe('completed')
        // The ended child's conversation still belongs to the owner above it.
        const late = yield* holdIn(session, tasks, ids.owned, 'late')
        yield* tasks.open('owner')
        yield* statusIs(session, ids.owner, 'completing')
        yield* tasks.open('late')
        expect(yield* outcome(scheduler, late)).toBe('completed')
        expect(yield* outcome(scheduler, ids.owner)).toBe('completed')
      }),
    ))

  test('cascades through ended tasks into an owned background subtree when aborting across background', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const tasks = holds()
        const { session, scheduler } = yield* open(storage, tasks.tasks)
        const root = yield* conversation(session)
        // root conversation -> A -> B -> conversation -> background C -> D
        const tree = yield* Effect.orDie(
          session.commit((tx) =>
            Effect.gen(function* () {
              const a = yield* tx.createTask(tasks.Hold, hold('a'), {
                ownership: { kind: 'conversation' },
                conversationId: root,
              })
              const b = yield* tx.createTask(tasks.Hold, hold('b'), {
                ownership: { kind: 'task', taskId: a },
              })
              const owned = yield* tx.createConversation({ ownership: { kind: 'task', taskId: b } })
              const c = yield* tx.createTask(tasks.Hold, hold('c'), {
                ownership: { kind: 'conversation' },
                conversationId: owned.id,
                background: true,
              })
              const d = yield* tx.createTask(tasks.Hold, hold('d'), {
                ownership: { kind: 'task', taskId: c },
              })
              return { a, c, d }
            }),
          ),
        )
        yield* tasks.open('b')
        yield* tasks.open('a')
        expect(yield* outcome(scheduler, tree.a)).toBe('completed')
        yield* scheduler.abortConversation(root, { background: true })
        expect(yield* outcome(scheduler, tree.c)).toBe('aborted')
        expect(yield* outcome(scheduler, tree.d)).toBe('aborted')
      }),
    ))

  test(
    'tracks ownership chains deeper than the call stack',
    () =>
      runTest((storage) =>
        Effect.gen(function* () {
          const tasks = holds()
          const { session, scheduler } = yield* open(storage, tasks.tasks)
          const root = yield* conversation(session)
          // Tasks each owning the next, created in one commit above a live leaf; they end top-down.
          const depth = 20_000
          const { top, leaf } = yield* Effect.orDie(
            session.commit((tx) =>
              Effect.gen(function* () {
                const first = yield* tx.createTask(tasks.Hold, hold('top'), {
                  ownership: { kind: 'conversation' },
                  conversationId: root,
                })
                let owner = first
                for (let n = 0; n < depth; n++) {
                  owner = yield* tx.createTask(tasks.Hold, hold(`link${n}`), {
                    ownership: { kind: 'task', taskId: owner },
                  })
                }
                const last = yield* tx.createTask(tasks.Hold, hold('leaf'), {
                  ownership: { kind: 'task', taskId: owner },
                })
                return { top: first, leaf: last }
              }),
            ),
          )
          for (let n = 0; n < depth; n++) yield* tasks.open(`link${n}`)
          yield* tasks.open('top')
          yield* statusIs(session, top, 'completing')
          yield* tasks.open('leaf')
          expect(yield* outcome(scheduler, leaf)).toBe('completed')
          expect(yield* outcome(scheduler, top)).toBe('completed')
        }),
      ),
    60_000,
  )

  for (const reopen of [false, true]) {
    test(`holds an owner for work created later below ended tasks${reopen ? ' loaded from storage' : ''}`, () =>
      runTest((storage) =>
        Effect.gen(function* () {
          const tasks = holds()
          let opened = yield* openClosable(storage, tasks.tasks)
          const root = yield* conversation(opened.session)
          // Live P owns B, which owns A, which owns the empty conversation C; B and A end.
          const tree = yield* Effect.orDie(
            opened.session.commit((tx) =>
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
                return { p, a, b, c: c.id }
              }),
            ),
          )
          yield* tasks.open('a')
          yield* tasks.open('b')
          yield* opened.scheduler.waitForTask(tree.b)
          if (reopen) {
            yield* opened.close
            // After reopen, B and A are known only once a chain load reads them.
            opened = yield* openClosable(storage, tasks.tasks)
          }
          const { session, scheduler } = opened
          const d = yield* holdIn(session, tasks, tree.c, 'd')
          yield* tasks.open('p')
          yield* statusIs(session, tree.p, 'completing')
          yield* tasks.open('d')
          expect(yield* outcome(scheduler, d)).toBe('completed')
          expect(yield* outcome(scheduler, tree.p)).toBe('completed')
          yield* opened.close
        }),
      ))
  }

  for (const depth of [2, 5]) {
    test(`aborts work below ${depth} ended tasks with their live owner, which waits for it`, () =>
      runTest((storage) =>
        Effect.gen(function* () {
          const tasks = holds()
          const { session, scheduler } = yield* open(storage, tasks.tasks)
          const { p, create } = yield* endedBridge(session, scheduler, tasks, depth)
          const d = yield* create('d')
          yield* scheduler.abort(p)
          expect(yield* outcome(scheduler, d)).toBe('aborted')
          // P's abort handler runs only after D ended.
          yield* tasks.open('abort.p')
          expect(yield* outcome(scheduler, p)).toBe('aborted')
        }),
      ))

    test(`keeps the root busy and aborts with it work below ${depth} ended tasks`, () =>
      runTest((storage) =>
        Effect.gen(function* () {
          const tasks = holds()
          const { session, scheduler } = yield* open(storage, tasks.tasks)
          const { root, p, create } = yield* endedBridge(session, scheduler, tasks, depth)
          yield* tasks.open('p')
          yield* scheduler.waitForTask(p)
          // P has ended: D and D2 hang below ended tasks only, and are still the root's work.
          const d = yield* create('d')
          const d2 = yield* create('d2')
          expect(yield* finishes(scheduler.waitForIdle(root))).toBe(false)
          expect(yield* finishes(scheduler.waitForIdle())).toBe(false)
          yield* scheduler.abortConversation(root)
          expect(yield* outcome(scheduler, d)).toBe('aborted')
          expect(yield* outcome(scheduler, d2)).toBe('aborted')
        }),
      ))
  }

  test('reaches work below ended tasks across two owned conversations', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const tasks = holds()
        const { session, scheduler } = yield* open(storage, tasks.tasks)
        const root = yield* conversation(session)
        // Live P -> ended B -> conversation C -> ended X -> ended A -> conversation C2.
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
              const x = yield* tx.createTask(tasks.Hold, hold('x'), {
                ownership: { kind: 'conversation' },
                conversationId: c.id,
              })
              const a = yield* tx.createTask(tasks.Hold, hold('a'), {
                ownership: { kind: 'task', taskId: x },
              })
              const c2 = yield* tx.createConversation({ ownership: { kind: 'task', taskId: a } })
              return { p, b, c2: c2.id }
            }),
          ),
        )
        for (const name of ['a', 'x', 'b']) yield* tasks.open(name)
        yield* scheduler.waitForTask(tree.b)
        const d = yield* holdIn(session, tasks, tree.c2, 'd')
        yield* tasks.open('p')
        yield* statusIs(session, tree.p, 'completing')
        expect(yield* finishes(scheduler.waitForIdle(root))).toBe(false)
        yield* scheduler.abortConversation(root)
        expect(yield* outcome(scheduler, d)).toBe('aborted')
        expect(yield* outcome(scheduler, tree.p)).toBe('completed')
      }),
    ))

  for (const ending of ['completes', 'is aborted'] as const) {
    test(`frees an owner that ${ending} once the last work below ended tasks ends after a background task there`, () =>
      runTest((storage) =>
        Effect.gen(function* () {
          const tasks = holds()
          const { session, scheduler } = yield* open(storage, tasks.tasks)
          const root = yield* conversation(session)
          // Live P owns B, which owns A, which owns C with background Z and, later, foreground D.
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
                const z = yield* tx.createTask(tasks.Hold, hold('z'), {
                  ownership: { kind: 'conversation' },
                  conversationId: c.id,
                  background: true,
                })
                return { p, b, c: c.id, z }
              }),
            ),
          )
          yield* tasks.open('a')
          yield* tasks.open('b')
          yield* scheduler.waitForTask(tree.b)
          const d = yield* holdIn(session, tasks, tree.c, 'd', true)
          if (ending === 'completes') {
            yield* tasks.open('p')
            yield* statusIs(session, tree.p, 'completing')
          } else {
            // P waits for D's abort before its own abort handler runs.
            yield* scheduler.abort(tree.p)
            yield* eventually(Effect.map(getTask(session, d), (r) => r?.abortRequested === true))
          }
          // Z ends first, D last.
          yield* tasks.open('z')
          yield* scheduler.waitForTask(tree.z)
          yield* tasks.open(ending === 'completes' ? 'd' : 'abort.d')
          yield* scheduler.waitForTask(d)
          const expected = ending === 'completes' ? 'completed' : 'aborted'
          expect(yield* outcome(scheduler, tree.p)).toBe(expected)
        }),
      ))
  }

  test('finalizes many held siblings that end together in one pass', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const tasks = holds()
        const { session, scheduler } = yield* open(storage, tasks.tasks)
        const root = yield* conversation(session)
        // Top holds 1000 held children, each holding one live leaf; the leaves end in one commit.
        const count = 1000
        const { top, leaves } = yield* Effect.orDie(
          session.commit((tx) =>
            Effect.gen(function* () {
              const first = yield* tx.createTask(tasks.Hold, hold('top'), {
                ownership: { kind: 'conversation' },
                conversationId: root,
              })
              const siblings: Array<TaskId> = []
              for (let n = 0; n < count; n++) {
                siblings.push(
                  yield* tx.createTask(tasks.Hold, hold(`s${n}`), {
                    ownership: { kind: 'task', taskId: first },
                  }),
                )
              }
              const created: Array<TaskId> = []
              for (const owner of siblings.toReversed()) {
                created.push(
                  yield* tx.createTask(tasks.Hold, hold(`l${owner}`), {
                    ownership: { kind: 'task', taskId: owner },
                  }),
                )
              }
              return { top: first, leaves: created }
            }),
          ),
        )
        for (let n = 0; n < count; n++) yield* tasks.open(`s${n}`)
        yield* tasks.open('top')
        yield* statusIs(session, top, 'completing')
        yield* Effect.orDie(
          session.commit((tx) =>
            Effect.gen(function* () {
              // Every read before the first write.
              const records = yield* Effect.forEach(leaves, (id) => tx.task(id))
              for (const record of records) {
                if (record === undefined) continue
                yield* tx.setTask({
                  ...record,
                  state: { status: 'terminal', outcome: { status: 'completed', result: null } },
                })
              }
            }),
          ),
        )
        expect(yield* outcome(scheduler, top)).toBe('completed')
      }),
    ))

  test('rejects a conversation created in the commit that ends its owner task', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const tasks = holds()
        const { session, scheduler } = yield* open(storage, tasks.tasks)
        const root = yield* conversation(session)
        const spawner = yield* Effect.orDie(
          session.commit((tx) =>
            tx.createTask(
              tasks.Spawner,
              { name: 'spawned' },
              {
                ownership: { kind: 'conversation' },
                conversationId: root,
              },
            ),
          ),
        )
        const receipt = yield* scheduler.waitForTask(spawner)
        expect(receipt.state.outcome).toMatchObject({ status: 'faulted' })
        const message = 'error' in receipt.state.outcome ? receipt.state.outcome.error.message : ''
        expect(message).toMatch(
          new RegExp(`^Conversation owner task ${spawner} is (completing|terminal)$`, 'u'),
        )
      }),
    ))

  test('stops idle traversal at a background owner', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const tasks = holds()
        const { session, scheduler } = yield* open(storage, tasks.tasks)
        const root = yield* conversation(session)
        const tree = yield* ownedChild(session, tasks, root, 'background', { background: true })
        yield* scheduler.waitForIdle(root)
        yield* scheduler.waitForIdle()
        // The background child is its own scope.
        expect(yield* finishes(scheduler.waitForIdle(tree.child))).toBe(false)
        yield* scheduler.abortConversation(tree.child)
      }),
    ))

  test('decides idle after reopen from owner edges it has to load first', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const tasks = holds()
        let opened = yield* openClosable(storage, tasks.tasks)
        const root = yield* conversation(opened.session)
        const foreground = yield* ownedChild(opened.session, tasks, root, 'fg')
        const deeper = yield* ownedChild(opened.session, tasks, foreground.child, 'fg2')
        const background = yield* ownedChild(opened.session, tasks, root, 'bg', {
          background: true,
        })
        yield* tasks.open('fg')
        yield* tasks.open('fg2')
        // Both owners hold their outcomes while the work below them runs.
        yield* statusIs(opened.session, foreground.owner, 'completing')
        yield* statusIs(opened.session, deeper.owner, 'completing')
        yield* opened.close

        opened = yield* openClosable(storage, tasks.tasks)
        const { session, scheduler } = opened
        // Two levels below completed foreground owners, the inner tasks keep the root busy.
        expect(yield* finishes(scheduler.waitForIdle(root))).toBe(false)
        expect(yield* finishes(scheduler.waitForIdle())).toBe(false)
        yield* tasks.open('fg.inner')
        yield* tasks.open('fg2.inner')
        // The background subtree does not count.
        yield* scheduler.waitForIdle(root)
        yield* scheduler.waitForIdle()
        expect((yield* stateOf(session, foreground.owner)).status).toBe('terminal')
        expect((yield* stateOf(session, background.inner)).status).not.toBe('terminal')
        yield* scheduler.abort(background.owner)
        yield* opened.close
      }),
    ))
})
