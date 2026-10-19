/* oxlint-disable max-lines, max-lines-per-function -- ported describe blocks */
/**
 * Ported from pi's `packages/durable/test/harness-tasks-recovery.test.ts`, "definition handover".
 */
import { describe, expect, test } from 'bun:test'
import { Deferred, Effect, Exit } from 'effect'
import type { Storage } from '../../storage/index.ts'
import { Registry, Task } from '../index.ts'
import {
  abortedWith,
  Boom,
  completed,
  conversation,
  createTask,
  eventually,
  getTask,
  open,
  runTest,
  settle,
} from './support.ts'

type Handover = { readonly phase: 'a' } | { readonly phase: 'b' } | { readonly phase: 'c' }

interface Gates {
  readonly a?: Deferred.Deferred<void>
  readonly b?: Deferred.Deferred<void>
}

interface Options {
  readonly gates?: Gates
  readonly migrate?: () => Effect.Effect<
    { readonly input: null; readonly checkpoint: Handover },
    Boom
  >
}

const handoverTask = (
  label: string,
  version: number,
  log: Array<string>,
  options: Options = {},
) => {
  const advance =
    (phase: 'a' | 'b', next: Handover) =>
    (_task: unknown, runtime: Task.Runtime<null, Handover, null>) =>
      Effect.gen(function* () {
        log.push(`${label}:${phase} start`)
        const gate = options.gates?.[phase]
        if (gate !== undefined) yield* Deferred.await(gate)
        yield* runtime.commit(() =>
          Effect.succeed({ status: 'running' as const, checkpoint: next }),
        )
        // Leave room for a wrongly started successor before this invocation ends.
        yield* settle
        log.push(`${label}:${phase} end`)
      })
  return Task.make<null, Handover, null>({
    name: 'test.handover',
    version,
    initial: () => ({ phase: 'a' }),
    phases: {
      a: advance('a', { phase: 'b' }),
      b: advance('b', { phase: 'c' }),
      c: (_task, runtime) =>
        Effect.andThen(
          Effect.sync(() => log.push(`${label}:c`)),
          runtime.commit(() => Effect.succeed(completed(null))),
        ),
    },
    abort: (_task, runtime) =>
      Effect.andThen(
        Effect.sync(() => log.push(`${label}:abort`)),
        runtime.commit(() => Effect.succeed(abortedWith(label))),
      ),
    ...(options.migrate === undefined ? {} : { migrate: options.migrate }),
  })
}

const migrateToC = () => Effect.succeed({ input: null, checkpoint: { phase: 'c' as const } })
const brokenMigration = () => Effect.fail(new Boom({ message: 'broken migration' }))

/** Run an `old` handover task until its first phase started. */
const startHandover = (storage: Storage.Service, log: Array<string>, gates: Gates) =>
  Effect.gen(function* () {
    const old = handoverTask('old', 1, log, { gates })
    const registry = yield* Registry.make([old])
    const opened = yield* open(storage, [], registry)
    const id = yield* createTask(opened.session, yield* conversation(opened.session), old, null)
    yield* eventually(Effect.sync(() => log.length === 1))
    return { ...opened, id }
  })

describe('definition handover', () => {
  test('hands over at the next phase boundary to a same-version replacement without overlap', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const log: Array<string> = []
        const gate = yield* Deferred.make<void>()
        const { registry, scheduler, id } = yield* startHandover(storage, log, { a: gate })
        yield* registry.install(handoverTask('new', 1, log))
        yield* Deferred.done(gate, Exit.void)
        yield* scheduler.waitForTask(id)
        expect(log).toEqual(['old:a start', 'old:a end', 'new:b start', 'new:b end', 'new:c'])
      }),
    ))

  test('keeps the memos across a handover', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const log: Array<string> = []
        const gate = yield* Deferred.make<void>()
        const reached = yield* Deferred.make<void>()
        const memoTask = (label: string) =>
          Task.make<null, { readonly phase: 'a' } | { readonly phase: 'b' }, string>({
            name: 'test.handover-memo',
            version: 1,
            initial: () => ({ phase: 'a' }),
            phases: {
              a: (_task, runtime) =>
                Effect.gen(function* () {
                  yield* runtime.memo('picked', label)
                  yield* Deferred.done(reached, Exit.void)
                  yield* Deferred.await(gate)
                  yield* runtime.commit(() =>
                    Effect.succeed({
                      status: 'running' as const,
                      checkpoint: { phase: 'b' as const },
                    }),
                  )
                }),
              b: (_task, runtime) =>
                Effect.gen(function* () {
                  // The candidate loses to the memo the old definition stored.
                  const picked = yield* runtime.memo('picked', label)
                  log.push(`${label}:b ${picked}`)
                  yield* runtime.commit(() => Effect.succeed(completed(picked)))
                }),
            },
            abort: () => Effect.void,
          })
        const registry = yield* Registry.make([memoTask('old')])
        const { session, scheduler } = yield* open(storage, [], registry)
        const id = yield* createTask(session, yield* conversation(session), memoTask('old'), null)
        yield* Deferred.await(reached)
        yield* registry.install(memoTask('new'))
        yield* Deferred.done(gate, Exit.void)
        const receipt = yield* scheduler.waitForTask(id)
        expect(receipt.state.outcome).toEqual({ status: 'completed', result: 'old' })
        expect(log).toEqual(['new:b old'])
      }),
    ))

  test('hands over to a newer version with a migration', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const log: Array<string> = []
        const gate = yield* Deferred.make<void>()
        const { registry, scheduler, id } = yield* startHandover(storage, log, { a: gate })
        yield* registry.install(handoverTask('v2', 2, log, { migrate: migrateToC }))
        yield* Deferred.done(gate, Exit.void)
        expect((yield* scheduler.waitForTask(id)).version).toBe(2)
        expect(log).toEqual(['old:a start', 'old:a end', 'v2:c'])
      }),
    ))

  test('hands over to a newer version whose migration fails and leaves the task blocked', () =>
    runTest((storage, reports) =>
      Effect.gen(function* () {
        const log: Array<string> = []
        const gate = yield* Deferred.make<void>()
        const { registry, session, id } = yield* startHandover(storage, log, { a: gate })
        yield* registry.install(handoverTask('broken', 2, log, { migrate: brokenMigration }))
        yield* Deferred.done(gate, Exit.void)
        yield* eventually(Effect.sync(() => reports.length === 1))
        yield* settle
        expect(reports[0]).toContain('broken migration')
        expect(yield* getTask(session, id)).toMatchObject({
          version: 1,
          state: { status: 'pending', checkpoint: { phase: 'b' } },
        })
        expect(log).toEqual(['old:a start', 'old:a end'])
      }),
    ))

  test('keeps running under the old definition when the replacement is missing or cannot take the task', () =>
    runTest((storage, reports) =>
      Effect.gen(function* () {
        const log: Array<string> = []
        const a = yield* Deferred.make<void>()
        const b = yield* Deferred.make<void>()
        const { registry, scheduler, id } = yield* startHandover(storage, log, { a, b })
        yield* registry.uninstall('test.handover')
        yield* Deferred.done(a, Exit.void)
        yield* eventually(Effect.sync(() => log.includes('old:b start')))
        // A newer definition without a migration cannot take the task either.
        yield* registry.install(handoverTask('incompatible', 2, log))
        yield* Deferred.done(b, Exit.void)
        yield* scheduler.waitForTask(id)
        expect(log).toEqual(['old:a start', 'old:a end', 'old:b start', 'old:b end', 'old:c'])
        expect(reports).toHaveLength(2)
        expect(reports[0]).toContain('missing_task')
        expect(reports[1]).toContain('incompatible_task')
      }),
    ))
})
