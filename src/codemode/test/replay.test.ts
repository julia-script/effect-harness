/* oxlint-disable max-lines-per-function -- describe blocks */
/**
 * Code mode across restarts, ported from pi's `codemode.test.ts`: replay from the journal, and
 * the divergences that end a call instead of replaying it.
 */
import { describe, expect, test } from 'bun:test'
import { Effect } from 'effect'
import { eventually, runTest } from '../../harness/test/generationSupport.ts'
import type { Session } from '../../session/index.ts'
import type { TaskId } from '../../storage/schema/Ids.ts'
import { Codemode } from '../index.ts'
import type { Global } from '../CodemodeHandlers.ts'
import { edit, restart } from './scripts.ts'
import { release, toolTasks, world } from './support.ts'

const setStore = (store: Record<string, unknown>) =>
  edit({
    run: (state) => {
      if (state.start !== undefined) state.start.store = store
    },
  })

/** Wait until the live run recorded `count` for result `seq` (0: its first run). */
const recorded =
  (seq: number, count: number) => (harness: { readonly session: Session.Service }, task: TaskId) =>
    eventually(
      Effect.map(Effect.orDie(harness.session.snapshot(Codemode.JournalDocument, task)), (state) =>
        seq === 0 ? state?.firstIssued === count : state?.entries[seq - 1]?.issuedAfter === count,
      ),
    )

const counter = (name: string, values: Array<number>): Global => ({
  name,
  execute: () => Effect.sync(() => values.push(values.length + 1)),
})

describe('code mode replay', () => {
  test('replays results in the order they arrived, not the order the calls were made', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const current = yield* world
        yield* release(current, 2)
        yield* Effect.forkChild(
          Effect.andThen(
            eventually(Effect.sync(() => current.finished.includes(2))),
            release(current, 1),
          ),
        )
        const { result } = yield* restart(
          storage,
          current,
          `const order = [];
          await Promise.all([tools.slow({ id: 1 }).then(() => order.push(1)), tools.slow({ id: 2 }).then(() => order.push(2))]);
          await tools.echo({ text: order.join(",") });
          await tools.hold({});
          return order;`,
        )
        expect(result).toMatchObject({ text: '[2,1]', isError: false })
        expect(Object.fromEntries(current.echo)).toEqual({ '2,1': 1 })
      }),
    ))

  test('journals globals with their results and hands a replay the recorded ones', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const values: Array<number> = []
        const { result } = yield* restart(
          storage,
          yield* world,
          'const n = await next(); await tools.hold({}); return [n, await next()];',
          { globals: [counter('next', values)] },
        )
        expect(result).toMatchObject({ text: '[1,2]', isError: false })
        expect(values).toEqual([1, 2])
      }),
    ))

  test("keeps the script's clock and randomness across a restart", () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const current = yield* world
        const { result } = yield* restart(
          storage,
          current,
          `const before = [Date.now(), Math.random()];
          await tools.hold({});
          await tools.echo({ text: String(before[0]) + "/" + String(before[1]) });
          return "ok";`,
        )
        expect(result.text).toBe('ok')
        expect([...current.echo.values()]).toEqual([1])
      }),
    ))

  test("keeps a global's undefined result undefined, live and replayed", () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const nothing: Global = { name: 'nothing', execute: () => Effect.undefined }
        const { result } = yield* restart(
          storage,
          yield* world,
          'const before = await nothing(); await tools.hold({}); return [before === undefined, (await nothing()) === undefined];',
          { globals: [nothing] },
        )
        expect(result).toMatchObject({ text: '[true,true]', isError: false })
      }),
    ))

  test('ends the call when a resumed script made a different number of calls than before', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const { result } = yield* restart(
          storage,
          yield* world,
          'await tools.echo({ text: "a" }); await tools.hold({});',
          {
            between: edit({
              journal: (state) => {
                const [entry] = state.entries
                if (entry !== undefined) entry.issuedAfter = 7
              },
            }),
          },
        )
        expect(result.isError).toBe(true)
        expect(result.text).toContain('after result 1 it had made 2 calls, where it had made 7')
      }),
    ))

  test('ends the call at once when a resumed script waits without making a call it made before', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const { harness, result } = yield* restart(
          storage,
          yield* world,
          'await tools.echo({ text: "a" }); await tools.hold({});',
          {
            between: edit({
              journal: (state) => {
                const [entry] = state.entries
                if (entry !== undefined) entry.id = 99
              },
            }),
          },
        )
        expect(result.isError).toBe(true)
        expect(result.text).toContain('it waits without making call 99')
        expect((yield* toolTasks(harness)).nested).toHaveLength(2)
      }),
    ))

  test('ends the call when a resumed script calls a global with other arguments', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const { result } = yield* restart(
          storage,
          yield* world,
          'await next(); await tools.hold({});',
          {
            globals: [counter('next', [])],
            between: edit({
              journal: (state) => {
                const global = state.entries[0]?.global
                if (global !== undefined) global.args = 'other'
              },
            }),
          },
        )
        expect(result.isError).toBe(true)
        expect(result.text).toContain('call 1 is next')
      }),
    ))

  test('admits no call of a diverging replay, even one the script never made before', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const current = yield* world
        const { harness, result } = yield* restart(
          storage,
          current,
          `await tools.echo({ text: "a" });
          const extra = load("k") ?? 0;
          for (let i = 0; i < extra; i++) tools.echo({ text: "extra" + i });
          await tools.hold({});`,
          { between: setStore({ k: 3 }), beforeClose: recorded(1, 2) },
        )
        expect(result.isError).toBe(true)
        expect(result.text).toContain('after result 1 it had made 5 calls, where it had made 2')
        expect(Object.fromEntries(current.echo)).toEqual({ a: 1 })
        expect((yield* toolTasks(harness)).nested).toHaveLength(2)
      }),
    ))

  test('ends the call when a resumed script ends where it went on before', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const { result } = yield* restart(
          storage,
          yield* world,
          'await tools.echo({ text: "a" }); if (load("k")) return "early"; await tools.hold({});',
          { between: setStore({ k: true }), beforeClose: recorded(1, 2) },
        )
        expect(result.isError).toBe(true)
        expect(result.text).toContain('it ended after result 1, where it went on before')
      }),
    ))

  test('ends the call when a resumed script makes a call an earlier attempt admitted with other arguments', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const current = yield* world
        const { result } = yield* restart(
          storage,
          current,
          'await tools.echo({ text: load("k") ?? "a" }); await tools.hold({});',
          {
            between: edit({
              run: (state) => {
                if (state.start !== undefined) state.start.store = { k: 'other' }
              },
              // As if the restart came before the first journal commit.
              journal: (state) => {
                state.entries.length = 0
              },
            }),
          },
        )
        expect(result.isError).toBe(true)
        expect(result.text).toContain('call 1 is tools.echo')
        expect(Object.fromEntries(current.echo)).toEqual({ a: 1 })
      }),
    ))

  test("ends the call when a rerun's first run makes fewer calls than an earlier attempt admitted", () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const current = yield* world
        const { result } = yield* restart(
          storage,
          current,
          `const k = load("k");
          const first = tools.hold({ id: 1 });
          if (!k) tools.hold({ id: 2 });
          await first;
          await pick();
          await tools.echo({ text: "new" });`,
          {
            globals: [counter('pick', [])],
            between: setStore({ k: true }),
            beforeClose: recorded(0, 2),
          },
        )
        expect(result.isError).toBe(true)
        expect(result.text).toContain('its first run made 1 calls, where it had made 2')
        expect(current.echo.size).toBe(0)
      }),
    ))
})
