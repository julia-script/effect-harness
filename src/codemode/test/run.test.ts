/* oxlint-disable max-lines-per-function -- describe blocks */
/**
 * Durable code mode, ported from pi's `codemode.test.ts`: scripts calling tools as nested calls,
 * and resuming after a restart without running finished calls again.
 */
import { describe, expect, test } from 'bun:test'
import { Deferred, Effect, Exit, Schema } from 'effect'
import { entriesOf, runTest } from '../../harness/test/generationSupport.ts'
import type * as Harness from '../../harness/Harness.ts'
import type { ConversationId } from '../../storage/schema/Ids.ts'
import { Faux } from '../../model/index.ts'
import { ToolTask } from '../../tool/index.ts'
import * as Extension from '../../harness/Extension.ts'
import { Codemode } from '../index.ts'
import type { Global } from '../CodemodeHandlers.ts'
import { restart, runScript, script } from './scripts.ts'
import { openChat, toolTasks, world } from './support.ts'

const DONE = Faux.message('done')

/** The text after the header of each code mode result, oldest first. */
const results = (harness: Harness.Service, conversationId: ConversationId) =>
  Effect.map(entriesOf(harness, conversationId), (entries) =>
    entries
      .filter((entry) => entry.kind === '$tool-result')
      .map((entry) => {
        const content: unknown = entry.model?.[0]?.['content']
        const block: unknown = Array.isArray(content) ? content[1] : undefined
        return typeof block === 'object' && block !== null && 'text' in block
          ? block.text
          : undefined
      }),
  )

const isNested = Schema.is(ToolTask.NestedInput)

describe('durable code mode', () => {
  test('runs a script that calls tools as nested calls', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const current = yield* world
        current.block = false
        const { harness, result } = yield* runScript(
          storage,
          current,
          'const a = await tools.echo({ text: "a" }); const b = await tools.echo({ text: "b" }); return [a, b];',
        )
        expect(result).toMatchObject({ text: '["echo a","echo b"]', isError: false })
        expect((yield* toolTasks(harness)).nested).toHaveLength(2)
      }),
    ))

  test('resumes a script after a restart without running its finished calls again', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const current = yield* world
        const { result } = yield* restart(
          storage,
          current,
          `const a = await tools.echo({ text: "a" });
          const held = await tools.hold({ id: 1 });
          const b = await tools.echo({ text: "b" });
          return [a, held, b, Date.now() > 0];`,
        )
        expect(result).toMatchObject({ text: '["echo a","held 1","echo b",true]', isError: false })
        expect(Object.fromEntries(current.echo)).toEqual({ a: 1, b: 1 })
        // The VM ran twice; hold, replay-safe, reran once after the restart.
        expect(current.starts).toBe(2)
        expect(current.holdRuns).toBe(2)
      }),
    ))

  test("lists the script's tool calls in its result's details", () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const current = yield* world
        const { result } = yield* runScript(
          storage,
          current,
          'await tools.echo({ text: "a" }); try { await tools.fails({}); } catch {} tools.hold({ id: 9 }); return 1;',
        )
        // A call still running, or not yet launched, when the script ended shows as cancelled.
        expect(result.details).toMatchObject({
          calls: [
            { id: 1, name: 'echo', args: '{"text":"a"}', status: 'ok' },
            { id: 2, name: 'fails', args: '{}', status: 'error', error: 'permission denied' },
            { id: 3, name: 'hold', args: '{"id":9}', status: 'cancelled' },
          ],
        })
      }),
    ))

  test('aborts a call still running when the script ends', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const current = yield* world
        const { harness, result } = yield* runScript(
          storage,
          current,
          'tools.hold({}); await tools.echo({ text: "a" }); return 1;',
        )
        expect(result).toMatchObject({ text: '1', isError: false })
        const { nested } = yield* toolTasks(harness)
        const hold = nested.find((task) => isNested(task.input) && task.input.call.name === 'hold')
        expect(hold?.state).toMatchObject({ status: 'terminal', outcome: { status: 'aborted' } })
      }),
    ))

  test('rejects a failed tool call with its error text', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const { result } = yield* runScript(
          storage,
          yield* world,
          'try { await tools.fails({}); } catch (e) { return e.message; }',
        )
        expect(result).toMatchObject({ text: 'permission denied', isError: false })
      }),
    ))

  test('resolves a structured tool to its value, and rejects its failure with it attached', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const { result } = yield* runScript(
          storage,
          yield* world,
          `const ok = await tools.value({ id: 2 });
          let e; try { await tools.value({ id: -1 }); } catch (error) { e = [error.message, error.failure]; }
          return [ok, e];`,
        )
        expect(JSON.parse(result.text)).toEqual([
          { doubled: 4 },
          ['{"reason":"negative"}', { reason: 'negative' }],
        ])
      }),
    ))

  test('resolves an output tool to its text, images left out', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const { result } = yield* runScript(
          storage,
          yield* world,
          'return [await tools.picture({ caption: true }), await tools.picture({ caption: false })];',
        )
        expect(result).toMatchObject({ text: '["a cat",""]', isError: false })
      }),
    ))

  test('calls a deferred tool the model is not offered, and finds it in ALL_TOOLS', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const { result } = yield* runScript(
          storage,
          yield* world,
          'return [await tools.later({}), ALL_TOOLS.some((tool) => tool.name === "later")];',
        )
        expect(result.text).toBe('["later ran",true]')
      }),
    ))

  test('returns a decided outcome after a restart without running the script again', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const current = yield* world
        // Stop after the handler returned, so after the outcome is stored, and before the result.
        const hooks = [
          Extension.hook(ToolTask.Hooks, {
            afterTool: (call) =>
              call.name !== 'codemode' || !current.block
                ? Effect.undefined
                : Effect.andThen(Deferred.done(current.reached, Exit.void), Effect.never),
          }),
        ]
        const { result } = yield* restart(storage, current, 'return "first"', {
          hooks,
          beforeClose: (harness, task) =>
            Effect.map(
              Effect.orDie(harness.session.snapshot(Codemode.RunDocument, task)),
              (run) => {
                expect(run?.result).toBeDefined()
              },
            ),
        })
        expect(result).toMatchObject({ text: 'first', isError: false })
        expect(current.starts).toBe(1)
      }),
    ))

  test('keeps store() values per conversation, as of a fork entry', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const current = yield* world
        current.block = false
        const { harness, faux } = yield* openChat(storage, current)
        yield* faux.setResponses([
          script('store("k", 1); return load("k")'),
          DONE,
          script('return [load("k") ?? null]'),
          DONE,
          script('return [load("k") ?? null]'),
          DONE,
        ])
        const root = yield* Effect.orDie(harness.root())
        const ask = (conversation: typeof root, content: string) =>
          Effect.flatMap(
            Effect.orDie(conversation.submit({ type: 'input', content })),
            (each) => each.wait,
          )
        yield* ask(root, 'first')
        const firstUser = (yield* entriesOf(harness, root.id)).find(
          (entry) => entry.kind === '$user',
        )
        yield* ask(root, 'second')
        expect(yield* results(harness, root.id)).toEqual(['1', '[1]'])
        // A fork at the first input is from before the first script stored anything.
        const at = yield* Effect.orDie(Effect.fromNullishOr(firstUser))
        const fork = yield* Effect.orDie(root.fork(at.id, { ownership: { kind: 'ownerless' } }))
        yield* ask(fork, 'fork')
        expect((yield* results(harness, fork.id)).at(-1)).toBe('[null]')
      }),
    ))

  test('journals results that finish together in one commit', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const released = yield* Deferred.make<void>()
        let waiting = 0
        const wait: Global = {
          name: 'wait',
          execute: () =>
            Effect.gen(function* () {
              waiting += 1
              if (waiting === 50) yield* Deferred.done(released, Exit.void)
              yield* Deferred.await(released)
              return null
            }),
        }
        const current = yield* world
        const opened = yield* openChat(storage, current, { globals: [wait] })
        let commits = 0
        yield* opened.harness.session.observeCommits((publication) =>
          Effect.sync(() => {
            for (const change of publication.changes)
              if (change.type === 'document' && change.record.kind === '$codemode.journal')
                commits += 1
          }),
        )
        yield* opened.faux.setResponses([
          script('return (await Promise.all(Array.from({ length: 50 }, () => wait()))).length'),
          DONE,
        ])
        const root = yield* Effect.orDie(opened.harness.root())
        yield* Effect.flatMap(
          Effect.orDie(root.submit({ type: 'input', content: 'go' })),
          (each) => each.wait,
        )
        expect(yield* results(opened.harness, root.id)).toEqual(['50'])
        // The journal's creation and a few groups, not one commit per result (51).
        expect(commits).toBeLessThan(10)
      }),
    ))
})
