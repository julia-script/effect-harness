import * as Identity from '../../src/Identity.ts'
import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as Fiber from 'effect/Fiber'
import * as Ref from 'effect/Ref'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as AiTool from 'effect/ai/Tool'
import * as Toolkit from 'effect/ai/Toolkit'
import * as AiPrompt from 'effect/ai/Prompt'
import { HookError, HookFailure } from '../../src/Error.ts'
import * as Agent from '../../src/Agent.ts'
import * as Hook from '../../src/Hook.ts'
import { Invocation } from '../../src/Invocation.ts'
import * as Registry from '../../src/Registry.ts'
import * as Tool from '../../src/Tool.ts'
import type * as Extension from '../../src/Extension.ts'

const entryId = Schema.decodeSync(Identity.EntryId)
const TestTool = AiTool.make('test', {
  parameters: Schema.Struct({ n: Schema.Finite }),
  success: Schema.Finite,
})
const Tools = Toolkit.make(TestTool)
const tools = Tool.bind(Tools).pipe(
  Effect.provide(Tools.toLayer({ test: ({ n }) => Effect.succeed(n) })),
)
const quiet = { cwd: '.', report: () => Effect.void, progress: () => Effect.void }
const section = (key: string, text: string): Extension.Section => ({
  key,
  render: () => Effect.succeed(text),
})

describe('atomic registry and selected code', () => {
  it.effect(
    'replacement retains position, uninstall by name and reinstall appends, old snapshots retain code',
    () =>
      Effect.gen(function* () {
        const a = { name: 'a', sections: [section('a', 'old')] }
        const registry = yield* Registry.make([a, { name: 'b' }])
        const old = yield* registry.snapshot
        yield* registry.install([{ name: 'a', sections: [section('a', 'new')] }])
        assert.deepStrictEqual(
          (yield* registry.snapshot).extensions.map((ext) => ext.name),
          ['a', 'b'],
        )
        assert.strictEqual(old.extensions[0], a)
        yield* registry.uninstall('a')
        yield* registry.install([a])
        assert.deepStrictEqual(
          (yield* registry.snapshot).extensions.map((ext) => ext.name),
          ['b', 'a'],
        )
      }),
  )
  it.effect('validates all candidate code before publishing any change', () =>
    Effect.gen(function* () {
      const registry = yield* Registry.make([{ name: 'a' }])
      const before = yield* registry.snapshot
      const result = yield* Effect.exit(
        registry.install([
          { name: 'b' },
          { name: 'c', sections: [section('instructions', 'invalid')] },
        ]),
      )
      assert.strictEqual(Exit.isFailure(result), true)
      assert.strictEqual(yield* registry.snapshot, before)
      assert.strictEqual(
        Exit.isFailure(
          yield* Effect.exit(
            registry.install([
              { name: 'b', sections: [section('same', '1'), section('same', '2')] },
            ]),
          ),
        ),
        true,
      )
    }),
  )
  it.effect(
    'invalid overwritten batch members reject atomically; valid same-name last replacement retains position',
    () =>
      Effect.gen(function* () {
        const registry = yield* Registry.make([{ name: 'a' }, { name: 'b' }])
        const before = yield* registry.snapshot
        const invalid = { name: 'a', sections: [section('instructions', 'invalid')] }
        const valid = { name: 'a', sections: [section('rules', 'last')] }
        assert.strictEqual(
          Exit.isFailure(yield* Effect.exit(registry.install([invalid, valid]))),
          true,
        )
        assert.strictEqual(yield* registry.snapshot, before)
        assert.strictEqual(
          Exit.isFailure(yield* Effect.exit(Registry.make([invalid, valid]))),
          true,
        )
        yield* registry.install([{ name: 'a', sections: [section('rules', 'first')] }, valid])
        const after = yield* registry.snapshot
        assert.strictEqual(after.extensions[0], valid)
        assert.deepStrictEqual(
          after.extensions.map((extension) => extension.name),
          ['a', 'b'],
        )
        assert.strictEqual(after.revision, 1)
        const initial = yield* Registry.make([{ name: 'a' }, valid])
        assert.strictEqual((yield* initial.snapshot).extensions[0], valid)
      }),
  )
  it.effect(
    'native Stream publications only actual changes; current revision is initial snapshot',
    () =>
      Effect.gen(function* () {
        const registry = yield* Registry.make()
        const events = yield* registry.changes.pipe(
          Stream.take(2),
          Stream.runCollect,
          Effect.forkChild,
        )
        yield* Effect.yieldNow
        yield* registry.install([])
        yield* registry.uninstall('missing')
        yield* registry.install([{ name: 'a' }])
        assert.deepStrictEqual(
          (yield* Fiber.join(events)).map((snap) => snap.revision),
          [0, 1],
        )
      }),
  )
  it.effect(
    'later overrides retain first position, wrappers run before filtering and tool name order filters',
    () =>
      Effect.gen(function* () {
        const bound = yield* tools
        const one = bound[0]
        assert.isDefined(one)
        if (one === undefined) return
        const wrapped = yield* Ref.make(0)
        const registry = yield* Registry.make([
          { name: 'first', tools: [one], sections: [section('one', 'old'), section('two', 'two')] },
          {
            name: 'later',
            sections: [section('one', 'new')],
            toolWraps: [
              {
                name: 'test',
                wrap: (tool) => Ref.update(wrapped, (n) => n + 1).pipe(Effect.as(tool)),
              },
            ],
          },
        ])
        const agent = yield* Registry.resolve(
          yield* registry.snapshot,
          { tools: [] },
          yield* Agent.settings(),
        )
        assert.strictEqual(yield* Ref.get(wrapped), 1)
        assert.strictEqual(agent.tools.length, 0)
        assert.deepStrictEqual(
          agent.sections.map((value) => value.key),
          ['one', 'two'],
        )
        const rendered = yield* Registry.render(
          agent,
          { head: undefined, entries: [], contributions: [], messages: [] },
          new Map(),
        )
        assert.strictEqual(rendered.get('one'), '<one>\nnew\n</one>')
      }).pipe(Effect.provideService(Invocation, quiet)),
  )
  it.effect('throwing/renaming wrappers drop targets and later wraps cannot resurrect them', () =>
    Effect.gen(function* () {
      const bound = yield* tools
      const reports = yield* Ref.make<ReadonlyArray<unknown>>([])
      const called = yield* Ref.make(0)
      const registry = yield* Registry.make([
        { name: 'base', tools: bound },
        {
          name: 'bad',
          toolWraps: [
            {
              name: 'test',
              wrap: () =>
                Effect.fail(
                  new HookError({ reason: new HookFailure({ message: 'wrap failure' }) }),
                ),
            },
          ],
        },
        {
          name: 'later',
          toolWraps: [
            {
              name: 'test',
              wrap: (value) => Ref.update(called, (n) => n + 1).pipe(Effect.as(value)),
            },
          ],
        },
      ])
      const agent = yield* Registry.resolve(
        yield* registry.snapshot,
        {},
        yield* Agent.settings(),
      ).pipe(
        Effect.provideService(Invocation, {
          ...quiet,
          report: (cause) => Ref.update(reports, (old) => [...old, cause]),
        }),
      )
      assert.strictEqual(agent.tools.length, 0)
      assert.strictEqual(yield* Ref.get(called), 0)
      assert.strictEqual((yield* Ref.get(reports)).length, 1)
    }),
  )
  it.effect('selection edits host default and missing extension returns when later installed', () =>
    Effect.gen(function* () {
      const registry = yield* Registry.make([{ name: 'a' }, { name: 'b' }])
      const state: Agent.State = { extensions: { add: ['missing'], remove: ['a'] } }
      assert.deepStrictEqual(
        (yield* Registry.resolve(
          yield* registry.snapshot,
          state,
          yield* Agent.settings({ extensions: ['a'] }),
        )).extensions,
        [],
      )
      yield* registry.install([{ name: 'missing' }])
      assert.deepStrictEqual(
        (yield* Registry.resolve(
          yield* registry.snapshot,
          state,
          yield* Agent.settings({ extensions: ['a'] }),
        )).extensions.map((extension) => extension.name),
        ['missing'],
      )
    }).pipe(Effect.provideService(Invocation, quiet)),
  )
  it.effect(
    'failed section keeps last shown value, explicit undefined omits, instructions last normally tagged',
    () =>
      Effect.gen(function* () {
        const registry = yield* Registry.make([
          {
            name: 'a',
            sections: [
              {
                key: 'broken',
                render: () =>
                  Effect.fail(new HookError({ reason: new HookFailure({ message: 'error' }) })),
              },
              { key: 'omit', render: () => Effect.as(Effect.void, undefined) },
            ],
          },
        ])
        const agent = yield* Registry.resolve(
          yield* registry.snapshot,
          { instructions: 'do it' },
          yield* Agent.settings(),
        )
        const rendered = yield* Registry.render(
          agent,
          { head: undefined, entries: [], contributions: [], messages: [] },
          new Map([
            ['broken', 'keep'],
            ['omit', 'delete'],
          ]),
        )
        assert.deepStrictEqual(
          [...rendered],
          [
            ['broken', 'keep'],
            ['instructions', '<instructions>\ndo it\n</instructions>'],
          ],
        )
      }).pipe(Effect.provideService(Invocation, quiet)),
  )
  it.effect(
    'hooks chain replacements, bind receiver and first decision wins after reported errors',
    () =>
      Effect.gen(function* () {
        const marker = {
          flag: true,
          beforeRequest(this: { flag: boolean }) {
            assert.strictEqual(this.flag, true)
            return Effect.succeed(AiPrompt.make('replacement'))
          },
        }
        const prompt = yield* Hook.beforeRequest(
          [
            {
              beforeRequest: () =>
                Effect.fail(new HookError({ reason: new HookFailure({ message: 'ignored' }) })),
            },
            marker,
            { beforeRequest: () => Effect.as(Effect.void, undefined) },
          ],
          AiPrompt.make('original'),
        )
        assert.deepStrictEqual(prompt.content, AiPrompt.make('replacement').content)
        const decision = yield* Hook.beforeCompact(
          [
            {
              beforeCompact: () =>
                Effect.fail(new HookError({ reason: new HookFailure({ message: 'ignored' }) })),
            },
            { beforeCompact: () => Effect.succeed({ decline: true }) },
            { beforeCompact: () => Effect.die('must not run') },
          ],
          {
            reason: 'manual',
            firstKept: entryId(1),
            view: { head: undefined, entries: [], contributions: [], messages: [] },
          },
        )
        assert.deepStrictEqual(decision, { decline: true })
      }).pipe(Effect.provideService(Invocation, quiet)),
  )
  it.effect('hook interruption propagates instead of being reported and omitted', () =>
    Effect.gen(function* () {
      const reports = yield* Ref.make(0)
      const outcome = yield* Effect.exit(
        Hook.beforeRequest([{ beforeRequest: () => Effect.interrupt }], AiPrompt.empty).pipe(
          Effect.provideService(Invocation, {
            ...quiet,
            report: () => Ref.update(reports, (n) => n + 1),
          }),
        ),
      )
      assert.strictEqual(Exit.isFailure(outcome), true)
      assert.strictEqual(yield* Ref.get(reports), 0)
    }),
  )
})
