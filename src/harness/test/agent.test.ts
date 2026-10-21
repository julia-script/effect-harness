/* oxlint-disable max-lines-per-function, effecttsgo/any-unknown-in-error-context -- describe blocks; sections may fail with anything */
/**
 * Agent resolution over an `Extensions` snapshot: selection, same-name replacement, wrappers, the
 * tool filter, instructions, and config. Ported in spirit from pi's `agent.test.ts`.
 */
import { describe as group, expect, test } from 'bun:test'
import { Effect, Layer, Option, Schema } from 'effect'
import { Registry } from '../../task/index.ts'
import { Tool, Toolkit } from '../../tool/index.ts'
import * as Agent from '../Agent.ts'
import type * as AgentState from '../../protocol/AgentState.ts'
import * as Extension from '../Extension.ts'
import * as Extensions from '../Extensions.ts'
import * as Settings from '../Settings.ts'

const Echo = Tool.make('echo', {
  description: 'Echo',
  parameters: Schema.Struct({ text: Schema.String }),
  success: Schema.String,
})
const echoes = Toolkit.make(Echo)
const LoudEcho = Tool.make('echo', {
  description: 'Echo, loudly',
  parameters: Schema.Struct({ text: Schema.String }),
  success: Schema.String,
})
const loud = Toolkit.make(LoudEcho)

const section = (key: string, text: string) => Extension.section(key, () => Effect.succeed(text))

const registries = Effect.gen(function* () {
  const registry = yield* Registry.make()
  return yield* Extensions.make.pipe(Effect.provideService(Registry.Registry, registry))
})

const implementations = Effect.gen(function* () {
  const quiet = yield* Layer.build(echoes.toLayer({ echo: ({ text }) => Effect.succeed(text) }))
  const loudly = yield* Layer.build(
    loud.toLayer({ echo: ({ text }) => Effect.succeed(text.toUpperCase()) }),
  )
  return [quiet, loudly] as const
})

/** The agent of `state` over two extensions with one tool name and two section keys. */
const resolved = (state?: AgentState.State, settings: Settings.Input = {}) =>
  Effect.scoped(
    Effect.gen(function* () {
      const extensions = yield* registries
      const [quiet, loudly] = yield* implementations
      const first = Extension.make('first', {
        toolkit: echoes,
        sections: [section('a', 'first a'), section('b', 'first b')],
      })
      const second = Extension.make('second', {
        toolkit: loud,
        sections: [section('a', 'second a')],
        wraps: [
          Extension.wrapSection('b', (each) => ({
            ...each,
            render: (input) => Effect.map(each.render(input), (text) => `${text}!`),
          })),
        ],
      })
      yield* Effect.provideContext(extensions.install(first), quiet)
      yield* Effect.provideContext(extensions.install(second), loudly)
      const agent = yield* Agent.resolve(
        state,
        yield* extensions.snapshot,
        Settings.resolve(settings),
      )
      return { agent, view: Agent.view(agent) }
    }),
  )

group('Agent.resolve', () => {
  test('selects every installed extension, later tools and sections replacing in place', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const { agent, view } = yield* resolved()
        expect(view).toEqual({
          reasoning: 'off',
          extensions: ['first', 'second'],
          tools: ['echo'],
          sections: ['a', 'b'],
        })
        expect(agent.tools[0]?.tool.description).toBe('Echo, loudly')
      }),
    ))

  test('edits the selection, filters tools, and appends instructions last', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const { view: only } = yield* resolved({ extensions: ['first'] })
        expect(only.sections).toEqual(['a', 'b'])
        const { view: removed } = yield* resolved({
          extensions: { remove: ['second'] },
          tools: { remove: ['echo'] },
          instructions: 'Be brief.',
          model: { provider: 'p', modelId: 'm' },
        })
        expect(removed).toEqual({
          model: { provider: 'p', modelId: 'm' },
          reasoning: 'off',
          extensions: ['first'],
          tools: [],
          sections: ['a', 'b', 'instructions'],
          instructions: 'Be brief.',
        })
        const { view: defaults } = yield* resolved(
          {},
          {
            extensions: ['second'],
            reasoning: 'high',
          },
        )
        expect(defaults).toMatchObject({ extensions: ['second'], reasoning: 'high' })
      }),
    ))

  test('drops a tool its wrapper renames, and uses stored config over its extension', () =>
    Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const extensions = yield* registries
          const [quiet] = yield* implementations
          const renaming = Extension.make('renaming', {
            wraps: [
              Extension.wrapTool(Echo, (runner) => ({
                ...runner,
                tool: { ...runner.tool, name: 'other' },
              })),
            ],
          })
          const timing = Extension.make('timing', {
            wraps: [
              Extension.wrapTool('echo', (runner) => ({
                ...runner,
                tool: { ...runner.tool, description: 'Timed' },
              })),
            ],
          })
          yield* Effect.provideContext(
            extensions.install(Extension.make('first', { toolkit: echoes })),
            quiet,
          )
          yield* extensions.install(renaming)
          yield* extensions.install(timing)
          const snapshot = yield* extensions.snapshot
          const settings = Settings.resolve()
          const dropped = yield* Agent.resolve(
            { extensions: ['first', 'renaming'] },
            snapshot,
            settings,
          )
          expect(dropped.tools).toEqual([])
          const timed = yield* Agent.resolve(
            { extensions: ['first', 'timing'], config: { echo: { root: 'x' } } },
            snapshot,
            settings,
          )
          expect(timed.tools.map(({ tool, config }) => [tool.description, config])).toEqual([
            ['Timed', { root: 'x' }],
          ])
          expect(Option.isSome(timed.tools[0]?.runner ?? Option.none())).toBe(true)
        }),
      ),
    ))
})
