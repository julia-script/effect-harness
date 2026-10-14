import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Hook from '../src/Hook.js'
import * as Search from '../src/ToolSearch.js'
import { execution } from './fixtures/Registry.js'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as LanguageModel from 'effect/ai/LanguageModel'
import * as HarnessRuntime from '../src/HarnessRuntime.js'
import * as Registry from '../src/Registry.js'
import * as Session from '../src/Session.js'
import * as Storage from '../src/Storage.js'
import * as Tool from '../src/Tool.js'
import * as ToolInventory from '../src/ToolInventory.js'
import * as Toolkit from '../src/Toolkit.js'
import * as AgentState from '../src/internal/AgentState.js'
import * as ScriptedStream from './fixtures/ScriptedStream.js'

const finish = {
  type: 'finish' as const,
  reason: 'tool-calls' as const,
  usage: {
    inputTokens: { total: 1, uncached: 1, cacheRead: undefined, cacheWrite: undefined },
    outputTokens: { total: 1, text: 1, reasoning: undefined },
  },
  response: undefined,
}

describe('deferred tools', () => {
  it.effect('rejects a model that guesses an unoffered deferred name', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const toolkit = Toolkit.make(
          Tool.make('secret', { success: Schema.String }).annotate(Tool.Deferred, true),
        )
        const registry = yield* Registry.make
        yield* registry.install(
          Registry.provide(
            Registry.toolkit({ id: 'owner', version: 1 }, toolkit),
            toolkit.toLayer({ secret: () => Effect.succeed('secret') }),
          ),
        )
        const lease = yield* registry.acquire
        assert.deepStrictEqual(ToolInventory.inventory(lease.snapshot, 'model'), [])
        assert.strictEqual(ToolInventory.inspect(lease.snapshot, 'secret', 'model')._tag, 'denied')
        assert.strictEqual(ToolInventory.inspect(lease.snapshot, 'secret', 'tools')._tag, 'allowed')
        const call = { id: 'guess', name: 'secret', arguments: {} }
        assert.strictEqual(
          (yield* ToolInventory.invoke(lease, call, 'model', { selection: 'permitted' }).pipe(
            Effect.flip,
          ))._tag,
          'ToolAdmissionDenied',
        )
        assert.strictEqual((yield* ToolInventory.invoke(lease, call, 'tools')).details, 'secret')
      }),
    ).pipe(Effect.provide(execution)),
  )

  it.effect('persists addTools with default model selection', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const toolkit = Toolkit.make(
          Tool.makeResult('load', { success: Schema.String }),
          Tool.make('lookup', { success: Schema.String }).annotate(Tool.Deferred, true),
        )
        const model = yield* ScriptedStream.make({
          generateText: () =>
            Effect.succeed([
              {
                type: 'tool-call',
                id: 'load-1',
                name: 'load',
                params: {},
                providerExecuted: false,
              },
              finish,
            ]),
        })
        const runtime = yield* HarnessRuntime.make({ tools: toolkit }).pipe(
          Effect.provideService(LanguageModel.LanguageModel, model),
          Effect.provide(
            toolkit.toLayer({
              load: () =>
                Effect.succeed({
                  content: [],
                  structuredOutput: 'loaded',
                  control: { addTools: ['lookup'], terminate: true },
                }),
              lookup: () => Effect.succeed('found'),
            }),
          ),
        )
        const root = yield* runtime.backend.root
        const job = yield* runtime.backend.submit({
          conversationId: root,
          draft: { type: 'input', content: 'load' },
        })
        assert.strictEqual((yield* runtime.backend.wait(job.id)).status, 'done')
        const state = Option.getOrThrow(
          yield* Session.snapshot(runtime.session, AgentState.document, AgentState.target(root)),
        ).value
        assert.isDefined(state.modelTools)
        assert.include(JSON.stringify(state.modelTools), 'lookup')
      }),
    ).pipe(Effect.provide(Storage.layerMemory)),
  )
  it.live('rejects a discovered owner replacement between the result and activation', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* Registry.make
        let executions = 0
        const hidden = Toolkit.make(
          Tool.make('lookup', { deferred: true, description: 'calendar', success: Schema.String }),
        )
        const registration = (version: number) =>
          Registry.provide(
            Registry.toolkit({ id: 'calendar', version }, hidden),
            hidden.toLayer({
              lookup: () =>
                Effect.sync(() => {
                  executions++
                  return 'calendar'
                }),
            }),
          )
        yield* registry.install(registration(1))
        const model = yield* ScriptedStream.make({
          generateText: () =>
            Effect.succeed([
              {
                type: 'tool-call',
                id: 'discover',
                name: 'tool_search',
                params: { query: 'calendar' },
                providerExecuted: false,
              },
              finish,
            ]),
        })
        const runtime = yield* HarnessRuntime.make({
          registry,
          tools: Search.toolkit,
          maxTurns: 2,
          hooks: [
            Hook.make({
              event: 'afterTool',
              execute: () => registry.install(registration(2)).pipe(Effect.orDie, Effect.asVoid),
            }),
          ],
        }).pipe(
          Effect.provide(Search.layer),
          Effect.provideService(LanguageModel.LanguageModel, model),
        )
        const root = yield* runtime.backend.root
        const job = yield* runtime.backend.submit({
          conversationId: root,
          draft: { type: 'input', content: 'calendar' },
        })
        const outcome = yield* runtime.backend.wait(job.id)
        assert.deepInclude(outcome, { status: 'unanswered', reason: 'failed' })
        assert.include(JSON.stringify(outcome), 'Tool lookup is unavailable for model selection')
        assert.isUndefined((yield* runtime.backend.agent(root)).modelTools)
        assert.strictEqual(executions, 0)
      }),
    ).pipe(Effect.provide(Storage.layerMemory)),
  )

  it.live(
    'a failed search result cannot broaden offers even when a hook returns valid controls',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          let request = 0
          const offers: string[][] = []
          const hidden = Toolkit.make(
            Tool.make('lookup', {
              deferred: true,
              description: 'calendar',
              success: Schema.String,
            }),
          )
          const model = yield* ScriptedStream.make({
            generateText: ({ tools }) => {
              offers.push(tools.map((tool) => tool.name))
              request++
              return Effect.succeed(
                request === 1
                  ? [
                      {
                        type: 'tool-call' as const,
                        id: 'discover',
                        name: 'tool_search',
                        params: { query: 'calendar' },
                        providerExecuted: false,
                      },
                      finish,
                    ]
                  : [
                      { type: 'text' as const, text: 'done' },
                      { ...finish, reason: 'stop' as const },
                    ],
              )
            },
          })
          const runtime = yield* HarnessRuntime.make({
            tools: Toolkit.merge(Search.toolkit, hidden),
            hooks: [
              Hook.make({
                event: 'afterTool',
                execute: () =>
                  Effect.succeed({
                    content: [],
                    isError: true,
                    diagnostics: [],
                    structuredOutput: {
                      _tag: 'ToolCatalogError',
                      name: 'lookup',
                      message: 'failed',
                    },
                    control: { addTools: ['lookup'] },
                  }),
              }),
            ],
          }).pipe(
            Effect.provide(
              Layer.merge(
                Search.layer,
                hidden.toLayer({ lookup: () => Effect.die('must not execute') }),
              ),
            ),
            Effect.provideService(LanguageModel.LanguageModel, model),
          )
          const root = yield* runtime.backend.root
          const job = yield* runtime.backend.submit({
            conversationId: root,
            draft: { type: 'input', content: 'calendar' },
          })
          assert.strictEqual((yield* runtime.backend.wait(job.id)).status, 'done')
          assert.deepStrictEqual(offers, [['tool_search'], ['tool_search']])
          assert.isUndefined((yield* runtime.backend.agent(root)).modelTools)
        }),
      ).pipe(Effect.provide(Storage.layerMemory)),
  )
  for (const change of [
    { modelTools: { mode: 'explicit' as const, names: [] } },
    { tools: ['tool_search'] },
  ])
    it.live(
      `an in-flight result cannot override later configuration ${JSON.stringify(change)}`,
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            let request = 0
            let configure: Effect.Effect<void, import('../src/HarnessError.js').HarnessError> =
              Effect.void
            const hidden = Toolkit.make(
              Tool.make('lookup', {
                deferred: true,
                description: 'calendar',
                success: Schema.String,
              }),
            )
            const model = yield* ScriptedStream.make({
              generateText: () =>
                Effect.succeed(
                  ++request === 1
                    ? [
                        {
                          type: 'tool-call' as const,
                          id: 'discover',
                          name: 'tool_search',
                          params: { query: 'calendar' },
                          providerExecuted: false,
                        },
                        finish,
                      ]
                    : [
                        { type: 'text' as const, text: 'done' },
                        { ...finish, reason: 'stop' as const },
                      ],
                ),
            })
            const runtime = yield* HarnessRuntime.make({
              tools: Toolkit.merge(Search.toolkit, hidden),
              hooks: [Hook.make({ event: 'afterTools', execute: () => configure })],
            }).pipe(
              Effect.provide(
                Layer.merge(
                  Search.layer,
                  hidden.toLayer({ lookup: () => Effect.die('must not execute') }),
                ),
              ),
              Effect.provideService(LanguageModel.LanguageModel, model),
            )
            const root = yield* runtime.backend.root
            configure = runtime.backend.configure({ conversationId: root, change })
            const job = yield* runtime.backend.submit({
              conversationId: root,
              draft: { type: 'input', content: 'calendar' },
            })
            assert.deepInclude(yield* runtime.backend.wait(job.id), {
              status: 'unanswered',
              reason: 'failed',
            })
            assert.deepStrictEqual(yield* runtime.backend.agent(root), change)
          }),
        ).pipe(Effect.provide(Storage.layerMemory)),
    )
})
