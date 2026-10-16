import * as TemporaryStorage from './fixtures/Storage.js'
import { assert, describe, it } from '@effect/vitest'
import { NodeServices } from '@effect/platform-node'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Scope from 'effect/Scope'
import * as Stream from 'effect/Stream'
import * as LanguageModel from 'effect/ai/LanguageModel'
import type * as Agent from '../src/Agent.js'
import * as HarnessRuntime from '../src/HarnessRuntime.js'
import * as Hook from '../src/Hook.js'
import * as Prepared from '../src/PreparedRequest.js'
import * as Session from '../src/Session.js'
import * as Storage from '../src/Storage.js'
import * as Tool from '../src/Tool.js'
import * as Toolkit from '../src/Toolkit.js'
import * as Search from '../src/ToolSearch.js'
import * as ScriptedStream from './fixtures/ScriptedStream.js'

const finish = {
  type: 'finish' as const,
  reason: 'stop' as const,
  usage: {
    inputTokens: { total: 1, uncached: 1, cacheRead: undefined, cacheWrite: undefined },
    outputTokens: { total: 1, text: 1, reasoning: undefined },
  },
  response: undefined,
}
const call = (name: string, params: Schema.JsonObject) => [
  { type: 'tool-call' as const, id: name, name, params, providerExecuted: false },
  { ...finish, reason: 'tool-calls' as const },
]
const deferred = Toolkit.make(
  Tool.make('lookup', {
    description: 'lookup calendar appointments',
    success: Schema.String,
    deferred: true,
  }),
  Tool.make('archive', {
    description: 'archive old emails',
    success: Schema.String,
    deferred: true,
  }),
)
const toolkit = Toolkit.merge(Search.toolkit, deferred)

describe('durable deferred activation', () => {
  for (const ending of ['terminate', 'maxTurns'] as const)
    it.live(`${ending}: immediate tip forks retain activation, including after cold reopen`, () =>
      Effect.scoped(
        Effect.gen(function* () {
          const directory = yield* (yield* FileSystem.FileSystem).makeTempDirectoryScoped()
          const storageLayer = Storage.layerJsonl({ filePath: `${directory}/session.jsonl` })
          let requests = 0
          const model = yield* ScriptedStream.make({
            generateText: () =>
              Effect.sync(() => {
                requests++
                return call('tool_search', { query: 'calendar' })
              }),
          })
          const handlers = Layer.merge(
            Search.layer,
            deferred.toLayer({
              lookup: () => Effect.die('Discovery must not invoke the deferred operation'),
              archive: () => Effect.die('Discovery must not invoke the deferred operation'),
            }),
          )
          const make = () =>
            HarnessRuntime.make({
              tools: toolkit,
              maxTurns: 1,
              hooks:
                ending === 'terminate'
                  ? [
                      Hook.make({
                        event: 'afterTool',
                        execute: ({ result }) =>
                          Effect.succeed({
                            ...result,
                            control: { ...result.control, terminate: true },
                          }),
                      }),
                    ]
                  : [],
            }).pipe(
              Effect.provide(handlers),
              Effect.provideService(LanguageModel.LanguageModel, model),
            )
          const scope = yield* Scope.make()
          yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void))
          const storage = yield* Layer.buildWithScope(Layer.fresh(storageLayer), scope)
          const runtime = yield* make().pipe(
            Effect.provideContext(storage),
            Effect.provideService(Scope.Scope, scope),
          )
          const root = yield* runtime.backend.root
          const job = yield* runtime.backend.submit({
            conversationId: root,
            draft: { type: 'input', content: 'Find my appointment' },
          })
          const outcome = yield* runtime.backend.wait(job.id)
          assert.strictEqual(outcome.status, ending === 'terminate' ? 'done' : 'unanswered')
          assert.strictEqual(requests, 1)
          const selected = (yield* runtime.backend.agent(root)).modelTools!
          assert.deepStrictEqual(selected.names, ['lookup'])
          assert.isDefined(selected.bindings?.lookup)
          const tip = yield* runtime.backend.fork({ conversationId: root })
          assert.deepStrictEqual((yield* runtime.backend.agent(tip)).modelTools, selected)
          const entries = yield* Session.scanEntries(runtime.session, {
            conversationId: root,
          }).pipe(Stream.runCollect)
          const result = entries.find((entry) => entry.kind === 'tool.result')!
          const before = yield* runtime.backend.fork({
            conversationId: root,
            options: { at: result.id },
          })
          assert.isUndefined((yield* runtime.backend.agent(before)).modelTools)
          yield* Scope.close(scope, Exit.void)
          const reopenedStorage = yield* Layer.build(Layer.fresh(storageLayer))
          const reopened = yield* make().pipe(Effect.provideContext(reopenedStorage))
          const reopenedTip = yield* reopened.backend.fork({ conversationId: root })
          assert.deepStrictEqual((yield* reopened.backend.agent(reopenedTip)).modelTools, selected)
          assert.deepStrictEqual((yield* reopened.backend.agent(tip)).modelTools, selected)
          assert.isUndefined((yield* reopened.backend.agent(before)).modelTools)
          assert.strictEqual(requests, 1)
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
    )

  for (const mode of ['default', 'explicit'] as const)
    for (const crash of ['before activation', 'after activation'] as const)
      it.live(
        `${mode}: cold reopen ${crash} loads exactly once and invokes the actual discovered handler`,
        () =>
          Effect.scoped(
            Effect.gen(function* () {
              const directory = yield* (yield* FileSystem.FileSystem).makeTempDirectoryScoped()
              const storageLayer = Storage.layerJsonl({ filePath: `${directory}/session.jsonl` })
              const gate = yield* Deferred.make<void>()
              let reopening = false
              let executions = 0
              const offers: string[][] = []
              const preparations: string[][] = []
              const model = yield* ScriptedStream.make({
                generateText: ({ tools, prompt }) =>
                  Effect.gen(function* () {
                    const names = tools.map((tool) => tool.name)
                    offers.push(names)
                    const prepared = Option.getOrThrow(
                      yield* Effect.serviceOption(Prepared.PreparedRequest),
                    ).current
                    preparations.push(prepared.tools.map((tool) => tool.declaration.name))
                    if (!names.includes('lookup')) return call('tool_search', { query: 'calendar' })
                    if (!reopening && crash === 'after activation') {
                      yield* Deferred.succeed(gate, undefined)
                      return yield* Effect.never
                    }
                    const invoked = prompt.content.some(
                      (message) =>
                        message.role === 'tool' &&
                        message.content.some(
                          (part) => part.type === 'tool-result' && part.name === 'lookup',
                        ),
                    )
                    if (!invoked) return call('lookup', {})
                    return [{ type: 'text' as const, text: 'done' }, finish]
                  }),
              })
              const handlers = Layer.merge(
                Search.layer,
                deferred.toLayer({
                  lookup: () =>
                    Effect.sync(() => {
                      executions++
                      return 'appointment at noon'
                    }),
                  archive: () => Effect.succeed('archived'),
                }),
              )
              const hook = Hook.make({
                event: 'afterTools',
                execute: () =>
                  Effect.gen(function* () {
                    if (!reopening && crash === 'before activation') {
                      yield* Deferred.succeed(gate, undefined)
                      return yield* Effect.never
                    }
                  }),
              })
              const agent: Agent.State =
                mode === 'default'
                  ? {}
                  : { modelTools: { mode: 'explicit', names: ['tool_search'] } }
              const scope = yield* Scope.make()
              yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void))
              const firstStorage = yield* Layer.buildWithScope(Layer.fresh(storageLayer), scope)
              const first = yield* HarnessRuntime.make({
                tools: toolkit,
                agent,
                hooks: [hook],
              }).pipe(
                Effect.provide(handlers),
                Effect.provideService(LanguageModel.LanguageModel, model),
                Effect.provideContext(firstStorage),
                Effect.provideService(Scope.Scope, scope),
              )
              const root = yield* first.backend.root
              const job = yield* first.backend.submit({
                conversationId: root,
                draft: { type: 'input', content: 'Find my appointment' },
              })
              yield* Deferred.await(gate)
              const before = yield* first.backend.agent(root)
              assert.strictEqual(
                before.modelTools?.names.includes('lookup') === true,
                crash === 'after activation',
              )
              assert.strictEqual(executions, 0)
              yield* Scope.close(scope, Exit.void)
              reopening = true
              const reopenedStorage = yield* Layer.build(Layer.fresh(storageLayer))
              const second = yield* HarnessRuntime.make({
                tools: toolkit,
                agent,
                hooks: [hook],
              }).pipe(
                Effect.provide(handlers),
                Effect.provideService(LanguageModel.LanguageModel, model),
                Effect.provideContext(reopenedStorage),
              )
              assert.strictEqual((yield* second.backend.wait(job.id)).status, 'done')
              assert.strictEqual(executions, 1)
              const selected = (yield* second.backend.agent(root)).modelTools!
              assert.strictEqual(selected.mode, mode)
              assert.deepStrictEqual(
                selected.names,
                mode === 'default' ? ['lookup'] : ['tool_search', 'lookup'],
              )
              assert.isDefined(selected.bindings?.lookup)
              assert.deepStrictEqual(offers[0], ['tool_search'])
              assert.isTrue(
                offers
                  .slice(1)
                  .every(
                    (names) => JSON.stringify(names) === JSON.stringify(['tool_search', 'lookup']),
                  ),
              )
              assert.deepStrictEqual(preparations, offers)
              const entries = yield* Session.scanEntries(second.session, {
                conversationId: root,
              }).pipe(Stream.runCollect)
              assert.strictEqual(
                entries.filter(
                  (entry) =>
                    entry.kind === 'tool.result' && JSON.stringify(entry.data).includes('addTools'),
                ).length,
                1,
              )
              assert.strictEqual(entries.filter((entry) => entry.kind === 'tool.result').length, 2)
              assert.strictEqual(
                entries.filter((entry) => entry.kind === 'agent.configure').length,
                1,
              )
              const again = yield* second.backend.submit({
                conversationId: root,
                draft: { type: 'input', content: 'Continue' },
              })
              yield* second.backend.wait(again.id)
              assert.deepStrictEqual((yield* second.backend.agent(root)).modelTools, selected)
            }),
          ).pipe(Effect.provide(NodeServices.layer)),
      )

  it.live(
    'historical forks inherit only prior loads, remain independent, and reset preserves configuration',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          let request = 0
          const model = yield* ScriptedStream.make({
            generateText: () =>
              Effect.succeed(
                [0, 2].includes(request++)
                  ? call('tool_search', { query: request === 1 ? 'calendar' : 'emails' })
                  : [{ type: 'text' as const, text: 'done' }, finish],
              ),
          })
          const runtime = yield* HarnessRuntime.make({ tools: toolkit }).pipe(
            Effect.provide(
              Layer.merge(
                Search.layer,
                deferred.toLayer({
                  lookup: () => Effect.succeed('ok'),
                  archive: () => Effect.succeed('ok'),
                }),
              ),
            ),
            Effect.provideService(LanguageModel.LanguageModel, model),
          )
          const root = yield* runtime.backend.root
          const job = yield* runtime.backend.submit({
            conversationId: root,
            draft: { type: 'input', content: 'calendar' },
          })
          yield* runtime.backend.wait(job.id)
          const entries = yield* Session.scanEntries(runtime.session, {
            conversationId: root,
            order: 'ascending',
          }).pipe(Stream.runCollect)
          const beforeEntry =
            entries.find((entry) => entry.kind === 'tool.call') ??
            entries.find((entry) => entry.kind === 'input')!
          const before = yield* runtime.backend.fork({
            conversationId: root,
            options: { at: beforeEntry.id },
          })
          const after = yield* runtime.backend.fork({
            conversationId: root,
            options: { at: entries.at(-1)!.id },
          })
          assert.isUndefined((yield* runtime.backend.agent(before)).modelTools)
          assert.deepStrictEqual((yield* runtime.backend.agent(after)).modelTools?.names, [
            'lookup',
          ])
          const branchJob = yield* runtime.backend.submit({
            conversationId: after,
            draft: { type: 'input', content: 'emails' },
          })
          yield* runtime.backend.wait(branchJob.id)
          assert.deepStrictEqual((yield* runtime.backend.agent(after)).modelTools?.names, [
            'lookup',
            'archive',
          ])
          assert.deepStrictEqual((yield* runtime.backend.agent(root)).modelTools?.names, ['lookup'])
          const reset = yield* runtime.backend.submit({
            conversationId: after,
            draft: { type: 'write', entry: { kind: 'reset', head: 'self' } },
          })
          yield* runtime.backend.wait(reset.id)
          assert.deepStrictEqual((yield* runtime.backend.agent(after)).modelTools?.names, [
            'lookup',
            'archive',
          ])
          yield* runtime.backend.configure({
            conversationId: after,
            change: { modelTools: { mode: 'explicit', names: ['tool_search'] } },
          })
          assert.deepStrictEqual((yield* runtime.backend.agent(after)).modelTools, {
            mode: 'explicit',
            names: ['tool_search'],
          })
          assert.deepStrictEqual((yield* runtime.backend.agent(root)).modelTools?.names, ['lookup'])
        }),
      ).pipe(Effect.provide(TemporaryStorage.layer)),
  )
})
