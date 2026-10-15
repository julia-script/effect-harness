import * as ScriptedStream from './fixtures/ScriptedStream.js'
import { NodeServices } from '@effect/platform-node'
import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import * as Scope from 'effect/Scope'
import * as Exit from 'effect/Exit'
import * as Stream from 'effect/Stream'
import * as Option from 'effect/Option'
import * as LanguageModel from 'effect/ai/LanguageModel'
import * as Prompt from 'effect/ai/Prompt'
import * as HarnessRuntime from '../src/HarnessRuntime.js'
import * as Session from '../src/Session.js'
import * as Storage from '../src/Storage.js'
import * as Tool from '../src/Tool.js'
import * as ToolResult from '../src/ToolResult.js'
import * as Toolkit from '../src/Toolkit.js'
import type * as Agent from '../src/Agent.js'
import * as Transaction from '../src/Transaction.js'
import * as AgentState from '../src/internal/AgentState.js'
import * as ResultEntry from '../src/internal/ToolResultEntry.js'

const finish = {
  type: 'finish' as const,
  reason: 'stop' as const,
  usage: {
    inputTokens: { total: 1, uncached: 1, cacheRead: undefined, cacheWrite: undefined },
    outputTokens: { total: 1, text: 1, reasoning: undefined },
  },
  response: undefined,
}

describe('model-call control receipt compatibility', () => {
  it.effect(
    'conversation forks retain agent inventories and stable selection at the fork entry',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const session = yield* Session.make()
          const state = {
            tools: ['work', 'lookup'],
            modelTools: { mode: 'explicit', names: ['work'] },
            toolkits: {
              inherit: false,
              registrations: ['owner'],
              disabled: ['unused'],
              bindings: { work: { binding: 'owner', version: 1 } },
            },
          } satisfies Agent.State
          const { root, marker } = yield* Session.commit(session, (tx) =>
            Effect.gen(function* () {
              const root = yield* Transaction.ensureRoot(tx)
              yield* Transaction.ensureDocument(tx, AgentState.document, AgentState.target(root.id))
              yield* Transaction.setDocument(
                tx,
                AgentState.document,
                AgentState.target(root.id),
                state,
              )
              const marker = yield* Transaction.appendEntry(tx, root.id, { kind: 'selection.fork' })
              return { root, marker }
            }),
          )
          yield* Session.commit(session, (tx) =>
            Transaction.setDocument(tx, AgentState.document, AgentState.target(root.id), {
              tools: [],
              modelTools: { mode: 'explicit', names: [] },
              toolkits: { inherit: true, registrations: [] },
            }),
          )
          const fork = yield* Session.commit(session, (tx) =>
            Transaction.forkConversation(tx, root.id, marker.id, {
              ownership: { _tag: 'ownerless' },
            }),
          )
          const snapshot = Option.getOrThrow(
            yield* Session.snapshot(session, AgentState.document, AgentState.target(fork.id)),
          )
          assert.deepStrictEqual(snapshot.value, state)
          const parent = Option.getOrThrow(
            yield* Session.snapshot(session, AgentState.document, AgentState.target(root.id)),
          )
          assert.deepStrictEqual(parent.value.tools, [])
        }).pipe(Effect.provide(Storage.layerMemory)),
      ),
  )
  it.effect(
    'activated terminate and handoff survive native JSONL reopen and stay out of provider content',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem
          const directory = yield* fs.makeTempDirectoryScoped()
          const filePath = `${directory}/controls.jsonl`
          const firstScope = yield* Scope.make()
          yield* Effect.addFinalizer(() => Scope.close(firstScope, Exit.void))
          const storage = yield* Layer.buildWithScope(Storage.layerJsonl({ filePath }), firstScope)
          let requests = 0
          let modelResult: unknown
          const model = yield* ScriptedStream.make({
            generateText: ({ prompt }) => {
              requests++
              const result = prompt.content
                .flatMap((message) => (message.role === 'tool' ? message.content : []))
                .find((part) => part.type === 'tool-result')
              if (result !== undefined) {
                modelResult = result.result
                return Effect.succeed([{ type: 'text', text: 'done' }, finish])
              }
              return Effect.succeed([
                { type: 'tool-call', id: 'one', name: 'work', params: {}, providerExecuted: false },
                { ...finish, reason: 'tool-calls' as const },
              ])
            },
          })
          const toolkit = Toolkit.make(
            Tool.makeResult('work', { success: Schema.String, resultValidation: 'strict' }),
          )
          const result = {
            content: [Prompt.textPart({ text: 'public' })],
            structuredOutput: 'programmatic-private',
            details: { panel: 'ui-private' },
            control: { terminate: true as const, handoff: 'next-private', addTools: ['work'] },
          }
          const runtime = yield* HarnessRuntime.make({ tools: toolkit }).pipe(
            Effect.provide(toolkit.toLayer({ work: () => Effect.succeed(result) })),
            Effect.provideService(LanguageModel.LanguageModel, model),
            Effect.provideContext(storage),
            Effect.provideService(Scope.Scope, firstScope),
          )
          const root = yield* runtime.backend.root
          const submission = yield* runtime.backend.submit({
            conversationId: root,
            draft: { type: 'input', content: 'go' },
          })
          assert.strictEqual((yield* runtime.backend.wait(submission.id)).status, 'done')
          assert.strictEqual(requests, 1)
          const before = yield* Session.scanEntries(runtime.session, { conversationId: root }).pipe(
            Stream.runCollect,
          )
          const receipt = before.find((entry) => entry.kind === 'tool.result')
          const messages = yield* ResultEntry.messages(receipt!)
          assert.deepStrictEqual(messages, [
            Prompt.toolMessage({
              content: [
                Prompt.toolResultPart({
                  id: 'one',
                  name: 'work',
                  providerExecuted: false,
                  isFailure: false,
                  result: 'public',
                }),
              ],
            }),
          ])
          modelResult = messages
            .flatMap((message) => (message.role === 'tool' ? message.content : []))
            .find((part) => part.type === 'tool-result')?.result
          assert.strictEqual(modelResult, 'public')
          assert.notInclude(JSON.stringify(modelResult), 'private')
          assert.deepStrictEqual((yield* ResultEntry.read(receipt!)).control, result.control)
          yield* Scope.close(firstScope, Exit.void)
          const secondStorage = yield* Layer.build(Storage.layerJsonl({ filePath }))
          const session = yield* Session.make().pipe(Effect.provideContext(secondStorage))
          const after = yield* Session.scanEntries(session, { conversationId: root }).pipe(
            Stream.runCollect,
          )
          const persisted = after.find((entry) => entry.kind === 'tool.result')
          const restored = yield* ResultEntry.read(persisted!)
          assert.deepStrictEqual(restored.control, result.control)
          assert.strictEqual(restored.structuredOutput, 'programmatic-private')
          assert.deepStrictEqual(restored.details, { panel: 'ui-private' })
          assert.notProperty(ToolResult.nested(restored, true), 'control')
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
  )
})
