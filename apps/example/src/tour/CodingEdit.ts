import * as NodeFileSystem from '@effect/platform-node/NodeFileSystem'
import * as NodePath from '@effect/platform-node/NodePath'
import * as NodeRuntime from '@effect/platform-node/NodeRuntime'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Stream from 'effect/Stream'
import * as LanguageModel from 'effect/ai/LanguageModel'
import * as HarnessRuntime from 'effect-harness/HarnessRuntime'
import * as Session from 'effect-harness/Session'
import * as Storage from 'effect-harness/Storage'
import * as Toolkit from 'effect-harness/Toolkit'
import * as ToolkitFileSystem from 'effect-harness/ToolkitFileSystem'
import * as Edit from 'effect-harness/coding/Edit'
import * as MutationCoordinator from 'effect-harness/coding/MutationCoordinator'
import * as Write from 'effect-harness/coding/Write'

// After the integration build: node apps/example/dist/tour/CodingEdit.js
// Native POSIX local filesystem supports replacement by same-filesystem rename.
// This demonstrates semantic all-or-none edits, without promising crash durability.
const native = Layer.merge(NodeFileSystem.layer, NodePath.layer)
const tools = Toolkit.merge(Edit.toolkit, Write.toolkit)
const binding = (root: string) => {
  const files = ToolkitFileSystem.layer({ root }).pipe(Layer.provide(native))
  const services = MutationCoordinator.layer.pipe(Layer.provideMerge(files))
  return Layer.merge(Edit.layer({ writeMode: 'rename' }), Write.layer).pipe(
    Layer.provideMerge(services),
  )
}

NodeRuntime.runMain(
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem
      const root = yield* fs.realPath(yield* fs.makeTempDirectoryScoped())
      const before = '\uFEFFoutside “kept”  \r\nunique Ａ “quote”—value  \r\nlast\r\n'
      const after = '\uFEFFoutside “kept”  \r\nunique B "quote"-value\r\nlast\r\n'
      yield* fs.writeFileString(`${root}/file`, before)
      let step = 0
      const finish = {
        type: 'finish' as const,
        reason: 'stop' as const,
        usage: {
          inputTokens: { total: 1, uncached: 1, cacheRead: undefined, cacheWrite: undefined },
          outputTokens: { total: 1, text: 1, reasoning: undefined },
        },
        response: undefined,
      }
      const model = yield* LanguageModel.make({
        generateText: () =>
          Effect.sync(() => {
            step++
            if (step === 1)
              return [
                {
                  type: 'tool-call' as const,
                  id: 'fuzzy-edit',
                  name: 'edit',
                  params: {
                    path: 'file',
                    edits: [
                      { oldText: 'unique A "quote"-value', newText: 'unique B "quote"-value' },
                    ],
                  },
                  providerExecuted: false,
                },
                { ...finish, reason: 'tool-calls' as const },
              ]
            if (step === 2)
              return [
                {
                  type: 'tool-call' as const,
                  id: 'invalid-plan',
                  name: 'edit',
                  params: {
                    path: 'file',
                    edits: [
                      { oldText: 'last', newText: 'must not write' },
                      { oldText: 'missing', newText: 'invalid' },
                    ],
                  },
                  providerExecuted: false,
                },
                { ...finish, reason: 'tool-calls' as const },
              ]
            return [
              { type: 'text' as const, text: 'Edited the file; the invalid plan left it intact.' },
              finish,
            ]
          }),
        streamText: () => Stream.empty,
      })
      const services = yield* Layer.build(Layer.merge(binding(root), Storage.layerMemory))
      const runtime = yield* HarnessRuntime.make({ tools }).pipe(
        Effect.provideContext(services),
        Effect.provideService(LanguageModel.LanguageModel, model),
      )
      const conversationId = yield* runtime.backend.root
      const job = yield* runtime.backend.submit({
        conversationId,
        draft: { type: 'input', content: 'Edit the file and demonstrate failed-plan isolation.' },
      })
      const settled = yield* runtime.backend.wait(job.id)
      if (settled.status !== 'done')
        return yield* Effect.die(`Coding edit example did not finish: ${settled.status}`)
      const saved = yield* Session.scanEntries(runtime.session, { conversationId }).pipe(
        Stream.runCollect,
      )
      const receipts = saved.filter((entry) => entry.kind === 'tool.result')
      if (receipts.length !== 2) return yield* Effect.die('Missing edit receipts')
      for (const entry of receipts) yield* Effect.log(entry.data)
      const actual = yield* fs.readFile(`${root}/file`)
      const expected = new TextEncoder().encode(after)
      if (
        actual.length !== expected.length ||
        actual.some((byte, index) => byte !== expected[index])
      )
        return yield* Effect.die(
          'Coding edit example changed unrelated bytes or partially applied an invalid plan',
        )
    }),
  ).pipe(Effect.provide(native)),
)
