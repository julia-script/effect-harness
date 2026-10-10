import * as ScriptedStream from '../ScriptedStream.js'
import * as NodeFileSystem from '@effect/platform-node/NodeFileSystem'
import * as NodePath from '@effect/platform-node/NodePath'
import * as NodeRuntime from '@effect/platform-node/NodeRuntime'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Stream from 'effect/Stream'
import * as LanguageModel from 'effect/ai/LanguageModel'
import * as FileReader from 'effect-harness/FileReader'
import * as HarnessRuntime from 'effect-harness/HarnessRuntime'
import * as Session from 'effect-harness/Session'
import * as Storage from 'effect-harness/Storage'
import * as Toolkit from 'effect-harness/Toolkit'
import * as ToolkitFileSystem from 'effect-harness/ToolkitFileSystem'
import * as MutationCoordinator from 'effect-harness/coding/MutationCoordinator'
import * as Read from 'effect-harness/coding/Read'
import * as Write from 'effect-harness/coding/Write'

// After the integration build: node apps/example/dist/tour/CodingFiles.js
// No custom read/write handlers, ambient cwd changes or real provider credentials.
const native = Layer.merge(NodeFileSystem.layer, NodePath.layer)
const tools = Toolkit.merge(Read.toolkit, Write.toolkit)
const binding = (root: string) => {
  const files = ToolkitFileSystem.layer({ root }).pipe(Layer.provide(native))
  const services = Layer.merge(FileReader.layer, MutationCoordinator.layer).pipe(
    Layer.provideMerge(files),
  )
  return Layer.merge(Read.layer(), Write.layer).pipe(Layer.provideMerge(services))
}

NodeRuntime.runMain(
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem
      const temporary = yield* fs.makeTempDirectoryScoped()
      const root = yield* fs.realPath(temporary)
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
      const model = yield* ScriptedStream.make({
        generateText: () =>
          Effect.sync(() => {
            step++
            if (step === 1)
              return [
                {
                  type: 'tool-call' as const,
                  id: 'write-file',
                  name: 'write',
                  params: { path: 'notes/greeting.txt', content: 'Hello from ready-made tools!\n' },
                  providerExecuted: false,
                },
                { ...finish, reason: 'tool-calls' as const },
              ]
            if (step === 2)
              return [
                {
                  type: 'tool-call' as const,
                  id: 'read-file',
                  name: 'read',
                  params: { path: 'notes/greeting.txt' },
                  providerExecuted: false,
                },
                { ...finish, reason: 'tool-calls' as const },
              ]
            return [{ type: 'text' as const, text: 'File created and read.' }, finish]
          }),
      })
      const services = yield* Layer.build(Layer.merge(binding(root), Storage.layerMemory))
      const runtime = yield* HarnessRuntime.make({ tools }).pipe(
        Effect.provideContext(services),
        Effect.provideService(LanguageModel.LanguageModel, model),
      )
      const conversationId = yield* runtime.backend.root
      const job = yield* runtime.backend.submit({
        conversationId,
        draft: { type: 'input', content: 'Write and read a greeting.' },
      })
      const settled = yield* runtime.backend.wait(job.id)
      if (settled.status !== 'done')
        return yield* Effect.die(`Coding example did not finish: ${settled.status}`)
      const saved = yield* Session.scanEntries(runtime.session, { conversationId }).pipe(
        Stream.runCollect,
      )
      for (const entry of saved.filter((entry) => entry.kind === 'tool.result'))
        yield* Effect.log(entry.data)
      if (
        (yield* fs.readFileString(`${root}/notes/greeting.txt`)) !==
        'Hello from ready-made tools!\n'
      )
        return yield* Effect.die('Coding example file mismatch')
    }),
  ).pipe(Effect.provide(native)),
)
