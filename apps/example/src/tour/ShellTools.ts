import * as NodeChildProcessSpawner from '@effect/platform-node/NodeChildProcessSpawner'
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
import * as ToolSpill from 'effect-harness/ToolSpill'
import * as Toolkit from 'effect-harness/Toolkit'
import * as ToolkitFileSystem from 'effect-harness/ToolkitFileSystem'
import * as ToolkitProcess from 'effect-harness/ToolkitProcess'
import * as Bash from 'effect-harness/coding/Bash'
import * as PowerShell from 'effect-harness/coding/PowerShell'
import * as ShellProcess from 'effect-harness/coding/ShellProcess'

// After the integration build: node apps/example/dist/tour/ShellTools.js
// Ready-made handlers; the host owns persistent spill cleanup separately from calls.
const files = Layer.merge(NodeFileSystem.layer, NodePath.layer)
const native = NodeChildProcessSpawner.layer.pipe(Layer.provideMerge(files))
const tools = Toolkit.merge(Bash.toolkit, PowerShell.toolkit)
const binding = (root: string) => {
  const bound = Layer.merge(ToolkitFileSystem.layer({ root }), ToolkitProcess.layer({ root })).pipe(
    Layer.provide(native),
  )
  const services = Layer.merge(
    ShellProcess.layerNative,
    ToolSpill.layer({
      provider: 'example-shell',
      directory: `${root}/spills`,
      lifetime: 'toolkit',
    }),
  ).pipe(Layer.provideMerge(bound))
  return Layer.merge(
    Bash.layer({
      programs: ['/bin/bash'],
      retention: { retain: 'tail', maxBytes: 64, maxLines: 2 },
    }),
    PowerShell.layer(),
  ).pipe(Layer.provideMerge(services))
}
NodeRuntime.runMain(
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem
      const root = yield* fs.realPath(yield* fs.makeTempDirectoryScoped())
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
                  id: 'bash-demo',
                  name: 'bash',
                  params: { command: 'printf "%10000s" x; printf "\\nHello from Bash\\n"' },
                  providerExecuted: false,
                },
                { ...finish, reason: 'tool-calls' as const },
              ]
            if (step === 2)
              return [
                {
                  type: 'tool-call' as const,
                  id: 'powershell-demo',
                  name: 'powershell',
                  params: { command: 'Write-Output "Hello from PowerShell"' },
                  providerExecuted: false,
                },
                { ...finish, reason: 'tool-calls' as const },
              ]
            return [
              {
                type: 'text' as const,
                text: 'Shell tool receipts saved, including any explicit unavailable-runtime diagnostic.',
              },
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
        draft: { type: 'input', content: 'Demonstrate ready-made shell tools.' },
      })
      const settled = yield* runtime.backend.wait(job.id)
      if (settled.status !== 'done')
        return yield* Effect.die(`Shell example did not finish: ${settled.status}`)
      const entries = yield* Session.scanEntries(runtime.session, { conversationId }).pipe(
        Stream.runCollect,
      )
      for (const entry of entries.filter((entry) => entry.kind === 'tool.result'))
        yield* Effect.log(entry.data)
    }),
  ).pipe(Effect.provide(files)),
)
