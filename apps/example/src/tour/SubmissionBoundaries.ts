import * as ScriptedStream from '../ScriptedStream.js'
/** Saved input/write IDs resume after a cold reopen; reset placement is a passive write. */
import { NodeServices } from '@effect/platform-node'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Scope from 'effect/Scope'
import * as LanguageModel from 'effect/ai/LanguageModel'
import * as Conversation from 'effect-harness/Conversation'
import * as Harness from 'effect-harness/Harness'
import { HarnessBackend } from 'effect-harness/HarnessBackend'
import * as HarnessRuntime from 'effect-harness/HarnessRuntime'
import * as Storage from 'effect-harness/Storage'
import * as Submission from 'effect-harness/Submission'

export const program = Effect.scoped(
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem
    const directory = yield* fs.makeTempDirectoryScoped()
    const disk = Storage.layerJsonl({ filePath: `${directory}/submissions.jsonl` })
    const model = yield* ScriptedStream.make({
      generateText: () =>
        Effect.succeed([
          { type: 'text', text: 'continued' },
          {
            type: 'finish',
            reason: 'stop',
            usage: {
              inputTokens: { total: 1, uncached: 1, cacheRead: undefined, cacheWrite: undefined },
              outputTokens: { total: 1, text: 1, reasoning: undefined },
            },
            response: undefined,
          },
        ]),
    })
    const open = Effect.gen(function* () {
      const storage = yield* Layer.build(Layer.fresh(disk))
      const runtime = yield* HarnessRuntime.make().pipe(
        Effect.provideContext(storage),
        Effect.provideService(LanguageModel.LanguageModel, model),
      )
      return yield* Harness.make.pipe(Effect.provideService(HarnessBackend, runtime.backend))
    })
    const firstScope = yield* Scope.make()
    yield* Effect.addFinalizer(() => Scope.close(firstScope, Exit.void))
    const first = yield* open.pipe(Effect.provideService(Scope.Scope, firstScope))
    const conversation = yield* first.root
    const reset = yield* Conversation.reset(conversation, {
      handoff: 'Continue from here',
      requestId: 'reset',
    })
    const input = yield* Conversation.submit(conversation, {
      type: 'input',
      content: 'Next step',
      requestId: 'next',
    })
    const saved = { reset: reset.id, input: input.id }
    yield* Scope.close(firstScope, Exit.void)
    const reopened = yield* open
    // Acquisition is read-only. wait resumes the single runtime scheduler automatically.
    const resetReceipt = yield* Submission.wait(yield* reopened.submission(saved.reset))
    const inputReceipt = yield* Submission.wait(yield* reopened.submission(saved.input))
    return { resetReceipt, inputReceipt }
  }),
).pipe(Effect.provide(NodeServices.layer))
