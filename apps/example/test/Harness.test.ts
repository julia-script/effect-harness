import * as Conversation from 'effect-harness/Conversation'
import * as Harness from 'effect-harness/Harness'
import * as Submission from 'effect-harness/Submission'
import { ConfigProvider, Effect, FileSystem, Layer, Path, Ref, Stream } from 'effect'
import { LanguageModel } from 'effect/ai'
import { BunServices } from '@effect/platform-bun'
import { assert, it } from '@effect/vitest'
import * as Application from '../dist/Application.js'
import * as DemoModel from '../dist/DemoModel.js'
import * as Uppercase from '../dist/Uppercase.js'
import { program, requestId } from '../dist/main.js'

it.live('reopens saved conversation and task outcomes without repeating model or tool work', () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem
      const path = yield* Path.Path
      const folder = yield* fs.makeTempDirectoryScoped({ prefix: 'effect-harness-example-test-' })
      const filename = path.join(folder, 'example.sqlite')
      const modelCalls = yield* Ref.make(0)
      const toolCalls = yield* Ref.make(0)
      const model = Layer.effect(
        LanguageModel.LanguageModel,
        LanguageModel.make({
          ...DemoModel.provider,
          streamText: (options) =>
            Stream.unwrap(
              Ref.update(modelCalls, (n) => n + 1).pipe(
                Effect.as(DemoModel.provider.streamText(options)),
              ),
            ),
        }),
      )
      const handlers = Uppercase.toolkit.toLayer({
        uppercase: Effect.fn(function* (params) {
          yield* Ref.update(toolCalls, (n) => n + 1)
          return yield* Uppercase.handle(params)
        }),
      })
      const runtime = Application.layerNoDeps.pipe(
        Layer.provide(
          Layer.merge(
            DemoModel.layerCatalogue.pipe(Layer.provide(model)),
            Uppercase.layerRegistry.pipe(Layer.provide(handlers)),
          ),
        ),
        Layer.provide(
          Layer.merge(
            BunServices.layer,
            ConfigProvider.layer(ConfigProvider.fromUnknown({ EXAMPLE_DB: filename })),
          ),
        ),
      )
      const first = yield* Effect.gen(function* () {
        const result = yield* program
        assert.strictEqual(result.greeting, 'Hello, Effect')
        assert.strictEqual(result.result._tag, 'InputDone')
        assert.strictEqual(result.text, 'HELLO')
        const harness = yield* Harness.Harness
        const root = yield* harness.root
        const replay = yield* Conversation.submit(root, 'uppercase hello', { requestId })
        assert.strictEqual((yield* Submission.wait(replay)).id, result.result.id)
        assert.strictEqual(yield* Ref.get(modelCalls), 2)
        assert.strictEqual(yield* Ref.get(toolCalls), 1)
        return result
      }).pipe(Effect.provide(runtime))
      const reopened = yield* program.pipe(Effect.provide(runtime))
      assert.strictEqual(reopened.greetingId, first.greetingId)
      assert.strictEqual(reopened.result.id, first.result.id)
      assert.strictEqual(reopened.text, first.text)
      assert.strictEqual(yield* Ref.get(modelCalls), 2)
      assert.strictEqual(yield* Ref.get(toolCalls), 1)
    }).pipe(Effect.provide(BunServices.layer)),
  ),
)
