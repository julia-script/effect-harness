import { Submission } from 'effect-harness/durable/workflow'
import { ConfigProvider, Effect, FileSystem, Layer, Option, Path, Ref, Stream } from 'effect'
import { LanguageModel } from 'effect/ai'
import { BunServices } from '@effect/platform-bun'
import { assert, it } from '@effect/vitest'

import * as Application from '../dist/Application.js'
import * as DemoModel from '../dist/DemoModel.js'
import * as Uppercase from '../dist/Uppercase.js'
import { input, program } from '../dist/main.js'

it.live(
  'executes, polls, resumes and reopens native Workflows without repeating model/tool work',
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem
      const path = yield* Path.Path
      const directory = yield* fs.makeTempDirectoryScoped({
        prefix: 'effect-harness-workflow-test-',
      })
      const filename = path.join(directory, 'example.sqlite')
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
        uppercase: Effect.fnUntraced(function* (params) {
          yield* Ref.update(toolCalls, (n) => n + 1)
          return yield* Uppercase.handle(params)
        }),
      })
      const catalogue = DemoModel.layerCatalogue.pipe(Layer.provide(model))
      const registry = Uppercase.layerRegistry.pipe(Layer.provide(handlers))
      const runtime = Application.layerNoDeps.pipe(
        Layer.provide(Layer.mergeAll(catalogue, registry)),
        Layer.provide(
          Layer.merge(
            BunServices.layer,
            ConfigProvider.layer(ConfigProvider.fromUnknown({ EXAMPLE_DB: filename })),
          ),
        ),
      )

      const first = yield* Effect.gen(function* () {
        const first = yield* program
        assert.strictEqual(first.greeting, 'Hello, Effect')
        assert.strictEqual(first.result._tag, 'InputDone')
        assert.strictEqual(first.text, 'HELLO')
        assert.strictEqual(yield* Ref.get(modelCalls), 2)
        assert.strictEqual(yield* Ref.get(toolCalls), 1)

        const executionId = yield* Submission.Submission.execute(input, { discard: true })
        const polled = yield* Submission.Submission.poll(executionId)
        assert.isTrue(Option.isSome(polled))
        assert.strictEqual(Option.getOrThrow(polled)._tag, 'Complete')
        yield* Submission.Submission.resume(executionId)
        const replayed = yield* Submission.Submission.execute(input)
        assert.strictEqual(replayed.id, first.result.id)
        assert.strictEqual(yield* Ref.get(modelCalls), 2)
        assert.strictEqual(yield* Ref.get(toolCalls), 1)
        return first
      }).pipe(Effect.provide(runtime))

      // The previous Layer scope has closed its Session, engine and SQL client.
      // A fresh acquisition recovers the results from the same database.
      const reopened = yield* program.pipe(Effect.provide(runtime))
      assert.strictEqual(reopened.greeting, first.greeting)
      assert.strictEqual(reopened.result.id, first.result.id)
      assert.strictEqual(reopened.text, first.text)
      assert.strictEqual(yield* Ref.get(modelCalls), 2)
      assert.strictEqual(yield* Ref.get(toolCalls), 1)
    }).pipe(Effect.provide(BunServices.layer)),
)
