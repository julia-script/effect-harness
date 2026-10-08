import { BunServices } from '@effect/platform-bun'
import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as Observation from 'effect-harness/Observation'
import * as Context from '../dist/tour/Context.js'
import * as Documents from '../dist/tour/Documents.js'

describe('document and context tour', () => {
  it.live(
    'atomically publishes document edits, renders them for the model, and forks their earlier value',
    () =>
      Effect.gen(function* () {
        const result = yield* Documents.run
        assert.deepEqual(result.rootTodos.items, [
          { text: 'Read the article', done: true },
          { text: 'Build the example', done: false },
        ])
        assert.deepEqual(result.forkTodos.items, [{ text: 'Read the article', done: false }])
        assert.deepEqual(result.progress, { stage: 'building', completed: 1 })
        assert.isTrue(result.atomicCommit)
        assert.include(result.managedSection, '[ ] Build the example')
        const changes = yield* Effect.forEach(result.wireChanges, (change) =>
          Schema.decodeEffect(Schema.toCodecJson(Observation.Change))(change),
        )
        assert.strictEqual(changes[0]?._tag, 'snapshot')
        assert.strictEqual(changes[1]?._tag, 'commit')
        const encoded = yield* Schema.encodeEffect(Schema.toCodecJson(Documents.Result))(result)
        assert.deepEqual(
          yield* Schema.decodeEffect(Schema.toCodecJson(Documents.Result))(encoded),
          result,
        )
      }).pipe(Effect.provide(BunServices.layer)),
  )

  it.live('compacts and resets native prompts while retaining searchable historical messages', () =>
    Effect.gen(function* () {
      const result = yield* Context.run
      assert.strictEqual(result.summaryCalls, 1)
      assert.isAbove(result.afterCompactCount, result.beforeCount)
      assert.isAbove(result.afterResetCount, result.afterCompactCount)
      assert.include(result.afterCompactionPrompt, result.summary)
      assert.notInclude(result.afterResetPrompt, 'launch')
      assert.include(result.afterResetPrompt, 'Start a fresh discussion')
      assert.isAtLeast(result.historyMatches.length, 2)
      assert.isTrue(result.historyMatches.every((entry) => entry.text.includes('launch')))
      const encoded = yield* Schema.encodeEffect(Schema.toCodecJson(Context.Result))(result)
      assert.deepEqual(
        yield* Schema.decodeEffect(Schema.toCodecJson(Context.Result))(encoded),
        result,
      )
    }).pipe(Effect.provide(BunServices.layer)),
  )
})
