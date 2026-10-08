import { assert, describe, it } from '@effect/vitest'
import { Effect, Layer, Stdio } from 'effect'
import { Command, CliError } from 'effect/cli'
import { BunServices } from '@effect/platform-bun'
import { command } from '../dist/ReproduceUnknownTool.js'

describe('ReproduceUnknownTool', () => {
  it.effect('parses default and explicit retry before entering the application handler', () =>
    Effect.gen(function* () {
      const seen: Array<boolean> = []
      const parsed = command.pipe(
        Command.withHandler(({ retry }) =>
          Effect.sync(() => {
            seen.push(retry)
          }),
        ),
      )
      const run = Command.runWith(parsed, { version: '0.0.0', renderErrors: false })
      yield* run([])
      yield* run(['--retry'])
      assert.deepStrictEqual(seen, [false, true])
      const error = yield* Effect.flip(run(['--invalid']))
      assert.deepStrictEqual(
        error,
        new CliError.ShowHelp({
          commandPath: ['unknown-tool'],
          errors: [
            new CliError.UnrecognizedOption({
              option: '--invalid',
              suggestions: [],
              command: ['unknown-tool'],
            }),
          ],
        }),
      )
      assert.deepStrictEqual(seen, [false, true])
    }).pipe(Effect.provide(Layer.merge(BunServices.layer, Stdio.layerTest({})))),
  )
})
