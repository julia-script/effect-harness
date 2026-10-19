/* oxlint-disable max-lines-per-function -- describe blocks */
/**
 * The `bash` handler, ported from pi's `tools.test.ts`, and the handlers through their layer.
 */
import { BunServices } from '@effect/platform-bun'
import { describe, expect, test } from 'bun:test'
import { Context, Effect, Layer, Option, Schema } from 'effect'
import { Toolkit } from '../../tool/index.ts'
import { CodingHandlers, CodingTools } from '../index.ts'
import { bash } from '../internal/bash.ts'
import { notesOf, run, textOf } from './support.ts'

describe('bash', () => {
  test('returns combined stdout and stderr with the exit code', () =>
    run(({ workspace, config }) =>
      Effect.gen(function* () {
        const result = yield* bash(workspace, { command: 'echo out; echo err >&2' }, config)
        expect(textOf(result)).toBe('out\nerr\n')
        expect(result.details).toEqual({ exitCode: 0 })
        expect(result.diagnostics).toEqual([])
      }),
    ))

  test('runs in the configured working directory, after the command prefix', () =>
    run(({ workspace, cwd }) =>
      Effect.gen(function* () {
        const result = yield* bash(
          workspace,
          { command: 'pwd; echo $GREETING' },
          { cwd, commandPrefix: 'export GREETING=hello' },
        )
        const real = yield* Effect.orDie(workspace.fs.realPath(cwd))
        expect(textOf(result)).toBe(`${real}\nhello\n`)
      }),
    ))

  test('answers a nonzero exit with a failure carrying the output and exit code', () =>
    run(({ workspace, config }) =>
      Effect.gen(function* () {
        const failure = yield* Effect.flip(
          bash(workspace, { command: 'echo nope; exit 3' }, config),
        )
        expect(textOf(failure)).toBe('nope\n')
        expect(failure.details).toEqual({ exitCode: 3 })
        expect(failure.diagnostics).toEqual([
          { severity: 'error', code: 'exit_code', message: 'Command exited with code 3' },
        ])
      }),
    ))

  test('times out with the output so far', () =>
    run(({ workspace, config }) =>
      Effect.gen(function* () {
        const failure = yield* Effect.flip(
          bash(workspace, { command: 'echo started; sleep 5', timeout: 0.3 }, config),
        )
        expect(textOf(failure)).toBe('started\n')
        expect(notesOf(failure)).toBe('Command timed out after 0.3 seconds')
      }),
    ))

  test('rejects an invalid timeout', () =>
    run(({ workspace, config }) =>
      Effect.gen(function* () {
        const failure = yield* Effect.flip(
          bash(workspace, { command: 'true', timeout: -1 }, config),
        )
        expect(textOf(failure)).toBe('Invalid timeout: must be a finite number of seconds')
      }),
    ))

  test('spills complete output beyond the default limits', () =>
    run(({ workspace, config }) =>
      Effect.gen(function* () {
        const result = yield* bash(workspace, { command: 'seq 1 3000' }, config)
        const details = yield* Effect.orDie(
          Schema.decodeUnknownEffect(CodingTools.BashDetails)(result.details),
        )
        expect(result.diagnostics?.[0]).toMatchObject({ severity: 'info', code: 'full_output' })
        const spilled = yield* Effect.orDie(
          workspace.fs.readFileString(details.fullOutputPath ?? ''),
        )
        expect(spilled.split('\n').length).toBe(3001)
        yield* Effect.orDie(workspace.fs.remove(details.fullOutputPath ?? ''))
      }),
    ))

  test('does not spill output within the limits', () =>
    run(({ workspace, config }) =>
      Effect.gen(function* () {
        const result = yield* bash(workspace, { command: 'seq 1 10' }, config)
        expect(result.details).toEqual({ exitCode: 0 })
      }),
    ))
})

describe('CodingHandlers.layer', () => {
  test('implements every coding tool, taking encoded values', () =>
    Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const context = yield* Layer.build(CodingHandlers.layer)
          const found = Context.getOption(context, Toolkit.implementation('bash'))
          const runner = yield* Option.match(found, {
            onNone: () => Effect.die('bash has no implementation'),
            onSome: Effect.succeed,
          })
          const outcome = yield* runner.run({ command: 'echo hi' }, { callId: 'c1', config: {} })
          expect(outcome).toMatchObject({
            type: 'success',
            value: { type: 'pi.tool-output', content: [{ type: 'text', text: 'hi\n' }] },
          })
          const invalid = yield* runner.run({}, { callId: 'c2', config: {} })
          expect(invalid.type).toBe('invalidArguments')
        }),
      ).pipe(
        // Each test is an entry point that provides its own platform.
        // oxlint-disable-next-line effecttsgo/strict-effect-provide
        Effect.provide(BunServices.layer),
      ),
    ))
})
