/* oxlint-disable max-lines-per-function -- describe blocks */
/**
 * The coding tools as the `pi.tool` task runs them: output limits, diagnostics and details reach
 * the result entry.
 */
import { BunServices } from '@effect/platform-bun'
import { describe, expect, test } from 'bun:test'
import { Effect, Layer } from 'effect'
import { conversation, open, runTest } from '../../task/test/support.ts'
import { ToolTask } from '../../tool/index.ts'
import { answer, call } from '../../tool/test/support.ts'
import { CodingHandlers, CodingTools } from '../index.ts'

const tasks = Effect.map(
  Layer.build(CodingHandlers.layer.pipe(Layer.provide(BunServices.layer))),
  (implementations) => ToolTask.make(CodingTools.toolkit, implementations),
)

describe('coding tools in the tool task', () => {
  test('bash keeps the tail of long output and points at the full output', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const { session, scheduler } = yield* open(storage, [yield* tasks])
        const id = yield* call(session, yield* conversation(session), 'bash', {
          command: 'seq 1 3000',
        })
        const { state } = yield* scheduler.waitForTask(id)
        const { message, diagnostics } = yield* answer(session, state.outcome)
        expect(message).toMatchObject({ isError: false, details: { exitCode: 0 } })
        const shown = JSON.stringify(message?.['content'])
        expect(shown).toContain('3000')
        expect(shown).not.toContain('"1\\n2\\n')
        expect(diagnostics.map((diagnostic) => diagnostic.code)).toContain('full_output')
      }),
    ))

  test('a failed edit is an error result whose text says why', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const { session, scheduler } = yield* open(storage, [yield* tasks])
        const id = yield* call(session, yield* conversation(session), 'edit', {
          path: '/nonexistent/file.txt',
          edits: [{ oldText: 'a', newText: 'b' }],
        })
        const { state } = yield* scheduler.waitForTask(id)
        const { message } = yield* answer(session, state.outcome)
        expect(message).toMatchObject({ isError: true })
        expect(JSON.stringify(message?.['content'])).toContain('Could not edit file')
      }),
    ))
})
