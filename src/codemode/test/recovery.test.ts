/* oxlint-disable max-lines-per-function -- describe blocks */
/**
 * Code mode recovery that refuses to replay, or stores nothing to replay later, ported from pi's
 * `codemode.test.ts`.
 */
import { CodemodeSandbox } from '@earendil-works/pi-codemode'
import { describe, expect, test } from 'bun:test'
import { Deferred, Effect } from 'effect'
import { ownerless, runTest } from '../../test/generation.ts'
import { Faux } from '../../model/index.ts'
import { Codemode } from '../index.ts'
import { edit, restart, script } from './scripts.ts'
import { openChat, resultOf, scriptTask, world } from './support.ts'

describe('code mode recovery', () => {
  for (const field of ['fingerprint', 'version'] as const)
    test(`refuses to resume when the ${field} changed`, () =>
      runTest((storage) =>
        Effect.gen(function* () {
          const current = yield* world
          const { result } = yield* restart(
            storage,
            current,
            'await tools.hold({}); await tools.echo({ text: "late" });',
            {
              between: edit({
                run: (state) => {
                  if (state.start === undefined) return
                  if (field === 'fingerprint') state.start.fingerprint = 'another sandbox'
                  else state.start.version = 0
                },
              }),
            },
          )
          expect(result.isError).toBe(true)
          expect(result.text).toContain('cannot resume')
          expect(current.echo.size).toBe(0)
        }),
      ))

  test('times a script out across a restart, without admitting new calls', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const current = yield* world
        const { result } = yield* restart(
          storage,
          current,
          '// @options: {"timeout_ms": 300}\nawait tools.hold({}); await tools.echo({ text: "late" });',
          { between: () => Effect.sleep('400 millis') },
        )
        expect(result.isError).toBe(true)
        expect(result.text).toContain('deadline passed')
        expect(current.echo.size).toBe(0)
        expect(current.starts).toBe(1)
      }),
    ))

  test('stores no outcome when the host closes the sandbox, and replays the script after a restart', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const current = yield* world
        const first = yield* openChat(storage, current)
        yield* first.faux.setResponses([
          script('await tools.echo({ text: "a" }); await tools.hold({}); return "done";'),
        ])
        const conversation = yield* ownerless(first.harness)
        const submission = yield* Effect.orDie(
          conversation.submit({ type: 'input', content: 'go' }),
        )
        yield* Deferred.await(current.reached)
        const task = yield* scriptTask(first.harness)
        // A host shutting down closes the sandbox before the Harness.
        yield* Effect.promise(() => current.sandbox.close())
        yield* Effect.sleep('100 millis')
        const run = yield* Effect.orDie(first.harness.session.snapshot(Codemode.RunDocument, task))
        expect(run?.result).toBeUndefined()
        yield* first.close
        // The next process has a new sandbox.
        current.block = false
        current.sandbox = yield* Effect.acquireRelease(
          Effect.sync(() => new CodemodeSandbox({ memoryLimitBytes: 64 * 1024 * 1024 })),
          (made) => Effect.promise(() => made.close()),
        )
        const second = yield* openChat(storage, current)
        yield* second.faux.setResponses([Faux.message('done')])
        const again = yield* Effect.orDie(second.harness.submission(submission.id))
        if (again !== undefined) yield* again.wait
        expect(yield* resultOf(second.harness, conversation.id)).toMatchObject({
          text: 'done',
          isError: false,
        })
        expect(Object.fromEntries(current.echo)).toEqual({ a: 1 })
      }),
    ))
})
