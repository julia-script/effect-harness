import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Ref from 'effect/Ref'
import * as Layer from 'effect/Layer'
import * as NodeServices from '@effect/platform-node/NodeServices'
import * as NodeEnv from '../../src/env/Node.ts'
import { Env } from '../../src/Env.ts'
import { Invocation, ToolCall, type Diagnostic } from '../../src/Invocation.ts'
import type { ToolError } from '../../src/Error.ts'
export const withEnv = <A, E, R>(
  program: Effect.Effect<A, E, R | Env | FileSystem.FileSystem | Invocation>,
) =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem
      const cwd = yield* fs.makeTempDirectoryScoped({ prefix: 'harness-tools-test-' })
      return yield* program.pipe(
        Effect.provide(
          Layer.mergeAll(
            NodeEnv.layer({
              cwd,
              shell: '/bin/sh',
              env: {
                BASH_ENV: '',
                PATH: '/usr/bin:/bin',
                SSH_CLIENT: '',
                SSH2_CLIENT: '',
                SSH_CONNECTION: '',
                SSH_TTY: '',
              },
            }),
            Layer.succeed(
              Invocation,
              Invocation.of({
                cwd,
                report: () => Effect.void,
                progress: () => Effect.void,
              }),
            ),
          ),
        ),
      )
    }),
  ).pipe(Effect.provide(NodeServices.layer))
export const recording = Effect.gen(function* () {
  const output = yield* Ref.make('')
  const diagnostics = yield* Ref.make<ReadonlyArray<Diagnostic>>([])
  const api = ToolCall.of({
    id: 'test',
    output: (text) =>
      Ref.update(
        output,
        (value) => value + (typeof text === 'string' ? text : new TextDecoder().decode(text)),
      ),
    details: () => Effect.void,
    diagnostic: (diagnostic) => Ref.update(diagnostics, (values) => [...values, diagnostic]),
  })
  return { output, diagnostics, api }
})
export const message = (result: {
  readonly content?:
    | ReadonlyArray<{ readonly type: string; readonly text?: string | undefined }>
    | undefined
}): string =>
  result.content?.flatMap((part) => (part.type === 'text' ? [part.text ?? ''] : [])).join('') ?? ''
export const toolFailure = (error: ToolError): string => error.message
