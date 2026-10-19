/**
 * Test support: a workspace on the local machine, in a fresh temp directory.
 */
import { BunServices } from '@effect/platform-bun'
import { Effect } from 'effect'
import type { Scope } from 'effect'
import type { Output } from '../../tool/ToolResult.ts'
import * as Workspace from '../internal/workspace.ts'

const raw = new TextDecoder('utf-8', { ignoreBOM: true })

export interface Fixture {
  readonly workspace: Workspace.Workspace
  readonly cwd: string
  readonly config: { readonly cwd: string }
  readonly write: (name: string, content: string | Uint8Array) => Effect.Effect<void>
  readonly readBack: (name: string) => Effect.Effect<string>
}

export const run = <A, E>(f: (fixture: Fixture) => Effect.Effect<A, E, Scope.Scope>) =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const workspace = yield* Workspace.make
        const { fs, path } = workspace
        const cwd = yield* fs.makeTempDirectoryScoped({ prefix: 'coding-' })
        const fixture: Fixture = {
          workspace,
          cwd,
          config: { cwd },
          write: (name, content) =>
            Effect.orDie(
              typeof content === 'string'
                ? fs.writeFileString(path.join(cwd, name), content)
                : fs.writeFile(path.join(cwd, name), content),
            ),
          readBack: (name) =>
            Effect.map(Effect.orDie(fs.readFile(path.join(cwd, name))), (bytes) =>
              raw.decode(bytes),
            ),
        }
        return yield* f(fixture)
      }),
    ).pipe(
      // Each test is an entry point that provides its own platform.
      // oxlint-disable-next-line effecttsgo/strict-effect-provide
      Effect.provide(BunServices.layer),
    ),
  )

/** The text blocks of `output`, joined. */
export const textOf = (output: Output) =>
  output.content.map((block) => (block.type === 'text' ? block.text : '')).join('')

/** The diagnostics of `output`, one message per line. */
export const notesOf = (output: Output) =>
  (output.diagnostics ?? []).map((diagnostic) => diagnostic.message).join('\n')
