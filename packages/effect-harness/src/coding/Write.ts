import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Path from 'effect/Path'
import type * as PlatformError from 'effect/PlatformError'
import * as Schema from 'effect/Schema'
import * as Prompt from 'effect/ai/Prompt'
import * as Tool from '../Tool.js'
import type * as ToolResult from '../ToolResult.js'
import * as Toolkit from '../Toolkit.js'
import { CodingFileError, FailureSchema, failureOutput } from './FileError.js'
import { MutationCoordinator } from './MutationCoordinator.js'

export const Parameters = Schema.Struct({ path: Schema.String, content: Schema.String })
export type Parameters = typeof Parameters.Type
export const Outcome = Schema.Struct({
  path: Schema.String,
  bytesWritten: Schema.Finite,
  outcome: Schema.Literals(['created', 'overwritten']),
})
export type Outcome = typeof Outcome.Type

/** Mutations are unsafe to replay: even an identical write can overwrite another writer's changes. */
export const tool = Tool.makeResult('write', {
  description:
    'Write literal UTF-8 content to a file, replacing existing content and creating parent directories.',
  parameters: Parameters,
  success: Outcome,
  failure: FailureSchema,
})
  .addDependency(FileSystem.FileSystem)
  .addDependency(Path.Path)
  .addDependency(MutationCoordinator)
export const toolkit = Toolkit.make(tool)

/**
 * Writes are cooperative, scoped and interruptible, but not transactional or
 * crash-atomic. A backend failure/interruption may leave partial content; never
 * report a successful receipt before the native write completes. External writers
 * are not locked. Unpaired UTF-16 surrogates are rejected instead of replaced.
 */
export const write = Effect.fn('CodingWrite.write')(function* (input: Parameters) {
  if (!input.content.isWellFormed())
    return yield* new CodingFileError({
      reason: 'encoding',
      path: input.path,
      message: 'content contains an unpaired UTF-16 surrogate',
    })
  const fs = yield* FileSystem.FileSystem
  const path = yield* Path.Path
  const coordinator = yield* MutationCoordinator
  return yield* coordinator.withPath(input.path, (canonical) =>
    Effect.gen(function* () {
      const operation = <A>(effect: Effect.Effect<A, PlatformError.PlatformError>) =>
        effect.pipe(
          Effect.mapError(
            (cause) =>
              new CodingFileError({
                reason: 'filesystem',
                path: input.path,
                message: cause.message,
                cause,
              }),
          ),
        )
      const existed = yield* operation(fs.exists(canonical))
      yield* operation(fs.makeDirectory(path.dirname(canonical), { recursive: true }))
      const bytes = new TextEncoder().encode(input.content)
      yield* operation(fs.writeFile(canonical, bytes))
      const outcome: Outcome = {
        path: canonical,
        bytesWritten: bytes.length,
        outcome: existed ? 'overwritten' : 'created',
      }
      const message = `Successfully ${outcome.outcome === 'created' ? 'created' : 'overwrote'} ${canonical} (${bytes.length} bytes).`
      return {
        content: [Prompt.textPart({ text: message })],
        structuredOutput: outcome,
        diagnostics: [{ severity: 'info', code: 'written', message }],
      } satisfies ToolResult.Output<Outcome>
    }),
  )
})

/** Ready-made handlers; share MutationCoordinator.layer across mutating tools in the binding. */
export const layer = toolkit.toLayer({
  write: (input) => write(input).pipe(Effect.mapError(failureOutput)),
})
