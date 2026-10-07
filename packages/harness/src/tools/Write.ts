/**
 * Canonical file replacement through host-owned mutation admission.
 */
import { MutationLocks } from '../MutationLocks.ts'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as AiTool from 'effect/ai/Tool'
// effect-review-allow P9-namespace-alias-equals-module: effect/ai/Tool and ../Tool.ts both bind Tool; AiTool preserves the checked imported-name collision.
import * as Prompt from 'effect/ai/Prompt'
import { Env } from '../Env.ts'
import { ToolError, ToolExecution } from '../ToolError.ts'
import { Invocation, Result } from '../Invocation.ts'
import * as Metadata from '../Tool.ts'
// effect-review-allow P9-namespace-alias-equals-module: ../Tool.ts and effect/ai/Tool both bind Tool; Metadata preserves the checked imported-name collision.
import * as mutation from './internal/mutation.ts'
import * as path from './internal/path.ts'
/**
 * Schema for file path and complete replacement text.
 *
 * @category schemas
 */
export const Parameters = Schema.Struct({ path: Schema.String, content: Schema.String })
/**
 * Decoded parameters passed to the coding-tool handler.
 *
 * @category models
 */
export type Input = Parameters
/**
 * Native write tool replacing a file with supplied text.
 *
 * **Details**
 *
 * Creates parent directories and serializes writes through the shared MutationLocks manager.
 *
 * **Gotchas**
 *
 * The action mutates the environment and uses unsafe replay by default.
 *
 * @category constants
 */
export const tool = AiTool.make('write', {
  description:
    'Write content to a file. Creates missing parent directories and overwrites existing content.',
  parameters: Parameters,
  success: Result,
  failure: ToolError,
})
  .addDependency(Env)
  .addDependency(MutationLocks)
  .addDependency(Invocation)
  .annotate(Metadata.Metadata, {
    replay: 'unsafe',
    project: (result) => Metadata.decodeResult('write', result),
  })
/**
 * Replaces file content under the host-owned canonical mutation lock.
 *
 * @category combinators
 */
export const handler = Effect.fnUntraced(function* (
  input: Input,
): Effect.fn.Return<
  { content: Array<Prompt.TextPart> },
  ToolError,
  Env | Invocation | MutationLocks
> {
  const env = yield* Env
  const absolute = yield* path.resolve(input.path).pipe(
    Effect.mapError(
      (cause) =>
        new ToolError({
          reason: new ToolExecution({ name: 'write', message: cause.message, cause: cause }),
        }),
    ),
  )
  return yield* mutation
    .withFile(
      absolute,
      env.writeFile(absolute, input.content).pipe(
        Effect.as({
          content: [Prompt.textPart({ text: `Successfully wrote to ${input.path}` })],
        }),
      ),
    )
    .pipe(
      Effect.mapError(
        (cause) =>
          new ToolError({
            reason: new ToolExecution({ name: 'write', message: cause.message, cause: cause }),
          }),
      ),
    )
})

/**
 * Checks whether a value satisfies the decoded `Parameters` schema.
 *
 * **Details**
 *
 * Does not decode, transform or coerce input. Use the schema decoder at an external data
 * boundary.
 *
 * @category guards
 */
export const isInput: (u: unknown) => u is Parameters = Schema.is(Parameters)

/**
 * File path and complete replacement text.
 *
 * @category models
 */
export type Parameters = typeof Parameters.Type
