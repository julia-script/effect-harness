/**
 * Canonical file replacement through host-owned mutation admission.
 *
 * @since 0.0.0
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
 * Schema for parameters.
 *
 * @category schemas
 * @since 0.0.0
 */
export const Parameters = Schema.Struct({ path: Schema.String, content: Schema.String })
/**
 * Write input contract.
 *
 * @category models
 * @since 0.0.0
 */
export type Input = Parameters
/**
 * Native write tool declaration using canonical mutation admission.
 *
 * @category constants
 * @since 0.0.0
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
 * @since 0.0.0
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
 * Checks whether an unknown value satisfies the Input contract.
 *
 * @category guards
 * @since 0.0.0
 */
export const isInput: (u: unknown) => u is Parameters = Schema.is(Parameters)

/**
 * Write parameters contract.
 *
 * @category models
 * @since 0.0.0
 */
export type Parameters = typeof Parameters.Type
