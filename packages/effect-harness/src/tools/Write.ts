/**
 * Canonical file replacement through host-owned mutation admission.
 */
import { MutationLocks } from '../MutationLocks.ts'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as Tool from 'effect/ai/Tool'
import * as Prompt from 'effect/ai/Prompt'
import { Env } from '../Env.ts'
import { ToolError, ToolExecutionError } from '../ToolError.ts'
import { Invocation, Result } from '../Invocation.ts'
import * as ToolRegistration from '../ToolRegistration.ts'
import * as mutation from './internal/mutation.ts'
import * as path from './internal/path.ts'
/**
 * Schema for file path and complete replacement text.
 *
 * @category schemas
 */
export const Parameters = Schema.Struct({ path: Schema.String, content: Schema.String })
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
export const tool = Tool.make('write', {
  description:
    'Write content to a file. Creates missing parent directories and overwrites existing content.',
  parameters: Parameters,
  success: Result,
  failure: ToolError,
})
  .addDependency(Env)
  .addDependency(MutationLocks)
  .addDependency(Invocation)
  .annotate(ToolRegistration.Metadata, {
    replay: 'unsafe',
    project: (result) => ToolRegistration.decodeResult('write', result),
  })
/**
 * Write result with a readonly payload and freshly owned mutable content array.
 *
 * @category models
 */
export interface Output {
  readonly content: Array<Prompt.TextPart>
}

/**
 * Replaces file content under the host-owned canonical mutation lock.
 *
 * @category combinators
 */
export const handler = Effect.fnUntraced(
  function* (
    input: Parameters,
  ): Effect.fn.Return<
    Output,
    ToolError | import('../FileError.ts').FileError,
    Env | Invocation | MutationLocks
  > {
    const env = yield* Env
    const absolute = yield* path.resolve(input.path).pipe(
      Effect.mapError(
        (cause) =>
          new ToolError({
            reason: new ToolExecutionError({ name: 'write', message: cause.message, cause: cause }),
          }),
      ),
    )
    return yield* mutation.withFile(
      absolute,
      env.writeFile(absolute, input.content).pipe(
        Effect.as({
          content: [Prompt.textPart({ text: `Successfully wrote to ${input.path}` })],
        }),
      ),
    )
  },
  Effect.mapError((cause) =>
    cause instanceof ToolError
      ? cause
      : new ToolError({
          reason: new ToolExecutionError({ name: 'write', message: cause.message, cause }),
        }),
  ),
)

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
export const isParameters: (u: unknown) => u is Parameters = Schema.is(Parameters)

/**
 * File path and complete replacement text.
 *
 * @category models
 */
export type Parameters = typeof Parameters.Type
