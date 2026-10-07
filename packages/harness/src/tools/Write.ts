import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as AiTool from 'effect/ai/Tool'
import * as Prompt from 'effect/ai/Prompt'
import { Env } from '../Env.ts'
import { ToolError, ToolExecution } from '../Error.ts'
import { Invocation, Result } from '../Invocation.ts'
import * as Metadata from '../Tool.ts'
import * as Mutation from './Mutation.ts'
import * as Path from './Path.ts'
export const Parameters = Schema.Struct({ path: Schema.String, content: Schema.String })
export type Input = typeof Parameters.Type
export const tool = AiTool.make('write', {
  description:
    'Write content to a file. Creates missing parent directories and overwrites existing content.',
  parameters: Parameters,
  success: Result,
  failure: ToolError,
})
  .addDependency(Env)
  .addDependency(Invocation)
  .annotate(Metadata.Metadata, {
    replay: 'unsafe',
    project: (result) => Metadata.decodeResult('write', result),
  })
export const handler = Effect.fnUntraced(function* (input: Input) {
  const env = yield* Env
  const absolute = yield* Path.resolve(input.path).pipe(
    Effect.mapError(
      (cause) =>
        new ToolError({
          reason: new ToolExecution({ name: 'write', message: cause.message, cause: cause }),
        }),
    ),
  )
  return yield* Mutation.withFile(
    absolute,
    env
      .writeFile(absolute, input.content)
      .pipe(
        Effect.as({ content: [Prompt.textPart({ text: `Successfully wrote to ${input.path}` })] }),
      ),
  ).pipe(
    Effect.mapError(
      (cause) =>
        new ToolError({
          reason: new ToolExecution({ name: 'write', message: cause.message, cause: cause }),
        }),
    ),
  )
})
