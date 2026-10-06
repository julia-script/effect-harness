import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as AiTool from 'effect/ai/Tool'
import * as Prompt from 'effect/ai/Prompt'
import { Env } from '../Env.ts'
import { ToolError, ToolExecution, ToolInvalidParameters } from '../Error.ts'
import { Invocation, Result } from '../Invocation.ts'
import * as Metadata from '../Tool.ts'
import * as Diff from './EditDiff.ts'
import * as Mutation from './Mutation.ts'
import * as Path from './Path.ts'
export const Parameters = Schema.Struct({
  path: Schema.String,
  edits: Schema.Array(Schema.Struct({ oldText: Schema.String, newText: Schema.String })),
})
export type Input = typeof Parameters.Type
const isEdit = (value: unknown): value is { readonly oldText: string; readonly newText: string } =>
  typeof value === 'object' &&
  value !== null &&
  !Array.isArray(value) &&
  'oldText' in value &&
  typeof value.oldText === 'string' &&
  'newText' in value &&
  typeof value.newText === 'string'
export const repair = (input: unknown): Effect.Effect<unknown> =>
  Effect.gen(function* () {
    if (typeof input !== 'object' || input === null || Array.isArray(input)) return input
    const args: Record<string, unknown> = { ...input }
    if (typeof args['edits'] === 'string') {
      const parsed = yield* Schema.decodeEffect(Schema.fromJsonString(Schema.Json))(
        args['edits'],
      ).pipe(Effect.option)
      if (parsed._tag === 'Some' && (Array.isArray(parsed.value) || isEdit(parsed.value)))
        args['edits'] = Array.isArray(parsed.value) ? parsed.value : [parsed.value]
    } else if (isEdit(args['edits'])) args['edits'] = [args['edits']]
    const oldText = args['oldText']
    const newText = args['newText']
    if (typeof oldText === 'string' && typeof newText === 'string') {
      const edits = Array.isArray(args['edits']) ? [...args['edits']] : []
      edits.push({ oldText, newText })
      delete args['oldText']
      delete args['newText']
      args['edits'] = edits
    }
    return args
  })
export const tool = AiTool.make('edit', {
  description:
    'Replace unique disjoint text targets in one file. Every oldText matches the original file; merge overlapping changes.',
  parameters: Parameters,
  success: Result,
  failure: ToolError,
})
  .addDependency(Env)
  .addDependency(Invocation)
  .annotate(Metadata.Metadata, {
    replay: 'unsafe',
    repair,
    project: (result) => Schema.decodeUnknownSync(Result)(result),
  })
export const handler = Effect.fnUntraced(function* (input: Input) {
  const env = yield* Env
  const absolute = yield* Path.resolve(input.path).pipe(
    Effect.mapError(
      (cause) =>
        new ToolError({
          reason: new ToolExecution({ name: 'edit', message: cause.message, cause: cause }),
        }),
    ),
  )
  return yield* Mutation.withFile(
    absolute,
    Effect.gen(function* () {
      if (input.edits.length === 0)
        return yield* new ToolError({
          reason: new ToolInvalidParameters({
            name: 'edit',
            message: 'edits must contain at least one replacement',
          }),
        })
      const info = yield* env.fileInfo(absolute)
      if (info.kind !== 'file' && info.kind !== 'symlink')
        return yield* new ToolError({
          reason: new ToolExecution({
            name: 'edit',
            message: `Could not edit file: ${input.path}. Path is not a file.`,
          }),
        })
      const original = yield* env.readTextFile(absolute)
      const { bom, text } = Diff.stripBom(original)
      const ending = Diff.detectLineEnding(text)
      const changed = yield* Effect.fromResult(
        Diff.applyEditsToNormalizedContent(Diff.normalizeToLF(text), input.edits, input.path),
      ).pipe(
        Effect.mapError(
          (cause) =>
            new ToolError({
              reason: new ToolExecution({ name: 'edit', message: cause.message, cause: cause }),
            }),
        ),
      )
      yield* env.writeFile(absolute, bom + Diff.restoreLineEndings(changed.newContent, ending))
      const display = Diff.generateDiffString(changed.baseContent, changed.newContent)
      return {
        content: [
          Prompt.textPart({
            text: `Successfully replaced ${input.edits.length} block(s) in ${input.path}.`,
          }),
        ],
        details: {
          diff: display.diff,
          patch: Diff.generateUnifiedPatch(input.path, changed.baseContent, changed.newContent),
          ...(display.firstChangedLine === undefined
            ? {}
            : { firstChangedLine: display.firstChangedLine }),
        },
      }
    }),
  ).pipe(
    Effect.mapError((cause) =>
      cause instanceof ToolError
        ? cause
        : new ToolError({
            reason: new ToolExecution({
              name: 'edit',
              message: `Could not edit file: ${input.path}. Error code: ${cause.code}. ${cause.message}`,
              cause: cause,
            }),
          }),
    ),
  )
})
