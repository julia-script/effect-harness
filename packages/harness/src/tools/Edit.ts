import { MutationLocks } from '../MutationLocks.ts'
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
const isEdit = Schema.is(Parameters.fields.edits.value)
const RepairObject = Schema.Record(Schema.String, Schema.Unknown)
const isRepairObject = Schema.is(RepairObject)
const isString = Schema.is(Schema.String)
const isArray = Schema.is(Schema.Array(Schema.Unknown))
const LegacyEdit = Schema.Struct({
  oldText: Parameters.fields.edits.value.fields.oldText,
  newText: Parameters.fields.edits.value.fields.newText,
})
const isLegacyEdit = Schema.is(LegacyEdit)
export const repair = Effect.fnUntraced(function* (input: unknown): Effect.fn.Return<unknown> {
  if (!isRepairObject(input)) return input
  const args: Record<string, unknown> = { ...input }
  if (isString(args['edits'])) {
    const parsed = yield* Schema.decodeEffect(Schema.fromJsonString(Schema.Json))(
      args['edits'],
    ).pipe(Effect.option)
    if (parsed._tag === 'Some' && (isArray(parsed.value) || isEdit(parsed.value)))
      args['edits'] = isArray(parsed.value) ? parsed.value : [parsed.value]
  } else if (isEdit(args['edits'])) args['edits'] = [args['edits']]
  const oldText = args['oldText']
  const newText = args['newText']
  if (isLegacyEdit({ oldText, newText })) {
    const edits = isArray(args['edits']) ? [...args['edits']] : []
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
  .addDependency(MutationLocks)
  .addDependency(Invocation)
  .annotate(Metadata.Metadata, {
    replay: 'unsafe',
    repair,
    project: (result) => Metadata.decodeResult('edit', result),
  })
export const handler = Effect.fnUntraced(function* (input: Input): Effect.fn.Return<
  {
    content: Prompt.TextPart[]
    details: { diff: string; patch: string; firstChangedLine?: number }
  },
  ToolError,
  Env | Invocation | MutationLocks
> {
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

export const isInput: (input: unknown) => input is typeof Parameters.Type = Schema.is(Parameters)
