/**
 * Validated disjoint edits that preserve unchanged text bytes.
 *
 * @since 0.0.0
 */
import { constUndefined } from 'effect/Function'
import * as Option from 'effect/Option'
import { MutationLocks } from '../MutationLocks.ts'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as AiTool from 'effect/ai/Tool'
// effect-review-allow P9-namespace-alias-equals-module: effect/ai/Tool and ../Tool.ts both bind Tool; AiTool preserves the checked imported-name collision.
import * as Prompt from 'effect/ai/Prompt'
import { Env } from '../Env.ts'
import { ToolError, ToolExecution, ToolInvalidParameters } from '../ToolError.ts'
import { Invocation, Result } from '../Invocation.ts'
import * as Metadata from '../Tool.ts'
// effect-review-allow P9-namespace-alias-equals-module: ../Tool.ts and effect/ai/Tool both bind Tool; Metadata preserves the checked imported-name collision.
import * as EditDiff from './EditDiff.ts'
import * as mutation from './internal/mutation.ts'
import * as path from './internal/path.ts'
/**
 * Schema for parameters.
 *
 * @category schemas
 * @since 0.0.0
 */
export const Parameters = Schema.Struct({
  path: Schema.String,
  edits: Schema.Array(Schema.Struct({ oldText: Schema.String, newText: Schema.String })),
})
/**
 * Edit input contract.
 *
 * @category models
 * @since 0.0.0
 */
export type Input = Parameters
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
/**
 * Repairs supported legacy argument shapes without discarding unknown keys.
 *
 * @category combinators
 * @since 0.0.0
 */
export const repair = Effect.fnUntraced(function* (input: unknown): Effect.fn.Return<unknown> {
  if (!isRepairObject(input)) return input
  const args: Record<string, unknown> = { ...input }
  if (isString(args['edits'])) {
    const parsed = yield* Schema.decodeEffect(Schema.fromJsonString(Schema.Json))(
      args['edits'],
    ).pipe(Effect.option)
    Option.match(parsed, {
      onNone: constUndefined,
      onSome: (self) => {
        if (isArray(self) || isEdit(self)) args['edits'] = isArray(self) ? self : [self]
      },
    })
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
/**
 * Native edit tool declaration with disjoint matching and preserved unchanged bytes.
 *
 * @category constants
 * @since 0.0.0
 */
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
/**
 * Applies validated edits under canonical mutation admission and reports the resulting diff.
 *
 * @category combinators
 * @since 0.0.0
 */
export const handler = Effect.fnUntraced(function* (input: Input): Effect.fn.Return<
  {
    content: Array<Prompt.TextPart>
    details: { diff: string; patch: string; firstChangedLine?: number }
  },
  ToolError,
  Env | Invocation | MutationLocks
> {
  const env = yield* Env
  const absolute = yield* path.resolve(input.path).pipe(
    Effect.mapError(
      (cause) =>
        new ToolError({
          reason: new ToolExecution({ name: 'edit', message: cause.message, cause: cause }),
        }),
    ),
  )
  return yield* mutation
    .withFile(
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
        const { bom, text } = EditDiff.stripBom(original)
        const ending = EditDiff.detectLineEnding(text)
        const changed = yield* Effect.fromResult(
          EditDiff.applyEditsToNormalizedContent(
            EditDiff.normalizeToLF(text),
            input.edits,
            input.path,
          ),
        ).pipe(
          Effect.mapError(
            (cause) =>
              new ToolError({
                reason: new ToolExecution({ name: 'edit', message: cause.message, cause: cause }),
              }),
          ),
        )
        yield* env.writeFile(
          absolute,
          bom + EditDiff.restoreLineEndings(changed.newContent, ending),
        )
        const display = EditDiff.generateDiffString(changed.baseContent, changed.newContent)
        return {
          content: [
            Prompt.textPart({
              text: `Successfully replaced ${input.edits.length} block(s) in ${input.path}.`,
            }),
          ],
          details: {
            diff: display.diff,
            patch: EditDiff.generateUnifiedPatch(
              input.path,
              changed.baseContent,
              changed.newContent,
            ),
            ...(display.firstChangedLine === undefined
              ? {}
              : { firstChangedLine: display.firstChangedLine }),
          },
        }
      }),
    )
    .pipe(
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

/**
 * Checks whether an unknown value satisfies the Input contract.
 *
 * @category guards
 * @since 0.0.0
 */
export const isInput: (u: unknown) => u is Parameters = Schema.is(Parameters)

/**
 * Edit parameters contract.
 *
 * @category models
 * @since 0.0.0
 */
export type Parameters = typeof Parameters.Type
