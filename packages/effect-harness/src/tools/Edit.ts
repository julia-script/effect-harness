/**
 * Validated disjoint edits that preserve unchanged text bytes.
 */
import { constUndefined } from 'effect/Function'
import * as Option from 'effect/Option'
import { MutationLocks } from '../MutationLocks.ts'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as Tool from 'effect/ai/Tool'
import * as Prompt from 'effect/ai/Prompt'
import { Env } from '../Env.ts'
import { ToolError, ToolExecutionError, ToolInvalidParametersError } from '../ToolError.ts'
import { Invocation, Result } from '../Invocation.ts'
import * as ToolRegistration from '../ToolRegistration.ts'
import * as EditDiff from './EditDiff.ts'
import * as mutation from './internal/mutation.ts'
import * as path from './internal/path.ts'
/**
 * Schema for file path and unique old/new text replacement.
 *
 * @category schemas
 */
export const Parameters = Schema.Struct({
  path: Schema.String,
  edits: Schema.Array(Schema.Struct({ oldText: Schema.String, newText: Schema.String })),
})
const isEdit = Schema.is(Parameters.fields.edits.value)
const RepairObject = Schema.Record(Schema.String, Schema.Unknown)
const isRepairObject = Schema.is(RepairObject)
const isString = Schema.is(Schema.String)
const isArray = Schema.is(Schema.Array(Schema.Unknown))
/**
 * Repairs JSON-encoded and singleton edit arrays without discarding unknown keys.
 *
 * @category combinators
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
  return args
})
/**
 * Native edit tool applying a unique old/new text replacement.
 *
 * **Details**
 *
 * Preserves BOM and line endings and reports a diff in result details. Controlled
 * whitespace/typography normalization can locate a fuzzy match.
 *
 * **Gotchas**
 *
 * Missing or ambiguous matches fail rather than replacing arbitrary occurrences. Mutation
 * requires the shared MutationLocks manager.
 *
 * @category constants
 */
export const tool = Tool.make('edit', {
  description:
    'Replace unique disjoint text targets in one file. Every oldText matches the original file; merge overlapping changes.',
  parameters: Parameters,
  success: Result,
  failure: ToolError,
})
  .addDependency(Env)
  .addDependency(MutationLocks)
  .addDependency(Invocation)
  .annotate(ToolRegistration.Metadata, {
    replay: 'unsafe',
    repair,
    project: (result) => ToolRegistration.decodeResult('edit', result),
  })
/**
 * Readonly diff metadata; omitted firstChangedLine remains absent from JSON.
 *
 * @category models
 */
export interface Details {
  readonly diff: string
  readonly patch: string
  readonly firstChangedLine?: number
}

/**
 * Edit result with a readonly payload and freshly owned mutable content array.
 *
 * @category models
 */
export interface Output {
  readonly content: Array<Prompt.TextPart>
  // The mapped readonly view retains structural compatibility with the native JSON result payload.
  readonly details: Readonly<Details>
}

/**
 * Applies validated edits under canonical mutation admission and reports the resulting diff.
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
            reason: new ToolExecutionError({ name: 'edit', message: cause.message, cause: cause }),
          }),
      ),
    )
    return yield* mutation.withFile(
      absolute,
      Effect.gen(function* () {
        if (input.edits.length === 0)
          return yield* new ToolError({
            reason: new ToolInvalidParametersError({
              name: 'edit',
              message: 'edits must contain at least one replacement',
            }),
          })
        const info = yield* env.fileInfo(absolute)
        if (info.kind !== 'file' && info.kind !== 'symlink')
          return yield* new ToolError({
            reason: new ToolExecutionError({
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
                reason: new ToolExecutionError({
                  name: 'edit',
                  message: cause.message,
                  cause: cause,
                }),
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
  },
  (effect, input) =>
    effect.pipe(
      Effect.mapError((cause) =>
        cause instanceof ToolError
          ? cause
          : new ToolError({
              reason: new ToolExecutionError({
                name: 'edit',
                message: `Could not edit file: ${input.path}. Error code: ${cause.reason._tag}. ${cause.message}`,
                cause,
              }),
            }),
      ),
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
 * File path and unique old/new text replacement.
 *
 * @category models
 */
export type Parameters = typeof Parameters.Type
