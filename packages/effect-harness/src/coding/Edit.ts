import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Path from 'effect/Path'
import type * as PlatformError from 'effect/PlatformError'
import * as Result from 'effect/Result'
import * as Schema from 'effect/Schema'
import * as SchemaTransformation from 'effect/SchemaTransformation'
import * as Prompt from 'effect/ai/Prompt'
import * as Tool from '../Tool.js'
import type * as ToolResult from '../ToolResult.js'
import * as Toolkit from '../Toolkit.js'
import * as EditDiff from '../internal/EditDiff.js'
import * as EditText from '../internal/EditText.js'
import { CodingFileError } from './FileError.js'
import { MutationCoordinator } from './MutationCoordinator.js'

export const Replacement = Schema.Struct({ oldText: Schema.String, newText: Schema.String })
export type Replacement = typeof Replacement.Type
const Replacements = Schema.Array(Replacement)
const EditForms = Schema.Union([Replacements, Replacement])
const CanonicalParameters = Schema.Struct({
  path: Schema.String,
  edits: Replacements.check(Schema.isMinLength(1)),
})
const RepairableParameters = Schema.Struct({
  path: Schema.String,
  edits: Schema.optionalKey(Schema.Union([EditForms, Schema.fromJsonString(EditForms)])),
  oldText: Schema.optionalKey(Schema.String),
  newText: Schema.optionalKey(Schema.String),
}).check(
  Schema.makeFilter((input) =>
    (input.oldText === undefined) === (input.newText === undefined)
      ? undefined
      : 'Legacy oldText and newText must both be strings',
  ),
)

/**
 * Decodes canonical arrays, single edit objects, JSON-string versions of either,
 * and legacy top-level oldText/newText pairs. A complete legacy pair appends to
 * edits; incomplete or malformed forms fail before handler execution. Encoding
 * always produces the canonical array form; the caller's arguments are untouched.
 */
export const Parameters = RepairableParameters.pipe(
  Schema.decodeTo(
    CanonicalParameters,
    SchemaTransformation.transform({
      decode: (input): typeof CanonicalParameters.Type => {
        const edits: Replacement[] = []
        if (input.edits !== undefined) {
          if ('oldText' in input.edits) edits.push(input.edits)
          else edits.push(...input.edits)
        }
        if (input.oldText !== undefined && input.newText !== undefined)
          edits.push({ oldText: input.oldText, newText: input.newText })
        return { path: input.path, edits }
      },
      encode: (input) => input,
    }),
  ),
)
export type Parameters = typeof Parameters.Type

/** Planning failures and native filesystem causes remain visible to direct consumers. */
export class CodingEditError extends Schema.TaggedError<CodingEditError>()('CodingEditError', {
  reason: Schema.Literals([
    'invalidArguments',
    'emptyOldText',
    'missing',
    'ambiguous',
    'overlap',
    'noOp',
    'notFile',
    'encoding',
    'filesystem',
  ]),
  path: Schema.String,
  message: Schema.String,
  cause: Schema.optionalKey(Schema.Defect()),
}) {}

export const FailureSchema = Schema.Struct({
  reason: Schema.Union([CodingEditError.fields.reason, CodingFileError.fields.reason]),
  path: Schema.String,
  message: Schema.String,
})
export type Failure = typeof FailureSchema.Type
export const Outcome = Schema.Struct({
  path: Schema.String,
  replacements: Schema.Int,
  bytesWritten: Schema.Int,
  fuzzy: Schema.Boolean,
  writeMode: Schema.Literals(['direct', 'rename']),
  diff: Schema.String,
  patch: Schema.String,
  firstChangedLine: Schema.Int,
})
export type Outcome = typeof Outcome.Type

/** External edits are unsafe to replay, including when native rename is supported. */
export const tool = Tool.makeResult('edit', {
  description:
    'Edit one file with unique, disjoint oldText/newText replacements matched against the original content. All edits must validate before writing. Supply edits as an array; merge overlapping changes into one edit.',
  parameters: Parameters,
  success: Outcome,
  failure: FailureSchema,
})
  .addDependency(FileSystem.FileSystem)
  .addDependency(Path.Path)
  .addDependency(MutationCoordinator)
export const toolkit = Toolkit.make(tool)

export interface Options {
  /**
   * direct (default) validates the entire plan, then performs one native write.
   * Select rename only for backends supporting replacement by same-filesystem
   * rename. It stages in the target directory, preserves permission mode and
   * cleans staging resources on failure/interruption. It never falls back to a
   * direct write if staging/rename fails. Rename replaces the inode: ownership,
   * hard links, ACLs and extended attributes are not preserved. Neither mode
   * promises crash durability or coordination with external writers. A native
   * operation can settle after cancellation; its outcome may remain unknown.
   */
  readonly writeMode?: 'direct' | 'rename' | undefined
}

/**
 * Validate and apply a complete edit plan under the shared write/edit permit.
 * Exact matching normalizes only newlines; fuzzy matching additionally uses
 * NFKC, trailing-space, quote, dash and space normalization on touched lines.
 * Unrelated lines, the UTF-8 BOM and original line endings are copied intact.
 * Mixed endings on inserted lines use the first existing ending. Invalid UTF-8
 * and unpaired UTF-16 are rejected rather than silently replacing bytes.
 * Semantic all-or-none planning does not make direct host writes transactional.
 */
export const edit = Effect.fn('CodingEdit.edit')(function* (
  input: Parameters,
  options: Options = {},
) {
  const decoded = yield* Schema.decodeEffect(CanonicalParameters)(input).pipe(
    Effect.mapError(
      (cause) =>
        new CodingEditError({
          reason: 'invalidArguments',
          path: input.path,
          message: cause.message,
          cause,
        }),
    ),
  )
  for (const [index, replacement] of decoded.edits.entries()) {
    if (!replacement.oldText.isWellFormed() || !replacement.newText.isWellFormed())
      return yield* new CodingEditError({
        reason: 'encoding',
        path: decoded.path,
        message: `edits[${index}] contains an unpaired UTF-16 surrogate`,
      })
  }
  const fs = yield* FileSystem.FileSystem
  const path = yield* Path.Path
  const coordinator = yield* MutationCoordinator
  const writeMode = options.writeMode ?? 'direct'
  return yield* coordinator.withPath(decoded.path, (canonical) =>
    Effect.scoped(
      Effect.gen(function* () {
        const operation = <A, R>(effect: Effect.Effect<A, PlatformError.PlatformError, R>) =>
          effect.pipe(
            Effect.mapError(
              (cause) =>
                new CodingEditError({
                  reason: 'filesystem',
                  path: decoded.path,
                  message: cause.message,
                  cause,
                }),
            ),
          )
        const info = yield* operation(fs.stat(canonical))
        if (info.type !== 'File')
          return yield* new CodingEditError({
            reason: 'notFile',
            path: decoded.path,
            message: 'The edit target must be a regular file',
          })
        const originalBytes = yield* operation(fs.readFile(canonical))
        const original = yield* Effect.try({
          try: () =>
            new TextDecoder('utf-8', { fatal: true, ignoreBOM: true }).decode(originalBytes),
          catch: (cause) =>
            new CodingEditError({
              reason: 'encoding',
              path: decoded.path,
              message: 'The edit target must contain valid UTF-8',
              cause,
            }),
        })
        const planned = EditText.plan(original, decoded.edits)
        if (Result.isFailure(planned))
          return yield* new CodingEditError({ ...planned.failure, path: decoded.path })
        const { content, fuzzy } = planned.success
        const bytes = new TextEncoder().encode(content)
        const details = EditDiff.describe(canonical, original, content)
        if (writeMode === 'rename') {
          const directory = yield* operation(
            fs.makeTempDirectoryScoped({
              directory: path.dirname(canonical),
              prefix: '.coding-edit-',
            }),
          )
          const staged = path.join(directory, 'content')
          yield* operation(fs.writeFile(staged, bytes))
          yield* operation(fs.chmod(staged, info.mode))
          yield* operation(fs.rename(staged, canonical))
        } else yield* operation(fs.writeFile(canonical, bytes))
        const outcome: Outcome = {
          path: canonical,
          replacements: decoded.edits.length,
          bytesWritten: bytes.length,
          fuzzy,
          writeMode,
          ...details,
        }
        const message = `Successfully replaced ${decoded.edits.length} block(s) in ${canonical}.\n${details.diff}`
        return {
          content: [Prompt.textPart({ text: message })],
          structuredOutput: outcome,
          details,
          diagnostics: [{ severity: 'info', code: 'edited', message: `Edited ${canonical}` }],
        } satisfies ToolResult.Output<Outcome>
      }),
    ),
  )
})

const failureOutput = (error: CodingEditError | CodingFileError): ToolResult.Output<Failure> => ({
  content: [Prompt.textPart({ text: error.message })],
  structuredOutput: { reason: error.reason, path: error.path, message: error.message },
  diagnostics: [{ severity: 'error', code: error.reason, message: error.message }],
})

/** Ready-made handlers; share one MutationCoordinator.layer with write in each binding. */
export const layer = (options: Options = {}) =>
  toolkit.toLayer({
    edit: (input) => edit(input, options).pipe(Effect.mapError(failureOutput)),
  })
