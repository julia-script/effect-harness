/**
 * The coding tools, ported from pi's `tools/`: `read`, `write`, `edit` and `bash`, declared here
 * and implemented by `CodingHandlers.layer` over Effect's `FileSystem`, `Path` and
 * `ChildProcessSpawner`. Whichever implementation of those services the loop runs with decides
 * where the files and commands are. Nothing installs the extension automatically.
 *
 * Each tool's config sets its working directory, `cwd`; relative paths resolve against it, and
 * without it against the process's. A conversation's `$agent` config can override it.
 */
import { Schema } from 'effect'
import * as Extension from '../harness/Extension.ts'
import * as Tool from '../tool/Tool.ts'
import * as Toolkit from '../tool/Toolkit.ts'
import { Output } from '../tool/ToolResult.ts'

/** Limits of what `read` shows of a text file, and of what `bash` keeps of its output. */
export const DEFAULT_MAX_LINES = 2000
export const DEFAULT_MAX_BYTES = 50 * 1024

const Workspace = { cwd: Schema.optionalKey(Schema.String) }

/** Every tool's config: where relative paths resolve. */
export const Config = Schema.Struct(Workspace)
export type Config = typeof Config.Type

const result = { success: Output, failure: Output }

export const Read = Tool.make('read', {
  description: `Read the contents of a file. Supports text files and images (jpg, png, gif, webp). Images are sent as attachments. For text files, output is truncated to ${DEFAULT_MAX_LINES} lines or ${DEFAULT_MAX_BYTES / 1024}KB (whichever is hit first). Use offset/limit for large files. When you need the full file, continue with offset until complete.`,
  parameters: Schema.Struct({
    path: Schema.String.annotate({
      description: 'Path to the file to read (relative or absolute)',
    }),
    offset: Schema.optionalKey(
      Schema.Finite.annotate({ description: 'Line number to start reading from (1-indexed)' }),
    ),
    limit: Schema.optionalKey(
      Schema.Finite.annotate({ description: 'Maximum number of lines to read' }),
    ),
  }),
  ...result,
  config: Config,
  replay: 'safe',
})

export const Write = Tool.make('write', {
  description:
    "Write content to a file. Creates the file if it doesn't exist, overwrites if it does. Automatically creates parent directories.",
  parameters: Schema.Struct({
    path: Schema.String.annotate({
      description: 'Path to the file to write (relative or absolute)',
    }),
    content: Schema.String.annotate({ description: 'Content to write to the file' }),
  }),
  ...result,
  config: Config,
})

export const Replacement = Schema.Struct({
  oldText: Schema.String.annotate({
    description:
      'Exact text for one targeted replacement. It must be unique in the original file and must not overlap with any other edits[].oldText in the same call.',
  }),
  newText: Schema.String.annotate({ description: 'Replacement text for this targeted edit.' }),
})
export type Replacement = typeof Replacement.Type

/** What `edit` shows a program or UI: a numbered diff, a unified patch, the first changed line. */
export const EditDetails = Schema.Struct({
  diff: Schema.String,
  patch: Schema.String,
  firstChangedLine: Schema.optionalKey(Schema.Int),
})
export type EditDetails = typeof EditDetails.Type

export const Edit = Tool.make('edit', {
  description:
    'Edit a single file using exact text replacement. Every edits[].oldText must match a unique, non-overlapping region of the original file. If two changes affect the same block or nearby lines, merge them into one edit instead of emitting overlapping edits. Do not include large unchanged regions just to connect distant changes.',
  parameters: Schema.Struct({
    path: Schema.String.annotate({
      description: 'Path to the file to edit (relative or absolute)',
    }),
    edits: Schema.Array(Replacement).annotate({
      description:
        'One or more targeted replacements. Each edit is matched against the original file, not incrementally. Do not include overlapping or nested edits. If two changes touch the same block or nearby lines, merge them into one edit instead.',
    }),
  }),
  ...result,
  config: Config,
})

/** What `bash` shows a program or UI besides its output. */
export const BashDetails = Schema.Struct({
  exitCode: Schema.Int,
  /** The file with the complete output, when it exceeded the limits. */
  fullOutputPath: Schema.optionalKey(Schema.String),
})
export type BashDetails = typeof BashDetails.Type

export const Bash = Tool.make('bash', {
  description: `Execute a bash command in the current working directory. Returns combined stdout and stderr. Output is truncated to last ${DEFAULT_MAX_LINES} lines or ${DEFAULT_MAX_BYTES / 1024}KB (whichever is hit first). If truncated, full output is saved to a temp file. Optionally provide a timeout in seconds.`,
  parameters: Schema.Struct({
    command: Schema.String.annotate({ description: 'Bash command to execute' }),
    timeout: Schema.optionalKey(
      Schema.Finite.annotate({ description: 'Timeout in seconds (optional, no default timeout)' }),
    ),
  }),
  ...result,
  config: Schema.Struct({
    ...Workspace,
    /** Lines run before each command. */
    commandPrefix: Schema.optionalKey(Schema.String),
  }),
  outputLimits: { retain: 'tail' },
})

export const toolkit = Toolkit.make(Read, Write, Edit, Bash)

/** The `coding-tools` extension; installing it needs `CodingHandlers.layer`. */
export const extension = Extension.make('coding-tools', { toolkit })
