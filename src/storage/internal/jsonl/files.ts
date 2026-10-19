import { Effect } from 'effect'
import type { FileSystem, Path, PlatformError } from 'effect'
import { MAIN_FILE, RECLAIM_SUFFIX } from './format.ts'
import { StorageError } from '../../StorageError.ts'

/** The storage directory and the platform services that reach it. */
export interface Files {
  readonly fs: FileSystem.FileSystem
  readonly path: Path.Path
  readonly directory: string
  /** Flush appended sidecars before the marker and replacements before their rename. */
  readonly fsync: boolean
}

/** One complete line, its decoded value, and the byte offset where it starts. */
export interface ParsedLine<T> {
  readonly text: string
  readonly value: T
  readonly start: number
}

export interface ParsedFile<T> {
  readonly file: string
  readonly lines: ReadonlyArray<ParsedLine<T>>
}

const NEWLINE = 0x0a
const decoder = new TextDecoder('utf-8', { ignoreBOM: true })
const encoder = new TextEncoder()

export const filePath = (files: Files, file: string): string =>
  files.path.join(files.directory, file)

/** Report a platform failure as a fatal storage failure. */
export const io =
  (action: string) =>
  (cause: PlatformError.PlatformError): StorageError =>
    new StorageError({ message: `JSONL ${action} failed: ${cause.message}`, cause })

export const append = (files: Files, file: string, content: string) =>
  files.fs
    .writeFileString(filePath(files, file), content, { flag: 'a' })
    .pipe(Effect.mapError(io(`append to ${file}`)))

/** Flush a file's written data to stable storage. */
export const flush = (files: Files, file: string) =>
  Effect.scoped(
    Effect.flatMap(files.fs.open(filePath(files, file), { flag: 'r+' }), (handle) => handle.sync),
  ).pipe(Effect.mapError(io(`flush of ${file}`)))

export const truncate = (files: Files, file: string, length: number, what: string) =>
  files.fs
    .truncate(filePath(files, file), length)
    .pipe(Effect.mapError(io(`${what} truncation of ${file}`)))

/**
 * Replace a sidecar with `content`, or remove it when `content` is empty. The commit that made
 * the old lines obsolete is already published, so this is best-effort maintenance: a failure
 * leaves the old lines, which the next open reclaims again.
 */
export const replaceSidecar = (
  files: Files,
  file: string,
  content: string,
): Effect.Effect<void> => {
  const target = filePath(files, file)
  if (content === '') return Effect.ignore(files.fs.remove(target, { force: true }))
  const temporary = `${target}${RECLAIM_SUFFIX}`
  return files.fs
    .writeFileString(temporary, content)
    .pipe(
      Effect.andThen(files.fsync ? flush(files, `${file}${RECLAIM_SUFFIX}`) : Effect.void),
      Effect.andThen(files.fs.rename(temporary, target)),
      Effect.ignore,
    )
}

/** A missing file reads as empty. */
const readBytes = (files: Files, file: string) =>
  files.fs.readFile(filePath(files, file)).pipe(
    Effect.catchReason('PlatformError', 'NotFound', () => Effect.succeed(new Uint8Array())),
    Effect.mapError(io(`read of ${file}`)),
  )

const sameBytes = (left: Uint8Array, right: Uint8Array): boolean =>
  left.length === right.length && left.every((byte, index) => byte === right[index])

/** Decode UTF-8 strictly: only valid input survives a replacing decode and re-encode unchanged. */
const decodeLine = (bytes: Uint8Array, file: string, line: number) => {
  const text = decoder.decode(bytes)
  return sameBytes(encoder.encode(text), bytes)
    ? Effect.succeed(text)
    : Effect.fail(new StorageError({ message: `Invalid UTF-8 in complete ${file} line ${line}` }))
}

/**
 * Read every complete line of a file. A torn final line (no trailing newline) is an append that
 * never finished, so it is truncated away before parsing.
 */
export const readLines = Effect.fnUntraced(function* <T>(
  files: Files,
  file: string,
  parse: (text: string, line: number) => Effect.Effect<T, StorageError>,
) {
  const bytes = yield* readBytes(files, file)
  const complete = bytes.lastIndexOf(NEWLINE) + 1
  if (complete < bytes.length) yield* truncate(files, file, complete, 'torn-line')
  const lines: Array<ParsedLine<T>> = []
  let start = 0
  for (let end = bytes.indexOf(NEWLINE); end !== -1 && end < complete;) {
    const text = yield* decodeLine(bytes.subarray(start, end), file, lines.length + 1)
    lines.push({ text, value: yield* parse(text, lines.length + 1), start })
    start = end + 1
    end = bytes.indexOf(NEWLINE, start)
  }
  const parsed: ParsedFile<T> = { file, lines }
  return parsed
})

/**
 * Apply sidecar replacements a published commit allows. With `fsync`, the main file is flushed
 * first so a crash cannot keep the rewrite while losing the marker that justified it.
 */
export const reclaim = (files: Files, replacements: ReadonlyMap<string, string>) => {
  if (replacements.size === 0) return Effect.void
  const replace = Effect.forEach(
    replacements,
    ([file, content]) => replaceSidecar(files, file, content),
    { discard: true },
  )
  return files.fsync
    ? flush(files, MAIN_FILE).pipe(
        Effect.matchEffect({ onFailure: () => Effect.void, onSuccess: () => replace }),
      )
    : replace
}
