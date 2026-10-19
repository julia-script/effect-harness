/**
 * What the coding tools run on: the file system, paths, and processes, with pi's path repairs
 * and its queue of file mutations.
 */
import { Effect, FileSystem, Option, Path, Semaphore } from 'effect'
import { ImageProcessor } from '../ImageProcessor.ts'
import { ChildProcessSpawner } from 'effect/process/ChildProcessSpawner'
import type { Config } from '../CodingTools.ts'

export interface Workspace {
  readonly fs: FileSystem.FileSystem
  readonly path: Path.Path
  readonly spawner: ChildProcessSpawner['Service']
  /** Run `mutate` alone among the mutations of the file at canonical path `key`. */
  readonly mutations: Map<string, Semaphore.Semaphore>
  /** The image processor in context, if any. */
  readonly images: Option.Option<ImageProcessor['Service']>
}

export const make = Effect.gen(function* () {
  const workspace: Workspace = {
    fs: yield* FileSystem.FileSystem,
    path: yield* Path.Path,
    spawner: yield* ChildProcessSpawner,
    mutations: new Map(),
    images: yield* Effect.serviceOption(ImageProcessor),
  }
  return workspace
})

const UNICODE_SPACES = /[  -   　]/gu
const NARROW_NO_BREAK_SPACE = ' '

/** The working directory of a call's config. */
export const cwdOf = (workspace: Workspace, config: Config) =>
  workspace.path.resolve(config.cwd ?? '.')

/** `path` against the call's working directory, with odd spaces and a leading `@` repaired. */
export const resolve = (workspace: Workspace, config: Config, path: string): string => {
  const normalized = path.replaceAll(UNICODE_SPACES, ' ')
  const repaired = normalized.startsWith('@') ? normalized.slice(1) : normalized
  return workspace.path.resolve(cwdOf(workspace, config), repaired)
}

/**
 * `resolve`, then the first variant of it that exists: macOS screenshot names use a narrow
 * no-break space before AM/PM, and some file systems keep names decomposed or with curly quotes.
 */
export const resolveExisting = (workspace: Workspace, config: Config, path: string) =>
  Effect.gen(function* () {
    const resolved = resolve(workspace, config, path)
    const variants = new Set([
      resolved,
      resolved.replaceAll(/ (AM|PM)\./giu, `${NARROW_NO_BREAK_SPACE}$1.`),
      resolved.normalize('NFD'),
      resolved.replaceAll("'", '’'),
      resolved.normalize('NFD').replaceAll("'", '’'),
    ])
    for (const variant of variants) {
      if (yield* Effect.orElseSucceed(workspace.fs.exists(variant), () => false)) return variant
    }
    return resolved
  })

/**
 * The canonical path of `absolute`; for a file that does not exist yet, its canonical parent with
 * its name, so a `write` that creates a file and a later `edit` of it share one key.
 */
const canonical = (workspace: Workspace, absolute: string): Effect.Effect<string> =>
  Effect.gen(function* () {
    const real = yield* Effect.option(workspace.fs.realPath(absolute))
    if (Option.isSome(real)) return real.value
    const parent = workspace.path.dirname(absolute)
    if (parent === absolute) return absolute
    return workspace.path.join(
      yield* canonical(workspace, parent),
      workspace.path.basename(absolute),
    )
  })

/**
 * Serialize `edit` and `write` mutations of one file within this layer, by canonical path; other
 * files never wait. Not a lock against `bash` or other processes.
 */
export const withMutation = <A, E>(
  workspace: Workspace,
  absolute: string,
  mutate: Effect.Effect<A, E>,
): Effect.Effect<A, E> =>
  Effect.flatMap(canonical(workspace, absolute), (key) => {
    const existing = workspace.mutations.get(key)
    const semaphore = existing ?? Semaphore.makeUnsafe(1)
    if (existing === undefined) workspace.mutations.set(key, semaphore)
    return Semaphore.withPermit(semaphore, mutate)
  })
