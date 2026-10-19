import * as Effect from 'effect/Effect'
import type * as Path from 'effect/Path'
import type * as ToolkitFileSystem from '../ToolkitFileSystem.js'

/** Pure path binding; the adapter and Path must describe the same backend namespace. */
export const binding = (path: Path.Path, options: ToolkitFileSystem.Options) => {
  const root = path.resolve(options.root)
  const resolve = (value: string) => {
    const resolved = path.resolve(root, value)
    const relative = path.relative(root, resolved)
    return {
      resolved,
      allowed:
        options.pathPolicy !== 'within-root' ||
        !(relative === '..' || relative.startsWith(`..${path.sep}`) || path.isAbsolute(relative)),
    }
  }
  return { root, resolve }
}

export const validPath = (value: string) => value.length > 0 && !value.includes('\0')

/** Native error inspection stays at the foreign boundary, without executable casts. */
export const nativeCode = (cause: unknown): string | undefined => {
  if (
    typeof cause === 'object' &&
    cause !== null &&
    'code' in cause &&
    typeof cause.code === 'string'
  )
    return cause.code
  return undefined
}

export const nativeMessage = (cause: unknown): string =>
  cause instanceof Error ? cause.message : String(cause)

/** Waiting for a native promise is interruptible; resource acquisition uses acquireRelease. */
export const native = <A, E>(
  operation: () => Promise<A>,
  fail: (cause: unknown) => E,
): Effect.Effect<A, E> => Effect.tryPromise({ try: operation, catch: fail })
