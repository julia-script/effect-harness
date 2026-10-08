import { constFalse, constUndefined } from 'effect/Function'
import * as Config from 'effect/Config'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Option from 'effect/Option'
import * as Path from 'effect/Path'
import * as NodeResourceAdapter from './NodeResourceAdapter.ts'
import * as NodeNativeFiles from '../NodeNativeFiles.ts'
import type { ShellConfiguration, WatchTarget } from '../Env.ts'
import { ExecutionError, ExecutionShellUnavailableError } from '../ExecutionError.ts'

import type { Host } from '../NodeEnv.ts'
export const makeShellResolver = Effect.fnUntraced(function* (host: Host) {
  const fs = yield* FileSystem.FileSystem
  const path = yield* Path.Path
  return Effect.fnUntraced(function* (
    custom?: string,
  ): Effect.fn.Return<ShellConfiguration, ExecutionError> {
    // Discovery needs a fresh access observation for each candidate; platform access has no bulk operation.
    const exists = (value: string) =>
      fs.access(value).pipe(Effect.as(true), Effect.orElseSucceed(constFalse))
    const configuration = (program: string): ShellConfiguration => {
      const normalized = program.replace(/\//g, '\\').toLowerCase()
      if (/^[a-z]:\\windows\\(?:system32|sysnative)\\bash\.exe$/.test(normalized))
        return { program, args: ['-s'], commandOnStdin: true }
      return { program, args: ['-c'] }
    }
    if (custom !== undefined) {
      if (yield* exists(custom)) return configuration(custom)
      return yield* new ExecutionError({
        reason: new ExecutionShellUnavailableError({
          message: `Custom shell path not found: ${custom}`,
        }),
      })
    }
    const configFailure = (cause: Config.ConfigError) =>
      new ExecutionError({
        reason: new ExecutionShellUnavailableError({
          message: `Shell discovery configuration failed: ${cause.message}`,
          cause,
        }),
      })
    const candidates: Array<string> = []
    if (host.platform === 'win32') {
      for (const key of ['ProgramFiles', 'ProgramFiles(x86)']) {
        const base = yield* Config.option(Config.String(key)).pipe(Effect.mapError(configFailure))
        if (Option.isSome(base)) candidates.push(path.join(base.value, 'Git', 'bin', 'bash.exe'))
      }
    } else candidates.push('/bin/bash')
    const searchPath = yield* Config.String('PATH').pipe(
      Config.withDefault(''),
      Effect.mapError(configFailure),
    )
    for (const directory of searchPath.split(host.searchPathDelimiter))
      candidates.push(path.join(directory, host.platform === 'win32' ? 'bash.exe' : 'bash'))
    for (const candidate of candidates)
      if (yield* exists(candidate)) return configuration(candidate)
    if (host.platform === 'win32')
      return yield* new ExecutionError({
        reason: new ExecutionShellUnavailableError({
          message: 'No Bash shell is available; install Git Bash or configure a shell path',
        }),
      })
    return { program: 'sh', args: ['-c'] }
  })
})
const unreliableFileSystems = new Set([
  0x6969, 0x517b, 0xff534d42, 0xfe534d42, 0x65735546, 0x01021997, 0x0bd00bd0, 0x47504653,
  0x00c36400, 0x5346414f, 0x6b414653, 0x5dca2df5,
])
/** Windows and Linux network/FUSE mounts require polling to see remote changes. */
export const makeWatchMode = Effect.fnUntraced(function* (platform: string) {
  const adapter = yield* NodeResourceAdapter.adapter
  const path = yield* Path.Path
  return Effect.fnUntraced(function* (
    targets: ReadonlyArray<WatchTarget>,
  ): Effect.fn.Return<'native' | 'polling'> {
    if (platform === 'win32') return 'polling'
    if (platform !== 'linux' && platform !== 'android') return 'native'
    for (const target of targets) {
      let candidate = target.path
      while (true) {
        const stat = yield* Effect.tryPromise({
          // The current ancestor mount capability is sampled independently; batching across ancestors changes fallback semantics.
          try: () => adapter.statfs(candidate),
          catch: (cause) => NodeNativeFiles.fileError(cause, candidate),
          // statfs is a best-effort capability probe: any unavailable probe climbs to an ancestor.
        }).pipe(Effect.orElseSucceed(constUndefined))
        if (stat !== undefined) {
          if (unreliableFileSystems.has(stat.type)) return 'polling'
          break
        }
        const parent = path.dirname(candidate)
        if (parent === candidate) break
        candidate = parent
      }
    }
    return 'native'
  })
})
