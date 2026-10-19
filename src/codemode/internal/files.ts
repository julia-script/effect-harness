/**
 * The result's files, written under fixed names into the run's output directory: the recorded
 * one, else a new temp directory recorded before any file is written, so a rerun after a crash
 * writes the same paths again.
 */
import { Effect, FileSystem, Path } from 'effect'
import type * as Call from '../../tool/Call.ts'
import type { Write } from './output.ts'
import { run } from './state.ts'

export interface Files {
  readonly fs: FileSystem.FileSystem
  readonly path: Path.Path
}

/** The files services, read where the handlers are built. */
export const files = Effect.gen(function* () {
  const services: Files = { fs: yield* FileSystem.FileSystem, path: yield* Path.Path }
  return services
})

const directory = (call: Call.Service, { fs }: Files) =>
  Effect.gen(function* () {
    const recorded = (yield* call.snapshot(run, call.taskId))?.outputDir
    if (recorded !== undefined) return recorded
    const created = yield* fs.makeTempDirectory({ prefix: 'pi-codemode-' })
    return yield* call.commit((tx) =>
      Effect.map(tx.doc(run, call.taskId), (state) => {
        state.outputDir ??= created
        return state.outputDir
      }),
    )
  })

/** The writer of one call's result files; a failure is reported in the result, not raised. */
export const writer = (call: Call.Service, services: Files): Effect.Effect<Write> =>
  Effect.map(
    Effect.cached(directory(call, services)),
    (dir): Write =>
      (name, data) =>
        Effect.gen(function* () {
          const path = services.path.join(yield* dir, name)
          yield* typeof data === 'string'
            ? services.fs.writeFileString(path, data)
            : services.fs.writeFile(path, data)
          return { path }
        }).pipe(Effect.catch((error) => Effect.succeed({ error: error.message }))),
  )
