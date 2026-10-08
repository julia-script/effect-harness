import { BunServices } from '@effect/platform-bun'
import { fileURLToPath } from 'node:url'
import { delimiter } from 'node:path'
import { assert, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Path from 'effect/Path'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as ChildProcess from 'effect/process/ChildProcess'
import { ChildProcessSpawner } from 'effect/process/ChildProcessSpawner'
import { Result } from '../dist/Portable.js'

const entrypoint = fileURLToPath(new URL('../dist/PortableMain.js', import.meta.url))
const nodeRunner = fileURLToPath(new URL('./fixtures/NodeRuntime.mjs', import.meta.url))
const browserRunner = fileURLToPath(new URL('./fixtures/BrowserRuntime.mjs', import.meta.url))
class MissingNodeError extends Schema.TaggedError<MissingNodeError>()('MissingNodeError', {
  message: Schema.String,
}) {}
const nodeExecutable = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem
  const path = yield* Path.Path
  const host = yield* fs.realPath(process.execPath)
  for (const directory of (process.env.PATH ?? '').split(delimiter)) {
    const candidate = path.join(directory, process.platform === 'win32' ? 'node.exe' : 'node')
    if (!(yield* fs.exists(candidate))) continue
    const executable = yield* fs.realPath(candidate)
    // Bun's lifecycle PATH can put a symlink to Bun ahead of the installed Node binary.
    if (process.versions.bun !== undefined && executable === host) continue
    return executable
  }
  return yield* new MissingNodeError({
    message: 'Node is required for the runtime portability checks',
  })
})
const execute = Effect.fn('portability.execute')(function* (
  command: string,
  args: ReadonlyArray<string>,
) {
  const spawner = yield* ChildProcessSpawner
  const child = yield* spawner.spawn(ChildProcess.make(command, args))
  const [stdout, stderr, exit] = yield* Effect.all(
    [
      child.stdout.pipe(
        Stream.decodeText,
        Stream.runCollect,
        Effect.map((parts) => parts.join('')),
      ),
      child.stderr.pipe(
        Stream.decodeText,
        Stream.runCollect,
        Effect.map((parts) => parts.join('')),
      ),
      child.exitCode,
    ],
    { concurrency: 'unbounded' },
  )
  assert.strictEqual(exit, 0, stderr)
  return stdout
})
const verify = Effect.fn('portability.verify')(function* (output: string) {
  const result = yield* Schema.decodeEffect(Schema.fromJsonString(Result))(
    output.trim().replace(/^portable: /, ''),
  )
  assert.include(result.imports, 'Harness')
  assert.include(result.imports, 'Memory')
  assert.notInclude(result.imports, 'SqliteBun')
  assert.strictEqual(result.forks.concurrentRequests, 2)
  assert.isTrue(result.extensions.firstWriterWins)
  assert.isTrue(result.subagent.reusedSubmission)
  assert.strictEqual(result.checkout.decline.status, 'failed')
  assert.strictEqual(result.reminder.recovered.status, 'completed')
  assert.isTrue(result.documents.atomicCommit)
  assert.isTrue(result.multiplayer.lateClientSawActiveWork)
})

it.live('runs the published package and portable tour under Node without Bun', () =>
  Effect.scoped(
    nodeExecutable.pipe(
      Effect.flatMap((node) => execute(node, [nodeRunner, entrypoint])),
      Effect.flatMap(verify),
    ),
  ).pipe(Effect.provide(BunServices.layer)),
)

it.live('bundles portable imports for browsers and runs without host runtime globals', () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem
      const path = yield* Path.Path
      const node = yield* nodeExecutable
      const folder = yield* fs.makeTempDirectoryScoped({ prefix: 'harness-portable-browser-' })
      const bundle = path.join(folder, 'portable.js')
      yield* execute('bun', [
        'build',
        '--target=browser',
        '--minify',
        `--outfile=${bundle}`,
        entrypoint,
      ])
      yield* execute(node, ['--experimental-vm-modules', browserRunner, bundle]).pipe(
        Effect.flatMap(verify),
      )
    }),
  ).pipe(Effect.provide(BunServices.layer)),
)
