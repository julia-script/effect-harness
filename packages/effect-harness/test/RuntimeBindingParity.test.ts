import { assert, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as FileSystem from 'effect/FileSystem'
import * as Stream from 'effect/Stream'
import * as ChildProcess from 'effect/process/ChildProcess'
import * as ChildProcessSpawner from 'effect/process/ChildProcessSpawner'
import { run, services } from './fixtures/RuntimeBindingParity/Consumer.ts'

for (const backend of ['memory', 'sqlite', 'jsonl'] as const)
  it.live(
    `${backend}: native bindings survive create configure historical fork and host reconstruction`,
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem
          const directory = yield* fs.makeTempDirectoryScoped({ prefix: 'runtime-binding-' })
          yield* run(backend, directory, 'seed')
          if (backend !== 'memory') {
            const spawner = yield* ChildProcessSpawner.ChildProcessSpawner
            const child = (
              location: string,
              phase: 'cold' | 'prepare-denial' | 'missing' | 'version',
            ) =>
              Effect.gen(function* () {
                const handle = yield* spawner.spawn(
                  ChildProcess.make(process.execPath, [
                    new URL('./fixtures/RuntimeBindingParity/Run.mjs', import.meta.url).pathname,
                    backend,
                    location,
                    phase,
                  ]),
                )
                const [code, stdout, stderr] = yield* Effect.all(
                  [
                    handle.exitCode,
                    Stream.runCollect(handle.stdout),
                    Stream.runCollect(handle.stderr),
                  ],
                  { concurrency: 'unbounded' },
                )
                assert.equal(code, 0, Buffer.concat(stderr).toString())
                const output = Buffer.concat(stdout).toString()
                assert.include(output, `"phase":"${phase}"`)
                assert.include(output, `"host":"${'Bun' in globalThis ? 'bun' : 'node'}"`)
              })
            yield* child(directory, 'cold')
            const denied = yield* fs.makeTempDirectoryScoped({ prefix: 'runtime-denied-' })
            const extension = backend === 'sqlite' ? 'sqlite' : 'jsonl'
            yield* fs.copyFile(`${directory}/store.${extension}`, `${denied}/store.${extension}`)
            if (backend === 'jsonl')
              yield* fs.copy(`${directory}/store.jsonl.parts`, `${denied}/store.jsonl.parts`)
            yield* fs.copyFile(`${directory}/manifest.json`, `${denied}/manifest.json`)
            yield* child(denied, 'prepare-denial')
            for (const phase of ['missing', 'version'] as const) yield* child(denied, phase)
          }
        }),
      ).pipe(Effect.provide(services)),
  )
