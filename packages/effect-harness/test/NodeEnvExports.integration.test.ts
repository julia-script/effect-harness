import * as NodeServices from '@effect/platform-node/NodeServices'

import { assert, describe, it } from '@effect/vitest'

import * as Context from 'effect/Context'

import * as Effect from 'effect/Effect'

import * as FileSystem from 'effect/FileSystem'

import * as Path from 'effect/Path'

import * as Layer from 'effect/Layer'

import * as Stream from 'effect/Stream'

import * as ChildProcess from 'effect/process/ChildProcess'

import * as ChildProcessSpawner from 'effect/process/ChildProcessSpawner'

import { fileURLToPath } from 'node:url'

/** Manufactured stale output is valid JavaScript: denial must come from exports, not missing files. */
const denied = [
  'internal',
  'env/internal',
  'tools/internal',
  'env/internal/exec',
  'env/internal/watch',
  'tools/internal/mutation',
  'tools/internal/path',
  'internal/nodeEnv',
  'internal/nodeNativeFiles',
]

class EmittedPackage extends Context.Service<EmittedPackage, { readonly directory: string }>()(
  'effect-harness/test/NodeEnvExports/EmittedPackage',
) {
  static layer = Layer.effect(EmittedPackage)(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem
      const path = yield* Path.Path
      const original = fileURLToPath(new URL('../', import.meta.url))
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: 'harness-exports-' })
      yield* fs.copy(path.join(original, 'dist'), path.join(directory, 'dist'))
      yield* fs.copyFile(path.join(original, 'package.json'), path.join(directory, 'package.json'))
      yield* fs.symlink(path.join(original, 'node_modules'), path.join(directory, 'node_modules'))
      for (const filePath of denied) {
        const target = path.join(directory, 'dist', filePath + '.js')
        yield* fs.makeDirectory(path.dirname(target), { recursive: true })
        if (!filePath.includes('internal/') || !(yield* fs.exists(target))) {
          yield* fs.writeFileString(target, 'export const staleOutput = true\n')
          yield* fs.writeFileString(
            target.replace(/\.js$/, '.d.ts'),
            'export declare const staleOutput: true\n',
          )
        }
        if (filePath.endsWith('internal')) {
          yield* fs.makeDirectory(path.join(directory, 'dist', filePath), { recursive: true })
          yield* fs.writeFileString(
            path.join(directory, 'dist', filePath, 'index.js'),
            'export const staleOutput = true\n',
          )
        }
      }
      return EmittedPackage.of({ directory })
    }),
  )
}

describe('NodeEnvExports', () => {
  // effect-nit-allow P8-it-live-or-withLive-for-real-time: a real child Node process resolves the emitted package exports table; TestClock and Vitest source aliases cannot drive this consumer proof.
  it.live(
    'resolves current emitted public concepts and denies manufactured stale/private output',
    () =>
      Effect.gen(function* () {
        const fixture = yield* EmittedPackage
        const spawner = yield* ChildProcessSpawner.ChildProcessSpawner
        const script = `const root = await import('effect-harness'); if(!root.ToolError?.ToolError || !root.Env?.Env || !root.FileError?.FileError || !root.ExecutionError?.ExecutionError || !root.Transcript?.make || !root.PromptPreparation?.plan || !root.ResponseAccumulator?.make || !root.ToolRegistration?.bind || !root.testing?.EnvConformance || !root.tools?.CodingTools) throw new Error('missing root namespaces'); const node = await import('effect-harness/NodeEnv'); if(typeof node.make !== 'function') throw new Error('missing NodeEnv'); await import('effect-harness/NodeNativeFiles'); await import('effect-harness/env/LineScan'); await import('effect-harness/tools/EditDiff'); for(const path of ${JSON.stringify(denied)}) { try { await import('effect-harness/'+path); throw new Error('exposed '+path) } catch(error) { if(error.code !== 'ERR_PACKAGE_PATH_NOT_EXPORTED') throw error } } console.log('public-and-private-resolution-ok')`
        const child = yield* spawner.spawn(
          ChildProcess.make('node', ['--input-type=module', '-e', script], {
            cwd: fixture.directory,
          }),
        )
        const [stdout, stderr] = yield* Effect.all(
          [
            child.stdout.pipe(
              Stream.decodeText(),
              Stream.runCollect,
              Effect.map((parts) => parts.join('')),
            ),
            child.stderr.pipe(
              Stream.decodeText(),
              Stream.runCollect,
              Effect.map((parts) => parts.join('')),
            ),
          ],
          { concurrency: 2 },
        )
        assert.strictEqual(yield* child.exitCode, 0, stderr)
        assert.strictEqual(stdout.trim(), 'public-and-private-resolution-ok')
      }).pipe(Effect.provide(EmittedPackage.layer.pipe(Layer.provideMerge(NodeServices.layer)))),
  )
})
