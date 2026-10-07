import { assert, describe, it } from '@effect/vitest'
import * as ChildProcess from 'node:child_process'
import * as Fs from 'node:fs'
import * as Path from 'node:path'
import * as Os from 'node:os'
import { fileURLToPath } from 'node:url'

/** Manufactured stale output is valid JavaScript: denial must come from exports, not missing files. */
const denied = [
  'Error',
  'env/Error',
  'env/Exec',
  'env/Watch',
  'tools/Mutation',
  'tools/Path',
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

describe('NodeEnvExports', () => {
  it('resolves current emitted public concepts and denies manufactured stale/private output', () => {
    const root = fileURLToPath(new URL('../../../', import.meta.url))
    const original = Path.join(root, 'packages/harness')
    const fixture = Fs.mkdtempSync(Path.join(Os.tmpdir(), 'harness-exports-'))
    try {
      Fs.cpSync(Path.join(original, 'dist'), Path.join(fixture, 'dist'), { recursive: true })
      Fs.copyFileSync(Path.join(original, 'package.json'), Path.join(fixture, 'package.json'))
      Fs.symlinkSync(Path.join(original, 'node_modules'), Path.join(fixture, 'node_modules'), 'dir')
      for (const path of denied) {
        const target = Path.join(fixture, 'dist', path + '.js')
        Fs.mkdirSync(Path.dirname(target), { recursive: true })
        if (!path.includes('internal/') || !Fs.existsSync(target)) {
          Fs.writeFileSync(target, 'export const staleOutput = true\n')
          Fs.writeFileSync(
            target.replace(/\.js$/, '.d.ts'),
            'export declare const staleOutput: true\n',
          )
        }
        // A bare private domain is denied even when a valid index exists too.
        if (path.endsWith('internal')) {
          Fs.mkdirSync(Path.join(fixture, 'dist', path), { recursive: true })
          Fs.writeFileSync(
            Path.join(fixture, 'dist', path, 'index.js'),
            'export const staleOutput = true\n',
          )
        }
      }
      const script = `const root = await import('@effect-harness/harness'); if(!root.ToolError?.ToolError || !root.Env?.Env || root.Env.FileError !== root.FileError?.FileError || root.Env.ExecutionError !== root.ExecutionError?.ExecutionError || !root.testing?.EnvConformance || !root.tools?.CodingTools) throw new Error('missing root namespaces'); const node = await import('@effect-harness/harness/NodeEnv'); if(typeof node.make !== 'function') throw new Error('missing NodeEnv'); await import('@effect-harness/harness/NodeNativeFiles'); await import('@effect-harness/harness/env/LineScan'); await import('@effect-harness/harness/tools/EditDiff'); for(const path of ${JSON.stringify(denied)}) { try { await import('@effect-harness/harness/'+path); throw new Error('exposed '+path) } catch(error) { if(error.code !== 'ERR_PACKAGE_PATH_NOT_EXPORTED') throw error } } console.log('public-and-private-resolution-ok')`
      const child = ChildProcess.spawnSync('node', ['--input-type=module', '-e', script], {
        cwd: fixture,
        encoding: 'utf8',
      })
      assert.strictEqual(child.status, 0, child.stderr)
      assert.strictEqual(child.stdout.trim(), 'public-and-private-resolution-ok')
    } finally {
      Fs.rmSync(fixture, { recursive: true, force: true })
    }
  })
})
