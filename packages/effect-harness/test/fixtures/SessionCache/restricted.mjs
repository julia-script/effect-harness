import assert from 'node:assert/strict'
import { execFileSync } from 'node:child_process'
import { mkdtempSync, readFileSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { createRequire } from 'node:module'
import { join } from 'node:path'
import { runInNewContext } from 'node:vm'

const directory = mkdtempSync(join(tmpdir(), 'cache-timers-'))
try {
  const bundle = join(directory, 'probe.cjs')
  execFileSync('bun', [
    'build',
    new URL('./restricted.ts', import.meta.url).pathname,
    '--target=node',
    '--format=cjs',
    `--outfile=${bundle}`,
  ])
  for (const mode of ['numeric', 'unavailable']) {
    // This realm supplies a host timer API without changing the test process's globals.
    let next = 0
    let cancelled = 0
    let unavailable = 0
    const pending = new Map()
    const module = { exports: {} }
    runInNewContext(readFileSync(bundle, 'utf8'), {
      module,
      exports: module.exports,
      require: createRequire(import.meta.url),
      process,
      Buffer,
      setImmediate,
      clearImmediate,
      performance,
      console,
      TextEncoder,
      TextDecoder,
      AbortController,
      queueMicrotask,
      setTimeout(callback, delay) {
        const maintenance = delay > 0 && delay <= 25
        if (maintenance && mode === 'unavailable') {
          unavailable++
          throw new Error('Host does not permit this timer')
        }
        const id = ++next
        const timer = setTimeout(() => {
          pending.delete(id)
          callback()
        }, delay)
        pending.set(id, { timer, maintenance })
        return id
      },
      clearTimeout(id) {
        const timer = pending.get(id)
        if (timer?.maintenance) cancelled++
        if (timer !== undefined) clearTimeout(timer.timer)
        pending.delete(id)
      },
    })
    await module.exports.verify
    assert.ok(mode === 'numeric' ? cancelled > 0 : unavailable > 0)
    assert.equal(pending.size, 0)
  }
  console.log('Restricted timers cancel or decline maintenance and sweep at commit')
} finally {
  rmSync(directory, { recursive: true, force: true })
}
