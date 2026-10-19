/**
 * The client barrel must not load what the loop runs: following its value imports (type-only
 * imports are erased) reaches no internal module and none of the runtime services.
 */
import { describe, expect, test } from 'bun:test'
import { readFileSync } from 'node:fs'
import { dirname, relative, resolve } from 'node:path'

const SRC = resolve(import.meta.dir, '../..')

/** Relative modules `file` loads at runtime. */
const valueImports = (file: string): ReadonlyArray<string> => {
  const source = readFileSync(file, 'utf8')
  const statements = source.matchAll(/^(?:import|export)\s+(type\s+)?[^'"]*?from\s+'(\.[^']+)'/gmu)
  return [...statements]
    .filter((match) => match[1] === undefined)
    .map((match) => resolve(dirname(file), match[2] ?? ''))
}

const reachable = (entry: string): ReadonlySet<string> => {
  const seen = new Set<string>()
  const pending = [entry]
  for (let file = pending.pop(); file !== undefined; file = pending.pop()) {
    if (seen.has(file)) continue
    seen.add(file)
    pending.push(...valueImports(file))
  }
  return new Set([...seen].map((file) => relative(SRC, file)))
}

const RUNTIME = new Set([
  'session/Session.ts',
  'session/Document.ts',
  'storage/Storage.ts',
  'task/Scheduler.ts',
  'task/Registry.ts',
  'harness/Harness.ts',
])

/**
 * Internal modules a client may load: the pure replay of op batches behind `Ops.apply`, and the
 * client's own watch streams.
 */
const PURE = new Set([
  'delta/internal/apply.ts',
  'delta/internal/json.ts',
  'delta/internal/ops.ts',
  'client/internal/watch.ts',
])

describe('client boundary', () => {
  test('loads no internal module and no runtime service', () => {
    const loaded = [...reachable(resolve(SRC, 'client/index.ts'))]
    expect(loaded.filter((file) => file.includes('/internal/') && !PURE.has(file))).toEqual([])
    expect(loaded.filter((file) => RUNTIME.has(file))).toEqual([])
    expect(loaded).toContain('client/HarnessRpcs.ts')
  })
})
