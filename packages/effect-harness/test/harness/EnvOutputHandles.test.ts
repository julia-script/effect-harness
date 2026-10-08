import { assert, describe, it } from '@effect/vitest'
import * as Output from 'effect-harness/Output'
import * as Env from 'effect-harness/Env'
import * as Stream from 'effect/Stream'

describe('EnvOutputHandles', () => {
  it('handle prototypes preserve frozen live getters and own writable chunk storage', () => {
    const source: Output.Buffer['chunks'] = [{ text: 'a', bytes: 1, newlines: 0 }]
    const original = Output.make()
    let current = 'native' as Env.Watcher['mode']
    let reads = 0
    const watcher = Env.makeWatcher(
      Object.freeze({
        get mode() {
          reads++
          return current
        },
        changes: Stream.empty,
      }),
    )
    assert.strictEqual(reads, 0)
    current = 'polling'
    assert.strictEqual(
      watcher.pipe((self) => self.mode),
      'polling',
    )
    assert.deepStrictEqual(watcher.toJSON(), { _id: '@effect-harness/harness/Env/Watcher' })
    const { chunks: _chunks, ...input } = original
    let limitReads = 0
    let chunkReads = 0
    let limits = input.limits
    const buffer = Output.makeBuffer(
      Object.freeze({
        ...input,
        get limits() {
          limitReads++
          return limits
        },
        get chunks() {
          chunkReads++
          return source
        },
      }),
    )
    assert.strictEqual(limitReads, 0)
    assert.strictEqual(chunkReads, 1)
    limits = { ...limits, maxBytes: 7 }
    assert.strictEqual(buffer.limits.maxBytes, 7)
    assert.strictEqual(limitReads, 1)
    buffer.started = true
    buffer.totalBytes = 42
    assert.strictEqual(buffer.started, true)
    assert.strictEqual(buffer.totalBytes, 42)
    buffer.chunks.push({ text: 'b', bytes: 1, newlines: 0 })
    assert.deepStrictEqual(source, [{ text: 'a', bytes: 1, newlines: 0 }])
    assert.strictEqual(
      buffer.pipe((self) => self.chunks.length),
      2,
    )
    assert.deepStrictEqual(buffer.toJSON(), { _id: '@effect-harness/harness/Output/Buffer' })
    assert.ok(!JSON.stringify(buffer).includes('decoder'))
  })
})
