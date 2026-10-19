import { execFileSync } from 'node:child_process'
import { assert, it } from '@effect/vitest'

for (const runtime of ['node', 'bun'])
  for (const mode of ['idle', 'eviction'])
    it(`${runtime} cache ${mode} preserves natural exit and live eviction`, () => {
      const output = execFileSync(
        runtime,
        [new URL('./fixtures/SessionCache/idle.mjs', import.meta.url).pathname, mode],
        { encoding: 'utf8', timeout: 3000 },
      )
      assert.include(output, mode === 'idle' ? 'idle session ready' : 'live eviction observed')
    })

it('sweeps at commit when the host cannot unreference maintenance timers', () => {
  const output = execFileSync(
    'node',
    [new URL('./fixtures/SessionCache/restricted.mjs', import.meta.url).pathname],
    { encoding: 'utf8', timeout: 5000 },
  )
  assert.include(output, 'Restricted timers cancel or decline maintenance and sweep at commit')
})
