import { assert, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import { execFileSync } from 'node:child_process'
import { fileURLToPath } from 'node:url'
import { tmpdir } from 'node:os'

for (const mode of ['portable', 'missing']) {
  it.effect(`keeps the strict JPEG codec boundary portable (${mode})`, () =>
    Effect.gen(function* () {
      const result = yield* Effect.try(() =>
        execFileSync(
          'node',
          [
            fileURLToPath(
              new URL('./fixtures/images-strict/PortableBoundary.mjs', import.meta.url),
            ),
            mode,
          ],
          { cwd: tmpdir(), encoding: 'utf8' },
        ),
      )
      assert.include(result, mode === 'portable' ? 'without Node' : 'fails semantically')
    }),
  )
}
