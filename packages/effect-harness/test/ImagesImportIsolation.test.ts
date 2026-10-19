import { assert, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import { execFileSync } from 'node:child_process'
import { fileURLToPath } from 'node:url'
import { tmpdir } from 'node:os'

for (const file of ['Isolation.mjs', 'RuntimeOutside.mjs']) {
  it.effect(`runs ${file} from outside the repository`, () =>
    Effect.gen(function* () {
      const result = yield* Effect.try(() =>
        execFileSync(
          'node',
          [fileURLToPath(new URL(`./fixtures/images-native/${file}`, import.meta.url))],
          { cwd: tmpdir(), encoding: 'utf8' },
        ),
      )
      assert.include(result, file === 'Isolation.mjs' ? 'without sharp' : 'outside repository cwd')
    }),
  )
}
