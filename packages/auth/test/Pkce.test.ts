import * as BunCrypto from '@effect/platform-bun/BunCrypto'
import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Redacted from 'effect/Redacted'
import * as Pkce from '@effect-harness/auth/Pkce'

describe('Pkce', () => {
  it.effect('PKCE uses secure fresh state/nonce and a 43-character S256 verifier', () =>
    Effect.gen(function* () {
      const first = yield* Pkce.make
      const second = yield* Pkce.make
      assert.match(Redacted.value(first.verifier), /^[a-zA-Z0-9_-]{43}$/)
      assert.match(first.challenge, /^[a-zA-Z0-9_-]{43}$/)
      assert.notStrictEqual(first.state, second.state)
      assert.notStrictEqual(first.nonce, first.state)
      assert.isFalse(JSON.stringify(first).includes(Redacted.value(first.verifier)))
    }).pipe(Effect.provide(BunCrypto.layer)),
  )
})
