import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as AuthError from 'effect-harness/auth/AuthError'

const cases = [
  [AuthError.AuthStorageError, AuthError.isAuthStorageError],
  [AuthError.AuthBusyError, AuthError.isAuthBusyError],
  [AuthError.AuthConfigurationError, AuthError.isAuthConfigurationError],
  [AuthError.AuthMissingError, AuthError.isAuthMissingError],
  [AuthError.AuthCallbackError, AuthError.isAuthCallbackError],
  [AuthError.AuthDeniedError, AuthError.isAuthDeniedError],
  [AuthError.AuthTokenError, AuthError.isAuthTokenError],
  [AuthError.AuthNetworkError, AuthError.isAuthNetworkError],
  [AuthError.AuthIdentityError, AuthError.isAuthIdentityError],
  [AuthError.AuthPermissionError, AuthError.isAuthPermissionError],
  [AuthError.AuthExpiredError, AuthError.isAuthExpiredError],
  [AuthError.AuthProtocolError, AuthError.isAuthProtocolError],
] as const

describe('AuthErrorGuards', () => {
  it.effect('decoded reason guards distinguish every native leaf after JSON hydration', () =>
    Effect.gen(function* () {
      const codec = Schema.toCodecJson(AuthError.AuthErrorReason)
      for (const [Reason, guard] of cases) {
        const reason = new Reason({ message: 'guarded reason' })
        const restored = yield* Schema.decodeEffect(codec)(
          yield* Schema.encodeEffect(codec)(reason),
        )
        assert.isTrue(guard(reason))
        assert.isTrue(guard(restored))
        assert.isTrue(AuthError.isAuthErrorReason(restored))
        for (const [OtherReason, otherGuard] of cases)
          assert.strictEqual(otherGuard(restored), OtherReason === Reason)
        for (const u of [undefined, null, 0, 'foreign error', {}]) assert.isFalse(guard(u))
        const wrapped = new AuthError.AuthError({ reason: restored })
        assert.isTrue(AuthError.isAuthError(wrapped))
        assert.isFalse(AuthError.isAuthError(reason))
      }
    }),
  )
})
