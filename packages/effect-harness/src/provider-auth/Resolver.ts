/** Fresh credential resolution for each network attempt, including recovered requests. */
import * as Clock from 'effect/Clock'
import * as Effect from 'effect/Effect'
import * as Fiber from 'effect/Fiber'
import * as Credential from './Credential.js'
import { CredentialStore, decode, encode } from './CredentialStore.js'
import { AuthError } from './AuthError.js'

export interface Options {
  readonly binding: Credential.Binding
  /** Default five minutes. Zero refreshes only expired credentials. */
  readonly minimumValidityMs?: number
  /** Default 15 seconds; must be finite and positive. */
  readonly refreshTimeoutMs?: number
  /** Explicit external-refresh mode: expired credentials fail without refresh. */
  readonly refresh?:
    | ((credential: Credential.OAuth) => Effect.Effect<Credential.OAuth, AuthError>)
    | undefined
}
export const make = Effect.fnUntraced(function* (options: Options) {
  const store = yield* CredentialStore
  const minimum = options.minimumValidityMs ?? 300_000
  const timeout = options.refreshTimeoutMs ?? 15_000
  if (
    !Number.isSafeInteger(minimum) ||
    minimum < 0 ||
    !Number.isSafeInteger(timeout) ||
    timeout <= 0
  )
    return yield* new AuthError({ reason: 'InvalidConfiguration' })
  const validate = (value: Credential.Stored | undefined) => {
    if (value === undefined) return Effect.fail(new AuthError({ reason: 'MissingCredential' }))
    if (
      value.binding.identity !== options.binding.identity ||
      value.binding.credentialId !== options.binding.credentialId
    )
      return Effect.fail(new AuthError({ reason: 'AccountMismatch' }))
    return decode(Credential.OAuth, value.value)
  }
  const fresh = (value: Credential.OAuth) =>
    Effect.map(
      Clock.currentTimeMillis,
      (now) => value.expiresAt > now + (options.refresh === undefined ? 0 : minimum),
    )
  const resolve = Effect.gen(function* () {
    const key = options.binding.credentialId
    const current = yield* validate(yield* store.read(key))
    if (yield* fresh(current)) return current
    return yield* store.modify(key, (value) =>
      Effect.gen(function* () {
        const latest = yield* validate(value)
        if (yield* fresh(latest)) return [latest, value] as const
        if (options.refresh === undefined)
          return yield* new AuthError({ reason: 'ExpiredCredential' })
        const worker = yield* options.refresh(latest).pipe(
          Effect.timeoutOrElse({
            duration: timeout,
            orElse: () => Effect.fail(new AuthError({ reason: 'Timeout' })),
          }),
          Effect.interruptible,
          Effect.forkDetach,
        )
        const next = yield* Fiber.join(worker)
        if (next.accountId !== latest.accountId)
          return yield* new AuthError({ reason: 'AccountMismatch' })
        if (next.expiresAt <= (yield* Clock.currentTimeMillis))
          return yield* new AuthError({ reason: 'InvalidResponse' })
        return [
          next,
          { binding: options.binding, value: yield* encode(Credential.OAuth, next) },
        ] as const
      }),
    )
  })
  return { resolve }
})
