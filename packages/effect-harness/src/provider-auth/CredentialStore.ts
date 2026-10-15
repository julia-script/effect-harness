/** Credential storage is independent of harness conversation storage. */
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Credential from './Credential.js'
import type { AuthError } from './AuthError.js'

export interface Service {
  readonly read: (key: string) => Effect.Effect<Credential.Credential | undefined, AuthError>
  readonly list: Effect.Effect<ReadonlyArray<Credential.Metadata>, AuthError>
  /**
   * Lock, reread, run the effect, persist its replacement, then unlock. Failed callbacks do not write.
   * StoreCommitUncertain means publication occurred but durability was not confirmed: reread before
   * deciding what to do next. Never assume that failure rolled back a remotely rotated token.
   * Every writer (including logout) MUST use this operation. Shared stores must lock across processes.
   * Once invoked, the callback and commit are masked from caller interruption; implementations and
   * callbacks must bound their I/O. Waiting to acquire the lock remains interruptible.
   */
  readonly modify: <A, E, R>(
    key: string,
    f: (
      current: Credential.Credential | undefined,
    ) => Effect.Effect<readonly [A, Credential.Credential | undefined], E, R>,
  ) => Effect.Effect<A, E | AuthError, R>
}
export class CredentialStore extends Context.Service<CredentialStore, Service>()(
  'effect-harness/provider-auth/CredentialStore',
) {}

/** Explicit login/import operation; callers must assign a fresh binding identity on replacement. */
export const save = (credential: Credential.Credential) =>
  Effect.flatMap(CredentialStore, (store) =>
    store.modify(Credential.key(credential.binding), () =>
      Effect.succeed([Credential.metadata(credential), credential] as const),
    ),
  )

/** Local logout fences refresh; it cannot retract a request already accepted by the provider. */
export const logout = (binding: Pick<Credential.Binding, 'provider' | 'profile'>) =>
  Effect.flatMap(CredentialStore, (store) =>
    store.modify(Credential.key(binding), () => Effect.succeed([undefined, undefined] as const)),
  )
