/** Credential storage is independent of harness conversation storage. */
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Credential from './Credential.js'
import * as Schema from 'effect/Schema'
import * as Redacted from 'effect/Redacted'
import { AuthError } from './AuthError.js'

export interface Service {
  readonly read: (key: string) => Effect.Effect<Credential.Stored | undefined, AuthError>
  readonly list: Effect.Effect<ReadonlyArray<Credential.Binding>, AuthError>
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
      current: Credential.Stored | undefined,
    ) => Effect.Effect<readonly [A, Credential.Stored | undefined], E, R>,
  ) => Effect.Effect<A, E | AuthError, R>
}
export class CredentialStore extends Context.Service<CredentialStore, Service>()(
  'effect-harness/provider-auth/CredentialStore',
) {}

/** Import a fully encoded credential. Use create for newly allocated references. */
export const save = (credential: Credential.Stored) =>
  Effect.flatMap(CredentialStore, (store) =>
    store.modify(credential.binding.credentialId, () =>
      Effect.succeed([Credential.metadata(credential), credential] as const),
    ),
  )

/** Allocate an independent credential reference; never derive keys from user labels. */
export const create = Effect.fnUntraced(function* <S extends Schema.Constraint>(
  schema: S,
  secrets: S['Type'],
) {
  const binding = { credentialId: crypto.randomUUID(), identity: crypto.randomUUID() }
  const value = yield* encode(schema, secrets)
  return yield* save({ binding, value })
})

/** Encode without exposing schema issues containing secret values. */
export const encode = <S extends Schema.Constraint>(schema: S, secrets: S['Type']) =>
  Schema.encodeEffect(Schema.toCodecJson(schema), { onExcessProperty: 'error' })(secrets).pipe(
    Effect.map(Redacted.make),
    Effect.mapError(() => new AuthError({ reason: 'InvalidConfiguration' })),
  )

export const decode = <S extends Schema.Constraint>(
  schema: S,
  value: Redacted.Redacted<Schema.Json>,
) =>
  Schema.decodeEffect(Schema.toCodecJson(schema), { onExcessProperty: 'error' })(
    Redacted.value(value),
  ).pipe(Effect.mapError(() => new AuthError({ reason: 'InvalidConfiguration' })))

export const read = Effect.fnUntraced(function* <S extends Schema.Constraint>(
  binding: Credential.Binding,
  schema: S,
) {
  const store = yield* CredentialStore
  const stored = yield* store.read(binding.credentialId)
  if (stored === undefined) return yield* new AuthError({ reason: 'MissingCredential' })
  if (
    stored.binding.identity !== binding.identity ||
    stored.binding.credentialId !== binding.credentialId
  )
    return yield* new AuthError({ reason: 'AccountMismatch' })
  return yield* decode(schema, stored.value)
})

/** Removal uses the same transaction boundary as refresh. */
export const remove = (binding: Credential.Binding) =>
  Effect.flatMap(CredentialStore, (store) =>
    store.modify(binding.credentialId, (current) => {
      if (current !== undefined && current.binding.identity !== binding.identity)
        return Effect.fail(new AuthError({ reason: 'AccountMismatch' }))
      return Effect.succeed([undefined, undefined] as const)
    }),
  )
