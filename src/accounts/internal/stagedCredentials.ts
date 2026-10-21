/** A sign-in sees only its own, disposable credential slot; durable writes happen at commit. */
import { Effect, Equal } from 'effect'
import type { Credential } from '../../credentials/Credential.ts'
import type { Service } from '../../credentials/CredentialStore.ts'
import { CredentialChanged, CredentialsError } from '../../credentials/CredentialsError.ts'

export interface Staged {
  readonly service: Service
  readonly current: () => Credential | undefined
  readonly close: () => void
}

export const make = (name: string): Staged => {
  let credential: Credential | undefined
  let closed = false
  const allowed = (provider: string) =>
    !closed && provider === name
      ? Effect.void
      : Effect.fail(new CredentialsError({ message: 'The sign-in credential slot is unavailable' }))
  const service: Service = {
    get: (provider) =>
      Effect.suspend(() =>
        Effect.andThen(
          allowed(provider),
          Effect.sync(() => credential),
        ),
      ),
    list: Effect.sync(() =>
      closed || credential === undefined ? [] : [{ provider: name, type: credential.type }],
    ),
    set: (provider, next) =>
      Effect.suspend(() =>
        Effect.andThen(
          allowed(provider),
          Effect.sync(() => {
            credential = next
          }),
        ),
      ),
    remove: (provider) =>
      Effect.suspend(() =>
        Effect.andThen(
          allowed(provider),
          Effect.sync(() => {
            credential = undefined
          }),
        ),
      ),
    update: (provider, expected, next) =>
      Effect.suspend(() =>
        Effect.andThen(
          allowed(provider),
          Effect.suspend(() => {
            if (!Equal.equals(credential, expected)) {
              return Effect.fail(
                new CredentialChanged({
                  provider: name,
                  message: 'The staged credential changed since it was read',
                }),
              )
            }
            credential = next
            return Effect.void
          }),
        ),
      ),
  }
  return {
    service,
    current: () => credential,
    close: () => {
      closed = true
      credential = undefined
    },
  }
}
