/** Atomic, cross-process credential collection at an explicit private file path. */
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Collection from '../internal/FileCollectionStore.js'
import * as Credential from '../provider-auth/Credential.js'
import { CredentialStore } from '../provider-auth/CredentialStore.js'
import { AuthError } from '../provider-auth/AuthError.js'
export const make = Effect.fnUntraced(function* (options: {
  readonly path: string
  readonly lockTimeoutMs?: number
}) {
  const store = yield* Collection.make(
    Credential.Stored,
    (reason) => new AuthError({ reason }),
    options,
  )
  return CredentialStore.of({
    ...store,
    list: Effect.map(store.list, (items) => items.map(Credential.metadata)),
  })
})
export const layer = (options: Parameters<typeof make>[0]) =>
  Layer.effect(CredentialStore, make(options))
