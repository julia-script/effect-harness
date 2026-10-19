/**
 * The memory store: one `Ref`, so every `update` compares and sets atomically.
 */
import { Effect, Equal, Ref } from 'effect'
import type { Credential } from '../Credential.ts'
import type { Service } from '../Credentials.ts'
import { CredentialChanged } from '../CredentialsError.ts'

type Store = ReadonlyMap<string, Credential>

const without = (store: Store, provider: string): Store =>
  new Map([...store].filter(([key]) => key !== provider))

export const make = (initial: Iterable<readonly [string, Credential]>) =>
  Effect.map(Ref.make<Store>(new Map(initial)), (ref): Service => ({
    get: (provider) => Effect.map(Ref.get(ref), (store) => store.get(provider)),
    list: Effect.map(Ref.get(ref), (store) =>
      [...store].map(([provider, credential]) => ({ provider, type: credential.type })),
    ),
    set: (provider, credential) =>
      Ref.update(ref, (store) => new Map(store).set(provider, credential)),
    remove: (provider) => Ref.update(ref, (store) => without(store, provider)),
    update: (provider, expected, next) =>
      Effect.flatten(
        Ref.modify(ref, (store): readonly [Effect.Effect<void, CredentialChanged>, Store] =>
          Equal.equals(store.get(provider), expected)
            ? [Effect.void, new Map(store).set(provider, next)]
            : [
                Effect.fail(
                  new CredentialChanged({
                    message: `The ${provider} credential changed since it was read`,
                    provider,
                  }),
                ),
                store,
              ],
        ),
      ),
  }))
