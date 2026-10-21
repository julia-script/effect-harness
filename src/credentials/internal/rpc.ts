/**
 * A remote store: `CredentialStore` over `CredentialsRpcs`, and the handlers that serve a store.
 */
import { Effect } from 'effect'
import { RpcClient } from 'effect/rpc'
import type { CredentialStore } from '../CredentialStore.ts'
import type { Service } from '../CredentialStore.ts'
import { CredentialsRpcs } from '../CredentialsRpcs.ts'
import { CredentialsError } from '../CredentialsError.ts'

/** A transport failure is a store failure: the store could not be reached. */
const unreachable = <A, E>(effect: Effect.Effect<A, E | RpcClientError>) =>
  Effect.catchTag(effect, 'RpcClientError', (cause) =>
    Effect.fail(new CredentialsError({ message: 'The credential store is unreachable', cause })),
  )

type RpcClientError = import('effect/rpc/RpcClientError').RpcClientError

export const makeClient = Effect.map(RpcClient.make(CredentialsRpcs), (rpcs): Service => ({
  get: (provider) => Effect.map(unreachable(rpcs.Get({ provider })), (found) => found ?? undefined),
  list: unreachable(rpcs.List()),
  set: (provider, credential) => unreachable(rpcs.Set({ provider, credential })),
  remove: (provider) => unreachable(rpcs.Remove({ provider })),
  update: (provider, expected, next) => unreachable(rpcs.Update({ provider, expected, next })),
}))

/** Handlers over the store `tag`; `CredentialStore` imports this module, so it passes itself. */
export const handlers = (tag: typeof CredentialStore) =>
  CredentialsRpcs.toLayer(
    Effect.map(tag, (store) =>
      CredentialsRpcs.of({
        Get: ({ provider }) => Effect.map(store.get(provider), (found) => found ?? null),
        List: () => store.list,
        Set: ({ provider, credential }) => store.set(provider, credential),
        Remove: ({ provider }) => store.remove(provider),
        Update: ({ provider, expected, next }) => store.update(provider, expected, next),
      }),
    ),
  )
