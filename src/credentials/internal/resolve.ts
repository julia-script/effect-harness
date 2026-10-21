/**
 * Fresh credentials for each request: OAuth tokens near expiry are refreshed and written back
 * with a compare-and-set. Refreshes of one provider run one at a time in this resolver, so
 * concurrent requests here refresh once; a writer elsewhere that wins the race leaves its
 * tokens, which are read back instead.
 */
import { Clock, Duration, Effect, Semaphore } from 'effect'
import type { Credential, OAuth } from '../Credential.ts'
import { CredentialStore } from '../CredentialStore.ts'
import type { ResolveOptions, Service } from '../CredentialStore.ts'
import type { CredentialsError } from '../CredentialsError.ts'

const DEFAULT_MIN_VALIDITY = Duration.minutes(5)

/** A credential to use as it is, or OAuth tokens to refresh first. */
type Found = { readonly stored: Credential | undefined } | { readonly expiring: OAuth }

const expiring = (credential: OAuth, now: number, minValidity: number) =>
  credential.expires - now < minValidity

export const make = <E>(options: ResolveOptions<E>) =>
  Effect.map(CredentialStore, (store: Service) => {
    const minValidity = Duration.toMillis(
      Duration.fromInputUnsafe(options.minValidity ?? DEFAULT_MIN_VALIDITY),
    )
    const locks = new Map<string, Semaphore.Semaphore>()
    const lock = (provider: string) => {
      const found = locks.get(provider) ?? Semaphore.makeUnsafe(1)
      locks.set(provider, found)
      return found
    }
    /** The stored credential, or `expiring` when it is OAuth and expires within `minValidity`. */
    const read = (provider: string): Effect.Effect<Found, CredentialsError> =>
      Effect.flatMap(store.get(provider), (stored) =>
        stored?._tag === 'OAuth'
          ? Effect.map(Clock.currentTimeMillis, (now): Found =>
              expiring(stored, now, minValidity) ? { expiring: stored } : { stored },
            )
          : Effect.succeed<Found>({ stored }),
      )
    /** Refresh under the provider's lock; a refresh that finished meanwhile is read back. */
    const refresh = (provider: string) =>
      Semaphore.withPermit(
        lock(provider),
        Effect.flatMap(read(provider), (found) =>
          'stored' in found
            ? Effect.succeed(found.stored)
            : Effect.flatMap(options.refresh(provider, found.expiring), (next) =>
                store.update(provider, found.expiring, next).pipe(
                  Effect.as<Credential | undefined>(next),
                  Effect.catchTag('CredentialChanged', () => store.get(provider)),
                ),
              ),
        ),
      )
    return (provider: string) =>
      Effect.flatMap(read(provider), (found) =>
        'stored' in found ? Effect.succeed(found.stored) : refresh(provider),
      )
  })
