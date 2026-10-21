/**
 * A store over a `KeyValueStore`: every credential under one key, as JSON, the way pi keeps
 * `auth.json`. Secrets are written as plain strings, so the store's own place must be private.
 * Writes run one at a time, so `update` compares and sets atomically within the process.
 */
import { Effect, Equal, Option, Schema, Semaphore } from 'effect'
import { KeyValueStore } from 'effect/persistence'
import { Credential, Info } from '../Credential.ts'
import type { Service } from '../CredentialStore.ts'
import { CredentialChanged, CredentialsError } from '../CredentialsError.ts'

const Stored = Schema.Record(Schema.String, Credential)
type Stored = typeof Stored.Type

const failed = (cause: unknown) =>
  new CredentialsError({ message: 'The credential store could not be read or written', cause })

export const make = Effect.fnUntraced(function* (key: string) {
  const store = KeyValueStore.toSchemaStore(yield* KeyValueStore.KeyValueStore, Stored)
  const line = yield* Semaphore.make(1)
  const read = store
    .get(key)
    .pipe(Effect.map(Option.getOrElse((): Stored => ({}))), Effect.mapError(failed))
  const write = (next: Stored) => Effect.mapError(store.set(key, next), failed)
  /** Read, change and write under the write lock; `change` may fail instead. */
  const modify = <E>(change: (stored: Stored) => Effect.Effect<Stored, E>) =>
    Semaphore.withPermit(line, Effect.flatMap(Effect.flatMap(read, change), write))
  const service: Service = {
    get: (provider) => Effect.map(read, (stored) => stored[provider]),
    list: Effect.map(read, (stored) =>
      Object.entries(stored).map(([provider, credential]) =>
        Info.make({ provider, type: credential._tag }),
      ),
    ),
    set: (provider, credential) =>
      modify((stored) => Effect.succeed({ ...stored, [provider]: credential })),
    remove: (provider) =>
      modify((stored) =>
        Effect.succeed(
          Object.fromEntries(Object.entries(stored).filter(([name]) => name !== provider)),
        ),
      ),
    update: (provider, expected, next) =>
      modify((stored) =>
        Equal.equals(stored[provider], expected)
          ? Effect.succeed({ ...stored, [provider]: next })
          : Effect.fail(
              new CredentialChanged({
                message: `The ${provider} credential changed since it was read`,
                provider,
              }),
            ),
      ),
  }
  return service
})
