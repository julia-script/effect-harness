/** Temporary, process-local provider credential storage. */
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Semaphore from 'effect/Semaphore'
import * as Credential from './Credential.js'
import { CredentialStore } from './CredentialStore.js'

/** Temporary, process-local storage. Share this service instance to coordinate refreshers. */
export const make = Effect.gen(function* () {
  const lock = yield* Semaphore.make(1)
  const values = new Map<string, Credential.Credential>()
  return CredentialStore.of({
    read: (key) => Effect.sync(() => values.get(key)),
    list: Effect.sync(() => [...values.values()].map(Credential.metadata)),
    modify: (key, f) =>
      lock.withPermits(1)(
        Effect.uninterruptible(
          Effect.gen(function* () {
            const [result, next] = yield* f(values.get(key))
            if (next === undefined) values.delete(key)
            else values.set(key, next)
            return result
          }),
        ),
      ),
  })
})
export const layer = Layer.effect(CredentialStore, make)
