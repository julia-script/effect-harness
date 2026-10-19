/** Transactional public-instance storage, independent of credential and conversation stores. */
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Semaphore from 'effect/Semaphore'
import type * as ProviderInstance from './ProviderInstance.js'
import type { ProviderError } from './ProviderError.js'
import type * as Collection from './internal/FileCollectionStore.js'

/** All writers must serialize read/callback/commit; uncertain commits must be reported explicitly. */
export interface Service extends Collection.Service<ProviderInstance.Stored, ProviderError> {}
export class ProviderStore extends Context.Service<ProviderStore, Service>()(
  'effect-harness/ProviderStore',
) {}
export const makeMemory = Effect.gen(function* () {
  const lock = yield* Semaphore.make(1)
  const values = new Map<string, ProviderInstance.Stored>()
  return ProviderStore.of({
    read: (key) =>
      Effect.sync(() => {
        const value = values.get(key)
        return value === undefined ? undefined : structuredClone(value)
      }),
    list: Effect.sync(() => structuredClone([...values.values()])),
    modify: (key, f) =>
      lock.withPermits(1)(
        Effect.uninterruptible(
          Effect.gen(function* () {
            const current = values.get(key)
            const [result, next] = yield* f(
              current === undefined ? undefined : structuredClone(current),
            )
            if (next === undefined) values.delete(key)
            else values.set(key, structuredClone(next))
            return result
          }),
        ),
      ),
  })
})
export const layerMemory = Layer.effect(ProviderStore, makeMemory)
