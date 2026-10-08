/**
 * Host-owned canonical file mutation admission shared across runtimes.
 */
import * as Context from 'effect/Context'
import * as Layer from 'effect/Layer'
import * as RcMap from 'effect/RcMap'
import * as Semaphore from 'effect/Semaphore'

/**
 * Shared service serializing admitted mutations of canonical file paths.
 *
 * **Details**
 *
 * Provide one manager instance to every environment operating in the same namespace. Locks
 * stay leased until admitted writes settle, including during cancellation.
 *
 * **Gotchas**
 *
 * Separate managers do not coordinate overlapping writes. This is process-local
 * serialization, not a cross-process filesystem lock.
 *
 * @category services
 */
export class MutationLocks extends Context.Service<
  MutationLocks,
  RcMap.RcMap<string, Semaphore.Semaphore>
>()('effect-harness/MutationLocks') {}
/**
 * Each execution creates one fresh manager; the host must join all consumer runtimes before closing this manager scope.
 *
 * @category constructors
 */
export const make = RcMap.make({ lookup: (_key: string) => Semaphore.make(1) })
/**
 * Provides a scoped mutation-lock manager.
 *
 * **Gotchas**
 *
 * Share this Layer instance across tool bindings and environments that can write the same
 * paths.
 *
 * @category layers
 */
export const layer: Layer.Layer<MutationLocks> = Layer.effect(MutationLocks, make)
