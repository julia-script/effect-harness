/**
 * Host-owned canonical file mutation admission shared across runtimes.
 *
 * @since 0.0.0
 */
import * as Context from 'effect/Context'
import * as Layer from 'effect/Layer'
import * as RcMap from 'effect/RcMap'
import * as Semaphore from 'effect/Semaphore'

/**
 * One host-owned manager shared by all Env namespaces and runtime boundaries.
 *
 * **Details**
 *
 * Build this layer once at the host Scope, then supply that same context to each Env/tool runtime. Rebuilding it per Env or invocation creates independent mutexes and defeats cross-runtime serialization.
 *
 * @category services
 * @since 0.0.0
 */
export class MutationLocks extends Context.Service<
  MutationLocks,
  RcMap.RcMap<string, Semaphore.Semaphore>
>()('@effect-harness/harness/MutationLocks') {}
/**
 * Each execution creates one fresh manager; the host must join all consumer runtimes before closing this manager scope.
 *
 * @category constructors
 * @since 0.0.0
 */
export const make = RcMap.make({ lookup: (_key: string) => Semaphore.make(1) })
/**
 * Layer for MutationLocks capabilities.
 *
 * @category layers
 * @since 0.0.0
 */
export const layer: Layer.Layer<MutationLocks> = Layer.effect(MutationLocks, make)
