import * as Context from 'effect/Context'
import * as Layer from 'effect/Layer'
import * as RcMap from 'effect/RcMap'
import * as Semaphore from 'effect/Semaphore'

/** One host-owned manager shared by all Env namespaces and runtime boundaries.
 * Build this layer once at the host Scope, then supply that same context to each Env/tool runtime.
 * Rebuilding it per Env or invocation creates independent mutexes and defeats cross-runtime serialization.
 *
 * @example
 * ```ts
 * import * as Context from 'effect/Context'
 *  * import * as Layer from 'effect/Layer'
 * import * as Scope from 'effect/Scope'
 * import * as Exit from 'effect/Exit'
 * import * as MutationLocks from '@effect-harness/harness/MutationLocks'
 *
 * const hostScope = Effect.runSync(Scope.make())
 * const shared = await Effect.runPromise(
 *   Layer.build(MutationLocks.layer).pipe(Effect.provideService(Scope.Scope, hostScope)),
 * )
 * // Merge this EXACT shared context into independently built Env/Invocation contexts.
 * // Await every consumer runtime before closing the host scope.
 * const manager = Context.get(shared, MutationLocks.MutationLocks)
 * await Effect.runPromise(Scope.close(hostScope, Exit.void))
 * ```
 */
export class MutationLocks extends Context.Service<
  MutationLocks,
  RcMap.RcMap<string, Semaphore.Semaphore>
>()('@effect-harness/harness/MutationLocks') {}
/** Each execution creates one fresh manager; the host must join all consumer runtimes before closing this manager scope. */
export const make = RcMap.make({ lookup: (_key: string) => Semaphore.make(1) })
export const layer: Layer.Layer<MutationLocks> = Layer.effect(MutationLocks, make)
