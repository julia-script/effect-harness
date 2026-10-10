/** Same-tag toolkit bindings are exercised through the actual public registry lease API. */
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as Scope from 'effect/Scope'
import type * as Registry from '../Registry.js'
import type { RegistryError } from '../RegistryError.js'
import type { Failure as ExecutionFailure } from '../ExecutionError.js'
import type * as Tool from '../Tool.js'
import type * as ToolResult from '../ToolResult.js'
import type * as ToolExecution from '../ToolExecution.js'
import {
  scopedCase,
  type ConformanceCase,
  type ScopedFixture,
  type ServiceConformanceError,
} from '../internal/ServiceConformance.js'

/**
 * Registrations share the same tool name and dependency tags but capture distinct Layers.
 * aReload keeps a's ID, increases its version and captures a new service value. read extracts an
 * observable value from the real tool response; expected values must be pairwise distinct.
 */
export interface ToolkitIsolationFixture<E = never, R = never> extends ScopedFixture<E, R> {
  readonly registry: Registry.Service
  readonly a: Registry.Registration<R, E>
  readonly b: Registry.Registration<R, E>
  readonly aReload: Registry.Registration<R, E>
  readonly call: Tool.Call
  readonly expectedA: unknown
  readonly expectedB: unknown
  readonly expectedReload: unknown
  readonly read: (result: ToolResult.Result) => unknown
  /** Optional resource probes after obsolete registration leases close and after uninstall. */
  readonly released?: (binding: 'a' | 'b' | 'reload') => Effect.Effect<boolean, E, R>
}

export interface ToolkitIsolationOptions<E = never, R = never, FE = E> {
  readonly make: Effect.Effect<ToolkitIsolationFixture<E, R>, FE, R | Scope.Scope>
  readonly capabilities?: { readonly leases?: boolean }
}

/**
 * Confirms last-installed selection, explicit binding selection, immutable old snapshots across
 * reload, uninstall denial and resource release. Supply ToolExecution explicitly; captured handler
 * services may not be satisfied by merging other registrations. Persistence/reopen routing belongs
 * to runtime integration and is deliberately not certified by this registry contract suite.
 */
export const toolkitIsolationConformance = <E = never, R = never, FE = E>(
  options: ToolkitIsolationOptions<E, R, FE>,
): ReadonlyArray<
  ConformanceCase<
    E | FE | RegistryError | ExecutionFailure | ServiceConformanceError,
    Exclude<R, Scope.Scope> | ToolExecution.ToolExecution
  >
> => {
  type Failure = E | RegistryError | ExecutionFailure | ServiceConformanceError
  const name = 'toolkit isolation: same-tag bindings survive reload'
  return [
    scopedCase<
      ToolkitIsolationFixture<E, R>,
      E,
      R,
      FE,
      Failure,
      R | Scope.Scope | ToolExecution.ToolExecution
    >(name, options.make, (f, { same, failure }) =>
      Effect.gen(function* () {
        yield* same('distinct binding IDs', f.a.definition.id === f.b.definition.id, false)
        yield* same('same ID for reload', f.aReload.definition.id, f.a.definition.id)
        yield* same(
          'reload version increases',
          f.aReload.definition.version > f.a.definition.version,
          true,
        )
        yield* same('distinct expected A/B', Object.is(f.expectedA, f.expectedB), false)
        yield* same('distinct reload value', Object.is(f.expectedA, f.expectedReload), false)
        yield* f.registry.install(f.a)
        yield* f.registry.install(f.b)
        const scope = yield* Scope.make()
        yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void))
        const old = yield* f.registry.acquire.pipe(Effect.provideService(Scope.Scope, scope))
        yield* same('canonical last wins', f.read(yield* old.invokeTool(f.call)), f.expectedB)
        yield* same(
          'explicit A retains own services',
          f.read(yield* old.invokeTool(f.call, { binding: f.a.definition.id })),
          f.expectedA,
        )
        yield* f.registry.install(f.aReload)
        yield* same(
          'old snapshot keeps A',
          f.read(yield* old.invokeTool(f.call, { binding: f.a.definition.id })),
          f.expectedA,
        )
        const freshScope = yield* Scope.make()
        yield* Effect.addFinalizer(() => Scope.close(freshScope, Exit.void))
        const fresh = yield* f.registry.acquire.pipe(Effect.provideService(Scope.Scope, freshScope))
        yield* same(
          'reload preserves canonical order',
          f.read(yield* fresh.invokeTool(f.call)),
          f.expectedB,
        )
        yield* same(
          'explicit reloaded A',
          f.read(yield* fresh.invokeTool(f.call, { binding: f.a.definition.id })),
          f.expectedReload,
        )
        yield* same(
          'B still separately bound',
          f.read(yield* fresh.invokeTool(f.call, { binding: f.b.definition.id })),
          f.expectedB,
        )
        if (options.capabilities?.leases) {
          if (f.released === undefined)
            return yield* same('selected lease probe required', false, true)
          yield* same('old A retained while leased', yield* f.released('a'), false)
        }
        yield* Scope.close(scope, Exit.void)
        if (options.capabilities?.leases && f.released !== undefined)
          yield* same('old A released after lease', yield* f.released('a'), true)
        yield* f.registry.uninstall(f.b.definition.id)
        const removedScope = yield* Scope.make()
        yield* Effect.addFinalizer(() => Scope.close(removedScope, Exit.void))
        const removed = yield* f.registry.acquire.pipe(
          Effect.provideService(Scope.Scope, removedScope),
        )
        yield* failure(
          'uninstalled binding has no fallback',
          removed.invokeTool(f.call, { binding: f.b.definition.id }),
          (e) => typeof e === 'object' && e !== null && 'reason' in e && e.reason === 'notFound',
        )
        yield* Scope.close(freshScope, Exit.void)
        if (options.capabilities?.leases && f.released !== undefined)
          yield* same('uninstalled B released after its lease', yield* f.released('b'), true)
        yield* f.registry.uninstall(f.aReload.definition.id)
        yield* Scope.close(removedScope, Exit.void)
        if (options.capabilities?.leases && f.released !== undefined)
          yield* same(
            'reload released after uninstall and lease close',
            yield* f.released('reload'),
            true,
          )
      }),
    ),
  ]
}
