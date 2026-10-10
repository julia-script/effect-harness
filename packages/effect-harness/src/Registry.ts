/** Scoped, independently bound capability registrations with atomic snapshot publication. */
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Scope from 'effect/Scope'
import type * as Stream from 'effect/Stream'
import { dual } from 'effect/Function'
import type * as Extension from './Extension.js'
import type * as Task from './Task.js'
import type * as Tool from './Tool.js'
import * as Toolkit from './Toolkit.js'
import type { RegistryError } from './RegistryError.js'
import type * as RegistryScope from './RegistryScope.js'
import * as Capability from './internal/Capability.js'
import * as Internal from './internal/Registry.js'

export interface Definition {
  readonly id: string
  /** Increase when changing a persisted execution contract; reload never invents compatibility. */
  readonly version: number
  readonly enabled?: boolean
}

/** Acquisition retains missing services and Layer failures until install actually succeeds. */
export interface Registration<R = never, E = never> {
  readonly definition: Definition
  readonly acquire: Effect.Effect<Internal.Contents, E, R | Scope.Scope>
}

/** Bind a toolkit independently. Same-name handler tags in another registration cannot overwrite it. */
export const toolkit = <Tools extends Record<string, Tool.Any>>(
  definition: Definition,
  self: Toolkit.Toolkit<Tools>,
): Registration<Toolkit.HandlersFor<Tools> | Toolkit.ContextServices<Tools>> => ({
  definition: Object.freeze({ ...definition }),
  acquire: Effect.gen(function* () {
    const context = yield* Effect.context<
      Toolkit.HandlersFor<Tools> | Toolkit.ContextServices<Tools>
    >()
    const bound = yield* self
    return {
      tools: Object.values(self.tools).map((declaration) => ({
        declaration,
        context,
        invoke: (call: Tool.Call) => bound.invoke(call),
      })),
      tasks: [],
      hooks: [],
      sections: [],
    }
  }),
})

/**
 * Capture extension Layers and each hook's Layers once in the installation Scope.
 * The existing Extension phantom channels own this bridge to Capability's private Layer store.
 * Callback failures are invocation failures, separate from construction errors.
 */
export const extension = <R, E>(
  definition: Definition,
  self: Extension.Extension<R, E>,
): Registration<R, E> => ({
  definition: Object.freeze({ ...definition }),
  acquire: Effect.gen(function* () {
    const context = yield* Capability.context(self)
    return yield* Effect.gen(function* () {
      // Extension's public R owns the erased dynamic tool-name boundary after context capture.
      const resolved = Toolkit.make(...self.tools) as unknown as Effect.Effect<
        Toolkit.WithHandler<Record<string, Tool.Any>>,
        never,
        R
      >
      const bound = yield* resolved
      const tools = self.tools.map((declaration) => ({
        declaration,
        context,
        invoke: bound.invoke,
      }))
      const hooks = yield* Effect.forEach(self.hooks, (declaration) =>
        Capability.context(declaration).pipe(Effect.map((context) => ({ declaration, context }))),
      )
      const sections = self.sections.map((declaration) => ({ declaration, context }))
      return { tools, tasks: [], hooks, sections }
    }).pipe(Effect.provideContext(context))
    // Capability erases its private providers; Extension's public R/E retain their exact types.
  }) as Effect.Effect<Internal.Contents, E, R | Scope.Scope>,
})

/** Add a raw task definition and captured codec/handler services. No task or migration executes. */
export const withTask = <R, E, T extends Task.Any, RTask, ETask>(
  self: Registration<R, E>,
  task: T & { readonly context: Effect.Effect<Context.Context<never>, ETask, RTask | Scope.Scope> },
): Registration<R | RTask, E | ETask> => ({
  definition: self.definition,
  acquire: Effect.gen(function* () {
    const contents = yield* self.acquire
    const context = yield* task.context
    return { ...contents, tasks: [...contents.tasks, { declaration: task, context }] }
  }),
})

/** Each registration gets a fresh Layer acquisition, even when sharing the same Layer object. */
export const provide: {
  <ROut, ELayer, RIn>(
    layer: Layer.Layer<ROut, ELayer, RIn>,
  ): <R, E>(self: Registration<R, E>) => Registration<Exclude<R, ROut> | RIn, E | ELayer>
  <R, E, ROut, ELayer, RIn>(
    self: Registration<R, E>,
    layer: Layer.Layer<ROut, ELayer, RIn>,
  ): Registration<Exclude<R, ROut> | RIn, E | ELayer>
} = dual(
  2,
  <R, E, ROut, ELayer, RIn>(
    self: Registration<R, E>,
    layer: Layer.Layer<ROut, ELayer, RIn>,
  ): Registration<Exclude<R, ROut> | RIn, E | ELayer> => ({
    definition: self.definition,
    acquire: Effect.gen(function* () {
      const scope = yield* Scope.Scope
      const supplied = yield* Layer.buildWithScope(Layer.fresh(layer), scope)
      return yield* self.acquire.pipe(Effect.provideContext(supplied))
    }),
  }),
)

export interface Service {
  readonly snapshot: Effect.Effect<RegistryScope.Snapshot>
  /** Latest-state notifications; slow consumers may coalesce generations and never block commits. */
  readonly changes: Stream.Stream<RegistryScope.Snapshot>
  /** Reinstalling an ID reloads in place, preserving its last-wins position. */
  readonly install: <R, E>(
    registration: Registration<R, E>,
  ) => Effect.Effect<RegistryScope.Snapshot, E | RegistryError, R>
  readonly uninstall: (id: string) => Effect.Effect<RegistryScope.Snapshot, RegistryError>
  readonly acquire: Effect.Effect<RegistryScope.Lease, RegistryError, Scope.Scope>
  /** Scoped listeners run independently of publication. */
  readonly listen: <E, R>(
    listener: (snapshot: RegistryScope.Snapshot) => Effect.Effect<void, E, R>,
  ) => Effect.Effect<void, never, Scope.Scope | R>
}

export class Registry extends Context.Service<Registry, Service>()('effect-harness/Registry') {}
export const make: Effect.Effect<Service, never, Scope.Scope> = Internal.make
export const layer: Layer.Layer<Registry> = Layer.effect(Registry, make)
