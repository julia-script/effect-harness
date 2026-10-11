/** Ordered transformations of a declaration and its already bound wire handler. */
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Scope from 'effect/Scope'
import { dual } from 'effect/Function'
import type * as Pipeable from 'effect/Pipeable'
import * as Capability from './internal/Capability.js'
import { RegistryError } from './RegistryError.js'
import type * as Tool from './Tool.js'
import type * as ToolResult from './ToolResult.js'
import type * as RegistryScope from './RegistryScope.js'
import type { Failure } from './ExecutionError.js'
import { ToolExecution } from './ToolExecution.js'
import * as Toolkit from './Toolkit.js'

export interface Item<R = never> {
  readonly declaration: Tool.Any
  /** The original owner and services are inspection data, never a context to merge into another toolkit. */
  readonly owner: RegistryScope.Owner
  readonly context: Context.Context<unknown>
  readonly codec?: RegistryScope.ToolCodec
  readonly invoke: (
    call: Tool.Call,
  ) => Effect.Effect<ToolResult.Result, Failure, R | ToolExecution | Scope.Scope>
}

export interface Compiled {
  readonly name: string
  readonly apply: (item: Item) => Effect.Effect<Item, RegistryError, Scope.Scope>
}

export interface Wrapper<R = never, E = never> extends Pipeable.Pipeable {
  readonly name: string
  readonly acquire: Effect.Effect<Compiled, E, R | Scope.Scope>
}

/** Layers/resources are acquired once per wrapper registration; delegates keep each owner's context. */
export const make = <R, E>(definition: {
  readonly name: string
  readonly wrap: (item: Item) => Effect.Effect<Item<R>, E, R | Scope.Scope>
}): Wrapper<Exclude<R, ToolExecution | Scope.Scope>, E> => {
  const spec = Object.freeze({ ...definition })
  return {
    pipe: Capability.pipe,
    name: spec.name,
    acquire: Effect.gen(function* () {
      // Fresh invocation access and Scope are supplied below, never required at installation.
      const captured = yield* Effect.context<Exclude<R, ToolExecution | Scope.Scope>>().pipe(
        Effect.map((context) => context as Context.Context<R>),
      )
      return {
        name: spec.name,
        apply: (item) =>
          Effect.gen(function* () {
            const scope = yield* Scope.Scope
            const wrapped = yield* Effect.suspend(() => spec.wrap(item)).pipe(
              Effect.provideContext(Context.add(captured, Scope.Scope, scope)),
            )
            if (
              wrapped.declaration.name !== item.declaration.name ||
              wrapped.declaration.parametersSchema !== item.declaration.parametersSchema ||
              wrapped.declaration.successSchema !== item.declaration.successSchema ||
              wrapped.declaration.failureSchema !== item.declaration.failureSchema
            )
              return yield* new RegistryError({
                reason: 'invalidRegistration',
                operation: 'registry.wrap',
                message:
                  'Wrappers must preserve the tool name; schema changes require typed ToolWrapper.bind',
              })
            return {
              declaration: wrapped.declaration,
              owner: item.owner,
              context: item.context,
              ...(item.codec === undefined ? {} : { codec: item.codec }),
              invoke: (call: Tool.Call) =>
                Effect.suspend(() => wrapped.invoke(call)).pipe(
                  Effect.updateContext((input: Context.Context<ToolExecution | Scope.Scope>) =>
                    Context.add(
                      Context.add(captured, ToolExecution, Context.get(input, ToolExecution)),
                      Scope.Scope,
                      Context.get(input, Scope.Scope),
                    ),
                  ),
                ),
            }
          }).pipe(
            Effect.mapError(
              (cause) =>
                new RegistryError({
                  reason: 'invalidRegistration',
                  operation: 'registry.wrap',
                  message: 'Wrapper transformation failed',
                  cause,
                }),
            ),
          ),
      }
    }),
  }
}

/**
 * Change the complete tool contract while retaining the original delegated handler's context.
 * The outer declaration's codec services are required at installation, independently of the
 * handler's services. Both captures remain alive under the publication's invocation lease.
 */
export const bind = <T extends Tool.Any, R, E>(
  declaration: T,
  wrap: (item: Item) => Effect.Effect<Item<R>, E, R | Scope.Scope>,
): Wrapper<Exclude<R, ToolExecution | Scope.Scope> | Tool.CodecServices<T>, E> => {
  const self = make({ name: declaration.name, wrap })
  return {
    ...self,
    acquire: Effect.gen(function* () {
      const context = yield* Effect.context<Tool.CodecServices<T>>()
      const compiled = yield* self.acquire
      const codec = yield* Toolkit.compileCodec(declaration)
      return {
        name: compiled.name,
        apply: (item) =>
          compiled.apply(item).pipe(
            Effect.map((wrapped) => ({
              ...wrapped,
              declaration,
              context: context as Context.Context<unknown>,
              codec,
            })),
          ),
      }
    }),
  }
}

export const provide: {
  <ROut, ELayer, RIn>(
    layer: Layer.Layer<ROut, ELayer, RIn>,
  ): <R, E>(self: Wrapper<R, E>) => Wrapper<Exclude<R, ROut> | RIn, E | ELayer>
  <R, E, ROut, ELayer, RIn>(
    self: Wrapper<R, E>,
    layer: Layer.Layer<ROut, ELayer, RIn>,
  ): Wrapper<Exclude<R, ROut> | RIn, E | ELayer>
} = dual(
  2,
  <R, E, ROut, ELayer, RIn>(
    self: Wrapper<R, E>,
    layer: Layer.Layer<ROut, ELayer, RIn>,
  ): Wrapper<Exclude<R, ROut> | RIn, E | ELayer> => ({
    ...self,
    name: self.name,
    acquire: Layer.build(Layer.fresh(layer)).pipe(
      Effect.flatMap((context) => self.acquire.pipe(Effect.provideContext(context))),
    ),
  }),
)
