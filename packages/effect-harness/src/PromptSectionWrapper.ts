/** Ordered prompt-section transformations with independently captured services. */
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Scope from 'effect/Scope'
import { dual } from 'effect/Function'
import type * as Pipeable from 'effect/Pipeable'
import * as Capability from './internal/Capability.js'
import { RegistryError } from './RegistryError.js'
import type * as Prompt from 'effect/ai/Prompt'
import type * as PromptSection from './PromptSection.js'
import type * as RegistryScope from './RegistryScope.js'
import type { Failure } from './ExecutionError.js'
import { HookExecution } from './HookExecution.js'

export interface Item<R = never> {
  readonly declaration: PromptSection.Any
  readonly owner: RegistryScope.Owner
  readonly context: Context.Context<unknown>
  readonly render: (
    prompt: Prompt.Prompt,
  ) => Effect.Effect<string | undefined, Failure, R | HookExecution | Scope.Scope>
}
export interface Compiled {
  readonly key: string
  readonly apply: (item: Item) => Effect.Effect<Item, RegistryError, Scope.Scope>
}
export interface Wrapper<R = never, E = never> extends Pipeable.Pipeable {
  readonly key: string
  readonly acquire: Effect.Effect<Compiled, E, R | Scope.Scope>
}
export const make = <R, E>(definition: {
  readonly key: string
  readonly wrap: (item: Item) => Effect.Effect<Item<R>, E, R | Scope.Scope>
}): Wrapper<Exclude<R, HookExecution | Scope.Scope>, E> => {
  const spec = Object.freeze({ ...definition })
  return {
    pipe: Capability.pipe,
    key: spec.key,
    acquire: Effect.gen(function* () {
      // Fresh invocation access and Scope are supplied below, never required at installation.
      const captured = yield* Effect.context<Exclude<R, HookExecution | Scope.Scope>>().pipe(
        Effect.map((context) => context as Context.Context<R>),
      )
      return {
        key: spec.key,
        apply: (item) =>
          Effect.gen(function* () {
            const scope = yield* Scope.Scope
            const wrapped = yield* Effect.suspend(() => spec.wrap(item)).pipe(
              Effect.provideContext(Context.add(captured, Scope.Scope, scope)),
            )
            return {
              declaration: wrapped.declaration,
              owner: item.owner,
              context: item.context,
              render: (prompt: Prompt.Prompt) =>
                Effect.suspend(() => wrapped.render(prompt)).pipe(
                  Effect.updateContext((input: Context.Context<HookExecution | Scope.Scope>) =>
                    Context.add(
                      Context.add(captured, HookExecution, Context.get(input, HookExecution)),
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
    key: self.key,
    acquire: Layer.build(Layer.fresh(layer)).pipe(
      Effect.flatMap((context) => self.acquire.pipe(Effect.provideContext(context))),
    ),
  }),
)
