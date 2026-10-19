/** Named contributions to a model request's system prompt. */
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Scope from 'effect/Scope'
import type * as Pipeable from 'effect/Pipeable'
import * as Schema from 'effect/Schema'
import * as Capability from './internal/Capability.js'
import type * as Prompt from 'effect/ai/Prompt'
import { HookExecution } from './HookExecution.js'
import * as HookReducer from './HookReducer.js'
import { ExecutionError, type Failure } from './ExecutionError.js'

export const TypeId = '~effect-harness/PromptSection'
export const DefinitionSchema = Schema.Struct({ key: Schema.NonEmptyString })
export type Definition = typeof DefinitionSchema.Type

export interface Any extends Pipeable.Pipeable {
  readonly [TypeId]: { readonly _Requirements: (_: never) => unknown }
  readonly key: string
  readonly render: (prompt: Prompt.Prompt) => unknown
  /** Optional for legacy static consumers; leased Registry sections must use typed make/bind. */
  readonly bind?: (
    context: Context.Context<never>,
  ) => (
    prompt: Prompt.Prompt,
  ) => Effect.Effect<string | undefined, Failure, HookExecution | Scope.Scope>
}
export type Requirements<S extends Any> = S extends {
  readonly [TypeId]: { readonly _Requirements: (_: never) => infer R }
}
  ? Exclude<R, HookExecution | Scope.Scope>
  : never

export interface PromptSection<E = never, R = never> extends Any {
  readonly [TypeId]: { readonly _Requirements: (_: never) => R }
  readonly render: (prompt: Prompt.Prompt) => Effect.Effect<string | undefined, E, R>
}

export const make = <E, R>(
  definition: Definition & {
    readonly render: (prompt: Prompt.Prompt) => Effect.Effect<string | undefined, E, R>
  },
): PromptSection<E, R> => {
  const spec = Object.freeze({ ...definition })
  return {
    ...spec,
    [TypeId]: { _Requirements: (_: never) => _ },
    pipe: Capability.pipe,
    bind: (context) => (prompt) =>
      HookReducer.protect(
        'section.render',
        Effect.suspend(() => spec.render(prompt)).pipe(
          Effect.updateContext((input: Context.Context<HookExecution | Scope.Scope>) =>
            Context.add(
              Context.add(
                context as Context.Context<R>,
                HookExecution,
                Context.get(input, HookExecution),
              ),
              Scope.Scope,
              Context.get(input, Scope.Scope),
            ),
          ),
        ),
      ).pipe(
        Effect.mapError(
          (cause) =>
            new ExecutionError({
              reason: 'invalid',
              operation: 'section.render',
              message: 'Section render failed',
              cause,
            }),
        ),
        Effect.flatMap(Schema.decodeEffect(Schema.Union([Schema.String, Schema.Undefined]))),
      ),
  }
}
