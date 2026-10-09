/** Named contributions to a model request's system prompt. */
import type * as Effect from 'effect/Effect'
import type * as Pipeable from 'effect/Pipeable'
import * as Schema from 'effect/Schema'
import * as Capability from './internal/Capability.js'
import type * as Prompt from 'effect/ai/Prompt'

export const TypeId = '~effect-harness/PromptSection'
export const DefinitionSchema = Schema.Struct({ key: Schema.NonEmptyString })
export type Definition = typeof DefinitionSchema.Type

export interface Any extends Pipeable.Pipeable {
  readonly [TypeId]: { readonly _Requirements: (_: never) => unknown }
  readonly key: string
  readonly render: (prompt: Prompt.Prompt) => unknown
}
export type Requirements<S extends Any> = S extends {
  readonly [TypeId]: { readonly _Requirements: (_: never) => infer R }
}
  ? R
  : never

export interface PromptSection<E = never, R = never> extends Any {
  readonly [TypeId]: { readonly _Requirements: (_: never) => R }
  readonly render: (prompt: Prompt.Prompt) => Effect.Effect<string | undefined, E, R>
}

export const make: <E, R>(
  definition: Definition & {
    readonly render: (prompt: Prompt.Prompt) => Effect.Effect<string | undefined, E, R>
  },
) => PromptSection<E, R> = (definition) => ({
  ...definition,
  [TypeId]: { _Requirements: (_: never) => _ },
  pipe: Capability.pipe,
})
