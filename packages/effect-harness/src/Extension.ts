/** Named static capability bundles and deferred runtime provisioning. */
import type * as Layer from 'effect/Layer'
import type * as Scope from 'effect/Scope'
import type { ToolExecution } from './ToolExecution.js'
import { dual } from 'effect/Function'
import * as Capability from './internal/Capability.js'
import type * as Pipeable from 'effect/Pipeable'
import * as Schema from 'effect/Schema'
import type * as Hook from './Hook.js'
import type * as PromptSection from './PromptSection.js'
import type * as Tool from './Tool.js'
import type * as Toolkit from './Toolkit.js'

export const TypeId = '~effect-harness/Extension'
export const DefinitionSchema = Schema.Struct({ name: Schema.NonEmptyString })
export type Definition = typeof DefinitionSchema.Type

export interface Any extends Pipeable.Pipeable {
  readonly [TypeId]: {
    readonly _Requirements: (_: never) => unknown
    readonly _ConstructionError: (_: never) => unknown
  }
  readonly name: string
  readonly tools: ReadonlyArray<Tool.Any>
  readonly hooks: ReadonlyArray<Hook.Any>
  readonly sections: ReadonlyArray<PromptSection.Any>
}

export type Requirements<X extends Any> = X extends {
  readonly [TypeId]: { readonly _Requirements: (_: never) => infer R }
}
  ? R
  : never
export type ConstructionError<X extends Any> = X extends {
  readonly [TypeId]: { readonly _ConstructionError: (_: never) => infer E }
}
  ? E
  : never

export interface Extension<R = never, E = never> extends Any {
  readonly [TypeId]: {
    readonly _Requirements: (_: never) => R
    readonly _ConstructionError: (_: never) => E
  }
}

type ToolsOf<T> =
  T extends ReadonlyArray<Tool.Any>
    ? T[number]
    : T extends { readonly tools: infer Tools }
      ? Extract<Tools[keyof Tools], Tool.Any>
      : never

export const make: <
  const Tools extends
    | ReadonlyArray<Tool.Any>
    | { readonly tools: Readonly<Record<string, Tool.Any>> } = readonly [],
  const Hooks extends ReadonlyArray<Hook.Any> = readonly [],
  const Sections extends ReadonlyArray<PromptSection.Any> = readonly [],
>(
  definition: Definition & {
    readonly tools?: Tools
    readonly hooks?: Hooks
    readonly sections?: Sections
  },
) => Extension<
  | Tool.CodecServices<ToolsOf<Tools>>
  | Exclude<Tool.RequestServices<ToolsOf<Tools>>, ToolExecution | Scope.Scope>
  | Toolkit.HandlerFor<ToolsOf<Tools>>
  | Hook.Requirements<Hooks[number]>
  | PromptSection.Requirements<Sections[number]>,
  Hook.ConstructionError<Hooks[number]>
> = (definition) => {
  let tools: ReadonlyArray<Tool.Any> = []
  if (definition.tools !== undefined) {
    tools = 'tools' in definition.tools ? Object.values(definition.tools.tools) : definition.tools
  }
  return {
    ...definition,
    tools,
    hooks: definition.hooks ?? [],
    sections: definition.sections ?? [],
    [TypeId]: { _Requirements: (_: never) => _, _ConstructionError: (_: never) => _ },
    pipe: Capability.pipe,
  }
}

/** Provisioning is deferred to the consuming Harness's Scope. */
export const provide: {
  <ROut, ELayer, RIn>(
    layer: Layer.Layer<ROut, ELayer, RIn>,
  ): <R, E>(self: Extension<R, E>) => Extension<Exclude<R, ROut> | RIn, E | ELayer>
  <R, E, ROut, ELayer, RIn>(
    self: Extension<R, E>,
    layer: Layer.Layer<ROut, ELayer, RIn>,
  ): Extension<Exclude<R, ROut> | RIn, E | ELayer>
} = dual(2, Capability.provide)
