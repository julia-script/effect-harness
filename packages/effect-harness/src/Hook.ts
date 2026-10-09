/** Schema-backed lifecycle events and runtime-owned Effect callbacks. */
import type * as Effect from 'effect/Effect'
import type * as Layer from 'effect/Layer'
import { dual } from 'effect/Function'
import * as Capability from './internal/Capability.js'
import type * as Pipeable from 'effect/Pipeable'
import * as Schema from 'effect/Schema'
import * as Prompt from 'effect/ai/Prompt'
import * as Record from './Record.js'
import * as Tool from './Tool.js'
import * as ToolResult from './ToolResult.js'

export const TypeId = '~effect-harness/Hook'

export const EventSchema = Schema.Union([
  Schema.TaggedStruct('conversationCreated', { conversationId: Record.ConversationId }),
  Schema.TaggedStruct('beforeRequest', { prompt: Prompt.Prompt }),
  Schema.TaggedStruct('afterResponse', { message: Prompt.AssistantMessage }),
  Schema.TaggedStruct('beforeTool', { call: Tool.CallSchema }),
  Schema.TaggedStruct('afterTool', { call: Tool.CallSchema, result: ToolResult.ResultSchema }),
  Schema.TaggedStruct('afterTools', { results: Schema.Array(ToolResult.ResultSchema) }),
  Schema.TaggedStruct('onYield', { message: Prompt.AssistantMessage }),
])
export type Event = typeof EventSchema.Type
export type Name = Event['_tag']

export const ToolDecisionSchema = Schema.Union([
  Schema.TaggedStruct('allow', {}),
  Schema.TaggedStruct('block', { message: Schema.String }),
  Schema.TaggedStruct('replaceArguments', { arguments: Schema.JsonObject }),
])
export type ToolDecision = typeof ToolDecisionSchema.Type

export const YieldDecisionSchema = Schema.TaggedStruct('continue', { input: Prompt.UserMessage })
export type YieldDecision = typeof YieldDecisionSchema.Type

export type Input<N extends Name> = Extract<Event, { readonly _tag: N }>
export interface Outputs {
  readonly conversationCreated: void
  readonly beforeRequest: Prompt.Prompt | void
  readonly afterResponse: void
  readonly beforeTool: ToolDecision | void
  readonly afterTool: ToolResult.Result | void
  readonly afterTools: void
  readonly onYield: YieldDecision | void
}
export type Output<N extends Name> = Outputs[N]

export interface Any extends Pipeable.Pipeable {
  readonly [TypeId]: {
    readonly _Requirements: (_: never) => unknown
    readonly _ConstructionError: (_: never) => unknown
  }
  readonly event: Name
  readonly execute: (event: never) => unknown
}

export type Requirements<H extends Any> = H extends {
  readonly [TypeId]: { readonly _Requirements: (_: never) => infer R }
}
  ? R
  : never
export type ConstructionError<H extends Any> = H extends {
  readonly [TypeId]: { readonly _ConstructionError: (_: never) => infer E }
}
  ? E
  : never

/** Callback failures are reported by Harness; they are not Layer construction failures. */
export interface Hook<N extends Name, E = never, R = never, EBuild = never> extends Any {
  readonly [TypeId]: {
    readonly _Requirements: (_: never) => R
    readonly _ConstructionError: (_: never) => EBuild
  }
  readonly event: N
  readonly execute: (event: Input<N>) => Effect.Effect<Output<N>, E, R>
}

export const make: <const N extends Name, E, R>(definition: {
  readonly event: N
  readonly execute: (event: Input<N>) => Effect.Effect<Output<N>, E, R>
}) => Hook<N, E, R> = (definition) => ({
  ...definition,
  [TypeId]: { _Requirements: (_: never) => _, _ConstructionError: (_: never) => _ },
  pipe: Capability.pipe,
})

export const provide: {
  <ROut, ELayer, RIn>(
    layer: Layer.Layer<ROut, ELayer, RIn>,
  ): <N extends Name, E, R, EBuild>(
    self: Hook<N, E, R, EBuild>,
  ) => Hook<N, E, Exclude<R, ROut> | RIn, EBuild | ELayer>
  <N extends Name, E, R, EBuild, ROut, ELayer, RIn>(
    self: Hook<N, E, R, EBuild>,
    layer: Layer.Layer<ROut, ELayer, RIn>,
  ): Hook<N, E, Exclude<R, ROut> | RIn, EBuild | ELayer>
} = dual(2, Capability.provide)
