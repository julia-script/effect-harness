/** Shared tool contracts contain schemas and durable policy, never execution handlers. */
import * as Context from 'effect/Context'
import * as Schema from 'effect/Schema'
import * as AiTool from 'effect/ai/Tool'
import * as Record from './Record.js'

export const ReplaySchema = Schema.Literals(['safe', 'unsafe'])
export type Replay = typeof ReplaySchema.Type
export const ExecutionModeSchema = Schema.Literals(['parallel', 'sequential'])
export type ExecutionMode = typeof ExecutionModeSchema.Type
export const PolicySchema = Schema.Struct({
  replay: ReplaySchema,
  executionMode: ExecutionModeSchema,
})
export type Policy = typeof PolicySchema.Type
export const Policy = Context.Reference<Policy>('effect-harness/Tool/Policy', {
  defaultValue: () => ({ replay: 'unsafe', executionMode: 'sequential' }),
})
export const CallSchema = Schema.Struct({
  id: Schema.NonEmptyString,
  name: Schema.NonEmptyString,
  arguments: Schema.JsonObject,
})
export type Call = typeof CallSchema.Type
export const IntentSchema = Schema.Struct({
  conversationId: Record.ConversationId,
  taskId: Record.TaskId,
  call: CallSchema,
  replay: ReplaySchema,
})
export type Intent = typeof IntentSchema.Type
export type Any = AiTool.Any
export type Tool<
  Name extends string,
  P extends Schema.Constraint,
  S extends Schema.Constraint,
  F extends Schema.Constraint,
> = AiTool.Tool<
  Name,
  {
    readonly parameters: P
    readonly success: S
    readonly failure: F
    readonly failureMode: 'error'
  }
>
export type RequestServices<T extends Any> =
  T extends AiTool.Tool<infer _Name, infer _Config, infer R> ? R : never
export type CodecServices<T extends Any> =
  | T['parametersSchema']['EncodingServices']
  | T['parametersSchema']['DecodingServices']
  | T['successSchema']['EncodingServices']
  | T['successSchema']['DecodingServices']
  | T['failureSchema']['EncodingServices']
  | T['failureSchema']['DecodingServices']

export const make = <
  const Name extends string,
  P extends Schema.Constraint = typeof AiTool.EmptyParams,
  S extends Schema.Constraint = typeof Schema.Void,
  F extends Schema.Constraint = typeof Schema.Never,
>(
  name: Name,
  options?: {
    readonly description?: string
    readonly parameters?: P
    readonly success?: S
    readonly failure?: F
    readonly replay?: Replay
    readonly executionMode?: ExecutionMode
  },
): Tool<Name, P, S, F> =>
  AiTool.make(name, options).annotate(Policy, {
    replay: options?.replay ?? 'unsafe',
    executionMode: options?.executionMode ?? 'sequential',
  })
export const policy = (tool: Any): Policy => Context.get(tool.annotations, Policy)
export type { Parameters, Success, Failure, Name } from 'effect/ai/Tool'
