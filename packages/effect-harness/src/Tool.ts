/** Shared tool contracts contain schemas and durable policy, never execution handlers. */
import * as Context from 'effect/Context'
import * as Schema from 'effect/Schema'
import * as AiTool from 'effect/ai/Tool'
import * as Record from './Record.js'
import * as ToolResult from './ToolResult.js'
import * as ProgressPolicy from './ProgressPolicy.js'
import * as OutputRetention from './OutputRetention.js'

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
/** Retention/progress declarations are policies, not runtime activation or ambient settings. */
export const Progress = Context.Reference<ProgressPolicy.Policy>('effect-harness/Tool/Progress', {
  defaultValue: () => ProgressPolicy.defaultPolicy,
})
export const Retention = Context.Reference<OutputRetention.Policy>(
  'effect-harness/Tool/Retention',
  {
    defaultValue: () => OutputRetention.defaultPolicy,
  },
)
export const progress = (tool: Any): ProgressPolicy.Policy =>
  Context.get(tool.annotations, Progress)
export const outputRetention = (tool: Any): OutputRetention.Policy =>
  Context.get(tool.annotations, Retention)

/** Deferred declarations require an explicit offer or a successful discovery round. */
export const Deferred = Context.Reference<boolean>('effect-harness/Tool/Deferred', {
  defaultValue: () => false,
})
export const isDeferred = (tool: Any): boolean => Context.get(tool.annotations, Deferred)

export const CallerSchema = Schema.Literals(['model', 'tools'])
export type Caller = typeof CallerSchema.Type
export const CallersSchema = Schema.Array(CallerSchema)
export const Callers = Context.Reference<ReadonlyArray<Caller>>('effect-harness/Tool/Callers', {
  defaultValue: () => ['model', 'tools'],
})
export const callers = (tool: Any): ReadonlyArray<Caller> => Context.get(tool.annotations, Callers)
/** Legacy intents are model-issued. This metadata does not activate a new runtime protocol. */
export const caller = (intent: Intent): Caller => intent.caller ?? 'model'

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
  caller: Schema.optionalKey(CallerSchema),
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
    readonly deferred?: boolean
    readonly callers?: ReadonlyArray<Caller>
    readonly progress?: ProgressPolicy.Policy
    readonly outputRetention?: OutputRetention.Policy
  },
): Tool<Name, P, S, F> =>
  AiTool.make(name, options)
    .annotate(Policy, {
      replay: options?.replay ?? 'unsafe',
      executionMode: options?.executionMode ?? 'sequential',
    })
    .annotate(Deferred, options?.deferred ?? false)
    .annotate(Callers, Object.freeze([...(options?.callers ?? ['model', 'tools'])]))
    .annotate(Progress, Object.freeze({ ...(options?.progress ?? ProgressPolicy.defaultPolicy) }))
    .annotate(
      Retention,
      Object.freeze({ ...(options?.outputRetention ?? OutputRetention.defaultPolicy) }),
    )
export const policy = (tool: Any): Policy => Context.get(tool.annotations, Policy)

/** Marks schema-backed handler envelopes; ordinary native tools keep value-style results. */
export const ResultChannels = Context.Reference<boolean>('effect-harness/Tool/ResultChannels', {
  defaultValue: () => false,
})
export const hasResultChannels = (tool: Any): boolean =>
  Context.get(tool.annotations, ResultChannels)

/** Whether nested projection uses a declared channel or native content fallback. */
export const StructuredOutput = Context.Reference<boolean>('effect-harness/Tool/StructuredOutput', {
  defaultValue: () => false,
})
export const hasStructuredOutput = (tool: Any): boolean =>
  Context.get(tool.annotations, StructuredOutput)

type ResultOptions<
  P extends Schema.Constraint,
  S extends Schema.Constraint,
  F extends Schema.Constraint,
> = {
  readonly description?: string
  readonly parameters?: P
  readonly success?: S
  readonly failure?: F
  readonly replay?: Replay
  readonly executionMode?: ExecutionMode
  readonly deferred?: boolean
  readonly callers?: ReadonlyArray<Caller>
  readonly progress?: ProgressPolicy.Policy
  readonly outputRetention?: OutputRetention.Policy
}

/**
 * Independent model content, programmatic data and UI details. The legacy default permits
 * omitted structuredOutput, including with a declared schema. resultValidation: 'strict'
 * requires it on success; errors may omit it. Without a schema, legacy JSON remains valid
 * and nested results fall back to native content only when structuredOutput is absent.
 * Codecs own receipt encoding. Hooks must revalidate their final channels before saving.
 */
export function makeResult<
  const Name extends string,
  P extends Schema.Constraint = typeof AiTool.EmptyParams,
  S extends Schema.Constraint = typeof Schema.Json,
  F extends Schema.Constraint = typeof Schema.Json,
>(
  name: Name,
  options: ResultOptions<P, S, F> & { readonly resultValidation: 'strict' },
): Tool<Name, P, ToolResult.StrictOutputSchema<S>, ToolResult.OutputSchema<F>>
export function makeResult<
  const Name extends string,
  P extends Schema.Constraint = typeof AiTool.EmptyParams,
  S extends Schema.Constraint = typeof Schema.Json,
  F extends Schema.Constraint = typeof Schema.Json,
>(
  name: Name,
  options?: ResultOptions<P, S, F> & { readonly resultValidation?: 'optional' },
): Tool<Name, P, ToolResult.OutputSchema<S>, ToolResult.OutputSchema<F>>
export function makeResult<
  const Name extends string,
  P extends Schema.Constraint = typeof AiTool.EmptyParams,
  S extends Schema.Constraint = typeof Schema.Json,
  F extends Schema.Constraint = typeof Schema.Json,
>(
  name: Name,
  options?: ResultOptions<P, S, F> & { readonly resultValidation?: 'optional' | 'strict' },
) {
  // Defaults are JSON codecs; generic callers retain explicitly supplied schemas.
  const success = (options?.success ?? Schema.Json) as S
  const failure = (options?.failure ?? Schema.Json) as F
  const successOutput =
    options?.resultValidation === 'strict'
      ? ToolResult.strictOutputSchema(success)
      : ToolResult.outputSchema(success)
  return make(name, {
    ...options,
    success: successOutput,
    failure: ToolResult.outputSchema(failure),
  })
    .annotate(ResultChannels, true)
    .annotate(
      StructuredOutput,
      options?.success !== undefined || options?.resultValidation === 'strict',
    )
}
export type { Parameters, Success, Failure, Name } from 'effect/ai/Tool'
