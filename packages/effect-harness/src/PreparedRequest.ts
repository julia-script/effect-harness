import * as PromptWire from './PromptWire.js'
/** Immutable request facts committed before provider I/O. Live capabilities remain outside the wire. */
import * as Context from 'effect/Context'
import * as Schema from 'effect/Schema'
import * as Agent from './Agent.js'
import * as Model from './Model.js'
import * as Identity from './Identity.js'
import * as Settings from './Settings.js'
import * as Usage from './Usage.js'
import * as Plan from './ContextPlan.js'
import * as Projection from './ContextProjection.js'
import * as Tool from './Tool.js'

export const BindingSchema = Schema.Struct({
  id: Schema.NonEmptyString,
  version: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: Number.MAX_SAFE_INTEGER })),
})
export type Binding = typeof BindingSchema.Type
export const ToolSchema = Schema.Struct({
  declaration: Projection.ToolDeclarationSchema,
  binding: BindingSchema,
  callers: Schema.Array(Tool.CallerSchema),
  replay: Tool.ReplaySchema,
  deferred: Schema.Boolean,
  strict: Schema.optionalKey(Schema.Boolean),
  success: Schema.Json,
  failure: Schema.Json,
})
export const SnapshotSchema = Schema.Struct({
  requestId: Identity.RequestId,
  /** Logical requests in this durable run, independent of retries and process restarts. */
  ordinal: Schema.Natural,
  attempt: Usage.AttemptId,
  prompt: PromptWire.Prompt,
  agent: Agent.StateSchema,
  settings: Settings.SnapshotSchema,
  affinity: Schema.NonEmptyString,
  model: Schema.Union([
    Schema.TaggedStruct('descriptor', {
      binding: BindingSchema,
      definition: Model.DefinitionSchema,
    }),
    Schema.TaggedStruct('native', { binding: BindingSchema }),
  ]),
  options: Schema.JsonObject,
  /** Missing on older accepted work means streaming, independent of current host policy. */
  execution: Schema.optionalKey(Schema.Literals(['streaming', 'deferred'])),
  configuration: Schema.Json,
  configurationBinding: BindingSchema,
  registryGeneration: Schema.Natural,
  requiredBindings: Schema.Array(BindingSchema),
  tools: Schema.Array(ToolSchema),
  /** Canonical invocable bindings across both caller inventories; modelTools only narrows tools. */
  allTools: Schema.Array(ToolSchema),
  translation: Plan.TranslationSchema,
})
export type Snapshot = typeof SnapshotSchema.Type

/**
 * Provider adapters can read the committed request identity, settings and positional frames.
 * Declaring positional support obliges the adapter to consume frames on its actual wire path.
 * Native generateText receives only real native fields; default translation uses leading/latest.
 */
export class PreparedRequest extends Context.Service<
  PreparedRequest,
  { readonly current: Snapshot }
>()('effect-harness/PreparedRequest') {}
