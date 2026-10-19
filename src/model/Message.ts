/**
 * Model messages, with pi-ai's field names: the transcript a model request carries and the
 * messages entries store. A system message also changes the request's tool set, so tools are
 * declared in the transcript where they become available.
 */
import { Schema } from 'effect'
import { JsonObject } from '../storage/schema/Json.ts'

export const TextContent = Schema.Struct({
  type: Schema.Literal('text'),
  text: Schema.String,
  /** Provider message metadata, such as an OpenAI Responses message ID. */
  textSignature: Schema.optionalKey(Schema.String),
})
export type TextContent = typeof TextContent.Type

export const ThinkingContent = Schema.Struct({
  type: Schema.Literal('thinking'),
  thinking: Schema.String,
  /** Provider-specific replay data for the reasoning. */
  thinkingSignature: Schema.optionalKey(Schema.String),
  redacted: Schema.optionalKey(Schema.Boolean),
})
export type ThinkingContent = typeof ThinkingContent.Type

export const ImageContent = Schema.Struct({
  type: Schema.Literal('image'),
  /** Base64-encoded image data. */
  data: Schema.String,
  mimeType: Schema.String,
})
export type ImageContent = typeof ImageContent.Type

/** A call the model asks for; our loop runs it as a tool task. */
export const ToolCall = Schema.Struct({
  type: Schema.Literal('toolCall'),
  id: Schema.String,
  name: Schema.String,
  arguments: JsonObject,
  /** Google-specific: opaque signature for reusing thought context. */
  thoughtSignature: Schema.optionalKey(Schema.String),
  namespace: Schema.optionalKey(Schema.String),
  /** Why the arguments the model sent could not be parsed; `arguments` is then empty. */
  argumentsError: Schema.optionalKey(Schema.String),
})
export type ToolCall = typeof ToolCall.Type

/** Token counts and their cost in USD. */
export const Usage = Schema.Struct({
  input: Schema.Finite,
  output: Schema.Finite,
  cacheRead: Schema.Finite,
  cacheWrite: Schema.Finite,
  cacheWrite1h: Schema.optionalKey(Schema.Finite),
  reasoning: Schema.optionalKey(Schema.Finite),
  totalTokens: Schema.Finite,
  cost: Schema.Struct({
    input: Schema.Finite,
    output: Schema.Finite,
    cacheRead: Schema.Finite,
    cacheWrite: Schema.Finite,
    total: Schema.Finite,
  }),
})
export type Usage = typeof Usage.Type

/** Why a response ended; `pending` marks a partial still streaming. */
export const StopReason = Schema.Literals([
  'pending',
  'stop',
  'length',
  'toolUse',
  'error',
  'aborted',
  'deferred',
])
export type StopReason = typeof StopReason.Type

export const ThinkingLevel = Schema.Literals(['minimal', 'low', 'medium', 'high', 'xhigh', 'max'])
export type ThinkingLevel = typeof ThinkingLevel.Type

/** A thinking level, or `off`. */
/** Which model to ask: `Model.Ref`, here so Schemas that name a model need no model runtime. */
export const ModelRef = Schema.Struct({ provider: Schema.String, modelId: Schema.String })
export type ModelRef = typeof ModelRef.Type

export const ModelThinkingLevel = Schema.Union([Schema.Literal('off'), ThinkingLevel])
export type ModelThinkingLevel = typeof ModelThinkingLevel.Type

/** A response the provider finishes later, as a batch; it is polled with this handle. */
export const DeferredHandle = Schema.Struct({
  provider: Schema.String,
  modelId: Schema.String,
  api: Schema.String,
  id: Schema.String,
  /** Epoch milliseconds after which the provider drops the response. */
  expiresAt: Schema.optionalKey(Schema.Finite),
  /** How long to wait before the next poll. */
  pollAfterMs: Schema.optionalKey(Schema.Finite),
  data: Schema.optionalKey(Schema.Json),
})
export type DeferredHandle = typeof DeferredHandle.Type

/**
 * What the model sees of a tool: its declaration, never its implementation. `parameters` is a
 * JSON Schema.
 */
export const ToolDefinition = Schema.Struct({
  name: Schema.String,
  description: Schema.String,
  parameters: JsonObject,
  /** Whether the provider should constrain the arguments to `parameters`. */
  constrainedSampling: Schema.optionalKey(Schema.Json),
})
export type ToolDefinition = typeof ToolDefinition.Type

export const ToolReference = Schema.Struct({ name: Schema.String })
export type ToolReference = typeof ToolReference.Type

/**
 * The system prompt, at its place in the transcript. A later system message appends to the
 * prompt, patches named `sections` (`null` removes one), and adds or removes tools.
 */
export const SystemMessage = Schema.Struct({
  role: Schema.Literal('system'),
  content: Schema.Union([Schema.String, Schema.Array(TextContent)]),
  sections: Schema.optionalKey(Schema.Record(Schema.String, Schema.NullOr(Schema.String))),
  toolsAdded: Schema.optionalKey(Schema.Array(ToolDefinition)),
  toolsRemoved: Schema.optionalKey(Schema.Array(ToolReference)),
  /** Epoch milliseconds. */
  timestamp: Schema.Finite,
})
export type SystemMessage = typeof SystemMessage.Type

export const UserMessage = Schema.Struct({
  role: Schema.Literal('user'),
  content: Schema.Union([Schema.String, Schema.Array(Schema.Union([TextContent, ImageContent]))]),
  timestamp: Schema.Finite,
})
export type UserMessage = typeof UserMessage.Type

export const AssistantContent = Schema.Union([TextContent, ThinkingContent, ToolCall])
export type AssistantContent = typeof AssistantContent.Type

/** A model response: final, partial (`pending`), failed, or deferred. */
export const AssistantMessage = Schema.Struct({
  role: Schema.Literal('assistant'),
  content: Schema.Array(AssistantContent),
  /** The provider API that produced it, such as `anthropic-messages`. */
  api: Schema.String,
  provider: Schema.String,
  model: Schema.String,
  /** The concrete model the provider reports, when it differs from `model`. */
  responseModel: Schema.optionalKey(Schema.String),
  responseId: Schema.optionalKey(Schema.String),
  providerThinkingLevel: Schema.optionalKey(Schema.String),
  thinkingLevel: Schema.optionalKey(ModelThinkingLevel),
  diagnostics: Schema.optionalKey(Schema.Array(JsonObject)),
  usage: Usage,
  stopReason: StopReason,
  deferred: Schema.optionalKey(DeferredHandle),
  errorMessage: Schema.optionalKey(Schema.String),
  rawStopReason: Schema.optionalKey(Schema.String),
  endTurn: Schema.optionalKey(Schema.Boolean),
  timestamp: Schema.Finite,
  durationMs: Schema.optionalKey(Schema.Finite),
})
export type AssistantMessage = typeof AssistantMessage.Type

/** The result of one tool call, as the model sees it. */
export const ToolResultMessage = Schema.Struct({
  role: Schema.Literal('toolResult'),
  toolCallId: Schema.String,
  toolName: Schema.String,
  content: Schema.Array(Schema.Union([TextContent, ImageContent])),
  /** Application-facing detail the model does not see. */
  details: Schema.optionalKey(Schema.Json),
  usage: Schema.optionalKey(Usage),
  /** Calls the tool made on the model's behalf, recorded by the tool task. */
  nestedCalls: Schema.optionalKey(Schema.Json),
  isError: Schema.Boolean,
  timestamp: Schema.Finite,
  durationMs: Schema.optionalKey(Schema.Finite),
})
export type ToolResultMessage = typeof ToolResultMessage.Type

export const Message = Schema.Union([
  SystemMessage,
  UserMessage,
  AssistantMessage,
  ToolResultMessage,
])
export type Message = typeof Message.Type

/** Zero usage. */
export const emptyUsage: Usage = {
  input: 0,
  output: 0,
  cacheRead: 0,
  cacheWrite: 0,
  totalTokens: 0,
  cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
}
