/**
 * Invocation context, execution callbacks and model-visible tool results.
 */
import type * as Duration from 'effect/Duration'
import * as SchemaField from './SchemaField.ts'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Prompt from 'effect/ai/Prompt'
import * as Schema from 'effect/Schema'
import * as Usage from './Usage.ts'
import type { ToolError } from './ToolError.ts'
import type * as Output from './Output.ts'

/**
 * Schema for structured tool diagnostic kept separately from output and details.
 *
 * @category schemas
 */
export const Diagnostic = Schema.Struct({
  kind: Schema.String,
  severity: SchemaField.optional(Schema.Literals(['info', 'warning', 'error'])),
  message: SchemaField.optional(Schema.String),
  detail: SchemaField.optional(Schema.Json),
})
/**
 * Structured tool diagnostic kept separately from output and details.
 *
 * @category models
 */
export type Diagnostic = typeof Diagnostic.Type
/**
 * Schema for tool requests to terminate, reset or add tools after settlement.
 *
 * @category schemas
 */
export const Control = Schema.Struct({
  terminate: SchemaField.optional(Schema.Boolean),
  reset: SchemaField.optional(Schema.Struct({ note: SchemaField.optional(Schema.String) })),
  addTools: SchemaField.optional(Schema.Array(Schema.String)),
})
/**
 * Tool requests to terminate, reset or add tools after settlement.
 *
 * @category models
 */
export type Control = typeof Control.Type
/**
 * Schema for tool content, private details, diagnostics, usage and optional controls.
 *
 * @category schemas
 */
export const Result = Schema.Struct({
  content: SchemaField.optional(Schema.Array(Prompt.UserMessagePart)),
  details: SchemaField.optional(Schema.Json),
  diagnostics: SchemaField.optional(Schema.Array(Diagnostic)),
  control: SchemaField.optional(Control),
  usage: SchemaField.optional(Usage.Usage),
  isError: SchemaField.optional(Schema.Boolean),
})
/**
 * Decoded tool content and metadata returned to the executor.
 *
 * @category models
 */
export type ToolResult = Result
/**
 * Partial tool output, details and diagnostics published during execution.
 *
 * @category models
 */
export interface Progress {
  readonly output?: string | undefined
  readonly droppedBytes?: number | undefined
  readonly droppedLines?: number | undefined
  readonly details?: Schema.Json | undefined
  readonly diagnostics?: ReadonlyArray<Diagnostic> | undefined
  readonly clear?: boolean | undefined
}
/**
 * Request-local context and progress reporting for a model or tool invocation.
 *
 * **Details**
 *
 * Carries resolved agent/context inputs and scoped reporting callbacks. It is supplied by an
 * executor, rather than captured as a global singleton.
 *
 * @category services
 */
export class Invocation extends Context.Service<
  Invocation,
  {
    readonly cwd: string
    readonly report: (error: unknown) => Effect.Effect<void>
    readonly progress: (value: Progress) => Effect.Effect<void>
  }
>()('@effect-harness/harness/Invocation') {}
/**
 * Request-local tool reporting and ownership boundary.
 *
 * **Details**
 *
 * Handlers publish output, details and diagnostics through this service. Reports remain
 * distinct from the final model-visible tool result and use the invocation’s lifetime.
 *
 * @category services
 */
export class ToolCall extends Context.Service<
  ToolCall,
  {
    readonly id: string
    /** Native Toolkit preliminary results replace the previous preview and share scoped progress pacing. */
    readonly preliminary?: ((value: ToolResult) => Effect.Effect<void, ToolError>) | undefined
    readonly output: (
      chunk: string | Uint8Array,
      skipped?: Output.Skip,
    ) => Effect.Effect<void, ToolError>
    readonly outputWindow?:
      | {
          readonly maxBytes: number
          readonly maxLines: number
          readonly minIntervalMs: Duration.Input
          readonly bytesPerSecond: number
        }
      | undefined
    readonly details: (value: Schema.Json) => Effect.Effect<void, ToolError>
    readonly diagnostic: (value: Diagnostic) => Effect.Effect<void, ToolError>
  }
>()('@effect-harness/harness/Invocation/ToolCall') {}
/**
 * Provides an Invocation with cwd set to . and no-op reporting.
 *
 * **When to use**
 *
 * Use when standalone execution needs invocation context without publishing progress.
 *
 * **Gotchas**
 *
 * This Layer provides Invocation only; it does not provide ToolCall or any durable ownership
 * service.
 *
 * @category layers
 */
export const layerSilent: Layer.Layer<Invocation> = Layer.succeed(
  Invocation,
  Invocation.of({ cwd: '.', report: () => Effect.void, progress: () => Effect.void }),
)

/**
 * Checks whether a value satisfies the decoded `Diagnostic` schema.
 *
 * **Details**
 *
 * Does not decode, transform or coerce input. Use the schema decoder at an external data
 * boundary.
 *
 * @category guards
 */
export const isDiagnostic: (u: unknown) => u is Diagnostic = Schema.is(Diagnostic)

/**
 * Checks whether a value satisfies the decoded `Control` schema.
 *
 * **Details**
 *
 * Does not decode, transform or coerce input. Use the schema decoder at an external data
 * boundary.
 *
 * @category guards
 */
export const isControl: (u: unknown) => u is Control = Schema.is(Control)

/**
 * Checks whether a value satisfies the decoded `Result` schema.
 *
 * **Details**
 *
 * Does not decode, transform or coerce input. Use the schema decoder at an external data
 * boundary.
 *
 * @category guards
 */
export const isToolResult: (u: unknown) => u is Result = Schema.is(Result)

/**
 * Tool content, private details, diagnostics, usage and optional controls.
 *
 * @category models
 */
export type Result = typeof Result.Type
