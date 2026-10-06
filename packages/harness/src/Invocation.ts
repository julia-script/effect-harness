import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Prompt from 'effect/ai/Prompt'
import * as Schema from 'effect/Schema'
import * as Usage from './Usage.ts'
import type { ToolError } from './Error.ts'
import type * as Output from './Output.ts'

export const Diagnostic = Schema.Struct({
  kind: Schema.String,
  severity: Schema.optionalKey(Schema.Literals(['info', 'warning', 'error'])),
  message: Schema.optionalKey(Schema.String),
  detail: Schema.optionalKey(Schema.Json),
})
export type Diagnostic = typeof Diagnostic.Type
export const Control = Schema.Struct({
  terminate: Schema.optionalKey(Schema.Boolean),
  reset: Schema.optionalKey(Schema.Struct({ note: Schema.optionalKey(Schema.String) })),
  addTools: Schema.optionalKey(Schema.Array(Schema.String)),
})
export type Control = typeof Control.Type
export const Result = Schema.Struct({
  content: Schema.optionalKey(Schema.Array(Prompt.UserMessagePart)),
  details: Schema.optionalKey(Schema.Json),
  diagnostics: Schema.optionalKey(Schema.Array(Diagnostic)),
  control: Schema.optionalKey(Control),
  usage: Schema.optionalKey(Usage.Usage),
  isError: Schema.optionalKey(Schema.Boolean),
})
export type ToolResult = typeof Result.Type
export interface Progress {
  readonly output?: string | undefined
  readonly droppedBytes?: number | undefined
  readonly droppedLines?: number | undefined
  readonly details?: Schema.Json | undefined
  readonly diagnostics?: ReadonlyArray<Diagnostic> | undefined
  readonly clear?: boolean | undefined
}
/** Request-local capabilities supplied per Activity. Handler Layers may depend on this service using Tool.addDependency. */
export class Invocation extends Context.Service<
  Invocation,
  {
    readonly cwd: string
    readonly report: (error: unknown) => Effect.Effect<void>
    readonly progress: (value: Progress) => Effect.Effect<void>
  }
>()('@effect-harness/harness/Invocation') {}
export const layer = (value: Invocation['Service']): Layer.Layer<Invocation> =>
  Layer.succeed(Invocation, value)
/** Injectable tool execution capabilities, scoped to one executor call. */
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
          readonly minIntervalMs: number
          readonly bytesPerSecond: number
        }
      | undefined
    readonly details: (value: Schema.Json) => Effect.Effect<void, ToolError>
    readonly diagnostic: (value: Diagnostic) => Effect.Effect<void, ToolError>
  }
>()('@effect-harness/harness/ToolCall') {}
export const silent = layer({ cwd: '.', report: () => Effect.void, progress: () => Effect.void })
