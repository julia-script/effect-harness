import * as Context from 'effect/Context'
import type * as Effect from 'effect/Effect'
import type * as Schema from 'effect/Schema'
import type * as Record from './Record.js'
import type * as Execution from './Execution.js'
import type { Failure } from './ExecutionError.js'
import type * as ToolResult from './ToolResult.js'

/** Supplied by Harness for one call, including Effects returned by application service methods. */
export class ToolExecution extends Context.Service<
  ToolExecution,
  Execution.Access & {
    readonly taskId: Record.TaskId
    readonly callId: string
    /** Request-pinned selected model policy; absent for direct toolkit execution. */
    readonly imageReadPolicy?: import('./coding/ImageReadPolicy.js').Policy | undefined
    readonly output: (chunk: string) => Effect.Effect<void, Failure>
    readonly details: (value: Schema.Json) => Effect.Effect<void, Failure>
    readonly diagnostic: (value: ToolResult.Diagnostic) => Effect.Effect<void, Failure>
  }
>()('effect-harness/ToolExecution') {}

export type { Failure } from './ExecutionError.js'
