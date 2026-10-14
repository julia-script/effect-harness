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
    /** Permission-filtered metadata borrowed for this handler; access expires on completion. */
    readonly deferredTools?: Effect.Effect<
      ReadonlyArray<import('./ToolCatalog.js').Metadata>,
      import('./ToolCatalog.js').CatalogError | import('./ExecutionError.js').ExecutionError
    >
    /** Durable per-call child identity in native tool rounds; legacy calls use their run ID. */
    readonly taskId: Record.TaskId
    /** Original provider-visible ID; repeated IDs in later rounds remain distinct children. */
    readonly callId: string
    /** Native owned tool calls. Direct reporting-only services may omit this capability. */
    readonly invocation?: import('./ToolInvocation.js').Access
    /** Borrowed for this handler only, including every returned conversation/submission. */
    readonly subagent?: import('./Subagent.js').Access
    /** Request-pinned selected model policy; absent for direct toolkit execution. */
    readonly imageReadPolicy?: import('./coding/ImageReadPolicy.js').Policy | undefined
    readonly output: (chunk: string) => Effect.Effect<void, Failure>
    /** Nested progress:false acknowledges local acceptance; its final receipt acknowledges durability. */
    readonly details: (value: Schema.Json) => Effect.Effect<void, Failure>
    readonly diagnostic: (value: ToolResult.Diagnostic) => Effect.Effect<void, Failure>
  }
>()('effect-harness/ToolExecution') {}

export type { Failure } from './ExecutionError.js'
