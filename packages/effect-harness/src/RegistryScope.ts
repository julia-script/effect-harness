/** Canonical snapshot records and explicit invocation leases. */
import type * as Context from 'effect/Context'
import type * as Effect from 'effect/Effect'
import type * as Extension from './Extension.js'
import type * as Hook from './Hook.js'
import type * as PromptSection from './PromptSection.js'
import type * as Task from './Task.js'
import type * as Tool from './Tool.js'
import type * as ToolResult from './ToolResult.js'
import type { Failure } from './ExecutionError.js'
import type { RegistryError } from './RegistryError.js'
import type { ToolExecution } from './ToolExecution.js'
import type * as Schema from 'effect/Schema'
import type * as Transaction from './Transaction.js'
import type * as TaskResolution from './TaskResolution.js'
import type { Failure as SessionFailure } from './SessionError.js'
import type { TaskError } from './TaskError.js'
import type { TaskExecution } from './TaskExecution.js'
import type * as Scope from 'effect/Scope'
import type { PhaseResult } from './internal/TaskRegistry.js'

/** Stable application-supplied contract version, independent of publication generation. */
export interface Owner {
  readonly id: string
  readonly version: number
  readonly enabled: boolean
}

/** Captured only after typed acquisition. Contexts from separate owners never merge. */
export interface Item<A> {
  readonly declaration: A
  readonly owner: Owner
  readonly context: Context.Context<unknown>
}
export type BoundExtension = Item<Extension.Any>
export type BoundTool = Item<Tool.Any>
export type BoundTask = Item<Task.Any>
export type BoundHook = Item<Hook.Any>
export type BoundSection = Item<PromptSection.Any>

/** Inspection is read-only and does not acquire resources or run task migrations. */
export interface Snapshot {
  readonly generation: number
  readonly registrations: ReadonlyArray<Owner>
  /** Last-wins declarations, including disabled owners: denial must not reveal a shadow. */
  readonly tools: ReadonlyArray<BoundTool>
  /** Every explicit binding in installation order. */
  readonly bindings: ReadonlyArray<BoundTool>
  readonly extensions: ReadonlyArray<BoundExtension>
  readonly tasks: ReadonlyArray<BoundTask>
  readonly hooks: ReadonlyArray<BoundHook>
  readonly sections: ReadonlyArray<BoundSection>
}

export interface Selection {
  /** Select this owner explicitly; missing owners never fall back to the canonical owner. */
  readonly binding?: string
  /** Apply caller/agent disabling to the chosen owner, after canonical resolution. */
  readonly disabled?: ReadonlyArray<string>
  /** A durable consumer must supply its recorded contract version. */
  readonly version?: number
}

/**
 * Acquire from Registry in the invocation Scope. The snapshot remains coherent across reloads.
 * invokeTool also pins the selected resources until the handler settles, even if the lease
 * Scope is closed concurrently. New calls through a released lease fail with revoked.
 * Wrappers and permissions must use resolveTool's exact item, not re-resolve another snapshot.
 */
export interface Lease {
  readonly snapshot: Snapshot
  readonly resolveTool: (
    name: string,
    selection?: Selection,
  ) => Effect.Effect<BoundTool, RegistryError>
  readonly resolveTask: (
    kind: string,
    selection?: Selection,
  ) => Effect.Effect<BoundTask, RegistryError>
  readonly invokeTool: (
    call: Tool.Call,
    selection?: Selection,
  ) => Effect.Effect<ToolResult.Result, RegistryError | Failure, ToolExecution>
  /** Wire entry points compiled at typed registration, retaining resources until each call settles. */
  readonly prepareTask: (
    kind: string,
    input: unknown,
    selection?: Selection,
  ) => Effect.Effect<
    { readonly input: Schema.Json; readonly checkpoint: Schema.Json },
    RegistryError | TaskError | Schema.SchemaError
  >
  readonly inspectTask: (
    record: import('./Record.js').Task,
    failure?: TaskResolution.MigrationFailure,
  ) => Effect.Effect<TaskResolution.Inspection<Task.Any>, RegistryError>
  readonly upgradeTask: (
    tx: Transaction.Transaction,
    record: import('./Record.js').Task,
  ) => Effect.Effect<
    import('./Record.js').Task,
    RegistryError | SessionFailure | TaskResolution.Blocked
  >
  readonly invokeTask: (
    record: import('./Record.js').Task,
    mode: 'run' | 'abort',
  ) => Effect.Effect<PhaseResult, RegistryError | SessionFailure, TaskExecution | Scope.Scope>
}
