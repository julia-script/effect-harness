/**
 * Agent configuration, selection policies and validated execution settings.
 */
import * as Arr from 'effect/Array'
import { dual } from 'effect/Function'
import * as Duration from 'effect/Duration'
import * as Effect from 'effect/Effect'
import * as Time from './Time.ts'
import * as SchemaField from './SchemaField.ts'
import * as Schema from 'effect/Schema'
import * as Record from 'effect/Record'

/**
 * Schema for provider name and model ID selected from a Model.Catalog.
 *
 * @category schemas
 */
export const ModelRef = Schema.Struct({ provider: Schema.String, modelId: Schema.String })
/**
 * Provider name and model ID selected from a Model.Catalog.
 *
 * @category models
 */
export type ModelRef = typeof ModelRef.Type
/**
 * Schema for names added to or removed from the existing extension selection.
 *
 * @category schemas
 */
export const SelectionEdit = Schema.Struct({
  add: SchemaField.optional(Schema.Array(Schema.String)),
  remove: SchemaField.optional(Schema.Array(Schema.String)),
})
/**
 * Schema for replacement names or incremental edits for extension selection.
 *
 * @category schemas
 */
export const Selection = Schema.Union([Schema.Array(Schema.String), SelectionEdit])
/**
 * Replacement names or incremental edits for extension selection.
 *
 * @category models
 */
export type Selection = typeof Selection.Type
/**
 * Schema for explicit tool names or names removed from the resolved selection.
 *
 * @category schemas
 */
export const ToolSelection = Schema.Union([
  Schema.Array(Schema.String),
  Schema.Struct({ remove: Schema.Array(Schema.String) }),
])
/**
 * Explicit tool names or names removed from the resolved selection.
 *
 * @category models
 */
export type ToolSelection = typeof ToolSelection.Type
/**
 * Schema for conversation overrides for model, thinking, extensions, tools, instructions and
 * cwd.
 *
 * @category schemas
 */
export const State = Schema.Struct({
  model: SchemaField.optional(ModelRef),
  thinking: SchemaField.optional(Schema.String),
  extensions: SchemaField.optional(Selection),
  tools: SchemaField.optional(ToolSelection),
  instructions: SchemaField.optional(Schema.String),
  cwd: SchemaField.optional(Schema.String),
})
/**
 * Conversation overrides for model, thinking, extensions, tools, instructions and cwd.
 *
 * @category models
 */
export type State = typeof State.Type
/**
 * Retry enablement, maximum attempts and delay bounds.
 *
 * @category models
 */
export type RetryPolicy = typeof RetryPolicy.Type
/**
 * Context reservation and recent-history retention policy.
 *
 * @category models
 */
export type CompactionPolicy = typeof CompactionPolicy.Type
/**
 * Intervals for committed partial responses and tool output.
 *
 * @category models
 */
export type ProgressPolicy = typeof ProgressPolicy.Type
/**
 * Default retry policy and native duration limits.
 *
 * @category constants
 */
export const defaultRetry: RetryPolicy = {
  enabled: true,
  maxRetries: 3,
  baseDelay: Duration.seconds(2),
  maxAgentDelay: Duration.minutes(1),
}
/**
 * Default context compaction token budgets.
 *
 * @category constants
 */
export const defaultCompaction: CompactionPolicy = {
  enabled: true,
  reserveTokens: 16384,
  keepRecentTokens: 20000,
  backgroundTokens: 32768,
}
/**
 * Default progress cadence, throughput and output-window settings.
 *
 * @category constants
 */
export const defaultProgress: ProgressPolicy = {
  partialInterval: Duration.millis(100),
  outputInterval: Duration.millis(100),
}
/**
 * Host settings for extensions, streams, retry, compaction and tool execution.
 *
 * @category models
 */
export type Settings = typeof Settings.Type
/** Configuration fields replace wholesale; null clears and undefined preserves. */
function configureImpl(self: State, change: State.Change): State {
  // effect-nit-allow P1-stdlib-collection-replacements: this public record admits accessors that delete later own keys. Native enumeration rechecks descriptors before reading; Record.collect can instead read a newly inherited value.
  const next: State = { ...self }
  // Stage own data without inherited setters; restore the ordinary public prototype after assignment.
  Object.setPrototypeOf(next, null)
  for (const [key, value] of Object.entries(change)) {
    if (value === null) Reflect.deleteProperty(next, key)
    else if (value !== undefined) Record.assignProperty(next, key, value)
  }
  Object.setPrototypeOf(next, Object.prototype)
  return next
}
/**
 * Applies configuration changes; null clears a field and undefined preserves it.
 *
 * @category combinators
 */
export const configure: {
  (change: State.Change): (self: State) => State
  (self: State, change: State.Change): State
} = dual(2, configureImpl)
const defaults = (): Settings => ({
  stream: {},
  retry: defaultRetry,
  compaction: defaultCompaction,
  progress: defaultProgress,
  toolExecution: 'parallel',
  steeringMode: 'one-at-a-time',
  followUpMode: 'one-at-a-time',
})
/**
 * Validated default domain settings; options are normalized lazily for each construction.
 *
 * @category constants
 */
export const defaultSettings: Settings = defaults()
/**
 * Validates execution settings and normalizes native duration inputs once.
 *
 * @category constructors
 */
export const settings = Effect.fnUntraced(function* (
  input: Settings.Input = {},
): Effect.fn.Return<Settings, Schema.SchemaError> {
  const progress = { ...defaultProgress, ...input.progress }
  return yield* Schema.decodeEffect(Schema.toType(Settings))({
    ...defaultSettings,
    ...(input.extensions === undefined ? {} : { extensions: input.extensions }),
    stream: { ...input.stream },
    retry: {
      enabled: input.retry?.enabled ?? defaultRetry.enabled,
      maxRetries: input.retry?.maxRetries ?? defaultRetry.maxRetries,
      baseDelay: yield* Time.duration(input.retry?.baseDelay ?? defaultRetry.baseDelay),
      maxAgentDelay: yield* Time.duration(input.retry?.maxAgentDelay ?? defaultRetry.maxAgentDelay),
    },
    compaction: {
      enabled: input.compaction?.enabled ?? defaultCompaction.enabled,
      reserveTokens: input.compaction?.reserveTokens ?? defaultCompaction.reserveTokens,
      keepRecentTokens: input.compaction?.keepRecentTokens ?? defaultCompaction.keepRecentTokens,
      backgroundTokens: input.compaction?.backgroundTokens ?? defaultCompaction.backgroundTokens,
    },
    progress: {
      partialInterval: yield* Time.duration(
        progress.partialInterval ?? defaultProgress.partialInterval,
      ),
      outputInterval: yield* Time.duration(
        progress.outputInterval ?? defaultProgress.outputInterval,
      ),
    },
    toolExecution: input.toolExecution ?? defaultSettings.toolExecution,
    steeringMode: input.steeringMode ?? defaultSettings.steeringMode,
    followUpMode: input.followUpMode ?? defaultSettings.followUpMode,
  })
})
/** Selection edits apply to host defaults, with remove winning and first occurrence order. */
function selectImpl(self: Selection | undefined, defaults: ReadonlyArray<string>): Array<string> {
  if (self === undefined) return Arr.dedupe(defaults)
  if (Array.isArray(self)) return Arr.dedupe(self)
  // Array.isArray does not narrow readonly arrays.
  const edit = self as SelectionEdit
  const removed = new Set(edit.remove)
  return Arr.filter(Arr.union(defaults, edit.add ?? []), (name) => !removed.has(name))
}
/**
 * Applies ordered selection edits to host defaults, with removal winning.
 *
 * @category combinators
 */
export const select: {
  (defaults: ReadonlyArray<string>): (self: Selection | undefined) => Array<string>
  (self: Selection | undefined, defaults: ReadonlyArray<string>): Array<string>
} = dual(2, selectImpl)
function retryDelayImpl(self: RetryPolicy, attempt: number): Duration.Duration {
  return Duration.min(
    Duration.times(self.baseDelay, 2 ** (Math.max(1, attempt) - 1)),
    self.maxAgentDelay,
  )
}
/**
 * Returns the bounded exponential retry delay for an attempt.
 *
 * @category combinators
 */
export const retryDelay: {
  (attempt: number): (self: RetryPolicy) => Duration.Duration
  (self: RetryPolicy, attempt: number): Duration.Duration
} = dual(2, retryDelayImpl)
function isRetryAllowedImpl(self: RetryPolicy, attempt: number, retryable: boolean): boolean {
  return retryable && self.enabled && attempt <= self.maxRetries
}
/**
 * Checks whether the retry policy permits another attempt.
 *
 * @category guards
 */
export const isRetryAllowed: {
  (attempt: number, retryable: boolean): (self: RetryPolicy) => boolean
  (self: RetryPolicy, attempt: number, retryable: boolean): boolean
} = dual(3, isRetryAllowedImpl)

/** Tool controls append exact-list offers or remove names from an exclusion list; an unset list already offers everything. */
function addToolsImpl(self: State, names: ReadonlyArray<string>): State {
  // effect-nit-allow P1-stdlib-collection-replacements: this public/native array may contain missing indices or inherited numeric accessors; native filter preserves HasProperty/Get and callback order, skips holes, and keeps explicit undefined distinct. Effect Array.filter visits missing slots.

  if (self.tools === undefined || Arr.isReadonlyArrayEmpty(names)) return self
  if (Array.isArray(self.tools)) return { ...self, tools: Arr.union(self.tools, names) }
  const selection = self.tools as { readonly remove: ReadonlyArray<string> }
  const added = new Set(names)
  const remove = selection.remove.filter((name) => !added.has(name))
  return remove.length === selection.remove.length ? self : { ...self, tools: { remove } }
}
/**
 * Adds offered tool names while preserving unchanged selection references.
 *
 * @category combinators
 */
export const addTools: {
  (names: ReadonlyArray<string>): (self: State) => State
  (self: State, names: ReadonlyArray<string>): State
} = dual(2, addToolsImpl)

const nonnegative = Schema.Natural
/**
 * Schema for retry enablement, maximum attempts and delay bounds.
 *
 * @category schemas
 */
export const RetryPolicy = Schema.Struct({
  enabled: Schema.Boolean,
  maxRetries: nonnegative,
  baseDelay: Time.NonnegativeDurationFromMillis,
  maxAgentDelay: Time.NonnegativeDurationFromMillis,
})
/**
 * Schema for context reservation and recent-history retention policy.
 *
 * @category schemas
 */
export const CompactionPolicy = Schema.Struct({
  enabled: Schema.Boolean,
  reserveTokens: nonnegative,
  keepRecentTokens: nonnegative,
  backgroundTokens: nonnegative,
})
/**
 * Schema for intervals for committed partial responses and tool output.
 *
 * @category schemas
 */
export const ProgressPolicy = Schema.Struct({
  partialInterval: Time.NonnegativeDurationFromMillis,
  outputInterval: Time.NonnegativeDurationFromMillis,
})
/**
 * Schema for host settings for extensions, streams, retry, compaction and tool execution.
 *
 * @category schemas
 */
export const Settings = Schema.Struct({
  extensions: SchemaField.optional(Schema.Array(Schema.String)),
  stream: Schema.Record(Schema.String, Schema.Unknown),
  retry: RetryPolicy,
  compaction: CompactionPolicy,
  progress: ProgressPolicy,
  toolExecution: Schema.Literals(['parallel', 'sequential']),
  steeringMode: Schema.Literals(['one-at-a-time', 'all']),
  followUpMode: Schema.Literals(['one-at-a-time', 'all']),
})

/**
 * Checks whether a value satisfies the decoded `ModelRef` schema.
 *
 * **Details**
 *
 * Does not decode, transform or coerce input. Use the schema decoder at an external data
 * boundary.
 *
 * @category guards
 */
export const isModelRef: (u: unknown) => u is ModelRef = Schema.is(ModelRef)

/**
 * Checks whether a value satisfies the decoded `SelectionEdit` schema.
 *
 * **Details**
 *
 * Does not decode, transform or coerce input. Use the schema decoder at an external data
 * boundary.
 *
 * @category guards
 */
export const isSelectionEdit: (u: unknown) => u is SelectionEdit = Schema.is(SelectionEdit)

/**
 * Checks whether a value satisfies the decoded `Selection` schema.
 *
 * **Details**
 *
 * Does not decode, transform or coerce input. Use the schema decoder at an external data
 * boundary.
 *
 * @category guards
 */
export const isSelection: (u: unknown) => u is Selection = Schema.is(Selection)

/**
 * Checks whether a value satisfies the decoded `ToolSelection` schema.
 *
 * **Details**
 *
 * Does not decode, transform or coerce input. Use the schema decoder at an external data
 * boundary.
 *
 * @category guards
 */
export const isToolSelection: (u: unknown) => u is ToolSelection = Schema.is(ToolSelection)

/**
 * Checks whether a value satisfies the decoded `State` schema.
 *
 * **Details**
 *
 * Does not decode, transform or coerce input. Use the schema decoder at an external data
 * boundary.
 *
 * @category guards
 */
export const isState: (u: unknown) => u is State = Schema.is(State)

/**
 * Checks whether a value satisfies the decoded `RetryPolicy` schema.
 *
 * **Details**
 *
 * Does not decode, transform or coerce input. Use the schema decoder at an external data
 * boundary.
 *
 * @category guards
 */
export const isRetryPolicy: (u: unknown) => u is RetryPolicy = Schema.is(RetryPolicy)

/**
 * Checks whether a value satisfies the decoded `CompactionPolicy` schema.
 *
 * **Details**
 *
 * Does not decode, transform or coerce input. Use the schema decoder at an external data
 * boundary.
 *
 * @category guards
 */
export const isCompactionPolicy: (u: unknown) => u is CompactionPolicy = Schema.is(CompactionPolicy)

/**
 * Checks whether a value satisfies the decoded `ProgressPolicy` schema.
 *
 * **Details**
 *
 * Does not decode, transform or coerce input. Use the schema decoder at an external data
 * boundary.
 *
 * @category guards
 */
export const isProgressPolicy: (u: unknown) => u is ProgressPolicy = Schema.is(ProgressPolicy)

/**
 * Checks whether a value satisfies the decoded `Settings` schema.
 *
 * **Details**
 *
 * Does not decode, transform or coerce input. Use the schema decoder at an external data
 * boundary.
 *
 * @category guards
 */
export const isSettings: (u: unknown) => u is Settings = Schema.is(Settings)

/**
 * Names added to or removed from the existing extension selection.
 *
 * @category models
 */
export type SelectionEdit = typeof SelectionEdit.Type

/**
 * Type-level contracts for `State`.
 *
 */
export declare namespace State {
  /**
   * Partial replacement of conversation agent overrides.
   *
   * @category models
   */
  type Change = { readonly [K in keyof State]?: State[K] | null | undefined }
}

/**
 * Type-level contracts for `Settings`.
 *
 */
export declare namespace Settings {
  /**
   * Input accepted by Settings, with optional fields preserving undefined.
   *
   * @category models
   */
  type Input = {
    readonly [K in keyof Settings]?: K extends 'retry' | 'compaction' | 'progress'
      ?
          | {
              readonly [P in keyof Settings[K]]?:
                | (Settings[K][P] extends Duration.Duration ? Duration.Input : Settings[K][P])
                | undefined
            }
          | undefined
      : Settings[K] | undefined
  }
}
