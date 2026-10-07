/**
 * Agent configuration, selection policies and validated execution settings.
 *
 * @since 0.0.0
 */
import * as Arr from 'effect/Array'
import { dual } from 'effect/Function'
import * as Duration from 'effect/Duration'
import * as Effect from 'effect/Effect'
import * as Time from './Time.ts'
import * as SchemaField from './SchemaField.ts'
import * as Schema from 'effect/Schema'

/**
 * Schema for model ref.
 *
 * @category schemas
 * @since 0.0.0
 */
export const ModelRef = Schema.Struct({ provider: Schema.String, modelId: Schema.String })
/**
 * Agent model ref contract.
 *
 * @category models
 * @since 0.0.0
 */
export type ModelRef = typeof ModelRef.Type
/**
 * Schema for selection edit.
 *
 * @category schemas
 * @since 0.0.0
 */
export const SelectionEdit = Schema.Struct({
  add: SchemaField.optional(Schema.Array(Schema.String)),
  remove: SchemaField.optional(Schema.Array(Schema.String)),
})
/**
 * Schema for selection.
 *
 * @category schemas
 * @since 0.0.0
 */
export const Selection = Schema.Union([Schema.Array(Schema.String), SelectionEdit])
/**
 * Agent selection contract.
 *
 * @category models
 * @since 0.0.0
 */
export type Selection = typeof Selection.Type
/**
 * Schema for tool selection.
 *
 * @category schemas
 * @since 0.0.0
 */
export const ToolSelection = Schema.Union([
  Schema.Array(Schema.String),
  Schema.Struct({ remove: Schema.Array(Schema.String) }),
])
/**
 * Agent tool selection contract.
 *
 * @category models
 * @since 0.0.0
 */
export type ToolSelection = typeof ToolSelection.Type
/**
 * Schema for state.
 *
 * @category schemas
 * @since 0.0.0
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
 * Agent state contract.
 *
 * @category models
 * @since 0.0.0
 */
export type State = typeof State.Type
/**
 * Agent change contract.
 *
 * @category models
 * @since 0.0.0
 */
export type Change = State.Change
/**
 * Agent retry policy contract.
 *
 * @category models
 * @since 0.0.0
 */
export type RetryPolicy = typeof RetryPolicy.Type
/**
 * Agent compaction policy contract.
 *
 * @category models
 * @since 0.0.0
 */
export type CompactionPolicy = typeof CompactionPolicy.Type
/**
 * Agent progress policy contract.
 *
 * @category models
 * @since 0.0.0
 */
export type ProgressPolicy = typeof ProgressPolicy.Type
/**
 * Default retry policy and native duration limits.
 *
 * @category constants
 * @since 0.0.0
 */
export const defaultRetry: RetryPolicy = {
  enabled: true,
  maxRetries: 3,
  baseDelayMs: Duration.seconds(2),
  maxAgentDelayMs: Duration.minutes(1),
}
/**
 * Default context compaction token budgets.
 *
 * @category constants
 * @since 0.0.0
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
 * @since 0.0.0
 */
export const defaultProgress: ProgressPolicy = {
  partialIntervalMs: Duration.millis(100),
  outputIntervalMs: Duration.millis(100),
}
/**
 * Agent settings contract.
 *
 * @category models
 * @since 0.0.0
 */
export type Settings = typeof Settings.Type
/**
 * Agent settings input contract.
 *
 * @category models
 * @since 0.0.0
 */
export type SettingsInput = Settings.Input
/** Configuration fields replace wholesale; null clears and undefined preserves. */
function configureImpl(self: State, change: Change): State {
  const next: State = { ...self }
  for (const [key, value] of Object.entries(change)) {
    if (value === null) Reflect.deleteProperty(next, key)
    else if (value !== undefined)
      Object.defineProperty(next, key, {
        value,
        enumerable: true,
        configurable: true,
        writable: true,
      })
  }
  return next
}
/**
 * Applies configuration changes; null clears a field and undefined preserves it.
 *
 * @category combinators
 * @since 0.0.0
 */
export const configure: {
  (change: Change): (self: State) => State
  (self: State, change: Change): State
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
 * @since 0.0.0
 */
export const defaultSettings: Settings = defaults()
/**
 * Validates execution settings and normalizes native duration inputs once.
 *
 * @category constructors
 * @since 0.0.0
 */
export const settings = Effect.fnUntraced(function* (
  input: SettingsInput = {},
): Effect.fn.Return<Settings, Schema.SchemaError> {
  const progress = { ...defaultProgress, ...input.progress }
  return yield* Schema.decodeEffect(Schema.toType(Settings))({
    ...defaultSettings,
    ...(input.extensions === undefined ? {} : { extensions: input.extensions }),
    stream: { ...input.stream },
    retry: {
      enabled: input.retry?.enabled ?? defaultRetry.enabled,
      maxRetries: input.retry?.maxRetries ?? defaultRetry.maxRetries,
      baseDelayMs: yield* Time.duration(input.retry?.baseDelayMs ?? defaultRetry.baseDelayMs),
      maxAgentDelayMs: yield* Time.duration(
        input.retry?.maxAgentDelayMs ?? defaultRetry.maxAgentDelayMs,
      ),
    },
    compaction: {
      enabled: input.compaction?.enabled ?? defaultCompaction.enabled,
      reserveTokens: input.compaction?.reserveTokens ?? defaultCompaction.reserveTokens,
      keepRecentTokens: input.compaction?.keepRecentTokens ?? defaultCompaction.keepRecentTokens,
      backgroundTokens: input.compaction?.backgroundTokens ?? defaultCompaction.backgroundTokens,
    },
    progress: {
      partialIntervalMs: yield* Time.duration(
        progress.partialIntervalMs ?? defaultProgress.partialIntervalMs,
      ),
      outputIntervalMs: yield* Time.duration(
        progress.outputIntervalMs ?? defaultProgress.outputIntervalMs,
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
  return Arr.union(defaults, edit.add ?? []).filter((name) => !removed.has(name))
}
/**
 * Applies ordered selection edits to host defaults, with removal winning.
 *
 * @category combinators
 * @since 0.0.0
 */
export const select: {
  (defaults: ReadonlyArray<string>): (self: Selection | undefined) => Array<string>
  (self: Selection | undefined, defaults: ReadonlyArray<string>): Array<string>
} = dual(2, selectImpl)
function retryDelayImpl(self: RetryPolicy, attempt: number): Duration.Duration {
  return Duration.min(
    Duration.times(self.baseDelayMs, 2 ** (Math.max(1, attempt) - 1)),
    self.maxAgentDelayMs,
  )
}
/**
 * Returns the bounded exponential retry delay for an attempt.
 *
 * @category combinators
 * @since 0.0.0
 */
export const retryDelay: {
  (attempt: number): (self: RetryPolicy) => Duration.Duration
  (self: RetryPolicy, attempt: number): Duration.Duration
} = dual(2, retryDelayImpl)
function isRetryAllowedImpl(u: RetryPolicy, attempt: number, retryable: boolean): boolean {
  return retryable && u.enabled && attempt <= u.maxRetries
}
/**
 * Checks whether the retry policy permits another attempt.
 *
 * @category guards
 * @since 0.0.0
 */
export const isRetryAllowed: {
  (attempt: number, retryable: boolean): (self: RetryPolicy) => boolean
  (self: RetryPolicy, attempt: number, retryable: boolean): boolean
} = dual(3, isRetryAllowedImpl)

/** Tool controls append exact-list offers or remove names from an exclusion list; an unset list already offers everything. */
function addToolsImpl(self: State, names: ReadonlyArray<string>): State {
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
 * @since 0.0.0
 */
export const addTools: {
  (names: ReadonlyArray<string>): (self: State) => State
  (self: State, names: ReadonlyArray<string>): State
} = dual(2, addToolsImpl)

const nonnegative = Schema.Natural
/**
 * Schema for retry policy.
 *
 * @category schemas
 * @since 0.0.0
 */
export const RetryPolicy = Schema.Struct({
  enabled: Schema.Boolean,
  maxRetries: nonnegative,
  baseDelayMs: Time.NonnegativeMillis,
  maxAgentDelayMs: Time.NonnegativeMillis,
})
/**
 * Schema for compaction policy.
 *
 * @category schemas
 * @since 0.0.0
 */
export const CompactionPolicy = Schema.Struct({
  enabled: Schema.Boolean,
  reserveTokens: nonnegative,
  keepRecentTokens: nonnegative,
  backgroundTokens: nonnegative,
})
/**
 * Schema for progress policy.
 *
 * @category schemas
 * @since 0.0.0
 */
export const ProgressPolicy = Schema.Struct({
  partialIntervalMs: Time.NonnegativeMillis,
  outputIntervalMs: Time.NonnegativeMillis,
})
/**
 * Schema for settings.
 *
 * @category schemas
 * @since 0.0.0
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
 * Checks whether an unknown value satisfies the ModelRef contract.
 *
 * @category guards
 * @since 0.0.0
 */
export const isModelRef: (u: unknown) => u is ModelRef = Schema.is(ModelRef)

/**
 * Checks whether an unknown value satisfies the SelectionEdit contract.
 *
 * @category guards
 * @since 0.0.0
 */
export const isSelectionEdit: (u: unknown) => u is SelectionEdit = Schema.is(SelectionEdit)

/**
 * Checks whether an unknown value satisfies the Selection contract.
 *
 * @category guards
 * @since 0.0.0
 */
export const isSelection: (u: unknown) => u is Selection = Schema.is(Selection)

/**
 * Checks whether an unknown value satisfies the ToolSelection contract.
 *
 * @category guards
 * @since 0.0.0
 */
export const isToolSelection: (u: unknown) => u is ToolSelection = Schema.is(ToolSelection)

/**
 * Checks whether an unknown value satisfies the State contract.
 *
 * @category guards
 * @since 0.0.0
 */
export const isState: (u: unknown) => u is State = Schema.is(State)

/**
 * Checks whether an unknown value satisfies the RetryPolicy contract.
 *
 * @category guards
 * @since 0.0.0
 */
export const isRetryPolicy: (u: unknown) => u is RetryPolicy = Schema.is(RetryPolicy)

/**
 * Checks whether an unknown value satisfies the CompactionPolicy contract.
 *
 * @category guards
 * @since 0.0.0
 */
export const isCompactionPolicy: (u: unknown) => u is CompactionPolicy = Schema.is(CompactionPolicy)

/**
 * Checks whether an unknown value satisfies the ProgressPolicy contract.
 *
 * @category guards
 * @since 0.0.0
 */
export const isProgressPolicy: (u: unknown) => u is ProgressPolicy = Schema.is(ProgressPolicy)

/**
 * Checks whether an unknown value satisfies the Settings contract.
 *
 * @category guards
 * @since 0.0.0
 */
export const isSettings: (u: unknown) => u is Settings = Schema.is(Settings)

/**
 * Agent selection edit contract.
 *
 * @category models
 * @since 0.0.0
 */
export type SelectionEdit = typeof SelectionEdit.Type

/**
 * Type contracts owned by `State`.
 *
 * @category utility types
 * @since 0.0.0
 */
export declare namespace State {
  /**
   * State change type contract.
   *
   * @category models
   * @since 0.0.0
   */
  type Change = { readonly [K in keyof State]?: State[K] | null | undefined }
}

/**
 * Type contracts owned by `Settings`.
 *
 * @category utility types
 * @since 0.0.0
 */
export declare namespace Settings {
  /**
   * Input accepted by Settings, with optional fields preserving undefined.
   *
   * @category models
   * @since 0.0.0
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
