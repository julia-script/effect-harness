import * as Duration from 'effect/Duration'
import * as Effect from 'effect/Effect'
import * as Time from './Time.ts'
import * as SchemaField from './SchemaField.ts'
import * as Schema from 'effect/Schema'

export const ModelRef = Schema.Struct({ provider: Schema.String, modelId: Schema.String })
export type ModelRef = typeof ModelRef.Type
export const SelectionEdit = Schema.Struct({
  add: SchemaField.optional(Schema.Array(Schema.String)),
  remove: SchemaField.optional(Schema.Array(Schema.String)),
})
export const Selection = Schema.Union([Schema.Array(Schema.String), SelectionEdit])
export type Selection = typeof Selection.Type
export const ToolSelection = Schema.Union([
  Schema.Array(Schema.String),
  Schema.Struct({ remove: Schema.Array(Schema.String) }),
])
export type ToolSelection = typeof ToolSelection.Type
export const State = Schema.Struct({
  model: SchemaField.optional(ModelRef),
  thinking: SchemaField.optional(Schema.String),
  extensions: SchemaField.optional(Selection),
  tools: SchemaField.optional(ToolSelection),
  instructions: SchemaField.optional(Schema.String),
  cwd: SchemaField.optional(Schema.String),
})
export type State = typeof State.Type
export type Change = { readonly [K in keyof State]?: State[K] | null | undefined }
export type RetryPolicy = typeof RetryPolicy.Type
export type CompactionPolicy = typeof CompactionPolicy.Type
export type ProgressPolicy = typeof ProgressPolicy.Type
export const defaultRetry: RetryPolicy = {
  enabled: true,
  maxRetries: 3,
  baseDelayMs: Duration.seconds(2),
  maxAgentDelayMs: Duration.minutes(1),
}
export const defaultCompaction: CompactionPolicy = {
  enabled: true,
  reserveTokens: 16384,
  keepRecentTokens: 20000,
  backgroundTokens: 32768,
}
export const defaultProgress: ProgressPolicy = {
  partialIntervalMs: Duration.millis(100),
  outputIntervalMs: Duration.millis(100),
}
export type Settings = typeof Settings.Type
export type SettingsInput = {
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
/** Configuration fields replace wholesale; null clears and undefined preserves. */
export function configure(state: State, change: Change): State {
  const next: State = { ...state }
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
const defaults = (): Settings => ({
  stream: {},
  retry: defaultRetry,
  compaction: defaultCompaction,
  progress: defaultProgress,
  toolExecution: 'parallel',
  steeringMode: 'one-at-a-time',
  followUpMode: 'one-at-a-time',
})
/** Validated default domain settings; options are normalized lazily for each construction. */
export const defaultSettings: Settings = defaults()
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
export function select(
  selection: Selection | undefined,
  defaults: ReadonlyArray<string>,
): ReadonlyArray<string> {
  if (selection === undefined) return [...new Set(defaults)]
  if (Array.isArray(selection)) return [...new Set(selection)]
  // Array.isArray does not narrow readonly arrays.
  const edit = selection as typeof SelectionEdit.Type
  const removed = new Set(edit.remove)
  return [...new Set([...defaults, ...(edit.add ?? [])])].filter((name) => !removed.has(name))
}
export function retryDelay(policy: RetryPolicy, attempt: number): Duration.Duration {
  return Duration.min(
    Duration.times(policy.baseDelayMs, 2 ** (Math.max(1, attempt) - 1)),
    policy.maxAgentDelayMs,
  )
}
export function shouldRetry(policy: RetryPolicy, attempt: number, retryable: boolean): boolean {
  return retryable && policy.enabled && attempt <= policy.maxRetries
}

/** Tool controls append exact-list offers or remove names from an exclusion list; an unset list already offers everything. */
export function addTools(state: State, names: ReadonlyArray<string>): State {
  if (state.tools === undefined || names.length === 0) return state
  if (Array.isArray(state.tools))
    return { ...state, tools: [...new Set([...state.tools, ...names])] }
  const selection = state.tools as { readonly remove: ReadonlyArray<string> }
  const added = new Set(names)
  const remove = selection.remove.filter((name) => !added.has(name))
  return remove.length === selection.remove.length ? state : { ...state, tools: { remove } }
}

const nonnegative = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))
export const RetryPolicy = Schema.Struct({
  enabled: Schema.Boolean,
  maxRetries: nonnegative,
  baseDelayMs: Time.NonnegativeMillis,
  maxAgentDelayMs: Time.NonnegativeMillis,
})
export const CompactionPolicy = Schema.Struct({
  enabled: Schema.Boolean,
  reserveTokens: nonnegative,
  keepRecentTokens: nonnegative,
  backgroundTokens: nonnegative,
})
export const ProgressPolicy = Schema.Struct({
  partialIntervalMs: Time.NonnegativeMillis,
  outputIntervalMs: Time.NonnegativeMillis,
})
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
