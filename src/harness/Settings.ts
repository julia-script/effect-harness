/**
 * Harness-wide run policy, ported from pi's `HarnessSettings`. The Harness and the generation task
 * both read the `Current` settings, at every use, so a host can change them while it runs.
 */
import { Context, Effect, Layer, Schema } from 'effect'
import { ModelThinkingLevel } from '../model/Message.ts'
import * as Model from '../model/Model.ts'

/** How many queued items of one mode a boundary places: the first, or all of them. */
export const QueueMode = Schema.Literals(['all', 'one-at-a-time'])
export type QueueMode = typeof QueueMode.Type

/** Whether a tool round runs its calls together, or one after another in call order. */
export const ToolExecution = Schema.Literals(['parallel', 'sequential'])
export type ToolExecution = typeof ToolExecution.Type

/** Durable retries of a generation attempt that failed `Retryable`. */
export const Retry = Schema.Struct({
  enabled: Schema.Boolean,
  /** Retries after the first attempt. */
  maxRetries: Schema.Finite,
  /** The delay before retry `n` is `baseDelayMs · 2^(n−1)`, at most `maxDelayMs`. */
  baseDelayMs: Schema.Finite,
  maxDelayMs: Schema.Finite,
})
export type Retry = typeof Retry.Type

/** Automatic compaction thresholds; manual compaction ignores `enabled`. */
export const Compaction = Schema.Struct({
  /** Threshold and overflow compaction. */
  enabled: Schema.Boolean,
  /** Room kept for the answer: a generation compacts first above `contextWindow - reserveTokens`. */
  reserveTokens: Schema.Finite,
  /** About how much recent context a summary keeps verbatim. */
  keepRecentTokens: Schema.Finite,
  /** Background compaction starts this far below the blocking threshold; `0` disables it. */
  backgroundTokens: Schema.Finite,
})
export type Compaction = typeof Compaction.Type

const PartialCompaction = Schema.Struct({
  enabled: Schema.optionalKey(Schema.Boolean),
  reserveTokens: Schema.optionalKey(Schema.Finite),
  keepRecentTokens: Schema.optionalKey(Schema.Finite),
  backgroundTokens: Schema.optionalKey(Schema.Finite),
})

/** Harness-wide compaction, with overrides keyed by exact `provider/modelId`. */
export const CompactionSettings = Schema.Struct({
  ...Compaction.fields,
  modelOverrides: Schema.Record(Schema.String, PartialCompaction),
})
export type CompactionSettings = typeof CompactionSettings.Type

/** Resolved settings: every field set. */
export const Settings = Schema.Struct({
  /** Steers a boundary places. Default: `one-at-a-time`. */
  steeringMode: QueueMode,
  /** Follow-ups a final boundary places. Default: `one-at-a-time`. */
  followUpMode: QueueMode,
  /** Default: parallel. */
  toolExecution: ToolExecution,
  /** Default: enabled, 3 retries, from 2 s up to 60 s. */
  retry: Retry,
  /** Request options of every generation; the generation sets `sessionId` and `reasoning`. */
  stream: Model.Options,
  /** Minimum pause between commits of the answer being generated. Default: 100 ms. */
  partialIntervalMs: Schema.Finite,
  /** Default: enabled, 16,384 reserved, 20,000 kept, background 32,768 below the threshold. */
  compaction: CompactionSettings,
  /** The default extension selection, by name; absent, every installed extension. */
  extensions: Schema.optionalKey(Schema.Array(Schema.String)),
  /** The model of a conversation whose `$agent` sets none; absent, such a run ends `no_model`. */
  model: Schema.optionalKey(Model.Ref),
  /** The reasoning level of a conversation whose `$agent` sets none. Default: `off`. */
  reasoning: Schema.optionalKey(ModelThinkingLevel),
})
export type Settings = typeof Settings.Type

/** Host settings: any field may be left to its default, `retry` and `compaction` field by field. */
export const Input = Schema.Struct({
  steeringMode: Schema.optionalKey(QueueMode),
  followUpMode: Schema.optionalKey(QueueMode),
  toolExecution: Schema.optionalKey(ToolExecution),
  retry: Schema.optionalKey(
    Schema.Struct({
      enabled: Schema.optionalKey(Schema.Boolean),
      maxRetries: Schema.optionalKey(Schema.Finite),
      baseDelayMs: Schema.optionalKey(Schema.Finite),
      maxDelayMs: Schema.optionalKey(Schema.Finite),
    }),
  ),
  stream: Schema.optionalKey(Model.Options),
  partialIntervalMs: Schema.optionalKey(Schema.Finite),
  compaction: Schema.optionalKey(
    Schema.Struct({
      ...PartialCompaction.fields,
      modelOverrides: Schema.optionalKey(Schema.Record(Schema.String, PartialCompaction)),
    }),
  ),
  extensions: Schema.optionalKey(Schema.Array(Schema.String)),
  model: Schema.optionalKey(Model.Ref),
  reasoning: Schema.optionalKey(ModelThinkingLevel),
})
export type Input = typeof Input.Type

export const defaults: Settings = {
  steeringMode: 'one-at-a-time',
  followUpMode: 'one-at-a-time',
  toolExecution: 'parallel',
  retry: { enabled: true, maxRetries: 3, baseDelayMs: 2000, maxDelayMs: 60_000 },
  stream: {},
  partialIntervalMs: 100,
  compaction: {
    enabled: true,
    reserveTokens: 16_384,
    keepRecentTokens: 20_000,
    backgroundTokens: 32_768,
    modelOverrides: {},
  },
}

/** Every field of `input` over its default. */
export const resolve = (input: Input = {}): Settings => ({
  steeringMode: input.steeringMode ?? defaults.steeringMode,
  followUpMode: input.followUpMode ?? defaults.followUpMode,
  toolExecution: input.toolExecution ?? defaults.toolExecution,
  retry: { ...defaults.retry, ...input.retry },
  stream: input.stream ?? defaults.stream,
  partialIntervalMs: input.partialIntervalMs ?? defaults.partialIntervalMs,
  compaction: { ...defaults.compaction, ...input.compaction },
  ...(input.extensions === undefined ? {} : { extensions: input.extensions }),
  ...(input.model === undefined ? {} : { model: input.model }),
  ...(input.reasoning === undefined ? {} : { reasoning: input.reasoning }),
})

/** The compaction policy for `model`: its override over the harness-wide one, field by field. */
export const compactionFor = (
  settings: Settings,
  model: { readonly provider: string; readonly modelId: string },
): Compaction => {
  const { modelOverrides, ...policy } = settings.compaction
  const key = `${model.provider}/${model.modelId}`
  const override = Object.hasOwn(modelOverrides, key) ? modelOverrides[key] : undefined
  return { ...policy, ...override }
}

/** The delay before retry `attempt`, counted from 1. */
export const retryDelayMs = (retry: Retry, attempt: number): number =>
  Math.min(retry.baseDelayMs * 2 ** Math.max(0, attempt - 1), retry.maxDelayMs)

/** The settings at this use: `defaults` unless a layer provides others. */
export const Current: Context.Reference<Effect.Effect<Settings>> = Context.Reference(
  'effect-harness/harness/Settings/Current',
  { defaultValue: () => Effect.succeed(defaults) },
)

/** `input`, or an Effect read at every use, resolved over the defaults. */
export const make = (input: Input | Effect.Effect<Input>): Effect.Effect<Settings> =>
  Effect.isEffect(input) ? Effect.map(input, resolve) : Effect.succeed(resolve(input))

/** Provide `Current`, for the Harness and the generation task together. */
export const layer = (input: Input | Effect.Effect<Input>): Layer.Layer<never> =>
  Layer.succeed(Current, make(input))
