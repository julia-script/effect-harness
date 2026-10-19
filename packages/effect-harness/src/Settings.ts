import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Struct from 'effect/Struct'
import * as SynchronizedRef from 'effect/SynchronizedRef'
import { SettingsError } from './SettingsError.js'

const Count = Schema.Int.check(
  Schema.isGreaterThanOrEqualTo(0),
  Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER),
)
const Millis = Schema.Finite.check(Schema.isGreaterThanOrEqualTo(0))

/** Independently select one queued item or every queued item at a commit boundary. */
export const QueueModeSchema = Schema.Literals(['one-at-a-time', 'all'])
export type QueueMode = typeof QueueModeSchema.Type
export const ToolExecutionModeSchema = Schema.Literals(['parallel', 'sequential'])
export type ToolExecutionMode = typeof ToolExecutionModeSchema.Type

/** Durable retries exclude the initial request and are separate from provider-internal retries. */
export const RetrySchema = Schema.Struct({
  enabled: Schema.Boolean,
  maxRetries: Count,
  baseDelayMs: Millis,
  /** Cap for each computed exponential backoff; zero means no delay. */
  maxAgentDelayMs: Millis,
})
export type Retry = typeof RetrySchema.Type

/** Manual compaction ignores enabled; zero backgroundTokens disables background compaction. */
export const CompactionSchema = Schema.Struct({
  enabled: Schema.Boolean,
  reserveTokens: Count,
  keepRecentTokens: Count,
  backgroundTokens: Count,
})
export type Compaction = typeof CompactionSchema.Type

/** Zero intervals allow immediate progress commits; consumers sample once per invocation. */
export const ProgressSchema = Schema.Struct({
  partialIntervalMs: Millis,
  outputIntervalMs: Millis,
})
export type Progress = typeof ProgressSchema.Type

const StreamFields = Schema.Struct({
  transport: Schema.Literals(['sse', 'websocket', 'websocket-cached', 'auto']),
  timeoutMs: Millis,
  maxRetries: Count,
  /** Provider retry hint cap; zero disables the cap, unlike retry.maxAgentDelayMs. */
  maxRetryDelayMs: Millis,
  headers: Schema.Record(Schema.String, Schema.String),
  metadata: Schema.JsonObject,
  cacheRetention: Schema.Literals(['none', 'short', 'long']),
  deferred: Schema.Union([
    Schema.Boolean,
    Schema.Struct({ window: Schema.optional(Schema.Literals(['15m', '1h', '24h'])) }),
  ]),
})

/**
 * Serializable request hints, with absent fields delegated to the selected adapter.
 * Effect AI has provider-specific Config services, not a universal pi-ai options bag.
 * Request preparation must map these hints to supported Config/HttpClient policies;
 * deferred polling needs its own adapter contract. This module does no provider I/O
 * and does not claim that every provider supports every hint.
 */
export const StreamSchema = StreamFields.mapFields(Struct.map(Schema.optionalKey))
export type Stream = typeof StreamSchema.Type

/** Complete, immutable host policy. Extension names are resolved against the current registry. */
export const SnapshotSchema = Schema.Struct({
  revision: Count,
  extensions: Schema.optionalKey(Schema.Array(Schema.NonEmptyString)),
  stream: StreamSchema,
  retry: RetrySchema,
  compaction: CompactionSchema,
  progress: ProgressSchema,
  toolExecution: ToolExecutionModeSchema,
  steeringMode: QueueModeSchema,
  followUpMode: QueueModeSchema,
  contextRetentionMs: Millis,
  maxTurns: Count.check(Schema.isGreaterThan(0)),
})
export type Snapshot = typeof SnapshotSchema.Type

/**
 * Omitted/undefined fields preserve current values (built-in defaults at construction).
 * Retry, compaction and progress merge field by field. A supplied stream replaces
 * the entire hint bundle; null or {} clears it. Extension arrays replace selection;
 * [] selects none and null restores the default of every installed extension.
 */
export const ChangeSchema = Schema.Struct({
  extensions: Schema.optional(Schema.NullOr(Schema.Array(Schema.NonEmptyString))),
  stream: Schema.optional(Schema.NullOr(StreamFields.mapFields(Struct.map(Schema.optional)))),
  retry: Schema.optional(RetrySchema.mapFields(Struct.map(Schema.optional))),
  compaction: Schema.optional(CompactionSchema.mapFields(Struct.map(Schema.optional))),
  progress: Schema.optional(ProgressSchema.mapFields(Struct.map(Schema.optional))),
  toolExecution: Schema.optional(ToolExecutionModeSchema),
  steeringMode: Schema.optional(QueueModeSchema),
  followUpMode: Schema.optional(QueueModeSchema),
  contextRetentionMs: Schema.optional(Millis),
  maxTurns: Schema.optional(Count.check(Schema.isGreaterThan(0))),
})
export type Change = typeof ChangeSchema.Type

// Only schema-decoded acyclic JSON trees reach this helper. Every nested value,
// including adapter metadata, must be frozen, not just the policy containers.
const freeze = <A>(value: A): A => {
  if (value !== null && typeof value === 'object') {
    for (const child of Object.values(value)) freeze(child)
    Object.freeze(value)
  }
  return value
}

/** Defaults follow the audited host policy; maxTurns retains the local positive-integer default. */
export const defaults: Snapshot = freeze({
  revision: 0,
  stream: {},
  retry: { enabled: true, maxRetries: 3, baseDelayMs: 2000, maxAgentDelayMs: 60000 },
  compaction: {
    enabled: true,
    reserveTokens: 16384,
    keepRecentTokens: 20000,
    backgroundTokens: 32768,
  },
  progress: { partialIntervalMs: 100, outputIntervalMs: 100 },
  toolExecution: 'parallel',
  steeringMode: 'one-at-a-time',
  followUpMode: 'one-at-a-time',
  contextRetentionMs: 600000,
  maxTurns: 100,
})

export interface SettingsService {
  readonly get: Effect.Effect<Snapshot>
  readonly update: (change: Change) => Effect.Effect<Snapshot, SettingsError>
}

/**
 * Live host policy, owned by one Layer construction; not durable conversation state.
 * Each update publishes one coherent snapshot. Successful updates, including no-op
 * patches, increment revision; rejected input leaves both snapshot and revision intact.
 * Copying happens when make/update executes. Getters are sampled once by the copy;
 * throwing getters, proxies and non-cloneable values fail through SettingsError.
 * Returned snapshots never retain caller objects or accessors.
 *
 * Runtime consumers must sample at these boundaries and checkpoint actual values
 * needed for recovery, rather than just the revision:
 *
 * | Policy | Boundary |
 * | --- | --- |
 * | Extension defaults and stream hints | Request preparation; pin the prepared request |
 * | Retry/backoff | New retry decision; pin the absolute deadline |
 * | Compaction | Preparation/range selection; pin the range; overflow classification |
 * | Tool execution | Tool-round start; pin the round decision |
 * | Steering/follow-up | Queue selection under Session commit coordination |
 * | Partial/output intervals | Start of each generation/tool progress invocation |
 * | Context retention | Idle transition/cache eviction; busy context stays retained |
 * | maxTurns | New request preparation against the durable run turn count |
 *
 * Later host updates never rewrite prepared requests, saved deadlines or active work.
 * Conversation Agent overrides are resolved separately at request preparation.
 * Filesystem/process/image environments and cwd remain toolkit-bound services.
 * Consumer/reopen/queue/round tests are required when those integrations land.
 */
export class Settings extends Context.Service<Settings, SettingsService>()(
  'effect-harness/Settings',
) {}

const invalid = (operation: SettingsError['operation']) => (cause: unknown) =>
  new SettingsError({ reason: 'invalid', operation, message: 'Invalid host policy', cause })

const apply = Effect.fn('Settings.apply')(function* (
  previous: Snapshot,
  input: Change,
  operation: SettingsError['operation'],
  revision: number,
) {
  // structuredClone is the host input boundary: it detaches mutable JSON and
  // materializes getters before schema validation or publication.
  const copied: unknown = yield* Effect.try({
    try: () => structuredClone(input),
    catch: (cause) =>
      new SettingsError({ reason: 'copy', operation, message: 'Cannot copy host policy', cause }),
  })
  const change = yield* Schema.decodeUnknownEffect(ChangeSchema)(copied, {
    onExcessProperty: 'error',
  }).pipe(Effect.mapError(invalid(operation)))
  const extensions = change.extensions === undefined ? previous.extensions : change.extensions
  const stream = change.stream === undefined ? previous.stream : change.stream
  const streamPolicy =
    stream === null
      ? {}
      : {
          ...Object.fromEntries(Object.entries(stream).filter(([, value]) => value !== undefined)),
          ...(typeof stream.deferred === 'object'
            ? {
                deferred:
                  stream.deferred.window === undefined ? {} : { window: stream.deferred.window },
              }
            : {}),
        }
  const snapshot = yield* Schema.decodeEffect(SnapshotSchema)(
    {
      revision,
      ...(extensions === undefined || extensions === null ? {} : { extensions }),
      stream: streamPolicy,
      retry: {
        enabled: change.retry?.enabled ?? previous.retry.enabled,
        maxRetries: change.retry?.maxRetries ?? previous.retry.maxRetries,
        baseDelayMs: change.retry?.baseDelayMs ?? previous.retry.baseDelayMs,
        maxAgentDelayMs: change.retry?.maxAgentDelayMs ?? previous.retry.maxAgentDelayMs,
      },
      compaction: {
        enabled: change.compaction?.enabled ?? previous.compaction.enabled,
        reserveTokens: change.compaction?.reserveTokens ?? previous.compaction.reserveTokens,
        keepRecentTokens:
          change.compaction?.keepRecentTokens ?? previous.compaction.keepRecentTokens,
        backgroundTokens:
          change.compaction?.backgroundTokens ?? previous.compaction.backgroundTokens,
      },
      progress: {
        partialIntervalMs:
          change.progress?.partialIntervalMs ?? previous.progress.partialIntervalMs,
        outputIntervalMs: change.progress?.outputIntervalMs ?? previous.progress.outputIntervalMs,
      },
      toolExecution: change.toolExecution ?? previous.toolExecution,
      steeringMode: change.steeringMode ?? previous.steeringMode,
      followUpMode: change.followUpMode ?? previous.followUpMode,
      contextRetentionMs: change.contextRetentionMs ?? previous.contextRetentionMs,
      maxTurns: change.maxTurns ?? previous.maxTurns,
    },
    { onExcessProperty: 'error' },
  ).pipe(Effect.mapError(invalid(operation)))
  return freeze(snapshot)
})

/** Construct an independent live service with a validated revision-zero snapshot. */
export const make = Effect.fn('Settings.make')(function* (initial: Change = {}) {
  const snapshot = yield* apply(defaults, initial, 'make', 0)
  const ref = yield* SynchronizedRef.make(snapshot)
  return Settings.of({
    get: SynchronizedRef.get(ref),
    update: Effect.fn('Settings.update')((change: Change) =>
      SynchronizedRef.updateAndGetEffect(ref, (previous) => {
        if (previous.revision === Number.MAX_SAFE_INTEGER)
          return Effect.fail(
            new SettingsError({
              reason: 'revision',
              operation: 'update',
              message: 'Host policy revision exhausted',
            }),
          )
        return apply(previous, change, 'update', previous.revision + 1)
      }),
    ),
  })
})

/** Each separate Layer build owns its reference; normal Layer memoization intentionally shares it. */
export const layer = (initial: Change = {}): Layer.Layer<Settings, SettingsError> =>
  Layer.effect(Settings, make(initial))

/** Live defaults without a validation failure channel; every build still allocates its own reference. */
export const layerDefault: Layer.Layer<Settings> = layer().pipe(Layer.orDie)

export const get: Effect.Effect<Snapshot, never, Settings> = Effect.flatMap(
  Settings,
  (self) => self.get,
)
export const update = Effect.fn('Settings.updateService')((change: Change) =>
  Effect.flatMap(Settings, (self) => self.update(change)),
)

/** Optional runtime consumers can use immutable built-in defaults when no live service is provided. */
export const getOrDefault: Effect.Effect<Snapshot> = Effect.flatMap(
  Effect.serviceOption(Settings),
  Option.match({ onNone: () => Effect.succeed(defaults), onSome: (self) => self.get }),
)

export { SettingsError } from './SettingsError.js'
