/** Bounded compaction policy foundations; these APIs perform no model or storage writes. */
import * as PromptWire from './PromptWire.js'
import * as Effect from 'effect/Effect'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Prompt from 'effect/ai/Prompt'
import * as Tokenizer from 'effect/ai/Tokenizer'
import type * as Model from './Model.js'
import { SelectionSchema as ModelSelectionSchema } from './Model.js'
import * as Projection from './ContextProjection.js'
import * as Identity from './Identity.js'
import * as Record from './Record.js'
import * as Settings from './Settings.js'
import * as Usage from './Usage.js'
import { CompactionError, modelFailure, type Failure } from './CompactionError.js'
import * as Selection from './internal/CompactionSelection.js'
import * as PromptUsage from './internal/PromptUsage.js'
import * as Value from './internal/DocumentValue.js'

export { CompactionError, modelFailure } from './CompactionError.js'
export type { Failure } from './CompactionError.js'

/** One vocabulary: manual compaction ignores enabled; backgroundTokens=0 disables background work. */
export const PolicySchema = Settings.CompactionSchema
export type Policy = Settings.Compaction
export const ReasonSchema = Schema.Literals(['manual', 'threshold', 'overflow'])
export type Reason = typeof ReasonSchema.Type
const Count = Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: Number.MAX_SAFE_INTEGER }))
export const InputSchema = Schema.Struct({
  reason: ReasonSchema,
  contextWindow: Schema.optionalKey(Count.check(Schema.isGreaterThan(0))),
  /** Final planned prompt; sections can occur at the front or in the middle. */
  prompt: Schema.optionalKey(PromptWire.Prompt),
  tools: Schema.optionalKey(Schema.Array(Projection.ToolDeclarationSchema)),
  overheadTokens: Schema.optionalKey(Count),
  baseline: Schema.optionalKey(PromptUsage.Baseline),
  instructions: Schema.optionalKey(Schema.String),
})
export type Input = typeof InputSchema.Type
export const MeasurementSchema = Schema.Struct({
  method: Schema.Literals(['tokenizer', 'estimate', 'usage']),
  /** Tool declarations and wire overhead remain estimated even with a prompt tokenizer. */
  estimatedOverheadTokens: Schema.optionalKey(Count),
  tokens: Count,
})
export type Measurement = typeof MeasurementSchema.Type
export const SelectedSchema = Schema.TaggedStruct('Selected', {
  conversationId: Record.ConversationId,
  firstKept: Record.EntryId,
  tail: Record.EntryId,
  legacyInterpretation: Projection.LegacyInterpretationSchema,
  reason: ReasonSchema,
  mode: Schema.Literals(['background', 'blocking']),
  policy: PolicySchema,
  instructions: Schema.optionalKey(Schema.String),
  /** Canonically repaired prefix and suffix; native Prompt carries no custom provider fields. */
  source: PromptWire.Prompt,
  retained: PromptWire.Prompt,
  measurement: MeasurementSchema,
})
export type Selected = typeof SelectedSchema.Type
export const ResultSchema = Schema.Union([
  Schema.TaggedStruct('Noop', {
    reason: Schema.Literals(['disabled', 'unknownWindow', 'belowThreshold', 'noSafeCut']),
    measurement: MeasurementSchema,
  }),
  SelectedSchema,
])
export type Result = typeof ResultSchema.Type

/** Serialized adapter config and identity, distinct from live Effect Config services. */
export const SummaryConfigSchema = Schema.Struct({
  requestId: Identity.RequestId,
  attempt: Usage.AttemptId,
  model: ModelSelectionSchema,
  stream: Settings.StreamSchema,
  maxOutputTokens: Count.check(Schema.isGreaterThan(0)),
})
export type SummaryConfig = typeof SummaryConfigSchema.Type
export const SummaryRequestSchema = Schema.Struct({
  selection: SelectedSchema,
  config: SummaryConfigSchema,
})
export type SummaryRequest = typeof SummaryRequestSchema.Type
export const SummarySchema = Schema.NonEmptyString

const freeze = <A>(value: A): A => {
  if (
    value !== null &&
    typeof value === 'object' &&
    !ArrayBuffer.isView(value) &&
    !Object.isFrozen(value)
  ) {
    for (const child of Object.values(value)) freeze(child)
    Object.freeze(value)
  }
  return value
}

const isErrorPart = Schema.is(Schema.Struct({ type: Schema.Literal('error') }))

/**
 * Select only from a canonical bounded view. Supply the complete planned prompt for
 * threshold counting. Strict thresholds are window-reserve and window-reserve-background.
 * Unknown windows disable thresholds; manual and explicit overflow can still select a cut.
 *
 * A supplied native Tokenizer is captured once and counts the prompt and each safe
 * retained suffix. Without one, the explicit approximation uses 3.5 characters/token and
 * 4800 characters/file; it is not a provider guarantee. Tokenizer failures never fall back.
 * Every selected cut retains a whole newest entry and at least keepRecentTokens according
 * to the selected measurement method. Head markers and tool groups are never split.
 */
export const select = Effect.fnUntraced(function* (
  inputView: Projection.ContextView,
  inputPolicy: Policy,
  input: Input,
): Effect.fn.Return<Result, Failure> {
  const view = yield* Value.copy(Schema.toCodecJson(Projection.ContextViewSchema), inputView)
  const policy = yield* Value.copy(PolicySchema, inputPolicy)
  const options = yield* Value.copy(Schema.toCodecJson(InputSchema), input)
  if (view.activeEntries.length !== view.contributions.length)
    return yield* new CompactionError({
      reason: 'invalid',
      message: 'Canonical contributions must align with entries',
    })
  const bound = yield* Effect.serviceOption(Tokenizer.Tokenizer)
  const tokenizer = Option.getOrUndefined(bound)
  const count = (prompt: Prompt.Prompt) =>
    tokenizer === undefined
      ? Effect.succeed(
          prompt.content.reduce((total, message) => total + Selection.estimateMessage(message), 0),
        )
      : tokenizer.tokenize(prompt).pipe(
          Effect.map((tokens) => tokens.length),
          Effect.mapError((cause) => modelFailure(cause)),
        )
  const prompt = options.prompt ?? view.prompt
  const tools = options.tools ?? []
  const overhead =
    (options.overheadTokens ?? 0) +
    (tools.length === 0 ? 0 : Math.ceil(JSON.stringify(tools).length / 3.5))
  let tokens = (yield* count(prompt)) + overhead
  let method: Measurement['method'] = tokenizer === undefined ? 'estimate' : 'tokenizer'
  if (
    tokenizer === undefined &&
    options.baseline !== undefined &&
    PromptUsage.applicable(options.baseline, prompt, tools)
  ) {
    tokens =
      options.baseline.inputTokens +
      (yield* count(
        Prompt.fromMessages(prompt.content.slice(options.baseline.prompt.content.length)),
      ))
    method = 'usage'
  }
  const measurement: Measurement = {
    method,
    tokens,
    ...(method === 'usage' ? {} : { estimatedOverheadTokens: overhead }),
  }
  const noop = (reason: Extract<Result, { readonly _tag: 'Noop' }>['reason']): Result =>
    freeze({ _tag: 'Noop', reason, measurement })
  let mode: Selected['mode'] = 'blocking'
  if (options.reason === 'threshold') {
    if (!policy.enabled) return noop('disabled')
    if (options.contextWindow === undefined) return noop('unknownWindow')
    const blocking = options.contextWindow - policy.reserveTokens
    if (tokens <= blocking) {
      if (policy.backgroundTokens === 0 || tokens <= blocking - policy.backgroundTokens)
        return noop('belowThreshold')
      mode = 'background'
    }
  }
  let cut: number | undefined
  if (tokenizer === undefined) {
    cut = Selection.selectCut(view, policy.keepRecentTokens)
  } else {
    // Native token counts need not be additive across entries: count each whole safe suffix.
    const safe = Selection.candidates(view)
    for (let index = safe.length - 1; index >= 0; index--) {
      const candidate = safe[index]!
      const suffix = Projection.projectMessages(view.contributions.slice(candidate).flat())
      if ((yield* count(suffix)) >= policy.keepRecentTokens) {
        cut = candidate
        break
      }
    }
  }
  if (cut === undefined) return noop('noSafeCut')
  const firstKept = view.activeEntries[cut]!.id
  const tail = view.bounds.tail
  if (tail === undefined || firstKept > tail)
    return yield* new CompactionError({
      reason: 'invalid',
      message: 'Selected range must lie within the canonical tail',
    })
  return freeze({
    _tag: 'Selected',
    conversationId: view.bounds.conversationId,
    firstKept,
    tail,
    legacyInterpretation: view.bounds.legacyInterpretation,
    reason: options.reason,
    mode,
    policy,
    ...(options.instructions === undefined ? {} : { instructions: options.instructions }),
    source: Projection.projectMessages(view.contributions.slice(0, cut).flat()),
    retained: Projection.projectMessages(view.contributions.slice(cut).flat()),
    measurement,
  })
})

/**
 * Capture detached immutable range/config/attempt inputs for later durable Task phases.
 * This does not invoke a provider, checkpoint a task, or place a summary. Recovery must
 * decode this schema and retain requestId whenever the prepared work is unchanged.
 */
export const pinRequest = Effect.fnUntraced(function* (
  selection: Selected,
  config: SummaryConfig,
): Effect.fn.Return<SummaryRequest, Failure> {
  const request = yield* Value.copy(Schema.toCodecJson(SummaryRequestSchema), { selection, config })
  if (request.selection.firstKept > request.selection.tail)
    return yield* new CompactionError({
      reason: 'invalid',
      message: 'First kept entry follows the pinned tail',
    })
  return freeze(request)
})

/** Only a clean native stop with nonempty text and no tool/error parts is a valid summary. */
export const validateSummary = Effect.fnUntraced(function* (
  response: Pick<Model.DeferredResponse, 'content' | 'finishReason'>,
  localBudget?: { readonly maximum: number; readonly outputTokens?: number },
): Effect.fn.Return<string, CompactionError> {
  if (response.finishReason !== 'stop')
    return yield* new CompactionError({
      reason: 'invalidSummary',
      message: `Summary did not finish cleanly: ${response.finishReason}`,
    })
  if (
    response.content.some(
      (part) =>
        part.type.startsWith('tool-') ||
        isErrorPart(part) ||
        (part.type === 'finish' && part.reason !== 'stop'),
    )
  )
    return yield* new CompactionError({
      reason: 'invalidSummary',
      message: 'Summary contains tool activity or an unsuccessful finish',
    })
  const text = response.content
    .flatMap((part) => (part.type === 'text' ? [part.text] : []))
    .join('\n')
    .trim()
  if (text.length === 0)
    return yield* new CompactionError({
      reason: 'invalidSummary',
      message: 'Summary contains no text',
    })
  if (
    localBudget !== undefined &&
    ((localBudget.outputTokens !== undefined && localBudget.outputTokens > localBudget.maximum) ||
      new TextEncoder().encode(text).length > localBudget.maximum * 4)
  )
    return yield* new CompactionError({
      reason: 'invalidSummary',
      message:
        'Summary exceeds the local acceptance bound (reported output tokens or four UTF-8 bytes per budget token)',
    })
  return text
})
