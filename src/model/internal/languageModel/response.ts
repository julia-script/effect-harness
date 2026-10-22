/**
 * An effect/ai response stream as our events. Parts are addressed by ID there and by content
 * index here; the response is built as the events apply, then finished by the `finish` part.
 */
import { Duration, Effect, Option, Predicate, Schema, Stream } from 'effect'
import { AiError } from 'effect/ai'
import type { Response } from 'effect/ai'
import { JsonObject } from '../../../storage/schema/Json.ts'
import type { AssistantContent, AssistantMessage, StopReason, Usage } from '../../Message.ts'
import type { Event, Info } from '../../Model.ts'
import {
  ContextOverflow,
  isOverflowText,
  ModelError,
  Rejected,
  Retryable,
  Unavailable,
} from '../../ModelError.ts'
import { apply, partial } from '../messages.ts'
import type { Tools } from './prompt.ts'

type Part = Response.StreamPart<Tools, 'deferred'>

interface State {
  readonly message: AssistantMessage
  /** The content index of each part ID. */
  readonly indices: ReadonlyMap<string, number>
  /** The provider metadata each part has sent so far, merged by provider. */
  readonly metadata: ReadonlyMap<string, Metadata>
}

/** Part metadata: each provider's fields under its name. */
type Metadata = Readonly<Record<string, unknown>>

type Step = Effect.Effect<readonly [State, ReadonlyArray<Event>], ModelError>

/** The failure an effect/ai error means. */
export const fromAiError = (error: unknown): ModelError => {
  const message = Predicate.hasProperty(error, 'message') ? String(error.message) : String(error)
  if (!AiError.isAiError(error))
    return new ModelError({ message, reason: new Rejected({}), cause: error })
  const { reason } = error
  if (reason._tag === 'RateLimitError') {
    const after = reason.retryAfter
    const retryAfterMs = after === undefined ? {} : { retryAfterMs: Duration.toMillis(after) }
    return new ModelError({ message, reason: new Retryable(retryAfterMs), cause: error })
  }
  if (reason._tag === 'InvalidRequestError' && isOverflowText(message))
    return new ModelError({ message, reason: new ContextOverflow({}), cause: error })
  if (reason._tag === 'QuotaExhaustedError' || reason._tag === 'AuthenticationError')
    return new ModelError({ message, reason: new Unavailable({}), cause: error })
  const next = error.isRetryable ? new Retryable({}) : new Rejected({})
  return new ModelError({ message, reason: next, cause: error })
}

const emit = (state: State, events: ReadonlyArray<Event>, indices = state.indices): Step =>
  Effect.succeed([
    {
      ...state,
      message: events.reduce((message, event) => apply(message, event), state.message),
      indices,
    },
    events,
  ])

/** `state` with the block at `id` changed by `change`. */
const updateBlock = (
  state: State,
  id: string,
  change: (block: AssistantContent) => AssistantContent,
): Step => {
  const index = state.indices.get(id)
  const block = index === undefined ? undefined : state.message.content[index]
  if (index === undefined || block === undefined) return emit(state, [])
  const content = state.message.content.with(index, change(block))
  return Effect.succeed([{ ...state, message: { ...state.message, content } }, []])
}

/** Run `next` on the state `step` leaves, keeping the events of both. */
const andThen = (step: Step, next: (state: State) => Step): Step =>
  Effect.flatMap(step, ([state, events]) =>
    Effect.map(next(state), ([after, more]) => [after, [...events, ...more]] as const),
  )

/** `previous` with each provider's fields in `next` laid over its own. */
const mergeMetadata = (previous: Metadata, next: Metadata): Metadata =>
  Object.fromEntries([
    ...Object.entries(previous),
    ...Object.entries(next).map(([provider, fields]) => {
      const before = previous[provider]
      return [
        provider,
        Predicate.isObject(before) && Predicate.isObject(fields)
          ? { ...before, ...fields }
          : fields,
      ]
    }),
  ])

/** Start a block for part `id` with `event`, at the next index. */
const start = (state: State, id: string, event: (contentIndex: number) => Event): Step => {
  const contentIndex = state.message.content.length
  return emit(state, [event(contentIndex)], new Map(state.indices).set(id, contentIndex))
}

const delta = (state: State, id: string, type: 'text_delta' | 'thinking_delta', text: string) => {
  const contentIndex = state.indices.get(id)
  if (contentIndex === undefined)
    return start(state, id, (index) => ({ type, contentIndex: index, delta: text }))
  return emit(state, [{ type, contentIndex, delta: text }])
}

const argumentsOf = Schema.decodeUnknownOption(JsonObject)

const toolCall = (state: State, part: Extract<Part, { type: 'tool-call' }>): Step => {
  if (part.providerExecuted) return emit(state, [])
  const decoded = argumentsOf(part.params)
  const call: AssistantContent = {
    type: 'toolCall',
    id: part.id,
    name: part.name,
    arguments: Option.getOrElse(decoded, () => ({})),
    ...(Option.isSome(decoded) ? {} : { argumentsError: 'The arguments are not a JSON object' }),
  }
  const known = state.indices.get(part.id)
  if (known !== undefined)
    return emit(state, [{ type: 'toolcall_end', contentIndex: known, toolCall: call }])
  const contentIndex = state.message.content.length
  return emit(
    state,
    [
      { type: 'toolcall_start', contentIndex, id: part.id, name: part.name },
      { type: 'toolcall_end', contentIndex, toolCall: call },
    ],
    new Map(state.indices).set(part.id, contentIndex),
  )
}

/** The cost of `tokens` at `rate` USD per million. */
const per = (tokens: number, rate: number) => (tokens * rate) / 1_000_000

const usageOf = (usage: Response.Usage, model: Info): Usage => {
  const cacheRead = usage.inputTokens.cacheRead ?? 0
  const cacheWrite = usage.inputTokens.cacheWrite ?? 0
  const input =
    usage.inputTokens.uncached ??
    Math.max(0, (usage.inputTokens.total ?? 0) - cacheRead - cacheWrite)
  const output = usage.outputTokens.total ?? 0
  const cost = {
    input: per(input, model.cost.input),
    output: per(output, model.cost.output),
    cacheRead: per(cacheRead, model.cost.cacheRead),
    cacheWrite: per(cacheWrite, model.cost.cacheWrite),
  }
  return {
    input,
    output,
    cacheRead,
    cacheWrite,
    ...(usage.outputTokens.reasoning === undefined
      ? {}
      : { reasoning: usage.outputTokens.reasoning }),
    totalTokens: input + output + cacheRead + cacheWrite,
    cost: { ...cost, total: cost.input + cost.output + cost.cacheRead + cost.cacheWrite },
  }
}

const stopReasons: Record<Response.FinishReason, StopReason> = {
  stop: 'stop',
  length: 'length',
  'tool-calls': 'toolUse',
  'content-filter': 'error',
  error: 'error',
  pause: 'stop',
  other: 'stop',
  unknown: 'stop',
}

const finish = (state: State, part: Extract<Part, { type: 'finish' }>, model: Info): Step => {
  const calls = state.message.content.some((block) => block.type === 'toolCall')
  const stopReason = calls ? 'toolUse' : stopReasons[part.reason]
  const message: AssistantMessage = {
    ...state.message,
    stopReason,
    rawStopReason: part.reason,
    usage: usageOf(part.usage, model),
  }
  if (stopReason !== 'error') return emit(state, [{ type: 'done', message }])
  const text = `The response ended with ${part.reason}`
  const response = { ...message, errorMessage: text }
  return Effect.fail(new ModelError({ message: text, reason: new Rejected({}), response }))
}

/**
 * Keep part metadata on its block, as JSON. A part may send it with any of its events: Anthropic
 * sends a thinking signature with a delta and redacted thinking with the start, OpenAI sends
 * reasoning items with the end. Later fields win within a provider.
 */
const sign = (
  state: State,
  id: string,
  metadata: Metadata | undefined,
  key: 'textSignature' | 'thinkingSignature',
): Step => {
  if (metadata === undefined || Object.keys(metadata).length === 0) return emit(state, [])
  const merged = mergeMetadata(state.metadata.get(id) ?? {}, metadata)
  const signed = { ...state, metadata: new Map(state.metadata).set(id, merged) }
  return updateBlock(signed, id, (block) => ({ ...block, [key]: JSON.stringify(merged) }))
}

const metadata = (
  state: State,
  part: Extract<Part, { type: 'response-metadata' }>,
  model: Info,
): Step => {
  const { id, modelId } = part
  const message = {
    ...state.message,
    ...(id === undefined ? {} : { responseId: id }),
    ...(modelId === undefined || modelId === model.id ? {} : { responseModel: modelId }),
  }
  return Effect.succeed([{ ...state, message }, []])
}

const step =
  (model: Info) =>
  (state: State, part: Part): Step => {
    switch (part.type) {
      case 'text-start':
        return andThen(
          start(state, part.id, (contentIndex) => ({
            type: 'text_delta',
            contentIndex,
            delta: '',
          })),
          (next) => sign(next, part.id, part.metadata, 'textSignature'),
        )
      case 'text-delta':
        return andThen(delta(state, part.id, 'text_delta', part.delta), (next) =>
          sign(next, part.id, part.metadata, 'textSignature'),
        )
      case 'text-end':
        return sign(state, part.id, part.metadata, 'textSignature')
      case 'reasoning-start':
        return andThen(
          start(state, part.id, (contentIndex) => ({
            type: 'thinking_delta',
            contentIndex,
            delta: '',
          })),
          (next) => sign(next, part.id, part.metadata, 'thinkingSignature'),
        )
      case 'reasoning-delta':
        return andThen(delta(state, part.id, 'thinking_delta', part.delta), (next) =>
          sign(next, part.id, part.metadata, 'thinkingSignature'),
        )
      case 'reasoning-end':
        return sign(state, part.id, part.metadata, 'thinkingSignature')
      case 'tool-params-start':
        if (part.providerExecuted) return emit(state, [])
        return start(state, part.id, (contentIndex) => ({
          type: 'toolcall_start',
          contentIndex,
          id: part.id,
          name: part.name,
        }))
      case 'tool-call':
        return toolCall(state, part)
      case 'response-metadata':
        return metadata(state, part, model)
      case 'finish':
        return finish(state, part, model)
      case 'error':
        return Effect.fail(fromAiError(part.error))
      case 'tool-params-delta':
      case 'tool-params-end':
      case 'tool-result':
      case 'tool-approval-request':
      case 'file':
      case 'source':
        break
    }
    // Parts with nothing for the transcript: argument fragments, provider results, and sources.
    return emit(state, [])
  }

/** `parts` as our events, for a response from `model`. */
export const events = <E, R>(
  parts: Stream.Stream<Part, E, R>,
  model: Info,
  timestamp: number,
): Stream.Stream<Event, ModelError | E, R> =>
  Stream.mapAccumEffect(
    parts,
    (): State => ({ message: partial(model, timestamp), indices: new Map(), metadata: new Map() }),
    step(model),
  )
