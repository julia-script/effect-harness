/**
 * A scripted provider for tests, ported from pi-ai's faux provider. Each request takes the next
 * scripted step and streams it in chunks; usage is estimated from the prompt, with cache reads for
 * a repeated session prefix. A request that asks for a deferred response is answered by polling.
 */
import type { Effect } from 'effect'
import type { NonEmptyReadonlyArray } from 'effect/Array'
import * as faux from './internal/faux.ts'
import type { AssistantMessage, DeferredHandle, StopReason } from './Message.ts'
import { emptyUsage } from './Message.ts'
import type { Definition, Info, Provider, Ref, Request } from './Model.ts'
import type { ModelError } from './ModelError.ts'

export interface Options {
  /** Default `faux`. */
  readonly provider?: string
  /** Default `faux`. */
  readonly api?: string
  /** Default one model, `faux-1`. */
  readonly models?: ReadonlyArray<Definition>
  readonly deferred?: {
    /** Polls that return the handle again before the response is ready. */
    readonly pendingFetches?: number
    readonly pollAfterMs?: number
  }
  /** Tokens per streamed chunk, four characters each; default 4. */
  readonly tokenSize?: number
  /** Stream at this rate on the clock; default as fast as possible. */
  readonly tokensPerSecond?: number
}

export interface State {
  readonly callCount: number
  readonly deferredFetchCount: number
  readonly cancelledDeferred: ReadonlyArray<DeferredHandle>
}

/** A step computed from its request. */
export type Respond = (
  request: Request,
  model: Info,
  state: State,
) => Effect.Effect<AssistantMessage, ModelError>

/**
 * What one request answers: a response, a failure, or a function of the request. A response that
 * ends in `error` fails `Rejected`, carrying it.
 */
export type Step = AssistantMessage | ModelError | Respond

export interface Faux {
  readonly provider: Provider
  readonly models: NonEmptyReadonlyArray<Info>
  /** A reference to `modelId`, default the first model. */
  readonly ref: (modelId?: string) => Ref
  readonly setResponses: (steps: ReadonlyArray<Step>) => Effect.Effect<void>
  readonly appendResponses: (steps: ReadonlyArray<Step>) => Effect.Effect<void>
  /** Steps not taken yet. */
  readonly pending: Effect.Effect<number>
  readonly state: Effect.Effect<State>
  /** Every request so far, oldest first. */
  readonly requests: Effect.Effect<ReadonlyArray<Request>>
}

export const make = (options: Options = {}): Effect.Effect<Faux> => faux.make(options)

type Block = AssistantMessage['content'][number]

export const text = (value: string): Block => ({ type: 'text', text: value })

export const thinking = (value: string): Block => ({ type: 'thinking', thinking: value })

/** A tool call; without an `id`, the provider assigns one per request and position. */
export const toolCall = (
  name: string,
  args: Extract<Block, { type: 'toolCall' }>['arguments'],
  id = '',
): Block => ({ type: 'toolCall', id, name, arguments: args })

/**
 * A response. The provider fills in its API, provider, model and usage; a zero timestamp takes
 * the clock's time.
 */
export const message = (
  content: string | Block | ReadonlyArray<Block>,
  options: {
    readonly stopReason?: StopReason
    readonly errorMessage?: string
    readonly responseId?: string
    readonly timestamp?: number
  } = {},
): AssistantMessage => {
  let blocks: ReadonlyArray<Block>
  if (typeof content === 'string') blocks = [text(content)]
  else if ('type' in content) blocks = [content]
  else blocks = content
  return {
    role: 'assistant',
    content: blocks,
    api: 'faux',
    provider: 'faux',
    model: 'faux-1',
    usage: emptyUsage,
    stopReason: options.stopReason ?? 'stop',
    ...(options.errorMessage === undefined ? {} : { errorMessage: options.errorMessage }),
    ...(options.responseId === undefined ? {} : { responseId: options.responseId }),
    timestamp: options.timestamp ?? 0,
  }
}
