/**
 * The model boundary: one request per generation, answered by a stream of events that ends with
 * the response. Our loop owns the tool loop, so a response that calls tools ends there, with
 * `toolUse`, and the calls run as our tool tasks; a backend never runs a tool itself.
 *
 * A `Provider` is what a backend implements: an effect/ai `LanguageModel`, an agent SDK with its
 * own tools turned off, or the `Faux` provider in tests. The `Model` service routes each request
 * to the provider its model names.
 */
import { Context, Effect, Layer, Schema } from 'effect'
import type { Stream } from 'effect'
import { JsonObject } from '../storage/schema/Json.ts'
import * as messages from './internal/messages.ts'
import * as route from './internal/route.ts'
import type { DeferredHandle } from './Message.ts'
import {
  AssistantMessage,
  Message,
  ModelRef,
  ModelThinkingLevel,
  ThinkingLevel,
  ToolCall,
} from './Message.ts'
import type { ModelError } from './ModelError.ts'

/** Which model to ask. */
export const Ref = ModelRef
export type Ref = typeof Ref.Type

/** Cost in USD per million tokens. */
export const Cost = Schema.Struct({
  input: Schema.Finite,
  output: Schema.Finite,
  cacheRead: Schema.Finite,
  cacheWrite: Schema.Finite,
})
export type Cost = typeof Cost.Type

/** What the loop needs to know about a model before asking it. */
export const Info = Schema.Struct({
  provider: Schema.String,
  id: Schema.String,
  name: Schema.String,
  /** The provider API its responses name, such as `anthropic-messages`. */
  api: Schema.String,
  reasoning: Schema.Boolean,
  input: Schema.Array(Schema.Literals(['text', 'image'])),
  contextWindow: Schema.Finite,
  maxTokens: Schema.Finite,
  cost: Cost,
})
export type Info = typeof Info.Type

/** A model as a provider declares it; `define` fills in the defaults. */
export interface Definition {
  readonly id: string
  readonly name?: string
  readonly reasoning?: boolean
  readonly input?: ReadonlyArray<'text' | 'image'>
  /** Default 128,000 tokens. */
  readonly contextWindow?: number
  /** Default 16,384 tokens. */
  readonly maxTokens?: number
  /** Default free. */
  readonly cost?: Cost
}

/** The `Info` of a model `provider` serves over `api`. */
export const define = (provider: string, api: string, definition: Definition): Info => ({
  provider,
  id: definition.id,
  name: definition.name ?? definition.id,
  api,
  reasoning: definition.reasoning ?? false,
  input: definition.input ?? ['text', 'image'],
  contextWindow: definition.contextWindow ?? 128_000,
  maxTokens: definition.maxTokens ?? 16_384,
  cost: definition.cost ?? { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
})

/** Request options every provider understands; one that does not support an option ignores it. */
export const Options = Schema.Struct({
  /** Stable per conversation, for providers that cache or route by session. */
  sessionId: Schema.optionalKey(Schema.String),
  reasoning: Schema.optionalKey(ThinkingLevel),
  maxTokens: Schema.optionalKey(Schema.Finite),
  temperature: Schema.optionalKey(Schema.Finite),
  toolChoice: Schema.optionalKey(Schema.Literals(['auto', 'none'])),
  cacheRetention: Schema.optionalKey(Schema.Literals(['none', 'short', 'long'])),
  /** Ask for a deferred (batch) response, polled later. */
  deferred: Schema.optionalKey(
    Schema.Union([
      Schema.Boolean,
      Schema.Struct({ window: Schema.optionalKey(Schema.Literals(['15m', '1h', '24h'])) }),
    ]),
  ),
  timeoutMs: Schema.optionalKey(Schema.Finite),
  headers: Schema.optionalKey(Schema.Record(Schema.String, Schema.String)),
  metadata: Schema.optionalKey(JsonObject),
})
export type Options = typeof Options.Type

/**
 * One generation's request. The system prompt and the tools live in the transcript's system
 * messages; `Transcript` replays them for providers that take them outside the message list.
 */
export const Request = Schema.Struct({
  model: Ref,
  messages: Schema.Array(Message),
  options: Schema.optionalKey(Options),
})
export type Request = typeof Request.Type

const index = { contentIndex: Schema.Finite }

/**
 * What a response streams. Deltas address content blocks by index; a delta to the next index
 * starts a block. `done` carries the whole response and ends the stream; a failure ends it in the
 * error channel instead.
 */
export const Event = Schema.Union([
  Schema.Struct({ type: Schema.Literal('text_delta'), ...index, delta: Schema.String }),
  Schema.Struct({ type: Schema.Literal('thinking_delta'), ...index, delta: Schema.String }),
  Schema.Struct({
    type: Schema.Literal('toolcall_start'),
    ...index,
    id: Schema.String,
    name: Schema.String,
  }),
  Schema.Struct({ type: Schema.Literal('toolcall_end'), ...index, toolCall: ToolCall }),
  /** The response: `stop`, `length`, `toolUse`, or `deferred` with its handle. */
  Schema.Struct({ type: Schema.Literal('done'), message: AssistantMessage }),
])
export type Event = typeof Event.Type

/**
 * What a backend implements to answer requests for its models: a plain value. `R` is what it
 * needs to answer, such as an `HttpClient` or `Credentials`; the `Model` layer over it needs the
 * same.
 */
export interface Provider<out R = never> {
  readonly name: string
  readonly models: ReadonlyArray<Info>
  readonly stream: (request: Request, model: Info) => Stream.Stream<Event, ModelError, R>
  /** Fetch a deferred response; it may still be deferred. */
  readonly poll?: (
    handle: DeferredHandle,
    model: Info,
  ) => Effect.Effect<AssistantMessage, ModelError, R>
  /** Cancel a deferred response, best effort. */
  readonly cancel?: (handle: DeferredHandle, model: Info) => Effect.Effect<void, ModelError, R>
}

/** A speed or priority tier a model can be served at, as Codex lists them. */
export const ServiceTier = Schema.Struct({
  id: Schema.String,
  name: Schema.String,
  description: Schema.String,
})
export type ServiceTier = typeof ServiceTier.Type

/** A model as its provider lists it now, for a model picker. */
export const Listed = Schema.Struct({
  provider: Schema.String,
  id: Schema.String,
  name: Schema.String,
  description: Schema.optionalKey(Schema.String),
  /** Whether requests to it route through this provider; one it does not serve needs `models`. */
  served: Schema.Boolean,
  /** The levels `reasoning` takes for it, where known. */
  thinkingLevels: Schema.optionalKey(Schema.Array(ModelThinkingLevel)),
  defaultThinkingLevel: Schema.optionalKey(ModelThinkingLevel),
  serviceTiers: Schema.Array(ServiceTier),
  contextWindow: Schema.optionalKey(Schema.Finite),
})
export type Listed = typeof Listed.Type

/** A stock provider, which can also list the models its account reaches, over its own credentials. */
export interface Listable<out R = never> extends Provider<R> {
  /** The models the provider's API lists now. */
  readonly listModels: Effect.Effect<ReadonlyArray<Listed>, ModelError, R>
}

export interface Service {
  /** The models its providers serve now. */
  readonly models: Effect.Effect<ReadonlyArray<Info>>
  /** The model `ref` names, if a provider serves it now. */
  readonly info: (ref: Ref) => Effect.Effect<Info | undefined>
  /** Stream one response; an unknown model fails `UnknownModel`. */
  readonly stream: (request: Request) => Stream.Stream<Event, ModelError>
  /** Fetch a deferred response; providers without deferred responses fail `Rejected`. */
  readonly poll: (ref: Ref, handle: DeferredHandle) => Effect.Effect<AssistantMessage, ModelError>
  /** Cancel a deferred response; a no-op for providers without deferred responses. */
  readonly cancel: (ref: Ref, handle: DeferredHandle) => Effect.Effect<void, ModelError>
}

export class Model extends Context.Service<Model, Service>()('effect-harness/model/Model') {}

/**
 * A `Model` over `providers`, answering with what they need from this context; a later provider of
 * the same name replaces an earlier one.
 */
export const make = <R = never>(
  providers: Iterable<Provider<R>>,
): Effect.Effect<Service, never, R> => {
  const routes = Effect.succeed(route.routes(providers))
  return Effect.map(Effect.context<R>(), (context) => route.make(routes, context))
}

/** A `Model` over `providers`; it needs what they need. */
export const layer = <R = never>(providers: Iterable<Provider<R>>): Layer.Layer<Model, never, R> =>
  Layer.effect(Model, make(providers))

/**
 * A `Model` over the providers `current` reads, at each call: for a host whose people add and
 * remove accounts while the loop runs, such as from a `SubscriptionRef`. Build each provider once,
 * when it is added, and keep it: a provider holds state, such as its token refresh lock. A request
 * whose provider was removed fails `UnknownModel`, and its run ends `no_model`.
 */
export const makeDynamic = <R = never>(
  current: Effect.Effect<Iterable<Provider<R>>>,
): Effect.Effect<Service, never, R> =>
  Effect.map(Effect.context<R>(), (context) =>
    route.make(Effect.map(current, route.routes), context),
  )

/** `makeDynamic` as a Layer; it needs what the providers need. */
export const layerDynamic = <R = never>(
  current: Effect.Effect<Iterable<Provider<R>>>,
): Layer.Layer<Model, never, R> => Layer.effect(Model, makeDynamic(current))

/**
 * The response after each event: partials while it streams (`pending`), then the final message.
 * A stream that ends without `done` fails `Retryable`.
 */
export const responses: (
  model: Info,
  timestamp: number,
) => <E, R>(
  events: Stream.Stream<Event, E, R>,
) => Stream.Stream<AssistantMessage, E | ModelError, R> = messages.messages

/** The final response of `events`; a stream that ends without `done` fails `Retryable`. */
export const collect: <E, R>(
  events: Stream.Stream<Event, E, R>,
  model: Info,
) => Effect.Effect<AssistantMessage, E | ModelError, R> = messages.collect
