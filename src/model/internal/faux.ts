/**
 * The `Faux` provider.
 */
import { Clock, Duration, Effect, Ref, Stream } from 'effect'
import type { Exit } from 'effect'
import type { NonEmptyReadonlyArray } from 'effect/Array'
import type { Faux, Options, State, Step } from '../Faux.ts'
import type { AssistantContent, AssistantMessage, DeferredHandle } from '../Message.ts'
import { emptyUsage } from '../Message.ts'
import type { Definition, Event, Info, Provider, Request } from '../Model.ts'
import { define } from '../Model.ts'
import { ModelError, Rejected } from '../ModelError.ts'
import { estimateTokens, withUsage } from './fauxUsage.ts'

interface Deferred {
  readonly handle: DeferredHandle
  readonly step: Step
  readonly request: Request
  readonly model: Info
  readonly pendingFetches: number
  readonly cancelled: boolean
  readonly final?: Exit.Exit<AssistantMessage, ModelError>
}

/** One faux provider's script and records. */
interface Core {
  readonly options: Options
  readonly api: string
  readonly name: string
  readonly steps: Ref.Ref<ReadonlyArray<Step>>
  readonly requests: Ref.Ref<ReadonlyArray<Request>>
  readonly state: Ref.Ref<State>
  readonly deferreds: Ref.Ref<ReadonlyMap<string, Deferred>>
  /** Each session's last prompt, for the cache estimate. */
  readonly prompts: Map<string, ReadonlyArray<string>>
}

const rejected = (message: string, response?: AssistantMessage) =>
  new ModelError({
    message,
    reason: new Rejected({}),
    ...(response === undefined ? {} : { response }),
  })

const infoOf = (core: Core): NonEmptyReadonlyArray<Info> => {
  const [first = { id: 'faux-1', name: 'Faux Model' }, ...rest] = core.options.models ?? []
  const info = (definition: Definition) => define(core.name, core.api, definition)
  return [info(first), ...rest.map((definition) => info(definition))]
}

/** An empty response from `model`. */
const blank = (core: Core, model: Info) =>
  Effect.map(Clock.currentTimeMillis, (timestamp): AssistantMessage => ({
    role: 'assistant',
    content: [],
    api: core.api,
    provider: core.name,
    model: model.id,
    usage: emptyUsage,
    stopReason: 'stop',
    timestamp,
  }))

/** `raw` as the provider returns it for request number `call`. */
const finish = (core: Core, raw: AssistantMessage, request: Request, model: Info, call: number) =>
  Effect.map(Clock.currentTimeMillis, (now) => {
    const content = raw.content.map((block, index): AssistantContent =>
      block.type === 'toolCall' && block.id === ''
        ? { ...block, id: `${core.name}:${call}:${index}` }
        : block,
    )
    const message: AssistantMessage = {
      ...raw,
      content,
      api: core.api,
      provider: core.name,
      model: model.id,
      timestamp: raw.timestamp === 0 ? now : raw.timestamp,
    }
    return withUsage(message, request, core.prompts)
  })

const resolve = Effect.fnUntraced(function* (
  core: Core,
  step: Step,
  request: Request,
  model: Info,
  call: number,
) {
  if (step instanceof ModelError) return yield* step
  const raw =
    typeof step === 'function' ? yield* step(request, model, yield* Ref.get(core.state)) : step
  const message = yield* finish(core, raw, request, model, call)
  if (message.stopReason === 'error' || message.stopReason === 'aborted')
    return yield* rejected(message.errorMessage ?? 'Faux response failed', message)
  if (message.stopReason === 'pending')
    return yield* rejected('Faux response ended without a stop reason')
  return message
})

const deferredMessage = (core: Core, model: Info, handle: DeferredHandle) =>
  Effect.map(blank(core, model), (message): AssistantMessage => ({
    ...message,
    stopReason: 'deferred',
    deferred: handle,
  }))

/** Register a deferred response to `step`, answered by polling. */
const defer = Effect.fnUntraced(function* (
  core: Core,
  step: Step,
  request: Request,
  model: Info,
  call: number,
) {
  const { pendingFetches = 0, pollAfterMs } = core.options.deferred ?? {}
  const handle: DeferredHandle = {
    provider: core.name,
    modelId: model.id,
    api: core.api,
    id: `deferred:${call}`,
    ...(pollAfterMs === undefined ? {} : { pollAfterMs }),
  }
  const entry: Deferred = {
    handle,
    step,
    request,
    model,
    pendingFetches: Math.max(0, Math.floor(pendingFetches)),
    cancelled: false,
  }
  yield* Ref.update(core.deferreds, (all) => new Map(all).set(handle.id, entry))
  return yield* deferredMessage(core, model, handle)
})

/** Take the next step for `request` and answer it. */
const answer = Effect.fnUntraced(function* (core: Core, request: Request, model: Info) {
  const step = yield* Ref.modify(core.steps, ([next, ...rest]) => [next, rest] as const)
  const { callCount: call } = yield* Ref.updateAndGet(core.state, (current) => ({
    ...current,
    callCount: current.callCount + 1,
  }))
  yield* Ref.update(core.requests, (all) => [...all, request])
  if (step === undefined) {
    const text = 'No more faux responses queued'
    const empty = yield* blank(core, model)
    const response = withUsage(
      { ...empty, stopReason: 'error', errorMessage: text },
      request,
      core.prompts,
    )
    return yield* rejected(text, response)
  }
  const deferred = request.options?.deferred
  if (deferred !== undefined && deferred !== false)
    return yield* defer(core, step, request, model, call)
  return yield* resolve(core, step, request, model, call)
})

const poll = Effect.fnUntraced(function* (core: Core, handle: DeferredHandle) {
  yield* Ref.update(core.state, (current) => ({
    ...current,
    deferredFetchCount: current.deferredFetchCount + 1,
  }))
  const entry = (yield* Ref.get(core.deferreds)).get(handle.id)
  if (entry === undefined || entry.handle.modelId !== handle.modelId)
    return yield* rejected(`Unknown faux deferred response: ${handle.id}`)
  if (entry.cancelled) return yield* rejected(`Faux deferred response was cancelled: ${handle.id}`)
  const save = (next: Deferred) =>
    Ref.update(core.deferreds, (all) => new Map(all).set(handle.id, next))
  if (entry.pendingFetches > 0) {
    yield* save({ ...entry, pendingFetches: entry.pendingFetches - 1 })
    return yield* deferredMessage(core, entry.model, entry.handle)
  }
  const { deferred: _, ...options } = entry.request.options ?? {}
  const request = { ...entry.request, options }
  const final =
    entry.final ?? (yield* Effect.exit(resolve(core, entry.step, request, entry.model, 0)))
  yield* save({ ...entry, final })
  return yield* final
})

const cancel = (core: Core, handle: DeferredHandle) =>
  Effect.andThen(
    Ref.update(core.state, (current) => ({
      ...current,
      cancelledDeferred: [...current.cancelledDeferred, handle],
    })),
    Ref.update(core.deferreds, (all) => {
      const entry = all.get(handle.id)
      return entry === undefined ? all : new Map(all).set(handle.id, { ...entry, cancelled: true })
    }),
  )

/** The chunks of `text`, `size` characters each; an empty text is one empty chunk. */
const chunks = (text: string, size: number): ReadonlyArray<string> => {
  const parts: Array<string> = []
  for (let index = 0; index < text.length; index += size)
    parts.push(text.slice(index, index + size))
  return parts.length > 0 ? parts : ['']
}

const blockEvents = (
  block: AssistantContent,
  contentIndex: number,
  size: number,
): ReadonlyArray<Event> => {
  if (block.type === 'text')
    return chunks(block.text, size).map((delta) => ({ type: 'text_delta', contentIndex, delta }))
  if (block.type === 'thinking')
    return chunks(block.thinking, size).map((delta) => ({
      type: 'thinking_delta',
      contentIndex,
      delta,
    }))
  return [
    { type: 'toolcall_start', contentIndex, id: block.id, name: block.name },
    { type: 'toolcall_end', contentIndex, toolCall: block },
  ]
}

/** The events that stream `message`, at `tokensPerSecond` when set. */
const events = (core: Core, message: AssistantMessage): Stream.Stream<Event> => {
  const size = Math.max(1, core.options.tokenSize ?? 4) * 4
  const all: ReadonlyArray<Event> = [
    ...message.content.flatMap((block, index) => blockEvents(block, index, size)),
    { type: 'done', message },
  ]
  const rate = core.options.tokensPerSecond
  if (rate === undefined || rate <= 0) return Stream.fromIterable(all)
  return Stream.mapEffect(Stream.fromIterable(all), (event) => {
    const text = 'delta' in event ? event.delta : ''
    return Effect.as(Effect.sleep(Duration.millis((estimateTokens(text) / rate) * 1000)), event)
  })
}

export const make = Effect.fnUntraced(function* (options: Options) {
  const core: Core = {
    options,
    api: options.api ?? 'faux',
    name: options.provider ?? 'faux',
    steps: yield* Ref.make<ReadonlyArray<Step>>([]),
    requests: yield* Ref.make<ReadonlyArray<Request>>([]),
    state: yield* Ref.make<State>({ callCount: 0, deferredFetchCount: 0, cancelledDeferred: [] }),
    deferreds: yield* Ref.make<ReadonlyMap<string, Deferred>>(new Map()),
    prompts: new Map(),
  }
  const models = infoOf(core)
  const provider: Provider = {
    name: core.name,
    models,
    stream: (request, model) =>
      Stream.unwrap(Effect.map(answer(core, request, model), (message) => events(core, message))),
    poll: (handle) => poll(core, handle),
    cancel: (handle) => cancel(core, handle),
  }
  const faux: Faux = {
    provider,
    models,
    ref: (modelId = models[0].id) => ({ provider: core.name, modelId }),
    setResponses: (next) => Ref.set(core.steps, next),
    appendResponses: (next) => Ref.update(core.steps, (all) => [...all, ...next]),
    pending: Effect.map(Ref.get(core.steps), (all) => all.length),
    state: Ref.get(core.state),
    requests: Ref.get(core.requests),
  }
  return faux
})
