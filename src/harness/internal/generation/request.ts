/* oxlint-disable effecttsgo/any-unknown-in-error-context -- extension code may fail with anything; a failure is reported or answers the call */
/**
 * The `request` and `poll` phases: one model response, streamed or fetched, then classified.
 */
import { Clock, Effect, Fiber, Option, Ref, Stream } from 'effect'
import { cast } from 'effect/Function'
import type { AssistantMessage, Message } from '../../../model/Message.ts'
import * as Model from '../../../model/Model.ts'
import { ModelError, Retryable } from '../../../model/ModelError.ts'
import * as Hook from '../../../task/Hook.ts'
import * as Agent from '../../Agent.ts'
import { Hooks } from '../../Generation.ts'
import type { Checkpoint } from '../../Generation.ts'
import * as Live from '../../Live.ts'
import { strictJson } from '../../../internal/strictJson.ts'
import { convertPartial } from '../run.ts'
import { classify, failNoModel } from './classify.ts'
import { agentOf, readView, sleepUntil } from './env.ts'
import type { Env, GenerationRuntime } from './env.ts'

/** Commit `partial` as the attempt's live message. */
const commitPartial = (runtime: GenerationRuntime, attempt: number, partial: AssistantMessage) =>
  runtime.commit((tx) =>
    Effect.map(tx.doc(Live.document, runtime.conversationId), (live) => {
      live.generation ??= { attempt }
      live.generation.message = cast(strictJson({ ...partial }))
    }),
  )

/**
 * Stream one response and return the final message. Partials commit at most every `interval`,
 * one commit at a time; each commit is uninterruptible, so stopping the flusher waits for the one
 * in flight and no stale partial lands after the outcome. A commit that fails, as after an abort
 * mark, leaves the committed state as it was.
 */
const streamResponse = Effect.fnUntraced(function* (
  runtime: GenerationRuntime,
  env: Env,
  request: Model.Request,
  info: Model.Info,
  attempt: number,
) {
  const { partialIntervalMs } = yield* env.settings
  const pending = yield* Ref.make(Option.none<AssistantMessage>())
  const final = yield* Ref.make(Option.none<AssistantMessage>())
  const flush = Effect.flatMap(Ref.getAndSet(pending, Option.none()), (partial) =>
    Option.isNone(partial)
      ? Effect.void
      : Effect.ignore(Effect.uninterruptible(commitPartial(runtime, attempt, partial.value))),
  )
  const flusher = yield* Effect.forkChild(
    Effect.forever(Effect.andThen(Effect.sleep(partialIntervalMs), flush)),
  )
  const responses = Model.responses(info, yield* Clock.currentTimeMillis)(env.model.stream(request))
  yield* Stream.runForEach(responses, (message) => {
    if (message.stopReason !== 'pending') return Ref.set(final, Option.some(message))
    // A partial without content, such as a deferred response before its handle, shows nothing.
    return message.content.length === 0 ? Effect.void : Ref.set(pending, Option.some(message))
  }).pipe(Effect.ensuring(Fiber.interrupt(flusher)))
  const message = yield* Ref.get(final)
  if (Option.isSome(message)) return message.value
  return yield* new ModelError({
    message: `The ${info.provider}/${info.id} response ended before it finished`,
    reason: new Retryable({}),
  })
})

/** Ask `beforeRequest` in order, each replacing the messages the earlier ones left. */
const beforeRequest = Effect.fnUntraced(function* (
  hooks: ReadonlyArray<Partial<Hooks>>,
  messages: ReadonlyArray<Message>,
  api: Hook.Api,
) {
  let current = messages
  for (const hook of hooks) {
    if (hook.beforeRequest === undefined) continue
    const replaced = yield* Hook.ask(
      hook.beforeRequest({ messages: current }, api),
      'beforeRequest',
    )
    if (Option.isSome(replaced)) current = replaced.value.messages
  }
  return current
})

type Phase<P extends Checkpoint['phase']> = {
  readonly state: { readonly checkpoint: Extract<Checkpoint, { readonly phase: P }> }
}

/**
 * The `request` phase: turn a partial an interrupted attempt left into an aborted entry, read the
 * context through the cutoff, stream the response, and classify it.
 */
export const requestPhase = (env: Env) =>
  Effect.fnUntraced(function* (task: Phase<'request'>, runtime: GenerationRuntime) {
    const { attempt, compacted, model: ref, options, cutoff } = task.state.checkpoint
    const { conversationId } = runtime
    yield* runtime.commit((tx) =>
      Effect.gen(function* () {
        const live = yield* tx.doc(Live.document, conversationId)
        yield* convertPartial(tx, live, conversationId)
        live.generation = { attempt }
      }),
    )
    const info = yield* env.model.info(ref)
    if (info === undefined) return yield* failNoModel(runtime, ref)
    const { messages } = yield* readView(runtime, cutoff)
    const hooks = Agent.hooks(yield* agentOf(env, runtime), Hooks)
    const request: Model.Request = {
      model: ref,
      messages: yield* beforeRequest(hooks, messages, runtime),
      options: { ...options, sessionId: String(conversationId) },
    }
    const outcome = yield* Effect.result(streamResponse(runtime, env, request, info, attempt))
    return yield* classify(
      runtime,
      env,
      { attempt, compacted, model: ref, cutoff, messages, hooks },
      outcome,
    )
  })

/** The `poll` phase: fetch a deferred response at its time, and classify it. */
export const pollPhase = (env: Env) =>
  Effect.fnUntraced(function* (task: Phase<'poll'>, runtime: GenerationRuntime) {
    const { attempt, compacted, model: ref, cutoff, handle, pollAt } = task.state.checkpoint
    if ((yield* env.model.info(ref)) === undefined) return yield* failNoModel(runtime, ref)
    yield* sleepUntil(pollAt)
    const outcome = yield* Effect.result(env.model.poll(ref, handle))
    const hooks = Agent.hooks(yield* agentOf(env, runtime), Hooks)
    const request = { attempt, compacted, model: ref, cutoff, pollAt, hooks }
    return yield* classify(runtime, env, request, outcome)
  })
