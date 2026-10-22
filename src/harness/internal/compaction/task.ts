/* oxlint-disable effecttsgo/any-unknown-in-error-context -- extension code may fail with anything; a failure is reported or answers the call */
/**
 * The compaction task: its phases and abort handler.
 */
import { Clock, Effect, Option, Result } from 'effect'
import type { Draft } from '../../../delta/Tracker.ts'
import type { AssistantMessage, Message } from '../../../model/Message.ts'
import * as Model from '../../../model/Model.ts'
import type { ModelError } from '../../../model/ModelError.ts'
import type { Tx } from '../../../session/Session.ts'
import type { EntryId } from '../../../storage/schema/Ids.ts'
import type { Definition, NextState, Runtime, RunningTask } from '../../../task/Task.ts'
import type { Checkpoint, Input, Pending, Result as CompactionResult } from '../../Compaction.ts'
import * as Live from '../../Live.ts'
import { compactionFor, retryDelayMs } from '../../Settings.ts'
import type { Settings } from '../../Settings.ts'
import * as Usage from '../../Usage.ts'
import { admit } from '../admission.ts'
import { agentOf, modelOf, readView, sleepUntil } from '../generation/env.ts'
import type { Env } from '../generation/env.ts'
import { beforeCompact, fixRequest } from './request.ts'
import { selectCut, summarizedMessages } from './select.ts'
import { COMPACTION_KIND, failStatus, removeStatus, statusOf } from './status.ts'
import * as Summary from './summary.ts'

type CompactionRuntime = Runtime<Input, Checkpoint, CompactionResult>
type Next = NextState<Checkpoint, CompactionResult>
type Request = Omit<Extract<Checkpoint, { readonly phase: 'summarize' }>, 'phase'>

type Phase<P extends Checkpoint['phase']> = {
  readonly input: Input
  readonly state: { readonly checkpoint: Extract<Checkpoint, { readonly phase: P }> }
}

const failNoModel = (runtime: CompactionRuntime, ref: Model.Ref | undefined) => {
  const message =
    ref === undefined
      ? 'No model is configured'
      : `Model ${ref.provider}/${ref.modelId} is not available`
  return runtime.commit((tx) =>
    Effect.map(tx.doc(Live.document, runtime.conversationId), (live): Next => {
      failStatus(live, runtime.taskId, message)
      const error = { message, detail: { reason: 'no_model' } }
      return { status: 'terminal', outcome: { status: 'failed', error } }
    }),
  )
}

/** Remove the status and complete without a summary. */
const complete = (runtime: CompactionRuntime) =>
  runtime.commit((tx) =>
    Effect.map(tx.doc(Live.document, runtime.conversationId), (live): Next => {
      removeStatus(live, runtime.taskId)
      return { status: 'terminal', outcome: { status: 'completed', result: {} } }
    }),
  )

/**
 * The `select` phase: choose the range to summarize, ask `beforeCompact`, and fix the
 * summarization request, or complete or place a summary a hook decided.
 */
const selectPhase = (env: Env) =>
  Effect.fnUntraced(function* (task: Phase<'select'>, runtime: CompactionRuntime) {
    const agent = yield* agentOf(env, runtime)
    const ref = yield* modelOf(runtime, agent)
    const info = ref === undefined ? undefined : yield* env.model.info(ref)
    if (ref === undefined || info === undefined) return yield* failNoModel(runtime, ref)
    const policy = compactionFor(yield* env.settings, ref)
    const view = yield* readView(runtime)
    const cut = selectCut(view, policy.keepRecentTokens)
    const firstKept = cut === undefined ? undefined : view.entries[cut]?.id
    if (cut === undefined || firstKept === undefined) return yield* complete(runtime)
    const { reason, instructions } = task.input
    const pending: Pending = {
      reason,
      entries: view.entries.slice(0, cut),
      messages: summarizedMessages(view, cut),
      firstKept,
      ...(instructions === undefined ? {} : { instructions }),
    }
    const decision = yield* beforeCompact(agent, pending, runtime)
    if (Option.isSome(decision))
      return yield* 'decline' in decision.value
        ? complete(runtime)
        : place(env, runtime, firstKept, decision.value.summary)
    const request = yield* fixRequest(env, agent, info, view, firstKept)
    return yield* runtime.commit((): Effect.Effect<Next> =>
      Effect.succeed({ status: 'running', checkpoint: { phase: 'summarize', ...request } }),
    )
  })

/** Place a summary a hook supplied, in a commit of its own. */
const place = (env: Env, runtime: CompactionRuntime, firstKept: EntryId, summary: string) =>
  Effect.flatMap(env.settings, (settings) =>
    runtime.commit((tx, current) =>
      Effect.flatMap(tx.doc(Live.document, runtime.conversationId), (live) =>
        placeSummary(tx, runtime, current, live, firstKept, summary, settings),
      ),
    ),
  )

/**
 * Place the summary and complete. A blocking compaction, owned by its generation, appends it: the
 * generation holds the run and waits. Any other is admitted as a write submission, placed at once
 * when idle, else at the next boundary, since nothing else may append to a busy conversation.
 */
const placeSummary = Effect.fnUntraced(function* (
  tx: Tx,
  runtime: CompactionRuntime,
  current: RunningTask<Input, Checkpoint>,
  live: Draft<Live.State>,
  firstKept: EntryId,
  summary: string,
  settings: Settings,
) {
  removeStatus(live, runtime.taskId)
  delete live.compactionFailure
  const text = `${Summary.SUMMARY_PREFIX}${summary}${Summary.SUMMARY_SUFFIX}`
  const entry = {
    kind: COMPACTION_KIND,
    head: firstKept,
    model: [
      {
        role: 'user',
        content: [{ type: 'text', text }],
        timestamp: yield* Clock.currentTimeMillis,
      },
    ],
    data: { reason: current.input.reason },
  }
  const { conversationId } = runtime
  const result: CompactionResult =
    current.owner === undefined
      ? {
          submissionId: yield* admit(
            tx,
            conversationId,
            { type: 'write', requestId: `compaction:${runtime.taskId}`, entry },
            settings,
          ),
        }
      : { entryId: (yield* tx.appendEntry(conversationId, entry)).id }
  const next: Next = { status: 'terminal', outcome: { status: 'completed', result } }
  return next
})

/** The summarizer's request over the range `select` chose; the context at `tail` is immutable. */
const summaryRequest = (
  runtime: CompactionRuntime,
  input: Input,
  request: Request,
  messages: ReadonlyArray<Message>,
  now: number,
): Model.Request => ({
  model: request.model,
  messages: [
    { role: 'system', content: Summary.SYSTEM_PROMPT, timestamp: now },
    {
      role: 'user',
      content: [{ type: 'text', text: Summary.prompt(messages, input.instructions) }],
      timestamp: now,
    },
  ],
  options: { ...request.options, sessionId: String(runtime.conversationId) },
})

/** What one summarization attempt produced. */
interface Attempt {
  readonly outcome: Result.Result<AssistantMessage, ModelError>
  readonly summary: string | undefined
  /** Whether to wait and try again, at `until`. */
  readonly retrying: boolean
  readonly until: number
}

/** Settle one attempt in a commit: record its usage, then place, retry, or fail. */
const settleAttempt = (
  runtime: CompactionRuntime,
  request: Request,
  attempt: Attempt,
  settings: Settings,
) =>
  runtime.commit((tx, current) =>
    Effect.gen(function* () {
      const { conversationId, taskId } = runtime
      const { outcome, summary } = attempt
      const response = Result.isSuccess(outcome) ? outcome.success : outcome.failure.response
      if (response !== undefined) {
        const key = `${response.provider}/${response.model}`
        yield* Usage.record(tx, conversationId, 'models', key, response.usage)
      }
      const live = yield* tx.doc(Live.document, conversationId)
      const { firstKept } = request
      if (summary !== undefined)
        return yield* placeSummary(tx, runtime, current, live, firstKept, summary, settings)
      const text = Result.isSuccess(outcome)
        ? Summary.failure(outcome.success)
        : `Summarization failed: ${outcome.failure.message}`
      if (attempt.retrying) {
        const status = statusOf(live, taskId)
        if (status !== undefined) status.retry = { at: attempt.until, error: text }
        const next: Next = {
          status: 'running',
          checkpoint: { phase: 'retry', ...request, until: attempt.until },
        }
        return next
      }
      failStatus(live, taskId, text)
      const error = { message: text, detail: { reason: 'model_error' } }
      const next: Next = { status: 'terminal', outcome: { status: 'failed', error } }
      return next
    }),
  )

/** The `summarize` phase: one summarization attempt, then place, retry, or fail. */
const summarizePhase = (env: Env) =>
  Effect.fnUntraced(function* (task: Phase<'summarize'>, runtime: CompactionRuntime) {
    const { phase: _phase, ...request } = task.state.checkpoint
    const info = yield* env.model.info(request.model)
    if (info === undefined) return yield* failNoModel(runtime, request.model)
    const view = yield* readView(runtime, request.tail)
    const cut = view.entries.findIndex((entry) => entry.id === request.firstKept)
    if (cut < 0) return yield* complete(runtime)
    const now = yield* Clock.currentTimeMillis
    const summarized = summarizedMessages(view, cut)
    const sent = summaryRequest(runtime, task.input, request, summarized, now)
    const outcome = yield* Effect.result(Model.collect(env.model.stream(sent), info))
    const settings = yield* env.settings
    const { retry } = settings
    const retrying =
      Result.isFailure(outcome) &&
      outcome.failure.reason._tag === 'Retryable' &&
      retry.enabled &&
      request.attempt <= retry.maxRetries
    const until = (yield* Clock.currentTimeMillis) + retryDelayMs(retry, request.attempt)
    const summary = Result.isSuccess(outcome) ? Summary.text(outcome.success) : undefined
    return yield* settleAttempt(runtime, request, { outcome, summary, retrying, until }, settings)
  })

/** The `retry` phase: wait out the backoff, then summarize again. */
const retryPhase = Effect.fnUntraced(function* (task: Phase<'retry'>, runtime: CompactionRuntime) {
  const { phase: _phase, until, ...request } = task.state.checkpoint
  yield* sleepUntil(until)
  const attempt = request.attempt + 1
  yield* runtime.commit((tx) =>
    Effect.map(tx.doc(Live.document, runtime.conversationId), (live): Next => {
      const status = statusOf(live, runtime.taskId)
      if (status !== undefined) {
        status.attempt = attempt
        delete status.retry
      }
      return { status: 'running', checkpoint: { phase: 'summarize', ...request, attempt } }
    }),
  )
})

export const definition = (env: Env): Definition<Input, Checkpoint, CompactionResult> => ({
  name: COMPACTION_KIND,
  version: 1,
  initial: () => ({ phase: 'select' }),
  phases: { select: selectPhase(env), summarize: summarizePhase(env), retry: retryPhase },
  abort: (_task, runtime) =>
    runtime.commit((tx) =>
      Effect.map(tx.doc(Live.document, runtime.conversationId), (live): Next => {
        removeStatus(live, runtime.taskId)
        return { status: 'terminal', outcome: { status: 'aborted' } }
      }),
    ),
})
