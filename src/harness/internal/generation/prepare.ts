/**
 * The `prepare` phase: the system prompt, the tool declarations, and compacting first.
 */
import { Clock, Effect, Option, Schema } from 'effect'
import type * as Model from '../../../model/Model.ts'
import type { EntryDraft } from '../../../session/Session.ts'
import type { TaskOutcome } from '../../../storage/schema/Task.ts'
import * as Tool from '../../../tool/Tool.ts'
import { Result as CompactionResult } from '../../Compaction.ts'
import type { Resolved } from '../../Agent.ts'
import type { Checkpoint } from '../../Generation.ts'
import { EmptyConversation } from '../../GenerationError.ts'
import * as Live from '../../Live.ts'
import { compactionFor } from '../../Settings.ts'
import { threshold } from '../compaction/select.ts'
import { createCompaction } from '../compaction/status.ts'
import { decodeAll } from '../context.ts'
import { planSystemEntries, renderSections, replaySections } from '../prompt.ts'
import { fail, failNoModel } from './classify.ts'
import { agentOf, readView } from './env.ts'
import type { Env, GenerationRuntime, Next } from './env.ts'

type Prepare = Extract<Checkpoint, { readonly phase: 'prepare' }>

const decodeResult = Schema.decodeUnknownOption(CompactionResult)

/** Why a blocking compaction that ended without completing leaves nothing to send. */
const compactionFailure = (outcome: TaskOutcome): string => {
  if (outcome.status === 'failed' || outcome.status === 'faulted')
    return `Compaction failed: ${outcome.error.message}`
  if (outcome.status === 'orphaned') return `Compaction failed: ${outcome.reason}`
  return 'Compaction was aborted'
}

/**
 * The blocking compaction this generation waited for, checked once as it resumes: after an
 * overflow, only a summary makes the request fit; after the threshold, one that found nothing to
 * compact leaves a request that may still fit, but one that failed does not.
 */
const checkCompacted = (runtime: GenerationRuntime, checkpoint: Prepare) =>
  Effect.gen(function* () {
    const { compacted, overflow } = checkpoint
    if (compacted === undefined || (overflow === undefined && checkpoint.threshold !== true))
      return true
    const [outcome] = yield* runtime.outcomes([compacted])
    if (outcome === undefined) return true
    if (overflow !== undefined) {
      const result = outcome.status === 'completed' ? decodeResult(outcome.result) : Option.none()
      if (Option.isSome(result) && result.value.entryId !== undefined) return true
      yield* fail(runtime, 'model_error', overflow)
      return false
    }
    if (outcome.status === 'completed') return true
    yield* fail(runtime, 'model_error', compactionFailure(outcome))
    return false
  })

/** Compact first, with a compaction the generation owns, and prepare again once it is done. */
const compactFirst = (runtime: GenerationRuntime, attempt: number) =>
  runtime.commit((tx) =>
    Effect.map(
      createCompaction(tx, runtime.conversationId, { reason: 'threshold' }, runtime.taskId),
      (child): Next => ({
        status: 'waiting',
        checkpoint: { phase: 'prepare', attempt, compacted: child, threshold: true },
        on: [child],
        policy: 'allSettled',
      }),
    ),
  )

type RequestCheckpoint = Extract<Checkpoint, { readonly phase: 'request' }>

/**
 * Append the planned `pi.system` entries and fix the request through the newest entry; start a
 * background compaction when one is due and none is live, checked in this commit.
 */
const fixRequest = (
  runtime: GenerationRuntime,
  entries: ReadonlyArray<EntryDraft>,
  background: boolean,
  request: Omit<RequestCheckpoint, 'phase' | 'cutoff'>,
) =>
  runtime.commit((tx) =>
    Effect.gen(function* () {
      const { conversationId } = runtime
      let cutoff = (yield* tx.scanEntries({ conversationId }, 1)).items[0]?.id
      for (const entry of entries) cutoff = (yield* tx.appendEntry(conversationId, entry)).id
      if (cutoff === undefined)
        return yield* new EmptyConversation({
          message: `Conversation ${conversationId} has no entries to send`,
          conversationId,
        })
      if (background && (yield* tx.doc(Live.document, conversationId)).compactions === undefined)
        yield* createCompaction(tx, conversationId, { reason: 'threshold' })
      const next: Next = { status: 'running', checkpoint: { phase: 'request', ...request, cutoff } }
      return next
    }),
  )

/**
 * Render the system prompt and tool declarations, plan the `pi.system` entries they need, and fix
 * the request, or compact first above the blocking threshold. Only the Harness writes model
 * context to a busy conversation, so the context read here is still current at the commit.
 */
const prepare = (
  env: Env,
  runtime: GenerationRuntime,
  checkpoint: Prepare,
  agent: Resolved,
  info: { readonly ref: Model.Ref; readonly contextWindow: number },
) =>
  Effect.gen(function* () {
    const { attempt, compacted } = checkpoint
    const view = yield* readView(runtime)
    const shown = replaySections(view.messages)
    const { conversationId } = runtime
    const desired = yield* renderSections({ conversationId, agent, read: runtime }, shown)
    const tools = agent.offered.map(({ tool }) => Tool.definition(tool))
    const entries = planSystemEntries(view, desired, tools, yield* Clock.currentTimeMillis)
    const settings = yield* env.settings
    const planned = entries.flatMap((entry) => decodeAll(entry.model ?? []))
    const over =
      compacted === undefined
        ? threshold(view, planned, info.contextWindow, compactionFor(settings, info.ref))
        : undefined
    // The transcript is unchanged until the summary lands.
    if (over === 'blocking') return yield* compactFirst(runtime, attempt)
    const { reasoning } = agent
    const options = reasoning === 'off' ? settings.stream : { ...settings.stream, reasoning }
    return yield* fixRequest(runtime, entries, over === 'background', {
      attempt,
      ...(compacted === undefined ? {} : { compacted }),
      model: info.ref,
      options,
    })
  })

/** The `prepare` phase, once the model is known and any blocking compaction is checked. */
export const preparePhase =
  (env: Env) =>
  (task: { readonly state: { readonly checkpoint: Prepare } }, runtime: GenerationRuntime) =>
    Effect.gen(function* () {
      const { checkpoint } = task.state
      const agent = yield* agentOf(env, runtime)
      const ref = agent.model
      const info = ref === undefined ? undefined : yield* env.model.info(ref)
      if (ref === undefined || info === undefined) return yield* failNoModel(runtime, ref)
      const ready = yield* checkCompacted(runtime, checkpoint)
      return yield* ready
        ? prepare(env, runtime, checkpoint, agent, { ref, contextWindow: info.contextWindow })
        : Effect.void
    })
