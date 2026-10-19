/**
 * The built-in compaction task, ported from pi's `CompactionTask` (`harness/compaction.ts`, spec
 * §8.7): select an old prefix of the model context, summarize it, and place a `$compaction`
 * entry whose `head` is the first entry kept, so the context starts at the summary.
 *
 * - A generation compacts first when its request would leave less than `reserveTokens` of the
 *   model's context window (`threshold`), or after the model reports a context overflow
 *   (`overflow`). It owns that compaction and waits for it; the summary is appended directly.
 * - Above the background threshold, preparation starts a conversation-owned background compaction
 *   and sends its request anyway. `Conversation.compact()` starts a manual one. Their summaries
 *   are admitted as write submissions: placed at once when idle, else at the next boundary.
 * - `beforeCompact` hooks may decline a compaction or supply its summary.
 * - Summarization retries `Retryable` failures as generations do. Each attempt's usage is
 *   recorded in `$usage`; a failure is kept in `$live.compactionFailure`.
 */
import { Effect, Schema } from 'effect'
import * as Model from '../model/Model.ts'
import { EntryId, SubmissionId } from '../storage/schema/Ids.ts'
import type { Message } from '../model/Message.ts'
import type { EntryRecord } from '../storage/schema/Entry.ts'
import * as Hook from '../task/Hook.ts'
import * as Task from '../task/Task.ts'
import { Extensions } from './Extensions.ts'
import * as internal from './internal/compaction/task.ts'
import { COMPACTION_KIND, compactionTask } from './internal/compaction/status.ts'
import { CompactionReason } from './Live.ts'
import * as Settings from './Settings.ts'

/** The task kind, also the kind of summary entries. */
export const name = COMPACTION_KIND

export const Reason = CompactionReason
export type Reason = CompactionReason

export const Input = Schema.Struct({
  reason: Reason,
  /** What the summary should focus on. */
  instructions: Schema.optionalKey(Schema.String),
})
export type Input = typeof Input.Type

/** The summarization request `select` fixed; a resend after recovery uses it unchanged. */
const request = {
  attempt: Schema.Finite,
  model: Model.Ref,
  options: Model.Options,
  /** The newest entry of the context the range was selected from. */
  tail: EntryId,
  /** The first entry kept verbatim; the summary's `head`. */
  firstKept: EntryId,
}

export const Checkpoint = Schema.Union([
  Schema.Struct({ phase: Schema.Literal('select') }),
  Schema.Struct({ phase: Schema.Literal('summarize'), ...request }),
  Schema.Struct({ phase: Schema.Literal('retry'), ...request, until: Schema.Finite }),
])
export type Checkpoint = typeof Checkpoint.Type

/**
 * A blocking compaction's summary entry, or the write submission of another's; neither when
 * there was nothing to compact.
 */
export const Result = Schema.Struct({
  entryId: Schema.optionalKey(EntryId),
  submissionId: Schema.optionalKey(SubmissionId),
})
export type Result = typeof Result.Type

export type Compaction = Task.Task<Input, Checkpoint, Result>

/** What `Tx.createTask()` needs to create a compaction. */
export const kind = compactionTask

/** What a compaction is about to summarize, as `beforeCompact` sees it. */
export interface Pending {
  readonly reason: Reason
  /** The active entries the summary replaces, the head marker first. */
  readonly entries: ReadonlyArray<EntryRecord>
  /** Their model context: what the summarizer reads. */
  readonly messages: ReadonlyArray<Message>
  /** The first entry kept verbatim. */
  readonly firstKept: EntryId
  readonly instructions?: string
}

/** The hooks the compaction asks (spec §7.2), for extensions to answer. */
export interface Hooks {
  /**
   * After the range is chosen, before summarizing; the first decision wins. `decline` completes
   * without a summary; `summary` places that text instead of asking the model.
   */
  readonly beforeCompact: (
    compaction: Pending,
    api: Hook.Api,
  ) => Effect.Effect<{ readonly decline: true } | { readonly summary: string } | undefined, unknown>
}
export const Hooks: Hook.Kind<Hooks> = Hook.kind<Hooks>(name)

/**
 * The compaction task, summarizing with the conversation agent's model, over the `Model` and
 * `Extensions` in context and `Settings.Current`. `Generation.tasks` installs it with the
 * generation.
 */
export const make: Effect.Effect<Compaction, never, Model.Model | Extensions> = Effect.gen(
  function* () {
    const model = yield* Model.Model
    const extensions = yield* Extensions
    const settings = yield* Settings.Current
    return Task.make(internal.definition({ model, settings, extensions }))
  },
)
