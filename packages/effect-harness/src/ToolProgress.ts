import type * as Cause from 'effect/Cause'
import type * as Effect from 'effect/Effect'
import type * as Option from 'effect/Option'
import type * as Scope from 'effect/Scope'
import * as Schema from 'effect/Schema'
import * as Document from './Document.js'
import type { Failure as ExecutionFailure } from './ExecutionError.js'
import * as OutputRetention from './OutputRetention.js'
import type * as ProgressPolicy from './ProgressPolicy.js'
import type * as Record from './Record.js'
import type { Failure as SessionFailure } from './SessionError.js'
import * as ToolResult from './ToolResult.js'
import type * as Transaction from './Transaction.js'
import * as Internal from './internal/ToolProgress.js'

const Count = Schema.Int.check(
  Schema.isGreaterThanOrEqualTo(0),
  Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER),
)
/** Text is bounded; latest JSON details and diagnostics remain independent channels. */
export const Snapshot = Schema.Struct({
  callId: Schema.NonEmptyString,
  attempt: Count,
  revision: Count,
  status: Schema.Literals(['running', 'completed', 'cancelled', 'interrupted']),
  retention: OutputRetention.Policy,
  output: OutputRetention.Snapshot,
  /** Required to resume exact whole-stream line counters when the retained text is empty. */
  endsWithNewline: Schema.Boolean,
  details: Schema.optionalKey(Schema.Json),
  diagnostics: Schema.Array(ToolResult.DiagnosticSchema),
})
export type Snapshot = typeof Snapshot.Type

/** Conversation-owned, fork-initial state survives task-document retirement. */
export const document = Document.family({
  kind: 'harness.tool.progress',
  version: 1,
  scope: 'conversation',
  history: 'latest',
  fork: 'initial',
  schema: Snapshot,
  initial: () => ({
    callId: 'uninitialized',
    attempt: 0,
    revision: 0,
    status: 'running' as const,
    retention: OutputRetention.defaultPolicy,
    output: {
      text: '',
      totalBytes: 0,
      totalLines: 0,
      retainedBytes: 0,
      retainedLines: 0,
      skippedBytes: 0,
      skippedLines: 0,
      truncated: false,
    },
    endsWithNewline: true,
    diagnostics: [],
  }),
})

export class ToolProgressError extends Schema.TaggedError<ToolProgressError>()(
  'ToolProgressError',
  {
    reason: Schema.Literals(['configuration', 'conflict', 'closed', 'settlement']),
    message: Schema.String,
    cause: Schema.optionalKey(Schema.Defect()),
  },
) {}
export type Failure<E = never> =
  | E
  | SessionFailure
  | ToolProgressError
  | OutputRetention.OutputRetentionError

/** Supply the invocation's gated commit, never an ungated second Session. R/E stay visible. */
export interface Gate<E = SessionFailure, R = never> {
  readonly commit: <A, E2, R2>(
    change: (tx: Transaction.Transaction) => Effect.Effect<A, E2, R2>,
  ) => Effect.Effect<A, E | E2, R | R2>
}
export interface Options {
  readonly conversationId: Record.ConversationId
  readonly callId: string
  readonly attempt: number
  readonly retention?: OutputRetention.Policy
  readonly policy?: ProgressPolicy.Policy
  /** resume requires the same attempt; retry requires a newer, independently authorized safe attempt. */
  readonly recovery?: 'resume' | 'retry' | 'interrupt'
  /** Bounds active details acknowledgements; excess callers wait before their update is accepted. */
  readonly maxPendingDetails?: number
}
export interface Writer<E = SessionFailure, R = never> {
  /** Acceptance is synchronous; persistence is scheduled. Check failure or settle to observe storage failure. */
  readonly output: (
    chunk: string | Uint8Array,
    skipped?: OutputRetention.Skip,
  ) => Effect.Effect<void, Failure<E>>
  /** Latest wins. The acknowledgement covers the accepted revision in a successful committed snapshot. */
  readonly details: (value: Schema.Json) => Effect.Effect<void, Failure<E>>
  readonly diagnostic: (value: ToolResult.Diagnostic) => Effect.Effect<void, Failure<E>>
  readonly snapshot: Effect.Effect<Snapshot, Failure<E>>
  readonly failure: Effect.Effect<Option.Option<Cause.Cause<Failure<E>>>>
  /**
   * Stages the frozen final window only inside this writer's settle callback and supplied tx.
   * Staging never acknowledges details; the surrounding gated commit must succeed first.
   */
  readonly flush: (tx: Transaction.Transaction) => Effect.Effect<Snapshot, Failure<E>>
  /** Stops admissions/timers, drains the writer, and commits final progress with the receipt atomically. */
  readonly settle: <A, E2, R2>(
    status: Exclude<Snapshot['status'], 'running'>,
    change: (tx: Transaction.Transaction, snapshot: Snapshot) => Effect.Effect<A, E2, R2>,
  ) => Effect.Effect<A, Failure<E> | E2, R | R2>
}

/** One scoped worker per invocation. Scope closure revokes reports and fails outstanding acknowledgements. */
export const make: <E = ExecutionFailure, R = never>(
  gate: Gate<E, R>,
  options: Options,
) => Effect.Effect<Writer<E, R>, Failure<E>, R | Scope.Scope> = Internal.make

/** Bound joined visible text after hooks, preserving media/options and every independent result field. */
export const retainResult: <A extends ToolResult.Output<unknown>>(
  result: A,
  policy?: OutputRetention.Policy,
) => Effect.Effect<
  Omit<A, 'content' | 'diagnostics'> & {
    readonly content: ToolResult.Content
    readonly diagnostics?: ReadonlyArray<ToolResult.Diagnostic>
  },
  OutputRetention.OutputRetentionError
> = Internal.retainResult
