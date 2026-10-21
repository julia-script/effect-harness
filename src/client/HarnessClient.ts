/**
 * The client of a Harness: what calling code uses. It sends `HarnessRpcs` over the
 * `RpcClient.Protocol` in context, so it runs wherever the loop's transport reaches, the same
 * process included, and imports nothing the loop runs.
 */
import { Context, Effect, Layer, Schema } from 'effect'
import type { Scope } from 'effect'
import { RpcClient } from 'effect/rpc'
import type { RpcClientError } from 'effect/rpc/RpcClientError'
import type * as AgentEvent from '../protocol/AgentEvent.ts'
import { SubmissionNotFound } from '../protocol/HarnessError.ts'
import type { ConversationBusy, InitFailed, InvalidSubmission } from '../protocol/HarnessError.ts'
import type * as AgentState from '../protocol/AgentState.ts'
import type { AbortResult, Draft, Settled } from '../protocol/Submission.ts'
import type {
  ContextView,
  ConversationView,
  TaskGraph,
  UsageState,
  Watch,
} from '../protocol/ViewState.ts'
import type { CommitError } from '../session/SessionError.ts'
import type { ConversationOwnership } from '../storage/schema/Conversation.ts'
import type { EntryRecord } from '../storage/schema/Entry.ts'
import type { ConversationId, EntryId, SubmissionId, TaskId } from '../storage/schema/Ids.ts'
import type { Cursor, Page } from '../storage/schema/Scan.ts'
import type { SubmissionRecord } from '../storage/schema/Submission.ts'
import type { TaskRecord } from '../storage/schema/Task.ts'
import type { StorageError } from '../storage/StorageError.ts'
import type { TerminalTask } from '../task/Task.ts'
import type { SchedulerClosed, TaskNotFound } from '../task/TaskError.ts'
import { HarnessRpcs } from '../protocol/HarnessRpcs.ts'
import type { ConversationEntryQuery, Inspection } from '../protocol/HarnessRpcs.ts'
import type { Bound, Call } from '../protocol/Init.ts'
import { Connections } from './HarnessTransport.ts'
import { broken, eventsFrom, resubscribing, watched } from './internal/watch.ts'
import type { Reopen } from './internal/watch.ts'

/** How a call fails on the loop, or on the way there. */
export type Failure<E = never> = E | CommitError | RpcClientError

/** Handle of one submission, by ID. */
export interface Submission {
  readonly id: SubmissionId
  /** The committed record. */
  readonly status: Effect.Effect<SubmissionRecord, Failure<SubmissionNotFound>>
  /** Wait until it settles; interrupting the wait leaves the submission as it is. */
  readonly wait: Effect.Effect<Settled, Failure<SubmissionNotFound>>
  /** Withdraw it while queued; placed and settled ones stay as they are. */
  readonly abort: Effect.Effect<AbortResult, Failure<SubmissionNotFound>>
}

export interface CreateOptions {
  readonly ownership: ConversationOwnership
  /** A named `init` the loop runs in the creating commit. */
  readonly init?: Bound
}

/** Handle of one conversation, by ID. */
export interface Conversation {
  readonly id: ConversationId
  readonly submit: (
    draft: Draft,
  ) => Effect.Effect<Submission, Failure<ConversationBusy | InvalidSubmission>>
  /** Start fresh context at a `$reset` entry, with optional handoff text. */
  readonly reset: (handoff?: string) => Effect.Effect<Submission, Failure>
  /** Start a manual compaction; its summary is placed at the next boundary, or at once when idle. */
  readonly compact: (instructions?: string) => Effect.Effect<TaskId, Failure>
  /** Change what the conversation asks with: a given field replaces it, `null` clears it. */
  readonly configure: (change: AgentState.Change) => Effect.Effect<void, Failure>
  /** The agent its next request would resolve, by name. */
  readonly agent: Effect.Effect<AgentState.View, Failure>
  readonly entries: (
    query: ConversationEntryQuery,
    limit: number,
    cursor?: Cursor,
  ) => Effect.Effect<Page<EntryRecord>, Failure>
  /** What the model sees, now or as of the visible entry `at`. */
  readonly context: (options?: { readonly at?: EntryId }) => Effect.Effect<ContextView, Failure>
  /** Watch its active transcript and built-in documents while the scope is open. */
  readonly view: Effect.Effect<Watch<ConversationView, Failure>, Failure, Scope.Scope>
  /** Watch its agent events, from a snapshot, while the scope is open. */
  readonly events: Effect.Effect<AgentEvent.Events<Failure>, Failure, Scope.Scope>
  readonly fork: (
    at: EntryId,
    options: CreateOptions,
  ) => Effect.Effect<Conversation, Failure<InitFailed>>
  /** Abort its live work; `background` includes background tasks. */
  readonly abort: (options?: {
    readonly background?: boolean
  }) => Effect.Effect<void, Failure<TaskNotFound | SchedulerClosed | StorageError>>
  readonly waitForIdle: Effect.Effect<void, Failure<SchedulerClosed>>
}

export interface Service {
  /** The reserved root conversation, created with `init` when absent. */
  readonly root: (options?: {
    readonly init?: Bound
  }) => Effect.Effect<Conversation, Failure<InitFailed>>
  readonly conversation: (id: ConversationId) => Effect.Effect<Conversation | undefined, Failure>
  readonly createConversation: (
    options: CreateOptions,
  ) => Effect.Effect<Conversation, Failure<InitFailed>>
  /** Reacquire a submission, for example after a restart. */
  readonly submission: (id: SubmissionId) => Effect.Effect<Submission | undefined, Failure>
  /** Withdraw a queued submission; `not_found` when it is not one of `conversationId`. */
  readonly abortSubmission: (
    id: SubmissionId,
    conversationId?: ConversationId,
  ) => Effect.Effect<AbortResult | 'not_found', Failure>
  /** The committed record of a task, live or terminal. */
  readonly task: (id: TaskId) => Effect.Effect<TaskRecord | undefined, Failure>
  /** Wait for the task's terminal record; interrupting the wait leaves the task running. */
  readonly waitForTask: (
    id: TaskId,
  ) => Effect.Effect<TerminalTask, Failure<TaskNotFound | SchedulerClosed>>
  /** Abort-mark the task and wait until its run ends; `terminal` when it had already ended. */
  readonly abortTask: (
    id: TaskId,
  ) => Effect.Effect<'marked' | 'terminal', Failure<TaskNotFound | StorageError>>
  /** Wait until every ownerless conversation is idle. */
  readonly waitForIdle: Effect.Effect<void, Failure<SchedulerClosed>>
  /** Every conversation's `$usage`, summed. */
  readonly usage: Effect.Effect<UsageState, Failure>
  /** Live tasks and unsettled submissions. */
  readonly inspect: Effect.Effect<Inspection, Failure<StorageError>>
  /** Watch every live task while the scope is open. */
  readonly taskGraph: Effect.Effect<Watch<TaskGraph, Failure>, Failure, Scope.Scope>
}

export class HarnessClient extends Context.Service<HarnessClient, Service>()(
  'effect-harness/client/HarnessClient',
) {}

type Rpcs = RpcClient.FromGroup<typeof HarnessRpcs, RpcClientError>

/** The wire form of a bound `init`: its payload encoded by its own Schema. */
const encodeInit = (bound: Bound | undefined): Effect.Effect<{ readonly init?: Call }> =>
  bound === undefined
    ? Effect.succeed({})
    : Effect.map(
        // The payload is typed by the Schema that encodes it.
        Effect.orDie(Schema.encodeEffect(bound.init.payload)(bound.payload)),
        (payload) => ({ init: { name: bound.init.name, payload } }),
      )

const isTerminal = (record: TaskRecord): record is TerminalTask =>
  record.state.status === 'terminal'

const submissionHandle = (rpcs: Rpcs, id: SubmissionId): Submission => ({
  id,
  status: rpcs.SubmissionStatus({ submissionId: id }),
  wait: rpcs.WaitSubmission({ submissionId: id }),
  abort: rpcs.AbortSubmission({ submissionId: id }).pipe(
    Effect.filterOrFail(
      (result): result is AbortResult => result !== 'not_found',
      () =>
        new SubmissionNotFound({ message: `Submission ${id} does not exist`, submissionId: id }),
    ),
  ),
})

const conversationHandle = (rpcs: Rpcs, reopen: Reopen, id: ConversationId): Conversation => ({
  id,
  submit: (draft) =>
    Effect.map(rpcs.Submit({ conversationId: id, draft }), (submissionId) =>
      submissionHandle(rpcs, submissionId),
    ),
  reset: (handoff) =>
    Effect.map(
      rpcs.Reset({ conversationId: id, ...(handoff === undefined ? {} : { handoff }) }),
      (submissionId) => submissionHandle(rpcs, submissionId),
    ),
  compact: (instructions) =>
    rpcs.Compact({
      conversationId: id,
      ...(instructions === undefined ? {} : { instructions }),
    }),
  configure: (change) => rpcs.Configure({ conversationId: id, change }),
  agent: rpcs.Agent({ conversationId: id }),
  entries: (query, limit, cursor) =>
    rpcs.Entries({ conversationId: id, query, limit, ...(cursor === undefined ? {} : { cursor }) }),
  context: (options) =>
    rpcs.Context({
      conversationId: id,
      ...(options?.at === undefined ? {} : { at: options.at }),
    }),
  view: watched(reopen(rpcs.WatchView({ conversationId: id }))),
  events: eventsFrom(reopen(rpcs.WatchEvents({ conversationId: id }))),
  fork: (at, { ownership, init }) =>
    encodeInit(init).pipe(
      Effect.flatMap((encoded) => rpcs.Fork({ conversationId: id, at, ownership, ...encoded })),
      Effect.map((forked) => conversationHandle(rpcs, reopen, forked)),
    ),
  abort: (options) => rpcs.AbortConversation({ conversationId: id, ...options }),
  waitForIdle: rpcs.WaitForIdle({ conversationId: id }),
})

/**
 * A client over the `RpcClient.Protocol` in context, for the lifetime of the scope. With the
 * transport's `Connections`, its watches open again as soon as a dropped connection is back.
 */
export const make: Effect.Effect<Service, never, RpcClient.Protocol | Scope.Scope> = Effect.zipWith(
  RpcClient.make(HarnessRpcs),
  Effect.map(Effect.serviceOption(Connections), resubscribing),
  (rpcs, reopen): Service => ({
    root: (options) =>
      encodeInit(options?.init).pipe(
        Effect.flatMap(rpcs.Root),
        Effect.map((id) => conversationHandle(rpcs, reopen, id)),
      ),
    conversation: (id) =>
      Effect.map(rpcs.HasConversation({ conversationId: id }), (exists) =>
        exists ? conversationHandle(rpcs, reopen, id) : undefined,
      ),
    createConversation: ({ ownership, init }) =>
      encodeInit(init).pipe(
        Effect.flatMap((encoded) => rpcs.CreateConversation({ ownership, ...encoded })),
        Effect.map((id) => conversationHandle(rpcs, reopen, id)),
      ),
    submission: (id) =>
      Effect.map(rpcs.HasSubmission({ submissionId: id }), (exists) =>
        exists ? submissionHandle(rpcs, id) : undefined,
      ),
    abortSubmission: (id, conversationId) =>
      rpcs.AbortSubmission({
        submissionId: id,
        ...(conversationId === undefined ? {} : { conversationId }),
      }),
    task: (id) => Effect.map(rpcs.Task({ taskId: id }), (record) => record ?? undefined),
    waitForTask: (id) =>
      Effect.filterOrFail(rpcs.WaitForTask({ taskId: id }), isTerminal, () =>
        broken(`The loop returned task ${id} before it was terminal`),
      ),
    abortTask: (id) => rpcs.AbortTask({ taskId: id }),
    waitForIdle: rpcs.WaitForIdle({}),
    usage: rpcs.Usage(),
    inspect: rpcs.Inspect(),
    taskGraph: watched(reopen(rpcs.WatchTaskGraph())),
  }),
)

/** `make` as a Layer: needs the client end of a transport, such as `HarnessTransport.layerMemory`. */
export const layer: Layer.Layer<HarnessClient, never, RpcClient.Protocol> = Layer.effect(
  HarnessClient,
  make,
)
