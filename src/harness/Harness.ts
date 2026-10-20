/**
 * The `Harness` service, ported from pi's `harness/harness.ts`: conversations, the admission of
 * submissions, the `$inbox` boundaries, and run control in `$live`, over one Session and its
 * task Scheduler.
 *
 * Every conversation gets the built-in documents in the commit that creates or forks it. Input to
 * an idle conversation starts a run: a `$generation` task that holds `$live.run` until it
 * settles the run's inputs. While the run is live, submissions queue in `$inbox`, and the run
 * places them at its boundaries. Aborting a conversation, or a cascade that reaches one, withdraws
 * its queued inputs; a run task the scheduler faults or orphans ends its run `unanswered`.
 *
 * This is the loop side, for code that runs with the loop, such as tasks. Calling code uses
 * `HarnessClient` instead, which reaches the loop through `HarnessServer` over a transport.
 */
import { Context, Effect, Layer } from 'effect'
import type { Duration, Scope } from 'effect'
import * as Rpcs from '../client/HarnessRpcs.ts'
import type { Service as SessionService, Tx } from '../session/Session.ts'
import type {
  CommitError,
  SessionFailed,
  SnapshotError,
  TxDocumentError,
} from '../session/SessionError.ts'
import type { ConversationRecord } from '../storage/schema/Conversation.ts'
import type { ConversationId, SubmissionId, TaskId } from '../storage/schema/Ids.ts'
import type { TaskRecord } from '../storage/schema/Task.ts'
import type { Storage } from '../storage/Storage.ts'
import type { StorageError } from '../storage/StorageError.ts'
import type { Registry } from '../task/Registry.ts'
import type { Service as SchedulerService } from '../task/Scheduler.ts'
import type { Conversation, CreateOptions, Init } from './Conversation.ts'
import type { Model } from '../model/Model.ts'
import type * as Task from '../task/Task.ts'
import type * as Toolkit from '../tool/Toolkit.ts'
import type { Extension } from './Extension.ts'
import { Extensions } from './Extensions.ts'
import type { InvalidExtension } from './HarnessError.ts'
import * as defaults from './internal/layer.ts'
import type * as Settings from './Settings.ts'
import * as internal from './internal/harness.ts'
import type { AbortResult, Submission } from './Submission.ts'
import type * as Usage from './Usage.ts'
import type { TaskGraph, Watch } from './ViewState.ts'

/** Live work: scheduling state, every live task, and the queued and placed submissions. */
export const Inspection = Rpcs.Inspection
export type Inspection = Rpcs.Inspection

/** How a Harness runs. */
export interface Options {
  /**
   * Runs in every commit that creates or forks a conversation, raw `tx.createConversation()`
   * included, after the built-in documents and before `init`.
   */
  readonly conversationCreated?: (
    tx: Tx,
    record: ConversationRecord,
  ) => Effect.Effect<void, TxDocumentError>
  /** The Session's document retention. */
  readonly documentRetention?: Duration.Input
}

export interface Service {
  /** The Session the Harness owns. */
  readonly session: SessionService
  /** The task Scheduler the Harness owns. */
  readonly scheduler: SchedulerService
  /** The reserved root conversation, created with `init` in one commit when absent. */
  readonly root: <E = never>(options?: {
    readonly init?: Init<E>
  }) => Effect.Effect<Conversation, E | CommitError>
  readonly conversation: (
    id: ConversationId,
  ) => Effect.Effect<Conversation | undefined, CommitError>
  readonly createConversation: <E = never>(
    options: CreateOptions<E>,
  ) => Effect.Effect<Conversation, E | CommitError>
  /** Reacquire a submission, for example after reopening. */
  readonly submission: (id: SubmissionId) => Effect.Effect<Submission | undefined, CommitError>
  /**
   * Withdraw a queued submission. `not_found` for an unknown submission, or one of another
   * conversation than `conversationId`.
   */
  readonly abortSubmission: (
    id: SubmissionId,
    conversationId?: ConversationId,
  ) => Effect.Effect<AbortResult | 'not_found', CommitError>
  /** The committed record of a task, live or terminal. Wait on it or abort it with `scheduler`. */
  readonly task: (id: TaskId) => Effect.Effect<TaskRecord | undefined, CommitError>
  /**
   * Every conversation's committed `$usage`, summed. Each ledger is read at its own point;
   * totals only grow.
   */
  readonly usage: Effect.Effect<Usage.State, CommitError | SnapshotError>
  /** Watch every live task, from the committed state now, while the scope is open. */
  readonly taskGraph: Effect.Effect<Watch<TaskGraph, SessionFailed>, CommitError, Scope.Scope>
  /** Live tasks and unsettled submissions. Writes nothing and runs no task code. */
  readonly inspect: Effect.Effect<Inspection, CommitError | StorageError>
}

export class Harness extends Context.Service<Harness, Service>()(
  'effect-harness/harness/Harness',
) {}

/**
 * The Harness running the current task, for its phases and the tool handlers they run, which read
 * it with `yield* Harness.current`; `undefined` elsewhere. A tool that delegates to another
 * conversation reaches it here, without depending on the Harness that installs the tool. Recovery
 * can run tasks while the Harness opens; they wait until it has opened.
 */
export const current: Effect.Effect<Service | undefined> = internal.current

/**
 * Open a Harness over the `Storage`, task `Registry` and `Extensions` in context: a Session and a Scheduler
 * with the Harness hooks. Run settings come from `Settings.Current`. Closing the scope closes them; pending submission waits fail.
 */
export const makeWith: (
  options: Options,
) => Effect.Effect<Service, CommitError, Storage | Registry | Extensions | Scope.Scope> =
  internal.make

/** `makeWith` with the default options. */
export const make: Effect.Effect<
  Service,
  CommitError,
  Storage | Registry | Extensions | Scope.Scope
> = makeWith({})

/** What the default loop takes besides the Harness options. */
export interface LayerOptions<
  Installs extends ReadonlyArray<Extension> = ReadonlyArray<Extension>,
> extends Options {
  /** Installed in order before the loop opens, with their tools' implementations from context. */
  readonly extensions?: Installs
  /** Provides `Settings.Current` to the loop; otherwise the one in context applies. */
  readonly settings?: Settings.Input | Effect.Effect<Settings.Input>
  /** Installed after the built-in tasks, replacing any of the same name. */
  readonly tasks?: Iterable<Task.Any>
}

/** The tool implementations installing `X` needs. */
export type Requires<X> = X extends Extension<infer Names> ? Toolkit.Requires<Names> : never

/**
 * The default loop over the `Storage` and `Model` in context: its own task `Registry` and
 * `Extensions`, the given extensions, the built-in generation, tool and compaction tasks, and a
 * `Harness` over them. It provides the Harness and the Extensions, to install more at run time.
 */
export function layer<const Installs extends ReadonlyArray<Extension> = readonly []>(
  options?: LayerOptions<Installs>,
): Layer.Layer<
  Harness | Extensions,
  CommitError | InvalidExtension,
  Storage | Model | Requires<Installs[number]>
>
export function layer(
  options: LayerOptions = {},
): Layer.Layer<
  Harness | Extensions,
  CommitError | InvalidExtension,
  Storage | Model | Toolkit.Requires<string>
> {
  return Layer.effectContext(
    Effect.map(defaults.open(options), ({ harness, extensions }) =>
      Context.make(Harness, harness).pipe(Context.add(Extensions, extensions)),
    ),
  )
}
