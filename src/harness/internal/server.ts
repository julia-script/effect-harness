/**
 * The loop side of `HarnessRpcs`: each request on the `Harness` in context.
 */
import { Effect, Stream } from 'effect'
import type { Rpc, RpcGroup } from 'effect/rpc'
import { HarnessRpcs } from '../../protocol/HarnessRpcs.ts'
import type { Call } from '../../protocol/Init.ts'
import { TransactionError } from '../../session/SessionError.ts'
import type { CommitError } from '../../session/SessionError.ts'
import type { ConversationId, SubmissionId } from '../../storage/schema/Ids.ts'
import type { Conversation, Init } from '../Conversation.ts'
import { Harness } from '../Harness.ts'
import type { Service } from '../Harness.ts'
import { InitFailed, SubmissionNotFound } from '../../protocol/HarnessError.ts'
import type { Initializer } from '../Initializer.ts'
import type { Submission } from '../../protocol/Submission.ts'
import type { Watch } from '../../protocol/ViewState.ts'

/** The `init` option a named call resolves to; unknown names fail before any commit. */
const resolver = (initializers: ReadonlyArray<Initializer>) => {
  const named = new Map(initializers.map((initializer) => [initializer.name, initializer]))
  return (
    call: Call | undefined,
  ): Effect.Effect<{ readonly init?: Init<InitFailed> }, InitFailed> => {
    if (call === undefined) return Effect.succeed({})
    const initializer = named.get(call.name)
    return initializer === undefined
      ? Effect.fail(
          new InitFailed({ message: `No initializer is named ${call.name}`, init: call.name }),
        )
      : Effect.succeed({ init: (tx, id) => initializer.run(tx, id, call.payload) })
  }
}

const withConversation = <A, E, R = never>(
  harness: Service,
  id: ConversationId,
  f: (conversation: Conversation) => Effect.Effect<A, E, R>,
): Effect.Effect<A, E | CommitError, R> =>
  Effect.flatMap(harness.conversation(id), (conversation) =>
    conversation === undefined
      ? Effect.fail<E | CommitError>(
          new TransactionError({ message: `Conversation ${id} does not exist` }),
        )
      : f(conversation),
  )

const withSubmission = <A, E>(
  harness: Service,
  id: SubmissionId,
  f: (submission: Submission) => Effect.Effect<A, E>,
): Effect.Effect<A, E | SubmissionNotFound | CommitError> =>
  Effect.flatMap(harness.submission(id), (submission) =>
    submission === undefined
      ? Effect.fail<E | SubmissionNotFound | CommitError>(
          new SubmissionNotFound({ message: `Submission ${id} does not exist`, submissionId: id }),
        )
      : f(submission),
  )

type Rpcs = RpcGroup.Rpcs<typeof HarnessRpcs>

/** Handlers that need no services. */
type Handlers = { readonly [Current in Rpcs as Current['_tag']]: Rpc.ToHandlerFn<Current, never> }
type Resolve = ReturnType<typeof resolver>

const creations = (
  harness: Service,
  resolve: Resolve,
): Pick<Handlers, 'Root' | 'CreateConversation' | 'Fork'> => ({
  Root: ({ init }) =>
    resolve(init).pipe(
      Effect.flatMap((options) => harness.root(options)),
      Effect.map(({ id }) => id),
    ),
  CreateConversation: ({ ownership, init }) =>
    resolve(init).pipe(
      Effect.flatMap((options) => harness.createConversation({ ownership, ...options })),
      Effect.map(({ id }) => id),
    ),
  Fork: ({ conversationId, at, ownership, init }) =>
    resolve(init).pipe(
      Effect.flatMap((options) =>
        withConversation(harness, conversationId, (conversation) =>
          conversation.fork(at, { ownership, ...options }),
        ),
      ),
      Effect.map(({ id }) => id),
    ),
})

const conversations = (
  harness: Service,
): Pick<
  Handlers,
  | 'HasConversation'
  | 'Submit'
  | 'Reset'
  | 'Compact'
  | 'Configure'
  | 'Agent'
  | 'Entries'
  | 'Context'
  | 'AbortConversation'
> => ({
  HasConversation: ({ conversationId }) =>
    Effect.map(harness.conversation(conversationId), (found) => found !== undefined),
  Submit: ({ conversationId, draft }) =>
    withConversation(harness, conversationId, (conversation) =>
      Effect.map(conversation.submit(draft), ({ id }) => id),
    ),
  Reset: ({ conversationId, handoff }) =>
    withConversation(harness, conversationId, (conversation) =>
      Effect.map(conversation.reset(handoff), ({ id }) => id),
    ),
  Compact: ({ conversationId, instructions }) =>
    withConversation(harness, conversationId, (conversation) => conversation.compact(instructions)),
  Configure: ({ conversationId, change }) =>
    withConversation(harness, conversationId, (conversation) => conversation.configure(change)),
  Agent: ({ conversationId }) =>
    withConversation(harness, conversationId, (conversation) => conversation.agent),
  Entries: ({ conversationId, query, limit, cursor }) =>
    withConversation(harness, conversationId, (conversation) =>
      conversation.entries(query, limit, cursor),
    ),
  Context: ({ conversationId, at }) =>
    withConversation(harness, conversationId, (conversation) =>
      conversation.context(at === undefined ? {} : { at }),
    ),
  AbortConversation: ({ conversationId, background }) =>
    withConversation(harness, conversationId, (conversation) =>
      conversation.abort(background === undefined ? {} : { background }),
    ),
})

const tasks = (
  harness: Service,
): Pick<Handlers, 'WaitForIdle' | 'Task' | 'WaitForTask' | 'AbortTask' | 'Usage'> => ({
  WaitForIdle: ({ conversationId }) =>
    conversationId === undefined
      ? harness.scheduler.waitForIdle()
      : withConversation(harness, conversationId, (conversation) => conversation.waitForIdle),
  Task: ({ taskId }) => Effect.map(harness.task(taskId), (record) => record ?? null),
  WaitForTask: ({ taskId }) => harness.scheduler.waitForTask(taskId),
  AbortTask: ({ taskId }) => harness.scheduler.abort(taskId),
  Usage: () => harness.usage,
})

/** A watch as frames: its value, then its changes' operations. */
const frames = <T, E>(watch: Watch<T, E>) =>
  Stream.concat(
    Stream.succeed({ type: 'value' as const, value: watch.value }),
    Stream.map(watch.changes, ({ ops }) => ({ type: 'change' as const, ops })),
  )

const watches = (
  harness: Service,
): Pick<Handlers, 'WatchView' | 'WatchEvents' | 'WatchTaskGraph'> => ({
  WatchView: ({ conversationId }) =>
    Stream.unwrap(
      withConversation(harness, conversationId, (conversation) =>
        Effect.map(conversation.view, frames),
      ),
    ),
  WatchEvents: ({ conversationId }) =>
    Stream.unwrap(
      withConversation(harness, conversationId, (conversation) =>
        Effect.map(conversation.events, (events) =>
          Stream.concat(Stream.succeed([events.snapshot]), events.batches),
        ),
      ),
    ),
  WatchTaskGraph: () => Stream.unwrap(Effect.map(harness.taskGraph, frames)),
})

const submissions = (
  harness: Service,
): Pick<
  Handlers,
  'HasSubmission' | 'SubmissionStatus' | 'WaitSubmission' | 'AbortSubmission' | 'Inspect'
> => ({
  HasSubmission: ({ submissionId }) =>
    Effect.map(harness.submission(submissionId), (found) => found !== undefined),
  SubmissionStatus: ({ submissionId }) =>
    withSubmission(harness, submissionId, (submission) => submission.status),
  WaitSubmission: ({ submissionId }) =>
    withSubmission(harness, submissionId, (submission) => submission.wait),
  AbortSubmission: ({ submissionId, conversationId }) =>
    harness.abortSubmission(submissionId, conversationId),
  Inspect: () => harness.inspect,
})

/** Handlers of every request, over the `Harness` in context. */
export const handlers = (initializers: ReadonlyArray<Initializer>) =>
  HarnessRpcs.toLayer(
    Effect.map(Harness, (harness) =>
      HarnessRpcs.of({
        ...creations(harness, resolver(initializers)),
        ...conversations(harness),
        ...submissions(harness),
        ...tasks(harness),
        ...watches(harness),
      }),
    ),
  )
