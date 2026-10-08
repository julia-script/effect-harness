/** A replay-safe triage tool owns a durable conversation and a stable child submission. */
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Ref from 'effect/Ref'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as Tool from 'effect/ai/Tool'
import * as Toolkit from 'effect/ai/Toolkit'
import * as Conversation from 'effect-harness/Conversation'
import * as Document from 'effect-harness/Document'
import type * as Extension from 'effect-harness/Extension'
import type * as Harness from 'effect-harness/Harness'
import * as Identity from 'effect-harness/Identity'
import * as Invocation from 'effect-harness/Invocation'
import * as Record from 'effect-harness/Record'
import * as Serialization from 'effect-harness/Serialization'
import { TaskRuntime } from 'effect-harness/TaskRuntime'
import * as ToolError from 'effect-harness/ToolError'
import * as ToolRegistration from 'effect-harness/ToolRegistration'
import * as Memory from 'effect-harness/storage/Memory'
import * as Runtime from './Runtime.ts'

export const Result = Schema.Struct({
  childConversationId: Record.ConversationId,
  childSubmissionId: Record.SubmissionId,
  ownerTaskId: Record.TaskId,
  toolExecutions: Schema.Int,
  childModelRequests: Schema.Int,
  childAnswer: Schema.String,
  parentAnswer: Schema.String,
  reusedChild: Schema.Boolean,
  reusedSubmission: Schema.Boolean,
})

const ChildState = Schema.Struct({ conversationId: Schema.optionalKey(Record.ConversationId) })
export const ChildDoc = Document.defineUnsafe({
  kind: 'tour.triage.child',
  version: 1,
  scope: 'task',
  schema: Serialization.object(ChildState),
  initial: (): typeof ChildState.Type => ({}),
})
const Triage = Tool.make('triage', {
  description: 'Label an issue using an owned conversation without tools.',
  parameters: Schema.Struct({ issue: Schema.String }),
  success: Schema.String,
  failure: ToolError.ToolError,
})
  .addDependency(Invocation.ToolCall)
  .addDependency(TaskRuntime)
const toolkit = Toolkit.make(Triage)

/** The application connects its scoped owner after opening; the tool receives its own TaskRuntime dynamically. */
export const make = Effect.fn('tour.triage')(function* (
  owner: Deferred.Deferred<Harness.Service>,
  options: {
    readonly calls: Ref.Ref<number>
    readonly afterAnswer?: Effect.Effect<void>
  },
) {
  const tools = yield* ToolRegistration.bind(toolkit, { triage: { replay: 'safe' } }, [
    TaskRuntime,
  ]).pipe(
    Effect.provide(
      toolkit.toLayer({
        triage: Effect.fn('tour.triage.handle')(
          function* ({ issue }) {
            const runtime = yield* TaskRuntime
            yield* Ref.update(options.calls, (count) => count + 1)
            const childId = yield* runtime.transaction(
              Effect.fn('tour.triage.child')(function* (tx) {
                const saved = yield* tx.doc(ChildDoc, { owner: runtime.taskId })
                if (saved.conversationId !== undefined) return saved.conversationId
                const child = yield* tx.createConversation({
                  ownership: { _tag: 'task', taskId: runtime.taskId },
                })
                Object.assign(yield* tx.doc(Conversation.AgentDoc, { owner: child.id }), {
                  model: Runtime.ref,
                  tools: [],
                  instructions: 'Answer with one word: bug, feature, or question.',
                })
                saved.conversationId = child.id
                return child.id
              }),
            )
            const harness = yield* Deferred.await(owner)
            const child = yield* harness.conversation(childId)
            const requestId = yield* Schema.decodeEffect(Identity.RequestId)(
              `triage:${runtime.taskId}`,
            )
            yield* (yield* Invocation.ToolCall).details({ conversationId: childId, requestId })
            const answer = yield* Runtime.ask(child, issue, { requestId })
            yield* options.afterAnswer ?? Effect.void
            return answer.text
          },
          Effect.mapError(
            (cause) =>
              new ToolError.ToolError({
                reason: new ToolError.ToolExecutionError({
                  name: Triage.name,
                  message: cause.message,
                  cause,
                }),
              }),
          ),
        ),
      }),
    ),
  )
  return { name: 'triage', tools } satisfies Extension.Extension
})

export const run = Effect.scoped(
  Effect.gen(function* () {
    const store = yield* Memory.make
    const calls = yield* Ref.make(0)
    const childRequests = yield* Ref.make(0)
    const entered = yield* Deferred.make<void>()
    const provider: Runtime.Provider = {
      generateText: () =>
        Effect.succeed([{ type: 'text', text: 'Summary' }, Runtime.finish('stop')]),
      streamText: ({ prompt }) => {
        const child = prompt.content.some(
          (message) =>
            message.role === 'system' &&
            message.content.includes('one word: bug, feature, or question'),
        )
        if (child)
          return Stream.unwrap(
            Ref.update(childRequests, (count) => count + 1).pipe(Effect.as(Runtime.answer('bug'))),
          )
        const completed = prompt.content.some(
          (message) =>
            message.role === 'tool' &&
            message.content.some(
              (part) => part.type === 'tool-result' && part.name === Triage.name,
            ),
        )
        if (completed) return Runtime.answer('Issue classified as bug')
        return Stream.fromIterable([
          {
            type: 'tool-call' as const,
            id: 'triage-call',
            name: Triage.name,
            params: { issue: 'Login fails when the token expires.' },
            providerExecuted: false,
          },
          Runtime.finish('tool-calls'),
        ])
      },
    }
    const firstOwner = yield* Deferred.make<Harness.Service>()
    const firstExtension = yield* make(firstOwner, {
      calls,
      afterAnswer: Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)),
    })
    const first = yield* Runtime.open({ store, provider, extensions: [firstExtension] })
    yield* Deferred.succeed(firstOwner, first.harness)
    const root = yield* first.harness.root
    const requestId = yield* Schema.decodeEffect(Identity.RequestId)('triage-parent')
    const admitted = yield* Conversation.submit(root, 'Triage the login issue', { requestId })
    yield* Deferred.await(entered)
    const children = yield* first.harness.transaction((tx) =>
      Stream.runCollect(tx.scanConversations()),
    )
    const child = children.find((conversation) => conversation.owner !== undefined)
    yield* Runtime.check(child !== undefined, 'Triage did not persist an owned conversation')
    if (child === undefined)
      return yield* new Runtime.ExampleError({ message: 'Missing triage child' })
    const before = yield* first.harness.snapshot(child.id)
    const childSubmission = before.submissions[0]
    yield* Runtime.check(
      before.submissions.length === 1 && childSubmission?.status === 'done',
      'Child did not settle exactly one submission',
    )
    if (childSubmission === undefined || child.owner === undefined)
      return yield* new Runtime.ExampleError({ message: 'Missing owned child submission' })
    yield* first.harness.close
    const secondOwner = yield* Deferred.make<Harness.Service>()
    const replacement = yield* make(secondOwner, { calls })
    const second = yield* Runtime.open({ store, provider, extensions: [replacement] })
    yield* Deferred.succeed(secondOwner, second.harness)
    yield* second.harness.resume
    const result = yield* Runtime.ask(yield* second.harness.root, 'Triage the login issue', {
      requestId,
    })
    const settled = yield* second.harness.awaitSubmission(admitted.id)
    const after = yield* second.harness.snapshot(child.id)
    const all = yield* second.harness.transaction((tx) => Stream.runCollect(tx.scanConversations()))
    yield* Runtime.check(
      all.filter((conversation) => conversation.owner !== undefined).length === 1,
      'Recovery created a second child',
    )
    yield* Runtime.check(
      after.submissions.length === 1 && after.submissions[0]?.id === childSubmission?.id,
      'Recovery created a second child submission',
    )
    yield* Runtime.check(
      (yield* Ref.get(calls)) === 2 && (yield* Ref.get(childRequests)) === 1,
      'Recovery repeated child inference',
    )
    yield* Runtime.check(settled.status === 'done', 'Recovered parent did not answer')
    return yield* Schema.decodeEffect(Result)({
      childConversationId: child.id,
      childSubmissionId: childSubmission.id,
      ownerTaskId: child.owner.taskId,
      toolExecutions: yield* Ref.get(calls),
      childModelRequests: yield* Ref.get(childRequests),
      childAnswer: 'bug',
      parentAnswer: result.text,
      reusedChild: true,
      reusedSubmission: true,
    })
  }),
)
