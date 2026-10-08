# How to compose native Workflows

Use this guide to register application Workflows alongside the harness executors. It assumes you already have a model catalogue, tool Registry and domain Session, as in [the first conversation tutorial](tutorials/first-conversation.md).

## Declare and implement your Workflow

Use Effect's native Workflow and Activity modules. This standalone example registers a greeting Workflow with an in-memory engine:

```ts
import * as Console from 'effect/Console'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import * as Activity from 'effect/workflow/Activity'
import * as Workflow from 'effect/workflow/Workflow'
import * as WorkflowEngine from 'effect/workflow/WorkflowEngine'

export const Greeting = Workflow.make('app/greeting/v1', {
  payload: { name: Schema.String },
  success: Schema.String,
  error: Schema.Never,
  idempotencyKey: ({ name }) => name,
})

export const GreetingLive = Greeting.toLayer(({ name }) =>
  Activity.make({
    name: 'greet',
    success: Schema.String,
    execute: Effect.succeed(`Hello, ${name}`),
  }),
)

const Runtime = GreetingLive.pipe(Layer.provideMerge(WorkflowEngine.layerMemory))
await Effect.runPromise(
  Greeting.execute({ name: 'Effect' }).pipe(Effect.flatMap(Console.log), Effect.provide(Runtime)),
)
```

The result is `Hello, Effect`. In your application, merge `GreetingLive` with `durable/Executor.layer` before providing the shared WorkflowEngine. Keep one engine Layer value for both sets of handlers. The harness does not supply a replacement Workflow declaration API.

## Submit a conversation input

Use the built-in `Submission` declaration after registering `durable/Executor.layer`. This helper accepts identities decoded at your application's boundary:

```ts
import type * as Identity from 'effect-harness/durable/Identity'
import type * as Record from 'effect-harness/durable/Record'
import { Submission } from 'effect-harness/durable/workflow/Submission'
import * as Prompt from 'effect/ai/Prompt'

export const submit = (
  sessionId: Identity.SessionId,
  conversationId: Record.ConversationId,
  requestId: Identity.RequestId,
  text: string,
) =>
  Submission.execute({
    sessionId,
    conversationId,
    requestId,
    submission: {
      _tag: 'input',
      message: Prompt.userMessage({ content: [Prompt.textPart({ text })] }),
      whenBusy: 'followUp',
    },
  })
```

Allocate `requestId` before sending the request and retain it if the caller retries. Allocate a new ID for new content. Choose `steer` to insert input at an eligible running boundary, `followUp` to queue a later turn, or `reject` to reject input while busy. See the [submission contract](reference/execution-and-observation.md#submissions) for defaults and receipt variants.

For admission without waiting for settlement, pass `{ discard: true }` to native `execute`. Retain the returned execution ID for native `poll` and `resume`. Use committed [observations](observations.md) for conversation UI state.

## Add custom owned work

An independent Workflow needs only its native handler registration. To make a custom Workflow participate in domain ownership, cancellation and draining:

1. Include its declaration and `Executor.workflows` in `Ownership.layerDeclarations`.
2. Register built-in handlers with `Executor.layerExecutors`, which consumes that declarations service.
3. Register the custom handler with its normal `toLayer`.
4. Use `workflow/Structured` inside the handler to bind, execute and join owned children. Commit the custom task's terminal domain projection before returning.

The declaration Layer captures schema services when built. The engine remains supplied separately. A native result alone does not settle an owned domain task; join/drain validates that terminal projection. See [ownership contracts](reference/execution-and-observation.md#owned-work) and [recovery](explanation/recovery.md).

## Stop or cancel work deliberately

Use the built-in Abort Workflow to cancel domain work. Use Scope closure to release a Session and pause recoverable work for reopening. They have different outcomes; [lifetime rules](reference/execution-and-observation.md#lifetime) describe them.

Keep the native engine in an outer Scope when closing and reopening an individual Session. `Conversation.awaitIdle(session, conversationId)` waits for non-background owned work using the declared native executions. It requires `Ownership.Declarations` and WorkflowEngine. An idle result means the relevant work has drained; it does not release the Session.
