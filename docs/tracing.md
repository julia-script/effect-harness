# Durable tracing

The durable runtime emits named Effect spans, using `Effect.fn` for execution operations through the application's Effect tracer. Install your tracer around runtime construction **and** use: scheduler fibers inherit the context in which the runtime starts. An existing Effect OpenTelemetry Layer can supply the tracer; the harness does not install an exporter.

For example, with an already configured `Tracer.Tracer` and `LanguageModel` service:

```ts
import * as Effect from 'effect/Effect'
import type * as Tracer from 'effect/Tracer'
import * as LanguageModel from 'effect/ai/LanguageModel'
import * as HarnessRuntime from 'effect-harness/HarnessRuntime'
import * as Storage from 'effect-harness/Storage'

export const conversation = (
  tracer: Tracer.Tracer,
  model: typeof LanguageModel.LanguageModel.Service,
) =>
  Effect.scoped(
    Effect.gen(function* () {
      const runtime = yield* HarnessRuntime.make()
      const root = yield* runtime.backend.root
      const submission = yield* runtime.backend.submit({
        conversationId: root,
        draft: { type: 'input', content: 'Hello' },
      })
      return yield* runtime.backend.wait(submission.id)
    }),
  ).pipe(
    Effect.provideService(LanguageModel.LanguageModel, model),
    Effect.provide(Storage.layerMemory),
    Effect.withSpan('conversation'),
    Effect.withTracer(tracer),
  )
```

Session commits, scheduler invocations and registered task phases use `Effect.useSpan` with an explicit parent service, without adding stack frames. These boundaries preserve Cause identity for durable fault provenance; the other new execution spans use named `Effect.fn`.

## Span reference

| Span                                                             | Boundary and attributes                                                                                                                                                                                                                                                               |
| ---------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Harness.admitSubmission`                                        | Submission validation/admission within its transaction; `conversation.id`, `submission.type`, `submission.id`, `submission.reused`. Persistence is enclosed by `Session.commit`.                                                                                                      |
| `Session.commit`                                                 | Coordination wait, callback, storage write and local publication; optional `task.id` / `conversation.id`, `commit.changes`, and `commit.sequence` after an acknowledged write. Read-only commits have zero changes and no sequence.                                                   |
| `TaskScheduler.resume`                                           | Recovery scan and scheduler wake; `scheduler.recovery` identifies the initial recovery pass.                                                                                                                                                                                          |
| `TaskScheduler.invoke`                                           | One reserved invocation including lifecycle hooks, settlement and cleanup; `task.id`, `conversation.id`, `task.kind`, `task.version`, `task.mode`, `task.phase`, `task.result`, and `task.outcome` when available.                                                                    |
| `Task.runPhase`                                                  | Registered task handler before failure normalization; task/conversation IDs, kind, mode and phase. Used by custom tasks and registered built-in tasks.                                                                                                                                |
| `RunLoop.run`                                                    | One native run invocation, which may execute several transitions; task/conversation/submission IDs and the entry `run.phase`.                                                                                                                                                         |
| `Model.stream`                                                   | Stream consumption for one generation attempt, before retry handling; `request.id`, `request.attempt`. The attempt number advances on retry/recovery even when the prepared request is reused. Publication finalization and terminal-response validation remain in the enclosing run. |
| `DeferredModel.run`                                              | Deferred start/poll lifecycle, including waits; request ID, attempt and `deferred.create`.                                                                                                                                                                                            |
| `DeferredModel.cancel`                                           | Bounded remote cancellation wait and cleanup scheduling.                                                                                                                                                                                                                              |
| `Tool.execute`                                                   | Durable tool pipeline including replay handling; task/conversation IDs, `tool.name`, `tool.call.id`, `tool.replay` (the replay safety policy).                                                                                                                                        |
| `Compaction.prepare`, `Compaction.summarize`, `Compaction.place` | Selection/preparation, summary execution and summary placement. Scheduler ancestors identify the owning task.                                                                                                                                                                         |

`Model.stream` and `Task.runPhase` expose failures before the runtime converts them into retry decisions or durable outcomes. Scheduler, tool, deferred cancellation and compaction operations can successfully handle an unsuccessful domain outcome; a successful span exit alone does not imply a completed task or successful remote cancellation. Inspect the persisted outcome and, where available, `task.result` / `task.outcome`.

Task registrations, tool bindings and streaming providers retain their captured application dependencies while inheriting the invocation's tracer, parent span, tracing enablement, timing, annotations and links. A tool handler's span nests under the existing `ToolInventory.invoke` span inside `Tool.execute`.

Background scheduler work inherits the runtime's trace context, rather than the context of each later `submit` caller. A cold reopen starts fresh spans under the new runtime context. Span IDs are not persisted: correlate reopen/retry activity using task, conversation, submission and request IDs. Task and conversation numeric IDs are scoped to their storage instance; add application/storage identity through your application's span annotations when aggregating several stores.

The added attributes contain identifiers, names, policies, counts and phases. They do not copy prompts, tool arguments/results, credentials or checkpoint payloads. Effect still supplies normal success/failure exits to the tracer; an exporter that records exception messages or return values should apply the application's data-handling policy.

Use `Effect.withTracerEnabled(false)` around construction and execution to disable tracing, or configure sampling/filtering in your tracer. No harness-specific telemetry option is required. Commit spans include read-only transactions and can be frequent; filter `Session.commit` if only execution-level traces are needed. Idle scheduler polling and individual stream deltas do not introduce new spans.
