/**
 * The built-in generation task, ported from pi's `GenerationTask` (`harness/generation.ts`): one
 * model request of a run, and the tool round its answer starts.
 *
 * - `prepare` resolves the conversation's agent (see `Agent`), renders its system prompt sections
 *   and tool declarations, and appends the
 *   positional `$system` entries the context needs, then fixes the request: the model, the
 *   request options, and the newest entry it includes.
 * - `request` streams the response. Partials commit to `$live.generation`, throttled; a partial
 *   an interruption leaves becomes an aborted `$assistant` entry. The response is classified:
 *   a deferred one is polled (`poll`), a `Retryable` failure waits durably (`retry`) and prepares
 *   again, other failures end the run `unanswered` with `model_error`, and an unknown model with
 *   `no_model`.
 * - Fallback: when the model is `Unavailable` or unknown, its retries run out, or it asks to wait
 *   longer than `retry.maxDelayMs`, the run moves to the agent's next `fallbacks` model a provider
 *   serves: it appends a `$fallback` entry (`FallbackNote`), sets `$live.run.fallback`, and
 *   prepares again. The next run starts on the agent's model.
 * - An answer (`stop`, `length`) ends the run at the final boundary.
 * - Tool calls start a tool round in the commit that appends the answer: each offered call gets a
 *   `$tool` task the generation owns, all at once or one after another (`toolExecution`); a call
 *   to a tool the request did not offer is answered `tool_unavailable` at once. `tools` waits for
 *   them, then passes the `postTools` boundary and hands the run to a fresh generation.
 * - Hooks (`Hooks`): `beforeRequest` may replace a request's messages, `afterResponse` sees every
 *   response, `onYield` may continue the run after an answer, and `afterTools` sees each round.
 * - Compaction (see `Compaction`): preparation compacts first above the blocking threshold, starts
 *   a background compaction above the background one, and a `ContextOverflow` failure compacts and
 *   prepares again, once per generation.
 * - Abort answers calls not started yet with `aborted`, cancels a deferred response, and ends the
 *   run `unanswered` with `aborted`.
 *
 * Credentials are the provider's: a provider reads them fresh for each request.
 */
import { Effect, Layer, Option, Schema } from 'effect'
import { DeferredHandle } from '../model/Message.ts'
import type { AssistantMessage, Message } from '../model/Message.ts'
import * as Model from '../model/Model.ts'
import { EntryId, TaskId } from '../storage/schema/Ids.ts'
import * as Hook from '../task/Hook.ts'
import { Registry } from '../task/Registry.ts'
import * as Task from '../task/Task.ts'
import * as ToolTask from '../tool/ToolTask.ts'
import * as Agent from './Agent.ts'
import * as Compaction from './Compaction.ts'
import { Extensions } from './Extensions.ts'
import { current as currentAgent } from './internal/agent.ts'
import { provide as provideHarness } from './internal/current.ts'
import * as internal from './internal/generation/task.ts'
import { FALLBACK_KIND, RUN_KIND, toolLifecycle } from './internal/run.ts'
import * as Settings from './Settings.ts'
import type { UserInput } from '../protocol/Submission.ts'

/** The task kind. */
export const name = RUN_KIND

/** The kind of the display entry a run appends when it moves to a fallback model. */
export const fallbackKind = FALLBACK_KIND

/** The `data` of a `$fallback` entry: the model the run left, the one it moved to, and why. */
export const FallbackNote = Schema.Struct({ from: Model.Ref, to: Model.Ref, error: Schema.String })
export type FallbackNote = typeof FallbackNote.Type

const attempt = {
  attempt: Schema.Finite,
  /** The blocking compaction this generation waited for; it starts no other. */
  compacted: Schema.optionalKey(TaskId),
}

export const Checkpoint = Schema.Union([
  Schema.Struct({
    phase: Schema.Literal('prepare'),
    ...attempt,
    /** The overflow that started `compacted`; checked once when `prepare` resumes. */
    overflow: Schema.optionalKey(Schema.String),
    /** Set when the blocking threshold started `compacted`; checked once when it resumes. */
    threshold: Schema.optionalKey(Schema.Literal(true)),
  }),
  /** The request preparation fixed; a resend after recovery uses it unchanged. */
  Schema.Struct({
    phase: Schema.Literal('request'),
    ...attempt,
    model: Model.Ref,
    options: Model.Options,
    /** The newest entry the request includes. */
    cutoff: EntryId,
  }),
  Schema.Struct({ phase: Schema.Literal('retry'), ...attempt, until: Schema.Finite }),
  Schema.Struct({
    phase: Schema.Literal('poll'),
    ...attempt,
    model: Model.Ref,
    cutoff: EntryId,
    handle: DeferredHandle,
    pollAt: Schema.Finite,
  }),
  /** Waiting on the round's tool tasks, which the generation owns. */
  Schema.Struct({
    phase: Schema.Literal('tools'),
    /** The tool-calling answer. */
    assistant: EntryId,
    /** Tool tasks created so far, in call order. */
    tools: Schema.Array(TaskId),
    /** Calls of a sequential round not started yet, in call order. */
    pending: Schema.Array(Schema.String),
  }),
])
export type Checkpoint = typeof Checkpoint.Type

/** The answer, or the tool-calling answer of a round. */
export const Result = Schema.Struct({ entryId: EntryId })
export type Result = typeof Result.Type

export type Generation = Task.Task<Record<string, never>, Checkpoint, Result>

/** The hooks the generation asks (spec §7.2), for extensions to answer. */
export interface Hooks {
  /** Before every request attempt, recovery included: replace the messages of that request only. */
  readonly beforeRequest: (
    request: { readonly messages: ReadonlyArray<Message> },
    api: Hook.Api,
  ) => Effect.Effect<{ readonly messages: ReadonlyArray<Message> } | undefined, unknown>
  /** Every response, before it is classified. */
  readonly afterResponse: (message: AssistantMessage, api: Hook.Api) => Effect.Effect<void, unknown>
  /**
   * A final answer: `continue` appends user input and continues the run, unless the final
   * boundary placed user input or a reset. The first continuation wins.
   */
  readonly onYield: (
    answer: AssistantMessage,
    api: Hook.Api,
  ) => Effect.Effect<{ readonly continue: UserInput } | undefined, unknown>
  /** Once every call of a round is terminal, with the round's result entries in call order. */
  readonly afterTools: (
    assistant: EntryId,
    results: ReadonlyArray<EntryId>,
    api: Hook.Api,
  ) => Effect.Effect<void, unknown>
}
export const Hooks: Hook.Kind<Hooks> = Hook.kind<Hooks>(name)

/**
 * The generation task, over the `Model` and `Extensions` in context and `Settings.Current`. Each
 * request resolves the conversation's agent: its model, reasoning, tools and sections. Calls
 * need the tool task, and compacting needs the compaction task, which `tasks` and `layer` install
 * with it.
 */
export const make: Effect.Effect<Generation, never, Model.Model | Extensions> = Effect.gen(
  function* () {
    const model = yield* Model.Model
    const extensions = yield* Extensions
    const settings = yield* Settings.Current
    return Task.make(internal.definition({ model, settings, extensions }))
  },
)

/** `installed` with its handler running under `Agent.Current` and the Harness. */
const withAgent = (
  agent: Agent.Resolved,
  installed: ToolTask.Installed | undefined,
): ToolTask.Installed | undefined =>
  installed === undefined
    ? undefined
    : {
        ...installed,
        runner: Option.map(installed.runner, (runner) => ({
          ...runner,
          run: (args, context) =>
            provideHarness(Effect.provideService(runner.run(args, context), Agent.Current, agent)),
        })),
      }

/** The tool task over the tools and hooks of each call's conversation agent. */
export const toolTask: Effect.Effect<ToolTask.ToolTask, never, Extensions> = Effect.gen(
  function* () {
    const extensions = yield* Extensions
    const settings = yield* Settings.Current
    return ToolTask.makeWith(
      (api) =>
        Effect.map(
          currentAgent(api, api.conversationId, extensions, settings),
          (agent): ToolTask.Resolution => ({
            tool: (tool) =>
              withAgent(
                agent,
                agent.tools.find((each) => each.tool.name === tool),
              ),
            hooks: Agent.hooks(agent, ToolTask.Hooks),
          }),
        ),
      toolLifecycle,
    )
  },
)

/** The generation, tool and compaction tasks. */
export const tasks: Effect.Effect<
  ReadonlyArray<Task.Any>,
  never,
  Model.Model | Extensions
> = Effect.all([make, toolTask, Compaction.make])

/** Install the generation, tool and compaction tasks in the `Registry`. */
export const layer: Layer.Layer<never, never, Model.Model | Registry | Extensions> =
  Layer.effectDiscard(
    Effect.gen(function* () {
      const registry = yield* Registry
      for (const task of yield* tasks) yield* registry.install(task)
    }),
  )
