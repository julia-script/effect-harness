/**
 * The harness in a Cloudflare Workflow, deployed with Alchemy. Where a `HarnessObject` keeps a
 * conversation going, a workflow runs one-off jobs: each run answers its input in a durable step,
 * and Cloudflare retries the step on failure and replays its answer once it has one.
 *
 * `make` is the body of an Alchemy `Cloudflare.Workflow`: start an instance with
 * `{ params: { content } }` and its output is the step's `Result`. `turn` is that step on its own,
 * for bodies of several steps, with sleeps or events between them.
 *
 * Each step opens a fresh loop over memory `Storage` with the given extensions and settings, so
 * steps share nothing: pass what a step needs from an earlier one in its content. The `Model` and
 * the extensions' tools come from the body's context.
 *
 * The coding tools and code mode do not run in a Worker, which has no file system or processes.
 */
import { task } from 'alchemy/Cloudflare/Workflows'
import type { WorkflowStep, WorkflowTaskConfig } from 'alchemy/Cloudflare/Workflows'
import { Effect, Layer, Option, Schema } from 'effect'
import type { Conversation } from '../harness/Conversation.ts'
import * as Harness from '../harness/Harness.ts'
import type { Extension } from '../harness/Extension.ts'
import { UserInput } from '../harness/Submission.ts'
import { AssistantMessage } from '../model/Message.ts'
import type { Model } from '../model/Model.ts'
import * as Storage from '../storage/Storage.ts'

/** What an instance starts with: the user input its run answers. */
export const Input = Schema.Struct({ content: UserInput })
export type Input = typeof Input.Type

/** How a step ended: the text of the last answer, and why there is none when unanswered. */
export const Result = Schema.Union([
  Schema.Struct({ status: Schema.Literal('done'), answer: Schema.String }),
  Schema.Struct({
    status: Schema.Literal('unanswered'),
    reason: Schema.String,
    answer: Schema.String,
  }),
])
export type Result = typeof Result.Type

export interface Options<
  Installs extends ReadonlyArray<Extension> = ReadonlyArray<Extension>,
> extends Harness.LayerOptions<Installs> {
  /** The step's retries, timeout and rollback. Default: Cloudflare's retries, no rollback. */
  readonly step?: WorkflowTaskConfig<Result>
}

/**
 * The loop failed before the run settled: it could not open, or admit or wait for the input.
 * `reason` is the tag of the loop's error. Cloudflare retries the step on it, and replays it as
 * the step's failure once the retries run out, so it carries only what survives that replay.
 */
export class TurnFailed extends Schema.TaggedError<TurnFailed, { readonly brand: unique symbol }>(
  'effect-harness/cloudflare/HarnessWorkflow/TurnFailed',
)('TurnFailed', {
  message: Schema.String,
  reason: Schema.String,
}) {}

/** The text blocks of the newest `$assistant` entry's messages, or `''` when there is none. */
const lastAnswer = (conversation: Conversation) =>
  Effect.map(conversation.entries({ order: 'descending' }, 20), (page) => {
    const entry = page.items.find((item) => item.kind === '$assistant')
    return (entry?.model ?? [])
      .flatMap((message) =>
        Option.match(Schema.decodeUnknownOption(AssistantMessage)(message), {
          onNone: () => [],
          onSome: ({ content }) =>
            content.flatMap((block) => (block.type === 'text' ? [block.text] : [])),
        }),
      )
      .join('')
  })

/** Answer `content` on a fresh loop, in the step's scope. */
const answer = <const Installs extends ReadonlyArray<Extension>>(
  content: UserInput,
  options: Harness.LayerOptions<Installs>,
): Effect.Effect<Result, TurnFailed, Model | Harness.Requires<Installs[number]>> =>
  Effect.gen(function* () {
    const harness = yield* Harness.Harness
    const root = yield* harness.root()
    const submission = yield* root.submit({ type: 'input', content })
    const settled = yield* submission.wait
    const text = yield* lastAnswer(root)
    return settled.status === 'done'
      ? ({ status: 'done', answer: text } satisfies Result)
      : ({ status: 'unanswered', reason: settled.reason, answer: text } satisfies Result)
  }).pipe(
    Effect.provide(Harness.layer(options).pipe(Layer.provide(Storage.layerMemory))),
    Effect.scoped,
    Effect.mapError((error) => new TurnFailed({ message: error.message, reason: error._tag })),
  )

/**
 * A durable step named `name` that answers `content` on a fresh loop. A failure of the loop fails
 * the attempt with `TurnFailed`, for Cloudflare to retry; an unanswered run is a result, not a
 * failure. The `Model` and the extensions' tools come from context.
 */
export const turn = <const Installs extends ReadonlyArray<Extension> = readonly []>(
  name: string,
  content: UserInput,
  options: Options<Installs> = {},
): Effect.Effect<Result, TurnFailed, WorkflowStep | Model | Harness.Requires<Installs[number]>> =>
  task(name, answer(content, options), options.step)

/**
 * The body of a `Cloudflare.Workflow`: each run answers its input's `content` in one step named
 * `turn`. Provide the `Model` and the extensions' tools around it.
 */
export const make =
  <const Installs extends ReadonlyArray<Extension> = readonly []>(
    options: Options<Installs> = {},
  ) =>
  (
    input: Input,
  ): Effect.Effect<Result, TurnFailed, WorkflowStep | Model | Harness.Requires<Installs[number]>> =>
    turn('turn', input.content, options)
