/**
 * The harness in a Cloudflare Workflow, deployed with Alchemy. Where a `HarnessObject` keeps a
 * conversation going, a workflow runs one-off jobs: each run answers its input in a durable step,
 * and Cloudflare retries the step when the model fails and replays its answer once it has one.
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
import { Effect, Layer, Schema } from 'effect'
import type { Conversation } from '../harness/Conversation.ts'
import * as Harness from '../harness/Harness.ts'
import type { Extension } from '../harness/Extension.ts'
import { UserInput } from '../harness/Submission.ts'
import type { Model } from '../model/Model.ts'
import * as Storage from '../storage/Storage.ts'

/** What an instance starts with: the user input its run answers. */
export const Input = Schema.Struct({ content: UserInput })
export type Input = typeof Input.Type

/** The run answered: the text of its last answer. */
export const Done = Schema.Struct({ status: Schema.Literal('done'), answer: Schema.String })
export type Done = typeof Done.Type

/**
 * The run ended without an answer for a reason another attempt would not change, such as
 * `no_model` or `overflow`. `answer` is the text of the last answer, if any.
 */
export const Unanswered = Schema.Struct({
  status: Schema.Literal('unanswered'),
  reason: Schema.String,
  detail: Schema.optionalKey(Schema.Json),
  answer: Schema.String,
})
export type Unanswered = typeof Unanswered.Type

/** How a step ended. */
export const Result = Schema.Union([Done, Unanswered])
export type Result = typeof Result.Type

/**
 * The run ended without an answer for a reason worth another attempt: the model failed
 * (`model_error`) or the run's task did (`faulted`). Cloudflare retries the step on it, and replays
 * it as the step's failure once the retries run out, so it carries only what survives that replay.
 */
export class TurnFailed extends Schema.TaggedError<TurnFailed, { readonly brand: unique symbol }>(
  'effect-harness/cloudflare/HarnessWorkflow/TurnFailed',
)('TurnFailed', {
  reason: Schema.String,
  detail: Schema.optionalKey(Schema.Json),
}) {}

/** The reasons an unanswered run fails the attempt with `TurnFailed` instead of settling. */
const RETRYABLE: ReadonlySet<string> = new Set(['model_error', 'faulted'])

export interface Options<
  Installs extends ReadonlyArray<Extension> = ReadonlyArray<Extension>,
> extends Harness.LayerOptions<Installs> {
  /** The step's retries, timeout and rollback. Default: Cloudflare's retries, no rollback. */
  readonly step?: WorkflowTaskConfig<Result>
}

/** The text blocks of the last assistant message the conversation's context holds. */
const lastAnswer = (conversation: Conversation) =>
  Effect.map(conversation.context(), ({ messages }) => {
    const last = messages.findLast((message) => message.role === 'assistant')
    return last === undefined
      ? ''
      : last.content.flatMap((block) => (block.type === 'text' ? [block.text] : [])).join('')
  })

/** Answer `content` on the root conversation of the Harness in context. */
const answer = Effect.fnUntraced(function* (
  content: UserInput,
): Effect.fn.Return<Result, TurnFailed, Harness.Harness> {
  const harness = yield* Harness.Harness
  const root = yield* Effect.orDie(harness.root())
  const submission = yield* Effect.orDie(root.submit({ type: 'input', content }))
  const settled = yield* Effect.orDie(submission.wait)
  const text = yield* Effect.orDie(lastAnswer(root))
  if (settled.status === 'done') return { status: 'done', answer: text }
  const detail = settled.detail === undefined ? {} : { detail: settled.detail }
  if (RETRYABLE.has(settled.reason))
    return yield* new TurnFailed({ reason: settled.reason, ...detail })
  return { status: 'unanswered', reason: settled.reason, ...detail, answer: text }
})

/**
 * A durable step named `name` that answers `content` on a fresh loop, built for each attempt. When
 * the model or the run's task fails, the attempt fails with `TurnFailed` for Cloudflare to retry;
 * any other unanswered run is a result. A loop that cannot open or admit the input is a defect,
 * which no retry fixes, so the step stops at once. The `Model` and the extensions' tools come from
 * context.
 */
export const turn = <const Installs extends ReadonlyArray<Extension> = readonly []>(
  name: string,
  content: UserInput,
  options: Options<Installs> = {},
): Effect.Effect<Result, TurnFailed, WorkflowStep | Model | Harness.Requires<Installs[number]>> =>
  task(
    name,
    answer(content).pipe(
      Effect.provide(Harness.layer(options).pipe(Layer.provide(Storage.layerMemory), Layer.orDie), {
        local: true,
      }),
    ),
    options.step,
  )

const decodeInput = Schema.decodeUnknownEffect(Input)

/**
 * The body of a `Cloudflare.Workflow`: each run decodes its params as `Input` and answers their
 * `content` in one step named `turn`. Params that do not decode are a defect. Provide the `Model`
 * and the extensions' tools around it.
 */
export const make =
  <const Installs extends ReadonlyArray<Extension> = readonly []>(
    options: Options<Installs> = {},
  ) =>
  (
    input: unknown,
  ): Effect.Effect<Result, TurnFailed, WorkflowStep | Model | Harness.Requires<Installs[number]>> =>
    decodeInput(input).pipe(
      Effect.orDie,
      Effect.flatMap(({ content }) => turn('turn', content, options)),
    )
