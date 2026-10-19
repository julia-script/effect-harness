/**
 * Folding a response's events into the message so far.
 */
import { Effect, Option, Ref, Stream } from 'effect'
import type { AssistantContent, AssistantMessage } from '../Message.ts'
import { emptyUsage } from '../Message.ts'
import type { Event, Info } from '../Model.ts'
import { ModelError, Retryable } from '../ModelError.ts'

/** An empty partial response from `model`. */
export const partial = (model: Info, timestamp: number): AssistantMessage => ({
  role: 'assistant',
  content: [],
  api: model.api,
  provider: model.provider,
  model: model.id,
  usage: emptyUsage,
  stopReason: 'pending',
  timestamp,
})

const put = (
  content: ReadonlyArray<AssistantContent>,
  index: number,
  block: AssistantContent,
): ReadonlyArray<AssistantContent> =>
  index < content.length ? content.with(index, block) : [...content, block]

/** `message` with `event` applied; `done` replaces it. */
export const apply = (message: AssistantMessage, event: Event): AssistantMessage => {
  if (event.type === 'done') return event.message
  const { content } = message
  const at = (block: AssistantContent) => ({
    ...message,
    content: put(content, event.contentIndex, block),
  })
  const current = content[event.contentIndex]
  if (event.type === 'text_delta') {
    const text = current?.type === 'text' ? current.text + event.delta : event.delta
    return at({ type: 'text', text })
  }
  if (event.type === 'thinking_delta') {
    const thinking = current?.type === 'thinking' ? current.thinking + event.delta : event.delta
    return at({ type: 'thinking', thinking })
  }
  if (event.type === 'toolcall_start')
    return at({ type: 'toolCall', id: event.id, name: event.name, arguments: {} })
  return at(event.toolCall)
}

interface State {
  readonly message: AssistantMessage
  readonly done: boolean
}

const unfinished = (model: Info) =>
  new ModelError({
    message: `The ${model.provider}/${model.id} response ended before it finished`,
    reason: new Retryable({}),
  })

/**
 * The message after each event of `events`: partials while it streams, then the final message of
 * `done`. Events after `done` are ignored; a stream that ends without one fails `Retryable`.
 */
export const messages =
  (model: Info, timestamp: number) =>
  <E, R>(events: Stream.Stream<Event, E, R>): Stream.Stream<AssistantMessage, E | ModelError, R> =>
    Stream.unwrap(
      Effect.map(Ref.make(false), (finished) =>
        events.pipe(
          Stream.mapAccum(
            (): State => ({ message: partial(model, timestamp), done: false }),
            (state: State, event: Event): readonly [State, ReadonlyArray<State>] => {
              if (state.done) return [state, []]
              const next = { message: apply(state.message, event), done: event.type === 'done' }
              return [next, [next]]
            },
          ),
          Stream.mapEffect((state) =>
            Effect.as(state.done ? Ref.set(finished, true) : Effect.void, state.message),
          ),
          Stream.concat(
            Stream.unwrap(
              Effect.map(Ref.get(finished), (done) =>
                done ? Stream.empty : Stream.fail(unfinished(model)),
              ),
            ),
          ),
        ),
      ),
    )

/** The final response of `events`. */
export const collect = <E, R>(
  events: Stream.Stream<Event, E, R>,
  model: Info,
): Effect.Effect<AssistantMessage, E | ModelError, R> =>
  Effect.flatMap(
    Stream.runLast(messages(model, 0)(events)),
    Option.match({
      onNone: () => Effect.fail(unfinished(model)),
      onSome: Effect.succeed,
    }),
  )
