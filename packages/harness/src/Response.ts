/**
 * Immutable native response accumulation and ordered partial publication.
 *
 * @since 0.0.0
 */
import * as HashMap from 'effect/HashMap'
import { dual } from 'effect/Function'
import * as Arr from 'effect/Array'
import * as Option from 'effect/Option'
import * as Prompt from 'effect/ai/Prompt'
import * as Response from 'effect/ai/Response'
import type { Part } from './Executor.ts'
import * as Schema from 'effect/Schema'
const samePart = Schema.toEquivalence(Prompt.AssistantMessagePart)
const sameOptions = Schema.toEquivalence(Prompt.ProviderOptions)

/**
 * Response state contract.
 *
 * @category models
 * @since 0.0.0
 */
export interface State {
  readonly parts: ReadonlyArray<Response.AnyPart>
  readonly completed: HashMap.HashMap<string, Response.AnyPart>
  readonly other: ReadonlyArray<Response.AnyPart>
  readonly text: HashMap.HashMap<string, string>
  readonly reasoning: HashMap.HashMap<string, string>
  readonly toolParams: HashMap.HashMap<
    string,
    { readonly name: string; readonly raw: string; readonly providerExecuted: boolean }
  >
  readonly order: ReadonlyArray<{
    readonly type: 'text' | 'reasoning' | 'tool-call'
    readonly id: string
  }>
}
/**
 * Creates an empty response state.
 *
 * @category constructors
 * @since 0.0.0
 */
export const empty = (): State => ({
  parts: [],
  completed: HashMap.empty(),
  other: [],
  text: HashMap.empty(),
  reasoning: HashMap.empty(),
  toolParams: HashMap.empty(),
  order: [],
})
// Streaming/full-part provider augmentations differ; retain their shared JSON metadata protocol.
const metadata = (self: Response.ProviderMetadata): Response.ProviderMetadata => ({ ...self })
const key = (self: string, id: string): string => JSON.stringify([self, id])
function completed(
  self: State,
  type: State['order'][number]['type'],
  id: string,
  part: Response.AnyPart,
): State {
  const values = HashMap.set(self.completed, key(type, id), part)
  const order = self.order.some((item) => item.type === type && item.id === id)
    ? self.order
    : [...self.order, { type, id }]
  return {
    ...self,
    completed: values,
    order,
    parts: [
      ...order.flatMap((item) => {
        return Option.toArray(HashMap.get(values, key(item.type, item.id)))
      }),
      ...self.other,
    ],
  }
}
/** Copy streaming data before progress publication; final parts preserve block start order rather than completion order. */
function appendImpl(self: State, part: Part): State {
  switch (part.type) {
    case 'text-start':
      return {
        ...self,
        text: HashMap.set(self.text, part.id, ''),
        order: [...self.order, { type: 'text', id: part.id }],
      }
    case 'reasoning-start':
      return {
        ...self,
        reasoning: HashMap.set(self.reasoning, part.id, ''),
        order: [...self.order, { type: 'reasoning', id: part.id }],
      }
    case 'text-delta':
      return {
        ...self,
        text: HashMap.set(
          self.text,
          part.id,
          Option.getOrElse(HashMap.get(self.text, part.id), () => '') + part.delta,
        ),
      }
    case 'reasoning-delta':
      return {
        ...self,
        reasoning: HashMap.set(
          self.reasoning,
          part.id,
          Option.getOrElse(HashMap.get(self.reasoning, part.id), () => '') + part.delta,
        ),
      }
    case 'text-end':
      return completed(
        self,
        'text',
        part.id,
        Response.makePart('text', {
          text: Option.getOrElse(HashMap.get(self.text, part.id), () => ''),
          metadata: metadata(part.metadata),
        }),
      )
    case 'reasoning-end':
      return completed(
        self,
        'reasoning',
        part.id,
        Response.makePart('reasoning', {
          text: Option.getOrElse(HashMap.get(self.reasoning, part.id), () => ''),
          metadata: metadata(part.metadata),
        }),
      )
    case 'tool-params-start':
      return {
        ...self,
        toolParams: HashMap.set(self.toolParams, part.id, {
          name: part.name,
          raw: '',
          providerExecuted: part.providerExecuted,
        }),
        order: [...self.order, { type: 'tool-call', id: part.id }],
      }
    case 'tool-params-delta': {
      const previous = HashMap.get(self.toolParams, part.id)
      return Option.match(previous, {
        onNone: () => self,
        onSome: (value) => ({
          ...self,
          toolParams: HashMap.set(self.toolParams, part.id, {
            ...value,
            raw: value.raw + part.delta,
          }),
        }),
      })
    }
    case 'tool-params-end':
      return self
    case 'tool-call':
      return completed(self, 'tool-call', part.id, part)
    default:
      return { ...self, other: [...self.other, part], parts: [...self.parts, part] }
  }
}
/**
 * Copies streaming response data while preserving block start order.
 *
 * @category combinators
 * @since 0.0.0
 */
export const append: {
  (part: Part): (self: State) => State
  (self: State, part: Part): State
} = dual(2, appendImpl)
/**
 * Incomplete tool argument JSON is retained as a native params string with a harness partial annotation.
 *
 * **Details**
 *
 * It is UI history, never model context.
 *
 * @category combinators
 * @since 0.0.0
 */
export function partial(self: State): Option.Option<Prompt.AssistantMessage> {
  const content: Array<Prompt.AssistantMessagePart> = []
  for (const item of self.order) {
    if (item.type === 'tool-call') {
      const final = HashMap.get(self.completed, key(item.type, item.id))
      const pending = HashMap.get(self.toolParams, item.id)
      const complete = Option.filter(
        final,
        (part): part is Response.ToolCallPart<string, unknown> => part.type === 'tool-call',
      )
      Option.match(complete, {
        onSome: (value) =>
          content.push(
            Prompt.toolCallPart({
              id: value.id,
              name: value.name,
              params: value.params,
              providerExecuted: value.providerExecuted,
            }),
          ),
        onNone: () =>
          Option.map(pending, (value) =>
            content.push(
              Prompt.toolCallPart({
                id: item.id,
                name: value.name,
                params: value.raw,
                providerExecuted: value.providerExecuted,
                options: { harness: { partial: true } },
              }),
            ),
          ),
      })
      continue
    }
    const text =
      item.type === 'text'
        ? Option.getOrElse(HashMap.get(self.text, item.id), () => '')
        : Option.getOrElse(HashMap.get(self.reasoning, item.id), () => '')
    if (text !== '')
      content.push(
        item.type === 'text' ? Prompt.textPart({ text }) : Prompt.reasoningPart({ text }),
      )
  }
  return content.length === 0 ? Option.none() : Option.some(Prompt.assistantMessage({ content }))
}
/**
 * Converts completed native response parts to a prompt.
 *
 * @category combinators
 * @since 0.0.0
 */
export const message = (self: State): Prompt.Prompt => Prompt.fromResponseParts(self.parts)
/**
 * Response change contract.
 *
 * @category models
 * @since 0.0.0
 */
export interface Change {
  readonly path: ReadonlyArray<string | number>
  readonly type: 'append' | 'set'
  readonly value: unknown
}
/** Native message deltas use append where safe; arbitrary replacements fall back to the whole immutable message. */
function deltaImpl(
  self: Prompt.AssistantMessage | undefined,
  that: Prompt.AssistantMessage,
): ReadonlyArray<Change> {
  if (
    self === undefined ||
    self.content.length > that.content.length ||
    !sameOptions(self.options, that.options)
  )
    return [{ type: 'set', path: [], value: that }]
  const changes: Array<Change> = []
  for (const [index, part] of that.content.entries()) {
    const found = Arr.get(self.content, index)
    if (Option.isNone(found)) {
      changes.push({ type: 'set', path: ['content', index], value: part })
      continue
    }
    const previous = found.value
    if (samePart(previous, part)) continue
    if (
      previous.type === part.type &&
      (part.type === 'text' || part.type === 'reasoning') &&
      (previous.type === 'text' || previous.type === 'reasoning') &&
      part.text.startsWith(previous.text)
    ) {
      changes.push({
        type: 'append',
        path: ['content', index, 'text'],
        value: part.text.slice(previous.text.length),
      })
      if (!sameOptions(previous.options, part.options))
        changes.push({ type: 'set', path: ['content', index, 'options'], value: part.options })
    } else if (
      part.type === 'tool-call' &&
      previous.type === 'tool-call' &&
      part.id === previous.id &&
      part.name === previous.name &&
      part.providerExecuted === previous.providerExecuted
    ) {
      const append =
        typeof part.params === 'string' &&
        typeof previous.params === 'string' &&
        part.params.startsWith(previous.params)
      changes.push({
        type: append ? 'append' : 'set',
        path: ['content', index, 'params'],
        value: append ? String(part.params).slice(String(previous.params).length) : part.params,
      })
      if (!sameOptions(previous.options, part.options))
        changes.push({ type: 'set', path: ['content', index, 'options'], value: part.options })
    } else return [{ type: 'set', path: [], value: that }]
  }
  return changes
}
/**
 * Returns incremental changes between the previous and current values.
 *
 * @category combinators
 * @since 0.0.0
 */
export const delta: {
  (
    that: Prompt.AssistantMessage,
  ): (self: Prompt.AssistantMessage | undefined) => ReadonlyArray<Change>
  (self: Prompt.AssistantMessage | undefined, that: Prompt.AssistantMessage): ReadonlyArray<Change>
} = dual(2, deltaImpl)
